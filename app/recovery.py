"""停机备份恢复：校验、切换与中断自愈。

场景：地质化验库管理员误装数据后，用本应用同次停机保存的两份库
（source.db / target.db）整体替换作业库与分析库。

设计要点
--------
* **校验不动原库**：上传文件先落到临时作业目录，只读校验；期间业务
  （更正/查询/同步）完全不受影响。校验失败直接拒绝并删临时文件。
* **只接受完整配对**：目标必须已完成首次全量（phase=ready）、目标水位不
  超过源水位、待同步日志连续完整；并在临时副本上把待同步事务重放到源库
  当前状态逐行对账，确认两份文件确属同一停机时刻、可无缝接续
  （不清库、不重做全量）。
* **切换有停机窗口**：确认后经 OperationGate 暂停新业务、等待在途操作结束，
  再做“备份完整旧配对 → 整体换入两份库”的替换；任何一步失败都回滚到
  完整旧配对，绝不混用两库。
* **两库独立卷**：上一配对备份与待换入副本分别放在两库各自目录下的
  ``.geosync-recovery`` 中，保证 ``os.replace`` 与库文件位于同一文件系统
  因而原子；两卷各自持久化，容器重启后仍可自愈。
* **中断可自愈**：替换前先把完整旧配对备份到两卷并写入 commit 标记
  （meta.json，最后写）。进程在切换途中被杀，重启时：
    - 无 commit 标记 ⇒ 替换尚未开始，旧配对原封不动；
    - 有 commit 标记且旧配对校验通过 ⇒ 整体回滚为完整旧配对；
    - 已写入 applied ⇒ 清理残留。
  结果只会是完整旧配对或完整备份配对。
* **旧执行者不落库**：两库替换只在独占区段内发生，此时本应用没有任何打开
  的数据库连接；所有跨库操作都完整处于一个读区段内，切换只发生在区段之间。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import threading
import time
import uuid
from contextlib import closing
from datetime import datetime

from . import target as target_mod
from .gate import OperationGate

log = logging.getLogger("geosync.recovery")

SQLITE_MAGIC = b"SQLite format 3\x00"

STATE_NONE = "none"
STATE_VERIFIED = "verified"
STATE_SWITCHING = "switching"
STATE_APPLIED = "applied"
STATE_FAILED = "failed"
STATE_INCOMPLETE = "incomplete"
STATE_CANCELLED = "cancelled"

_SRC_TABLES = {
    "samples": {"sample_no", "site", "updated_at"},
    "results": {"id", "sample_no", "item", "value"},
    "tx_log": {"tx_id", "note", "created_at"},
    "tx_ops": {"tx_id", "seq", "entity", "op", "sample_no", "item", "site", "value"},
}
_TGT_TABLES = {
    "samples": {"sample_no", "site", "applied_tx"},
    "results": {"sample_no", "item", "value", "applied_tx"},
    "sync_state": {"id", "phase", "high_water", "last_tx_id",
                   "full_started_at", "full_done_at", "updated_at"},
}


class RestoreRejected(Exception):
    """备份不满足接收条件；原库不变。"""

    def __init__(self, errors: list[str]) -> None:
        super().__init__("；".join(errors))
        self.errors = list(errors)


class RestoreConflict(Exception):
    """当前状态不允许该操作（例如没有待确认的备份）。"""


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(os.path.abspath(path)))


def _remove_db_files(path: str) -> None:
    """删除库文件及其 WAL 边车文件（仅在确认无连接时调用）。"""
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


def _copy_db(src: str, dst: str) -> None:
    """用 SQLite 在线备份 API 复制出单文件、自洽的库（不带 WAL 边车）。"""
    _remove_db_files(dst)
    with closing(sqlite3.connect(src, timeout=30.0)) as s, \
            closing(sqlite3.connect(dst, timeout=30.0)) as d:
        s.backup(d)
    with open(dst, "rb+") as f:
        os.fsync(f.fileno())


def _read_header(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read(16)


def _db_integrity_ok(path: str) -> bool:
    try:
        with closing(sqlite3.connect(path, timeout=10.0)) as c:
            return [tuple(r) for r in c.execute("PRAGMA integrity_check")] == [("ok",)]
    except Exception:
        return False


class RecoveryManager:
    def __init__(self, gate: OperationGate, source_path: str, target_path: str,
                 staging_dir: str, max_bytes: int, drain_timeout: float,
                 pause_after: str = "", pause_ms: int = 0,
                 fail_after: str = "", deep_verify_max_txs: int = 50000,
                 verify_delay_ms: int = 0) -> None:
        self.gate = gate
        self.source_path = os.path.abspath(source_path)
        self.target_path = os.path.abspath(target_path)
        self.staging_dir = os.path.abspath(staging_dir)
        self.max_bytes = max_bytes
        self.drain_timeout = drain_timeout
        self.pause_after = pause_after
        self.pause_ms = pause_ms
        self.fail_after = fail_after
        self.deep_verify_max_txs = deep_verify_max_txs
        self.verify_delay_ms = verify_delay_ms

        # 中间状态放在两库各自目录下，保证与库文件同一文件系统、且随卷持久化。
        self.src_rec = os.path.join(os.path.dirname(self.source_path),
                                    ".geosync-recovery")
        self.tgt_rec = os.path.join(os.path.dirname(self.target_path),
                                    ".geosync-recovery")
        self.state_path = os.path.join(self.src_rec, "state.json")
        self.prev_src = os.path.join(self.src_rec, "previous", "source.db")
        self.prev_tgt = os.path.join(self.tgt_rec, "previous", "target.db")
        self.prev_meta = os.path.join(self.src_rec, "previous", "meta.json")
        self.inc_src = os.path.join(self.src_rec, "incoming", "source.db")
        self.inc_tgt = os.path.join(self.tgt_rec, "incoming", "target.db")
        self.verify_dir = os.path.join(self.src_rec, "verify")

        self._lock = threading.RLock()
        self._hook_lock = threading.Lock()
        self._pause_budget = 1 if (pause_after and pause_ms > 0) else 0
        self._fail_budget = 1 if fail_after else 0
        self.last_apply_result: dict | None = None

    # ================================================================ 生命周期
    def ensure_dirs(self) -> None:
        for d in (self.staging_dir, self.src_rec, self.tgt_rec,
                  os.path.dirname(self.prev_src), os.path.dirname(self.prev_tgt),
                  os.path.dirname(self.inc_src), os.path.dirname(self.inc_tgt)):
            os.makedirs(d, exist_ok=True)

    def load_state(self) -> dict | None:
        try:
            with open(self.state_path, encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            log.error("恢复状态文件不可读: %s", exc)
            return {"state": STATE_INCOMPLETE,
                    "error": f"恢复状态文件损坏: {exc}"}

    def _write_state(self, data: dict) -> None:
        data["updated_at"] = _now()
        _atomic_write_json(self.state_path, data)

    def startup(self) -> dict:
        """进程启动时调用：自愈未完成的切换，决定存储是否可用。"""
        with self._lock:
            self.ensure_dirs()
            state = self.load_state()
            if state is None:
                if os.path.exists(self.prev_meta):
                    state = {"state": STATE_SWITCHING,
                             "error": "发现遗留切换标记（状态文件缺失）"}
                else:
                    self._cleanup_leftovers()
                    self.gate.set_storage_ok(True)
                    return {"state": STATE_NONE, "storage_ok": True}

            st = state.get("state")
            if st in (STATE_SWITCHING, STATE_INCOMPLETE):
                state = self._resolve_interrupted(state)
            elif st == STATE_VERIFIED:
                self._cleanup_leftovers()
                if not self._candidate_files_ok(state):
                    state.update({
                        "state": STATE_FAILED,
                        "error": "候选备份文件已丢失（可能容器重建），请重新上传校验",
                        "candidate_dir": None,
                    })
                    self._write_state(state)
                self.gate.set_storage_ok(True)
            else:  # none / applied / failed / cancelled
                self._cleanup_leftovers()
                self.gate.set_storage_ok(True)
            return state

    # ------------------------------------------------------------ 中断自愈
    def _resolve_interrupted(self, state: dict) -> dict:
        """崩溃后只恢复为“完整旧配对”或“完整备份配对”之一。"""
        # 情形一：两个库都已换入备份（最后一步写 state 前被杀）。
        # 用状态中记录的两份候选校验和确认“两库都是备份”，而非一旧一新。
        committed = state.get("committed_prev") or {}
        if (self._verify_file(self.source_path,
                              (committed.get("source") or {}).get("sha256"),
                              (committed.get("source") or {}).get("size"))
                and self._verify_file(self.target_path,
                                      (committed.get("target") or {}).get("sha256"),
                                      (committed.get("target") or {}).get("size"))):
            log.warning("检测到两库均已换入备份，补记为已启用并清理")
            state.update({
                "state": STATE_APPLIED,
                "applied_at": state.get("applied_at") or _now(),
                "error": None,
                "after": self._watermarks_unlocked(),
                "interrupted_recovered": True,
            })
            self._write_state(state)
            self.gate.set_storage_ok(True)
            self._cleanup_leftovers()
            return state

        prev_meta = self._read_json(self.prev_meta)
        previous_ok = bool(prev_meta) and self._verify_file(
            self.prev_src, prev_meta.get("source_sha256"),
            prev_meta.get("source_size")) and self._verify_file(
            self.prev_tgt, prev_meta.get("target_sha256"),
            prev_meta.get("target_size"))

        if previous_ok:
            log.warning("检测到中断的两库切换，使用完整旧配对整体回滚")
            try:
                self._restore_from_previous()
                state.update({
                    "state": STATE_FAILED,
                    "error": ("上次切换途中中断，已自动回滚为完整旧配对；"
                              "如仍需要恢复，请重新上传校验并确认"),
                    "interrupted_recovered": True,
                    "retryable": True,
                })
                self._write_state(state)
                self.gate.set_storage_ok(True)
                self._cleanup_leftovers()
                return state
            except Exception as exc:
                log.error("回滚旧配对失败: %s", exc)
                state.update({
                    "state": STATE_INCOMPLETE,
                    "error": f"切换中断且回滚旧配对失败，业务已暂停: {exc}",
                })
                self._write_state(state)
                self.gate.set_storage_ok(False)
                return state

        # 无完整 previous ⇒ 替换尚未开始（旧配对原封不动）。仍要确认当前
        # 两库确实构成一对完整配对，避免极端情况下把混用的两库当成可用。
        log.warning("检测到未开始或已完成的切换残留，核对当前两库")
        ok = self._live_pair_ok()
        detail = None
        if ok:
            try:
                self.validate_pair(self.source_path, self.target_path)
            except RestoreRejected as exc:
                ok = False
                detail = "；".join(exc.errors[:5])
        if ok:
            state.update({
                "state": STATE_FAILED,
                "error": "上次切换在替换两库前中断，原两库未被改动；可重试",
                "interrupted_recovered": True,
                "retryable": True,
            })
        else:
            state.update({
                "state": STATE_INCOMPLETE,
                "error": ("未找到完整回滚备份，且当前两库不构成一致配对，"
                          "业务已暂停，需人工处理"
                          + (f"：{detail}" if detail else "")),
            })
        self._write_state(state)
        self.gate.set_storage_ok(ok)
        if ok:
            self._cleanup_leftovers()
        return state

    @staticmethod
    def _read_json(path: str) -> dict | None:
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    @staticmethod
    def _verify_file(path: str, sha: str | None, size: int | None) -> bool:
        try:
            if not sha or size is None or os.path.getsize(path) != size:
                return False
            return _sha256(path) == sha
        except OSError:
            return False

    def _live_pair_ok(self) -> bool:
        return _db_integrity_ok(self.source_path) and \
            _db_integrity_ok(self.target_path)

    def _restore_from_previous(self) -> None:
        """把 previous 中的完整旧配对复制回两库（幂等，可反复重试）。"""
        if not (os.path.exists(self.prev_src) and os.path.exists(self.prev_tgt)):
            raise RuntimeError("旧配对备份不完整")
        for src, live in ((self.prev_src, self.source_path),
                          (self.prev_tgt, self.target_path)):
            tmp = live + ".rollback-tmp"
            _remove_db_files(tmp)
            shutil.copy2(src, tmp)
            with open(tmp, "rb+") as f:
                os.fsync(f.fileno())
            _remove_db_files(live)
            os.replace(tmp, live)
            _fsync_dir(os.path.dirname(live))
        if not self._live_pair_ok():
            raise RuntimeError("回滚后的旧配对完整性校验失败")

    def _cleanup_leftovers(self) -> None:
        """删除本次切换的中间副本，保留状态文件。"""
        for d in (os.path.dirname(self.prev_src), os.path.dirname(self.inc_src),
                  self.verify_dir):
            shutil.rmtree(d, ignore_errors=True)
        for d in (os.path.dirname(self.prev_tgt), os.path.dirname(self.inc_tgt)):
            shutil.rmtree(d, ignore_errors=True)

    def _candidate_files_ok(self, state: dict) -> bool:
        d = state.get("candidate_dir") or ""
        f = state.get("files") or {}
        return (self._verify_file(os.path.join(d, "source.db"),
                                  (f.get("source") or {}).get("sha256"),
                                  (f.get("source") or {}).get("size"))
                and self._verify_file(os.path.join(d, "target.db"),
                                      (f.get("target") or {}).get("sha256"),
                                      (f.get("target") or {}).get("size")))

    # ================================================================ 上传/校验
    def stage_upload(self, source_stream, target_stream,
                     manifest_stream=None) -> dict:
        """接收上传并校验。通过则留下候选备份；失败抛 RestoreRejected。"""
        with self._lock:
            self.ensure_dirs()
            cur = self.load_state()
            if cur and cur.get("state") in (STATE_SWITCHING, STATE_INCOMPLETE):
                raise RestoreConflict("存在未解决的切换状态，请先重启应用自愈")
            if self.gate.storage_ok is False:
                raise RestoreConflict("存储处于未解决状态，无法校验备份")

            job_id = uuid.uuid4().hex[:12]
            job_dir = os.path.join(self.staging_dir, "jobs", job_id)
            os.makedirs(job_dir, exist_ok=True)
            src = os.path.join(job_dir, "source.db")
            tgt = os.path.join(job_dir, "target.db")
            try:
                src_size = self._save_stream(source_stream, src)
                tgt_size = self._save_stream(target_stream, tgt)
                report = self.validate_pair(
                    src, tgt,
                    extra_manifest=self._load_manifest_stream(manifest_stream))
            except Exception:
                shutil.rmtree(job_dir, ignore_errors=True)
                raise

            state = {
                "job_id": job_id,
                "state": STATE_VERIFIED,
                "created_at": _now(),
                "error": None,
                "errors": [],
                "report": report,
                "files": {
                    "source": {"sha256": report["source"]["sha256"],
                               "size": src_size},
                    "target": {"sha256": report["target"]["sha256"],
                               "size": tgt_size},
                },
                "candidate_dir": job_dir,
            }
            self._write_state(state)
            self._prune_other_jobs(job_id)
            log.info("恢复备份校验通过 job=%s 源水位=%s 目标水位=%s 待同步=%s",
                     job_id, report["source"]["max_tx"],
                     report["target"]["last_tx_id"], report["pending_txs"])
            return state

    def _save_stream(self, stream, dest: str) -> int:
        total = 0
        with open(dest, "wb") as out:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > self.max_bytes:
                    raise RestoreRejected(
                        [f"{os.path.basename(dest)} 超过大小上限 "
                         f"{self.max_bytes} 字节"])
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
        if total == 0:
            raise RestoreRejected([f"{os.path.basename(dest)} 为空文件"])
        return total

    @staticmethod
    def _load_manifest_stream(stream) -> dict | None:
        if stream is None:
            return None
        raw = stream.read(1024 * 1024)
        if not raw:
            return None
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise RestoreRejected([f"restore.json 不是有效 JSON: {exc}"])
        if not isinstance(data, dict):
            raise RestoreRejected(["restore.json 结构不正确（应为 JSON 对象）"])
        return data

    def _prune_other_jobs(self, keep: str) -> None:
        jobs = os.path.join(self.staging_dir, "jobs")
        for name in os.listdir(jobs):
            if name != keep:
                shutil.rmtree(os.path.join(jobs, name), ignore_errors=True)

    # ------------------------------------------------------------ 校验主体
    def validate_pair(self, src: str, tgt: str,
                      extra_manifest: dict | None = None) -> dict:
        errors: list[str] = []
        for label, path in (("source.db", src), ("target.db", tgt)):
            try:
                if os.path.getsize(path) == 0:
                    errors.append(f"{label} 为空文件")
                elif _read_header(path) != SQLITE_MAGIC:
                    errors.append(f"{label} 不是 SQLite 数据库文件")
            except OSError as exc:
                errors.append(f"{label} 不可读: {exc}")
        if errors:
            raise RestoreRejected(errors)

        if self.verify_delay_ms > 0:  # 测试钩子：模拟较大的备份校验耗时
            time.sleep(self.verify_delay_ms / 1000.0)
        src_info = self._inspect_source(src, errors)
        tgt_info = self._inspect_target(tgt, errors)
        if errors:
            raise RestoreRejected(errors)

        src_sha, tgt_sha = _sha256(src), _sha256(tgt)
        if src_sha == tgt_sha:
            raise RestoreRejected(["source.db 与 target.db 是同一个文件"])
        if extra_manifest is not None:
            self._check_extra_manifest(extra_manifest, src_info, tgt_info,
                                       src_sha, tgt_sha, errors)

        # ---- 跨库条件：目标不超前于源，待同步日志完整
        max_tx = src_info["max_tx"]
        last = tgt_info["last_tx_id"]
        not_ahead = last <= max_tx
        if not not_ahead:
            errors.append(
                f"目标水位 {last} 超过源水位 {max_tx}，"
                "两份文件不是同次停机的一致配对")
        pending = max(0, max_tx - last)
        logs_complete = False
        if not_ahead:
            missing = self._missing_pending_txs(src, last, max_tx)
            logs_complete = not missing
            if missing:
                errors.append(
                    "待同步日志不完整，缺少 tx: "
                    + ", ".join(str(x) for x in missing[:20])
                    + (" …" if len(missing) > 20 else ""))

        deep_verified = False
        deep_note = None
        if not errors:
            if pending > self.deep_verify_max_txs:
                deep_note = (f"待同步事务 {pending} 笔超过深校验上限 "
                             f"{self.deep_verify_max_txs}，仅做结构校验")
            else:
                deep_verified, detail = self._deep_verify(src, tgt, last, max_tx)
                if not deep_verified:
                    errors.append(
                        "两份库内容不属于同一条同步链：待同步事务重放到目标副本后"
                        "与源库逐行对账不一致" + (f"（{detail}）" if detail else ""))
        if errors:
            raise RestoreRejected(errors)

        return {
            "validated_at": _now(),
            "source": {**src_info, "sha256": src_sha,
                       "size": os.path.getsize(src)},
            "target": {**tgt_info, "sha256": tgt_sha,
                       "size": os.path.getsize(tgt)},
            "pending_txs": pending,
            "deep_verified": deep_verified,
            "deep_note": deep_note,
            "conditions": {
                "target_first_full_done":
                    tgt_info["phase"] == target_mod.PHASE_READY,
                "target_not_ahead_of_source": not_ahead,
                "pending_logs_complete": logs_complete,
            },
            "manifest": extra_manifest,
        }

    @staticmethod
    def _manifest_hash(files, key, m, *alts):
        for alt in alts:
            if m.get(alt):
                return m[alt]
        entry = files.get(key)
        if isinstance(entry, dict):
            return entry.get("sha256")
        return entry

    def _check_extra_manifest(self, m: dict, src_info: dict, tgt_info: dict,
                              src_sha: str, tgt_sha: str, errors: list[str]) -> None:
        files = m.get("files") if isinstance(m.get("files"), dict) else {}
        want_src = self._manifest_hash(files, "source", m, "source_sha256",
                                       "source_sha")
        want_tgt = self._manifest_hash(files, "target", m, "target_sha256",
                                       "target_sha")
        if want_src and want_src != src_sha:
            errors.append("restore.json 中的 source.db 校验和不匹配")
        if want_tgt and want_tgt != tgt_sha:
            errors.append("restore.json 中的 target.db 校验和不匹配")
        wm = m.get("watermarks") if isinstance(m.get("watermarks"), dict) else {}
        exp_max = wm.get("source_max_tx", m.get("source_max_tx"))
        exp_last = wm.get("target_last_tx_id", m.get("target_last_tx_id"))
        if exp_max is not None and exp_max != src_info["max_tx"]:
            errors.append("restore.json 记录的源水位与 source.db 不符")
        if exp_last is not None and exp_last != tgt_info["last_tx_id"]:
            errors.append("restore.json 记录的目标水位与 target.db 不符")

    @staticmethod
    def _open_ro(path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=1")
        return conn

    def _check_schema(self, conn, required: dict, label: str,
                      errors: list[str]) -> bool:
        existing = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        missing_tables = sorted(set(required) - existing)
        if missing_tables:
            errors.append(f"{label} 缺少表: {', '.join(missing_tables)}"
                          "（损坏或不是本应用的库）")
            return False
        for table, cols in required.items():
            have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            missing = sorted(cols - have)
            if missing:
                errors.append(f"{label}.{table} 缺少字段: {', '.join(missing)}"
                              "（版本不兼容）")
                return False
        return True

    def _inspect_source(self, path: str, errors: list[str]) -> dict:
        info: dict = {"max_tx": 0, "samples": 0, "results": 0, "txs": 0,
                      "ops": 0}
        try:
            with closing(self._open_ro(path)) as c:
                if not self._integrity_ok(c, "source.db", errors):
                    return info
                if not self._check_schema(c, _SRC_TABLES, "source.db", errors):
                    return info
                fk = c.execute("PRAGMA foreign_key_check").fetchall()
                if fk:
                    errors.append(f"source.db 外键校验失败（{len(fk)} 处）")
                    return info
                info["max_tx"] = int(c.execute(
                    "SELECT COALESCE(MAX(tx_id),0) FROM tx_log").fetchone()[0])
                info["samples"] = c.execute(
                    "SELECT COUNT(*) FROM samples").fetchone()[0]
                info["results"] = c.execute(
                    "SELECT COUNT(*) FROM results").fetchone()[0]
                info["txs"] = c.execute(
                    "SELECT COUNT(*) FROM tx_log").fetchone()[0]
                info["ops"] = c.execute(
                    "SELECT COUNT(*) FROM tx_ops").fetchone()[0]

                if info["txs"] == 0:
                    # 首次全量后尚未发生任何更正属合法情形：此时 max_tx=0、
                    # 目标 last_tx_id 也应为 0。非空日志才要求 1..max 连续。
                    return info
                lo, cnt = c.execute(
                    "SELECT MIN(tx_id), COUNT(*) FROM tx_log").fetchone()
                if lo != 1 or cnt != info["max_tx"]:
                    errors.append(
                        f"source.db 事务日志不连续（1..{info['max_tx']} 中仅 "
                        f"{cnt} 条），待同步日志不完整")
                bad_ops = c.execute(
                    "SELECT COUNT(*) FROM (SELECT tx_id FROM tx_ops "
                    "GROUP BY tx_id HAVING MIN(seq)<>0 OR MAX(seq)<>COUNT(*)-1)"
                ).fetchone()[0]
                if bad_ops:
                    errors.append(
                        f"source.db 有 {bad_ops} 笔事务的操作流水序号不连续")
                n_tx_with_ops = c.execute(
                    "SELECT COUNT(DISTINCT tx_id) FROM tx_ops").fetchone()[0]
                if n_tx_with_ops != info["txs"]:
                    errors.append(
                        f"source.db 有事务缺少操作流水（{info['txs']} 笔事务仅 "
                        f"{n_tx_with_ops} 笔有流水），无法整笔重放")
                bad_kind = c.execute(
                    "SELECT COUNT(*) FROM tx_ops WHERE entity NOT IN "
                    "('sample','result') OR op NOT IN ('upsert','delete')"
                ).fetchone()[0]
                if bad_kind:
                    errors.append(f"source.db 有 {bad_kind} 条无法识别的日志操作")
                orphans = c.execute(
                    "SELECT COUNT(*) FROM results r LEFT JOIN samples s "
                    "ON s.sample_no=r.sample_no WHERE s.sample_no IS NULL"
                ).fetchone()[0]
                if orphans:
                    errors.append(f"source.db 有 {orphans} 条结果缺少对应样品")
        except (sqlite3.Error, OSError, ValueError) as exc:
            errors.append(f"source.db 无法读取（损坏或不兼容）: {exc}")
        return info

    def _inspect_target(self, path: str, errors: list[str]) -> dict:
        info: dict = {"phase": None, "high_water": None, "last_tx_id": 0,
                      "samples": 0, "results": 0, "full_done_at": None}
        try:
            with closing(self._open_ro(path)) as c:
                if not self._integrity_ok(c, "target.db", errors):
                    return info
                if not self._check_schema(c, _TGT_TABLES, "target.db", errors):
                    return info
                row = c.execute("SELECT * FROM sync_state WHERE id=1").fetchone()
                if row is None:
                    errors.append("target.db 缺少同步进度记录（未初始化或不兼容）")
                    return info
                info["phase"] = row["phase"]
                info["high_water"] = row["high_water"]
                info["last_tx_id"] = int(row["last_tx_id"] or 0)
                info["full_done_at"] = row["full_done_at"]
                info["samples"] = c.execute(
                    "SELECT COUNT(*) FROM samples").fetchone()[0]
                info["results"] = c.execute(
                    "SELECT COUNT(*) FROM results").fetchone()[0]

                if info["phase"] != target_mod.PHASE_READY:
                    errors.append(
                        f"target.db 尚未完成首次全量（phase={info['phase']}），"
                        "不能作为恢复备份")
                if info["high_water"] is None or info["high_water"] < 0:
                    errors.append("target.db 缺少有效的全量水位 high_water")
                elif info["last_tx_id"] < info["high_water"]:
                    errors.append(
                        f"target.db 进度 {info['last_tx_id']} 低于全量快照水位 "
                        f"{info['high_water']}，备份不完整")
                ahead = c.execute(
                    "SELECT (SELECT COUNT(*) FROM samples WHERE applied_tx>?) + "
                    "(SELECT COUNT(*) FROM results WHERE applied_tx>?)",
                    (info["last_tx_id"], info["last_tx_id"])).fetchone()[0]
                if ahead:
                    errors.append(
                        f"target.db 有 {ahead} 行数据的来源事务超过已应用水位，"
                        "进度与数据不一致")
                orphans = c.execute(
                    "SELECT COUNT(*) FROM results r LEFT JOIN samples s "
                    "ON s.sample_no=r.sample_no WHERE s.sample_no IS NULL"
                ).fetchone()[0]
                if orphans:
                    errors.append(f"target.db 有 {orphans} 条结果缺少对应样品")
        except (sqlite3.Error, OSError, ValueError) as exc:
            errors.append(f"target.db 无法读取（损坏或不兼容）: {exc}")
        return info

    @staticmethod
    def _integrity_ok(conn: sqlite3.Connection, label: str,
                      errors: list[str]) -> bool:
        try:
            rows = [tuple(r) for r in conn.execute("PRAGMA integrity_check")]
        except sqlite3.Error as exc:
            errors.append(f"{label} 完整性检查无法执行: {exc}")
            return False
        if rows != [("ok",)]:
            errors.append(f"{label} 完整性检查失败: {rows[:3]}")
            return False
        return True

    def _missing_pending_txs(self, src: str, last: int, max_tx: int) -> list[int]:
        with closing(self._open_ro(src)) as c:
            have = {r[0] for r in c.execute(
                "SELECT tx_id FROM tx_log WHERE tx_id>? AND tx_id<=?",
                (last, max_tx))}
        return [tx_id for tx_id in range(last + 1, max_tx + 1)
                if tx_id not in have]

    def _deep_verify(self, src: str, tgt: str, last: int,
                     max_tx: int) -> tuple[bool, str | None]:
        if last > max_tx:
            return False, "目标水位超过源水位"
        """在目标副本上重放待同步事务，再与源库逐行对账。

        证明两份文件同源、待同步日志足以无缝接续，且无需清库/重做全量。
        """
        shutil.rmtree(self.verify_dir, ignore_errors=True)
        os.makedirs(self.verify_dir, exist_ok=True)
        copy_path = os.path.join(self.verify_dir, "target-copy.db")
        try:
            _copy_db(tgt, copy_path)
            after = last
            while after < max_tx:
                with closing(self._open_ro(src)) as c:
                    rows = c.execute(
                        "SELECT tx_id, note, created_at FROM tx_log "
                        "WHERE tx_id>? ORDER BY tx_id LIMIT 500", (after,)
                    ).fetchall()
                    if not rows:
                        return False, "待同步区间内没有可用事务"
                    ops_rows = c.execute(
                        "SELECT tx_id, seq, entity, op, sample_no, item, site, "
                        "value FROM tx_ops WHERE tx_id>? AND tx_id<=? "
                        "ORDER BY tx_id, seq",
                        (after, rows[-1]["tx_id"])).fetchall()
                by_tx: dict[int, list[dict]] = {r["tx_id"]: [] for r in rows}
                for o in ops_rows:
                    by_tx[o["tx_id"]].append(dict(o))
                for r in rows:
                    tx = dict(r)
                    tx["ops"] = by_tx[tx["tx_id"]]
                    if not target_mod.apply_tx(copy_path, tx):
                        return False, f"tx {tx['tx_id']} 重放未生效"
                    after = tx["tx_id"]
            if target_mod.dump_state(copy_path) != self._source_dump(src):
                return False, "逐行对账存在差异"
            return True, None
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"
        finally:
            shutil.rmtree(self.verify_dir, ignore_errors=True)

    def _source_dump(self, src: str) -> dict:
        with closing(self._open_ro(src)) as c:
            samples = [tuple(r) for r in c.execute(
                "SELECT sample_no, site FROM samples ORDER BY sample_no")]
            results = [tuple(r) for r in c.execute(
                "SELECT sample_no, item, value FROM results "
                "ORDER BY sample_no, item")]
        return {"samples": samples, "results": results}

    # ================================================================ 确认切换
    def confirm(self) -> dict:
        """确认替换：暂停新业务 → 等在途结束 → 原子换入两份备份。"""
        with self._lock:
            state = self.load_state()
            if not state or state.get("state") not in (STATE_VERIFIED,
                                                       STATE_FAILED):
                raise RestoreConflict(
                    "没有可确认的恢复备份，请先上传并校验"
                    + ("（当前状态需重启应用自愈）"
                       if state and state.get("state") == STATE_INCOMPLETE else ""))
            if self.gate.storage_ok is False:
                raise RestoreConflict("存储处于未解决状态，请先重启应用自愈")
            if not self._candidate_files_ok(state):
                raise RestoreConflict("候选备份文件缺失或被改动，请重新上传校验")

            job_dir = state["candidate_dir"]
            src_new = os.path.join(job_dir, "source.db")
            tgt_new = os.path.join(job_dir, "target.db")

            before = self._live_watermarks()
            state.update({"state": STATE_SWITCHING, "error": None,
                          "switch_started_at": _now(), "before": before})
            self._write_state(state)

            try:
                # 先在暂停窗口之外把两份备份复制到各库所在文件系统，
                # 让真正的停机窗口只剩下一对 os.replace。
                os.makedirs(os.path.dirname(self.inc_src), exist_ok=True)
                os.makedirs(os.path.dirname(self.inc_tgt), exist_ok=True)
                _copy_db(src_new, self.inc_src)
                _copy_db(tgt_new, self.inc_tgt)
                # 并准备断电时的回滚依据：状态里记录候选校验和，重启后若
                # 发现 committed 标记即可据此判定两库已换入备份。
                state["committed_prev"] = {
                    "source": {"sha256": _sha256(src_new),
                               "size": os.path.getsize(src_new)},
                    "target": {"sha256": _sha256(tgt_new),
                               "size": os.path.getsize(tgt_new)},
                }
                # 持久化“若两库都已是备份则视为已完成”的判据，供重启自愈使用。
                self._write_state(state)
                with self.gate.exclusive(timeout=self.drain_timeout):
                    # 冻结业务后的真实水位，作为“切换前水位”记录
                    before = self._watermarks_unlocked()
                    state["before"] = before
                    result = self._perform_swap(state, before)
            except TimeoutError as exc:
                state.update({
                    "state": STATE_FAILED,
                    "error": f"等待在途操作结束超时，未做任何替换：{exc}",
                    "retryable": True,
                })
                self._write_state(state)
                self.gate.set_storage_ok(True)
                log.warning("恢复放弃（等待在途操作超时）: %s", exc)
                raise RestoreConflict(str(exc))

            self.last_apply_result = result
            log.info("停机备份恢复完成 job=%s：目标水位 %s → %s",
                     state.get("job_id"), result["before"]["last_tx_id"],
                     result["after"]["last_tx_id"])
            return result

    def _perform_swap(self, state: dict, before: dict) -> dict:
        os.makedirs(os.path.dirname(self.prev_src), exist_ok=True)
        os.makedirs(os.path.dirname(self.prev_tgt), exist_ok=True)
        os.makedirs(os.path.dirname(self.inc_src), exist_ok=True)
        os.makedirs(os.path.dirname(self.inc_tgt), exist_ok=True)
        # 清掉上次遗留（本次尚无任何替换动作）
        _remove_db_files(self.prev_src)
        _remove_db_files(self.prev_tgt)
        try:
            os.remove(self.prev_meta)
        except FileNotFoundError:
            pass
        try:
            # 1) 备份完整旧配对：两库各自卷内各一份，最后写 commit 标记
            _copy_db(self.source_path, self.prev_src)
            _copy_db(self.target_path, self.prev_tgt)
            prev_src_sha = _sha256(self.prev_src)
            prev_tgt_sha = _sha256(self.prev_tgt)
            _atomic_write_json(self.prev_meta, {
                "source_sha256": prev_src_sha,
                "target_sha256": prev_tgt_sha,
                "source_size": os.path.getsize(self.prev_src),
                "target_size": os.path.getsize(self.prev_tgt),
                "created_at": _now(),
            })
            # 校验旧配对备份确实可读，才允许进入替换阶段
            if not (_db_integrity_ok(self.prev_src)
                    and _db_integrity_ok(self.prev_tgt)):
                raise RuntimeError("旧配对备份完整性校验失败")

            # 2) 整体替换：先源后目标；中途崩溃由 startup() 用 previous 回滚
            self._maybe_pause("before_source_swap")
            self._maybe_fail("before_source_swap")
            self._replace_live(self.inc_src, self.source_path)
            self._maybe_pause("after_source_swap")
            self._maybe_fail("after_source_swap")
            self._replace_live(self.inc_tgt, self.target_path)
            self._maybe_pause("after_target_swap")
            self._maybe_fail("after_target_swap")

            # 3) 换入后自检
            if not self._live_pair_ok():
                raise RuntimeError("换入后的两库完整性校验失败")
            state.update({"state": STATE_APPLIED, "applied_at": _now(),
                          "error": None, "after": self._watermarks_unlocked()})
            self._write_state(state)
            self.gate.set_storage_ok(True)
            # 已落 applied 后才清理回滚备份与候选上传副本
            self._cleanup_leftovers()
            job_dir = state.get("candidate_dir")
            if job_dir:
                shutil.rmtree(job_dir, ignore_errors=True)
                state["candidate_removed"] = True
            return {"ok": True, "job_id": state.get("job_id"),
                    "before": before, "after": state["after"],
                    "report": state.get("report")}
        except Exception as exc:
            log.error("恢复切换失败（将回滚完整旧配对）: %s", exc)
            try:
                self._restore_from_previous()
                state.update({
                    "state": STATE_FAILED,
                    "error": f"切换失败，已回滚为完整旧配对，可重试：{exc}",
                    "retryable": True,
                })
                self._write_state(state)
                self.gate.set_storage_ok(True)
                self._cleanup_leftovers()
            except Exception as rb_exc:
                state.update({
                    "state": STATE_INCOMPLETE,
                    "error": f"切换失败且回滚失败（业务已暂停）：{exc} / {rb_exc}",
                })
                self._write_state(state)
                self.gate.set_storage_ok(False)
            raise RestoreConflict(state["error"])

    @staticmethod
    def _replace_live(new_path: str, live_path: str) -> None:
        # 替换前移除 WAL 边车，防止旧日志被应用到新文件
        for suffix in ("-wal", "-shm", "-journal"):
            try:
                os.remove(live_path + suffix)
            except FileNotFoundError:
                pass
        os.replace(new_path, live_path)
        _fsync_dir(os.path.dirname(live_path))

    def _live_watermarks(self) -> dict:
        """在线读取两库水位；切换窗口内不读，避免看到替换到一半的状态。"""
        try:
            with self.gate.read("restore-status"):
                return self._watermarks_unlocked()
        except Exception as exc:  # OperationPaused / StorageUnavailable
            return {"paused": True, "reason": str(exc), "source_max_tx": None,
                    "phase": None, "high_water": None, "last_tx_id": None}

    def _watermarks_unlocked(self) -> dict:
        """切换独占区段内读取水位（此时本线程持有独占权，绕过 gate 检查）。"""
        try:
            with closing(sqlite3.connect(self.source_path, timeout=10.0)) as c:
                max_tx = c.execute(
                    "SELECT COALESCE(MAX(tx_id),0) FROM tx_log").fetchone()[0]
            with closing(sqlite3.connect(self.target_path, timeout=10.0)) as c:
                row = c.execute("SELECT phase, high_water, last_tx_id "
                                "FROM sync_state WHERE id=1").fetchone()
            return {"source_max_tx": int(max_tx),
                    "phase": row[0] if row else None,
                    "high_water": row[1] if row else None,
                    "last_tx_id": int(row[2] or 0) if row else 0}
        except Exception as exc:
            return {"error": str(exc), "source_max_tx": None, "phase": None,
                    "high_water": None, "last_tx_id": None}

    # ------------------------------------------------------------ 测试钩子
    def _maybe_pause(self, step: str) -> None:
        with self._hook_lock:
            if self._pause_budget <= 0 or self.pause_after != step:
                return
            self._pause_budget -= 1
        log.warning("测试钩子：切换在 %s 后停顿 %d ms", step, self.pause_ms)
        time.sleep(self.pause_ms / 1000.0)

    def _maybe_fail(self, step: str) -> None:
        with self._hook_lock:
            if self._fail_budget <= 0 or self.fail_after != step:
                return
            self._fail_budget -= 1
        raise OSError(f"注入写盘失败（步骤 {step}）")

    # ================================================================ 查询/取消
    def status(self) -> dict:
        state = self.load_state() or {"state": STATE_NONE}
        out = dict(state)
        out["storage_ok"] = self.gate.storage_ok
        out["paused"] = self.gate.paused
        out["live"] = self._live_watermarks() if self.gate.storage_ok else {
            "unavailable": True}
        if out.get("state") in (STATE_VERIFIED, STATE_FAILED):
            out["retryable"] = True
            out["confirmable"] = self._candidate_files_ok(out)
        else:
            out["confirmable"] = False
        return out

    def cancel(self) -> dict:
        with self._lock:
            state = self.load_state()
            if not state or state.get("state") not in (STATE_VERIFIED,
                                                       STATE_FAILED):
                raise RestoreConflict("当前没有可取消的候选备份")
            job_dir = state.get("candidate_dir")
            if job_dir:
                shutil.rmtree(job_dir, ignore_errors=True)
            state.update({"state": STATE_CANCELLED, "error": None,
                          "cancelled_at": _now(), "candidate_dir": None,
                          "files": {}})
            self._write_state(state)
            self.gate.set_storage_ok(True)
            return state
