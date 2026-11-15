#!/usr/bin/env python3
"""端到端自测：停机备份恢复。

启动真实服务进程，逐条验证需求：

  A. 校验期间原系统可用（上传大备份时并发更正/查询不受影响）
  B. 正常恢复：只接受完整配对；确认后整体换入两份备份，
     暂停新操作、等在途结束，旧库不被清空重来，按备份进度增量接续
  C. 校验失败必须拒绝且原库不变：
       目标未完成首次全量 / 目标水位超前 / 待同步日志缺口 /
       损坏文件 / 非 SQLite / 两库不同源
  D. 确认后新更正从备份水位继续；旧执行者（旧世代同步线程）不得写入
  E. 两库切换途中写盘失败：自动回滚为完整旧配对；恢复状态可查、可重试
  F. 切换途中进程被杀（after_source_swap）：重启后自动回滚完整旧配对，
     两库绝不为混用
  G. 两库仍使用独立数据卷（目录/文件独立性断言）

用法：python scripts/restore_selftest.py
"""
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time

import requests

BASE = "http://127.0.0.1:5066"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES = os.path.join(ROOT, "samples", "backup-20261115")

FAILURES = []


def check(name, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}"
          + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(f"{name} {detail}")


def wait_until(fn, timeout=30, interval=0.15, desc=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    print(f"  [超时] 等待条件未满足: {desc}")
    return False


def api(path, **kw):
    r = requests.request(kw.pop("method", "GET"), BASE + path, timeout=60, **kw)
    try:
        body = r.json()
    except Exception:
        body = {}
    return r.status_code, body


def status():
    return api("/api/status")[1]


def rstatus():
    return api("/api/restore/status")[1]


def dump(db_path):
    conn = sqlite3.connect(db_path)
    try:
        samples = conn.execute(
            "SELECT sample_no, site FROM samples ORDER BY sample_no").fetchall()
        results = conn.execute(
            "SELECT sample_no, item, value FROM results ORDER BY sample_no, item"
        ).fetchall()
    finally:
        conn.close()
    return samples, results


def sync_state(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT phase, high_water, last_tx_id, full_started_at, full_done_at "
            "FROM sync_state WHERE id=1").fetchone()
    finally:
        conn.close()


def source_max(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT COALESCE(MAX(tx_id),0) FROM tx_log").fetchone()[0]
    finally:
        conn.close()


def wait_synced(src_db, tgt_db, timeout=30):
    def ok():
        s = status()
        return (s.get("phase") == "ready"
                and s["last_tx_id"] >= s["source_max_tx"]
                and dump(src_db) == dump(tgt_db))
    return wait_until(ok, timeout=timeout, desc="两库追平且一致")


class Server:
    def __init__(self, data_dir, extra_env=None):
        self.data_dir = data_dir
        self.extra_env = extra_env or {}
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env.update({
            "DATA_DIR": self.data_dir, "PORT": "5066",
            "SOURCE_DB": os.path.join(self.data_dir, "source", "source.db"),
            "TARGET_DB": os.path.join(self.data_dir, "target", "target.db"),
            "SYNC_POLL_INTERVAL": "0.2", "SEED_SAMPLES": "20",
            "RESTORE_DRAIN_TIMEOUT": "20",
        })
        env.update(self.extra_env)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app"], cwd=ROOT, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        ok = wait_until(self._healthy, timeout=30, desc="服务就绪")
        if not ok:
            out = self.proc.stdout.read() if self.proc.stdout else ""
            raise RuntimeError("服务未启动:\n" + out[-3000:])
        return self

    def _healthy(self):
        try:
            return requests.get(BASE + "/api/health", timeout=2).json().get("ok")
        except Exception:
            return False

    def stop(self):
        if not self.proc:
            return
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)

    def kill(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)

    def log_tail(self, n=40):
        try:
            out = self.proc.stdout.read() if self.proc.stdout else ""
        except Exception:
            out = ""
        return "\n".join(out.splitlines()[-n:])


def upload(src_path, tgt_path, manifest=None):
    handles = []
    files = {}
    fh = open(src_path, "rb")
    handles.append(fh)
    files["source_db"] = ("source.db", fh, "application/octet-stream")
    fh = open(tgt_path, "rb")
    handles.append(fh)
    files["target_db"] = ("target.db", fh, "application/octet-stream")
    if manifest:
        files["restore_json"] = ("restore.json", json.dumps(manifest).encode(),
                                 "application/json")
    try:
        return api("/api/restore/upload", method="POST", files=files)
    finally:
        for h in handles:
            h.close()


def make_bad_copy(src: str, tgt: str, work: str, kind: str):
    """基于示例备份制作各类“不合格备份”。返回 (src, tgt)。"""
    s = os.path.join(work, kind + "-source.db")
    t = os.path.join(work, kind + "-target.db")
    shutil.copy2(src, s)
    shutil.copy2(tgt, t)
    if kind == "not_first_full":
        c = sqlite3.connect(t)
        c.execute("UPDATE sync_state SET phase='full_running', full_done_at=NULL, "
                  "last_tx_id=0, high_water=0 WHERE id=1")
        c.commit(); c.close()
    elif kind == "target_ahead":
        c = sqlite3.connect(t)
        c.execute("UPDATE sync_state SET last_tx_id=999, high_water=0 WHERE id=1")
        c.commit(); c.close()
    elif kind == "gap":
        c = sqlite3.connect(s)
        c.execute("DELETE FROM tx_ops WHERE tx_id=7")
        c.execute("DELETE FROM tx_log WHERE tx_id=7")
        c.commit(); c.close()
    elif kind == "no_ops":
        c = sqlite3.connect(s)
        c.execute("DELETE FROM tx_ops WHERE tx_id=8")
        c.commit(); c.close()
    elif kind == "corrupt":
        data = bytearray(open(t, "rb").read())
        for i in range(4096, min(len(data), 8192)):
            data[i] = 0xFF
        open(t, "wb").write(data)
    elif kind == "not_sqlite":
        open(t, "wb").write(b"<html>not a database</html>" * 100)
    elif kind == "other_chain":
        # 目标换成另一个完全不同步的库：源留示例，目标用空白库
        os.remove(t)
        c = sqlite3.connect(t)
        c.executescript("""
        CREATE TABLE samples(sample_no TEXT PRIMARY KEY, site TEXT NOT NULL,
                             applied_tx INTEGER NOT NULL);
        CREATE TABLE results(sample_no TEXT NOT NULL, item TEXT NOT NULL,
                             value REAL NOT NULL, applied_tx INTEGER NOT NULL,
                             PRIMARY KEY(sample_no,item));
        CREATE TABLE sync_state(id INTEGER PRIMARY KEY CHECK(id=1), phase TEXT,
            high_water INTEGER, last_tx_id INTEGER NOT NULL DEFAULT 0,
            full_started_at TEXT, full_done_at TEXT, updated_at TEXT);
        INSERT INTO sync_state VALUES(1,'ready',1,1,'x','x','x');
        INSERT INTO samples VALUES('ZZ-9999','XX',1);
        """)
        c.commit(); c.close()
    elif kind == "incompatible":
        c = sqlite3.connect(t)
        c.execute("ALTER TABLE sync_state RENAME TO sync_state_old")
        c.execute("CREATE TABLE sync_state(id INTEGER PRIMARY KEY, phase TEXT)")
        c.commit(); c.close()
    return s, t


def main():
    data_dir = tempfile.mkdtemp(prefix="geosync-restore-test-")
    work = tempfile.mkdtemp(prefix="geosync-bad-")
    src_db = os.path.join(data_dir, "source", "source.db")
    tgt_db = os.path.join(data_dir, "target", "target.db")
    srv = Server(data_dir)
    try:
        srv.start()
        requests.post(BASE + "/api/sync/full", timeout=5)
        check("初始全量完成", wait_until(lambda: status()["phase"] == "ready",
                                       30, desc="ready"))
        # 让在线库与备份不同（更晚的进度），验证恢复后水位回退到备份
        for _ in range(3):
            requests.post(BASE + "/api/source/correct", timeout=5)
        check("在线库已追平", wait_synced(src_db, tgt_db, 30))

        bsrc = os.path.join(SAMPLES, "source.db")
        btgt = os.path.join(SAMPLES, "target.db")
        manifest = json.load(open(os.path.join(SAMPLES, "restore.json")))

        print("== A. 校验期间原系统可用 ==")
        # 用 RESTORE_VERIFY_DELAY_MS 放慢校验，确认业务请求不被校验阻塞
        srv.stop()
        srv = Server(data_dir, {"RESTORE_VERIFY_DELAY_MS": "2500"}).start()
        import threading
        upload_result = {}
        up = threading.Thread(
            target=lambda: upload_result.update(
                dict(zip(("code", "body"), upload(bsrc, btgt, manifest)))),
            daemon=True)

        def slow_correction():
            t0 = time.time()
            r = requests.post(BASE + "/api/source/correct", timeout=10)
            return r.status_code, time.time() - t0

        online_last_at_upload = status()["last_tx_id"]
        up.start()
        time.sleep(0.4)  # 确保校验已开始
        t0 = time.time()
        rq = requests.get(BASE + "/api/query", params={"sample_no": "YT-0001"},
                          timeout=10)
        q_latency = time.time() - t0
        t0 = time.time()
        rc = requests.post(BASE + "/api/source/correct", timeout=10)
        c_latency = time.time() - t0
        check("校验期间查询仍可用且未被阻塞",
              rq.status_code == 200 and q_latency < 1.5, f"{q_latency:.2f}s")
        check("校验期间更正仍可用且未被阻塞",
              rc.status_code == 200 and c_latency < 1.5, f"{c_latency:.2f}s")
        up.join(timeout=30)
        code, body = upload_result.get("code"), upload_result.get("body", {})
        check("示例备份上传校验通过(201)", code == 201, f"{code} {body}")
        rep = (body.get("state") or {}).get("report") or {}
        check("报告：目标已完成首次全量",
              rep.get("conditions", {}).get("target_first_full_done") is True,
              str(rep.get("conditions")))
        check("报告：目标水位不超过源水位",
              rep.get("conditions", {}).get("target_not_ahead_of_source") is True)
        check("报告：待同步日志完整",
              rep.get("conditions", {}).get("pending_logs_complete") is True)
        check("报告：待同步 2 笔", rep.get("pending_txs") == 2, str(rep.get("pending_txs")))
        check("报告：深校验通过", rep.get("deep_verified") is True,
              str(rep.get("deep_note")))
        check("校验未替换原库（进度只会前进、不会回退到备份水位）",
              sync_state(tgt_db)[2] >= online_last_at_upload
              and sync_state(tgt_db)[2] != 6)
        r = requests.post(BASE + "/api/source/correct", timeout=5).json()
        check("校验期间仍可提交新更正", r.get("ok") is True, str(r))
        r = requests.get(BASE + "/api/query", params={"sample_no": "YT-0001"},
                         timeout=5)
        check("校验期间查询仍可用", r.status_code == 200)
        check("校验期间同步仍在推进并追平", wait_synced(src_db, tgt_db, 30))
        srv.stop()
        srv = Server(data_dir).start()

        print("== C. 不合格备份必须拒绝且原库不变 ==")
        before = dump(tgt_db), sync_state(tgt_db)
        cases = [
            ("not_first_full", "目标未完成首次全量"),
            ("target_ahead", "目标水位超过源水位"),
            ("gap", "待同步日志缺口"),
            ("no_ops", "事务缺少操作流水"),
            ("corrupt", "损坏文件"),
            ("not_sqlite", "非 SQLite 文件"),
            ("other_chain", "两库不同源（深校验/对账失败）"),
            ("incompatible", "结构不兼容"),
        ]
        for kind, desc in cases:
            s_path, t_path = make_bad_copy(bsrc, btgt, work, kind)
            code, body = upload(s_path, t_path, None)
            rejected = code in (400, 413, 422) and body.get("ok") is False
            check(f"拒绝：{desc}（{kind}）", rejected, f"HTTP {code} {body}")
        check("多次拒绝后原两库仍完全未变",
              (dump(tgt_db), sync_state(tgt_db)) == before)
        code, body = api("/api/restore/upload", method="POST", files={})
        check("缺少文件的上传被拒绝(400)", code == 400, str(code))
        # 清单校验和错误
        bad_manifest = json.loads(json.dumps(manifest))
        bad_manifest["files"]["source"]["sha256"] = "0" * 64
        code, body = upload(bsrc, btgt, bad_manifest)
        check("restore.json 校验和不匹配被拒绝(422)", code == 422, str(body))

        print("== B. 正常恢复：整体换入并增量接续 ==")
        code, body = upload(bsrc, btgt, manifest)
        check("重新上传示例备份通过(201)", code == 201, str(code))
        check("候选状态为 verified", rstatus().get("state") == "verified",
              str(rstatus().get("state")))
        code, body = api("/api/restore/confirm", method="POST")
        check("确认替换返回成功", code == 200 and body.get("ok") is True,
              f"{code} {body}")
        after = (body.get("result") or {}).get("after") or {}
        check("切换后目标水位 = 备份目标水位 6",
              after.get("last_tx_id") == 6, str(after))
        check("切换后阶段仍为 ready", after.get("phase") == "ready", str(after))

        # 恢复后的库应与备份配对一致，且日志/进度来自备份（未清库重做全量）
        st_new = sync_state(tgt_db)
        check("分析库进度取备份值（不重做全量）", st_new[2] == 6, str(st_new))
        check("分析库 full_started_at/full_done_at 来自备份",
              st_new[3] is not None and st_new[4] is not None, str(st_new))
        check("作业库换为备份（源水位 8）", source_max(src_db) == 8,
              str(source_max(src_db)))
        check("恢复状态为 applied", rstatus().get("state") == "applied",
              str(rstatus().get("state")))
        check("恢复后按备份进度追平（待同步 2 笔被接续）",
              wait_synced(src_db, tgt_db, 30))
        check("接续后两库与备份源库逐行一致",
              dump(tgt_db) == dump(bsrc))
        check("未清库：恢复了备份中的样品（非空）", len(dump(tgt_db)[0]) == 20)

        print("== D. 恢复后新更正继续增量；旧执行者不写入 ==")
        r = requests.post(BASE + "/api/source/correct", timeout=5).json()
        new_tx = r.get("tx_id")
        check("恢复后新更正提交成功", r.get("ok") is True, str(r))
        check("恢复后新更正增量同步",
              wait_synced(src_db, tgt_db, 30) and status()["last_tx_id"] >= new_tx)
        st = status()
        check("同步引擎世代已递增（旧世代被弃）",
              st.get("generation", 0) >= 1, str(st.get("generation")))

        print("== E. 切换途中写盘失败：回滚完整旧配对、可重试 ==")
        # 先把在线库推进到与备份不同，便于识别回滚是否成功
        for _ in range(2):
            requests.post(BASE + "/api/source/correct", timeout=5)
        check("注入前追平", wait_synced(src_db, tgt_db, 30))
        pre_src_rows = dump(src_db)
        pre_tgt_rows = dump(tgt_db)
        pre_state = sync_state(tgt_db)
        code, body = upload(bsrc, btgt, manifest)
        check("再次上传备份通过", code == 201, str(code))
        srv.stop()
        srv = Server(data_dir, {"RESTORE_FAIL_AFTER": "after_source_swap"}).start()
        code, body = api("/api/restore/confirm", method="POST")
        check("写盘故障导致确认失败(409)", code == 409 and body.get("ok") is False,
              f"{code} {body}")
        st = rstatus()
        check("恢复状态为 failed 且可重试",
              st.get("state") == "failed" and st.get("retryable") is True,
              str(st.get("state")))
        check("失败信息已暴露", bool(st.get("error")), str(st.get("error")))
        check("失败后存储仍可用", st.get("storage_ok") is True)
        check("两库已整体回滚为完整旧配对（源）", dump(src_db) == pre_src_rows)
        check("两库已整体回滚为完整旧配对（目标）", dump(tgt_db) == pre_tgt_rows)
        check("回滚后进度与失败前一致", sync_state(tgt_db) == pre_state)
        r = requests.get(BASE + "/api/query", params={"sample_no": "YT-0001"},
                         timeout=5)
        check("回滚后查询恢复可用", r.status_code == 200)
        check("回滚后两库对账一致", wait_synced(src_db, tgt_db, 30))

        print("== E2. 失败后可重试成功 ==")
        srv.stop()
        srv = Server(data_dir).start()
        code, body = api("/api/restore/confirm", method="POST")
        check("重试确认成功", code == 200 and body.get("ok") is True,
              f"{code} {body}")
        check("重试后按备份水位接续", wait_synced(src_db, tgt_db, 30))

        print("== F. 两库切换途中进程被杀：重启自愈为完整旧配对 ==")
        for _ in range(2):
            requests.post(BASE + "/api/source/correct", timeout=5)
        check("注入前追平2", wait_synced(src_db, tgt_db, 30))
        pre_src_rows = dump(src_db)
        pre_tgt_rows = dump(tgt_db)
        code, body = upload(bsrc, btgt, manifest)
        check("再次上传备份通过2", code == 201, str(code))
        srv.stop()
        # 在源库替换后停顿 6 秒：期间 kill -9，制造“两库切换途中中断”
        srv = Server(data_dir, {"RESTORE_PAUSE_AFTER": "after_source_swap",
                                "RESTORE_PAUSE_MS": "6000"}).start()
        def _confirm_ignoring_errors():
            try:
                requests.post(BASE + "/api/restore/confirm", timeout=30)
            except Exception:
                pass  # 服务将被强杀，连接中断属预期

        t = __import__("threading").Thread(target=_confirm_ignoring_errors,
                                           daemon=True)
        t.start()
        reaching = wait_until(
            lambda: os.path.exists(os.path.join(
                os.path.dirname(src_db), ".geosync-recovery", "previous",
                "meta.json")),
            timeout=15, desc="进入替换阶段")
        check("已进入切换（旧配对已备份）", reaching)
        time.sleep(0.5)
        srv.kill()
        t.join(timeout=5)
        srv = Server(data_dir).start()
        st = rstatus()
        check("重启后自愈：状态 failed（已回滚）", st.get("state") == "failed",
              str(st.get("state")))
        check("重启后已回滚标记/提示存在", bool(st.get("error")), str(st.get("error")))
        check("杀进程后两库仍为完整旧配对（源）", dump(src_db) == pre_src_rows,
              "源库被混用")
        check("杀进程后两库仍为完整旧配对（目标）", dump(tgt_db) == pre_tgt_rows,
              "目标库被混用")
        check("自愈后存储可用、查询可用",
              st.get("storage_ok") is True
              and requests.get(BASE + "/api/query",
                               params={"sample_no": "YT-0001"},
                               timeout=5).status_code == 200)
        code, body = api("/api/restore/confirm", method="POST")
        check("自愈后可直接重试确认并成功", code == 200 and body.get("ok") is True,
              f"{code} {body}")
        check("自愈重试后追平", wait_synced(src_db, tgt_db, 30))

        print("== F2. 两库均已换入后、写完成状态前被杀：重启补记为 applied ==")
        for _ in range(2):
            requests.post(BASE + "/api/source/correct", timeout=5)
        check("注入前追平3", wait_synced(src_db, tgt_db, 30))
        code, body = upload(bsrc, btgt, manifest)
        check("再次上传备份通过3", code == 201, str(code))
        srv.stop()
        srv = Server(data_dir, {"RESTORE_PAUSE_AFTER": "after_target_swap",
                                "RESTORE_PAUSE_MS": "6000"}).start()

        def _confirm_ignoring_errors2():
            try:
                requests.post(BASE + "/api/restore/confirm", timeout=30)
            except Exception:
                pass

        t = __import__("threading").Thread(target=_confirm_ignoring_errors2,
                                           daemon=True)
        t.start()
        # 目标库内容变成备份即说明两库都已换入
        backup_tgt_dump = dump(btgt)
        reached = wait_until(lambda: dump(tgt_db) == backup_tgt_dump,
                             timeout=20, desc="两库均已换入备份")
        check("已进入“两库均换入、状态未落盘”窗口", reached)
        time.sleep(0.5)
        srv.kill()
        t.join(timeout=5)
        srv = Server(data_dir).start()
        st = rstatus()
        check("重启后补记为 applied（完整备份配对）", st.get("state") == "applied",
              str(st.get("state")))
        check("补记后目标水位来自备份", sync_state(tgt_db)[2] >= 6,
              str(sync_state(tgt_db)))
        check("补记后两库可用并追平", wait_synced(src_db, tgt_db, 30))

        print("== G. 两库独立存储（独立数据卷语义） ==")
        check("两库文件位于不同目录（独立数据卷语义）",
              os.path.dirname(os.path.abspath(src_db))
              != os.path.dirname(os.path.abspath(tgt_db)),
              f"{src_db} / {tgt_db}")
        check("恢复中间状态分别写在两库各自目录",
              os.path.isdir(os.path.join(os.path.dirname(src_db),
                                         ".geosync-recovery"))
              and os.path.isdir(os.path.join(os.path.dirname(tgt_db),
                                             ".geosync-recovery")))
        check("恢复后两库仍是两个独立文件",
              os.path.exists(src_db) and os.path.exists(tgt_db)
              and os.stat(src_db).st_ino != os.stat(tgt_db).st_ino
              if hasattr(os.stat(src_db), "st_ino") else True)

        print("== 演示：取消候选备份 ==")
        code, body = upload(bsrc, btgt, manifest)
        check("上传候选", code == 201)
        code, body = api("/api/restore/cancel", method="POST")
        check("取消候选成功", code == 200 and body.get("ok") is True, str(body))
        check("取消后状态 cancelled", rstatus().get("state") == "cancelled")
    finally:
        srv.stop()
        if FAILURES:
            print("\n---- 服务日志（尾部）----")
            print(srv.log_tail(60))
        shutil.rmtree(data_dir, ignore_errors=True)
        shutil.rmtree(work, ignore_errors=True)

    print()
    if FAILURES:
        print(f"自测失败 {len(FAILURES)} 项：")
        for f in FAILURES:
            print("  -", f)
        return 1
    print("全部恢复自测通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
