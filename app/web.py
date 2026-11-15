"""Flask Web 入口：状态面板 + 操作 + 关联明细查询 + 停机备份恢复。"""
import logging
import threading

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import RequestEntityTooLarge

from . import config, source, target
from .gate import OperationGate, OperationPaused, StorageUnavailable
from .recovery import RecoveryManager, RestoreConflict, RestoreRejected
from .syncer import FaultInjector, Syncer

log = logging.getLogger("geosync.web")


class AutoCorrector(threading.Thread):
    """自动更正发生器：模拟源端持续产生更正事务。"""

    def __init__(self, source_path: str, interval: float,
                 gate: OperationGate | None = None) -> None:
        super().__init__(name="geosync-autocorrect", daemon=True)
        self.source_path = source_path
        self.interval = interval
        self.gate = gate or OperationGate()
        self.enabled = threading.Event()
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.is_set():
            if self.enabled.is_set():
                try:
                    with self.gate.read("auto-corrector"):
                        source.random_correction(self.source_path,
                                                 note="auto correction")
                except (OperationPaused, StorageUnavailable):
                    pass
                except Exception as exc:
                    log.error("自动更正失败: %s", exc)
            self._stop_event.wait(self.interval)


def _paused_response(exc: Exception):
    return jsonify({"ok": False, "paused": True, "retryable": True,
                    "message": str(exc)}), 503


def create_app() -> Flask:
    app = Flask(__name__)
    # 恢复上传大小上限 + 余量（由 recovery 再做精确字节数校验）
    app.config["MAX_CONTENT_LENGTH"] = config.RESTORE_MAX_BYTES * 2 + 1024 * 1024

    gate = OperationGate()
    recovery = RecoveryManager(
        gate, config.SOURCE_DB, config.TARGET_DB, config.RESTORE_DIR,
        max_bytes=config.RESTORE_MAX_BYTES,
        drain_timeout=config.RESTORE_DRAIN_TIMEOUT,
        pause_after=config.RESTORE_PAUSE_AFTER,
        pause_ms=config.RESTORE_PAUSE_MS,
        fail_after=config.RESTORE_FAIL_AFTER,
        verify_delay_ms=config.RESTORE_VERIFY_DELAY_MS,
    )
    # 启动自愈必须先于任何建库/播种：上次若在切换途中中断，先恢复成
    # 完整旧配对或清理完整备份配对，之后才允许触碰两库文件。
    recovery.startup()

    if gate.storage_ok:
        # 两库独立初始化；作业库为空时播种示例数据（重启不会重复播种）。
        source.init_db(config.SOURCE_DB)
        source.seed_if_empty(config.SOURCE_DB, config.SEED_SAMPLES)
        target.init_db(config.TARGET_DB)
    else:
        log.error("存储处于未解决状态，已暂停建库与业务，请查看恢复页")

    injector = FaultInjector()
    syncer = Syncer(config.SOURCE_DB, config.TARGET_DB, injector,
                    poll_interval=config.POLL_INTERVAL,
                    snapshot_delay_ms=config.SNAPSHOT_DELAY_MS,
                    gate=gate)
    syncer.start()
    auto = AutoCorrector(config.SOURCE_DB, config.AUTO_CORRECT_INTERVAL,
                         gate=gate)
    auto.start()
    app.extensions["syncer"] = syncer
    app.extensions["injector"] = injector
    app.extensions["auto"] = auto
    app.extensions["gate"] = gate
    app.extensions["recovery"] = recovery

    @app.errorhandler(RequestEntityTooLarge)
    def _too_large(_exc):
        return jsonify({"ok": False, "rejected": True, "errors": [
            f"上传内容超过上限 {config.RESTORE_MAX_BYTES} 字节（request）"],
            "message": "备份被拒绝，原库未改动"}), 413

    # ------------------------------------------------------------- 页面
    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/restore")
    def restore_page():
        return render_template("restore.html")

    # ------------------------------------------------------------- 状态
    @app.get("/api/health")
    def health():
        return jsonify({"ok": True})

    @app.get("/api/status")
    def status():
        try:
            with gate.read("status"):
                st = target.get_state(config.TARGET_DB)
                src_max = source.max_tx(config.SOURCE_DB)
                src_counts = source.counts(config.SOURCE_DB)
                tgt_counts = target.counts(config.TARGET_DB)
                consistent = (source.dump_state(config.SOURCE_DB)
                              == target.dump_state(config.TARGET_DB))
        except (OperationPaused, StorageUnavailable) as exc:
            return _paused_response(exc)
        return jsonify({
            "phase": st["phase"],
            "last_tx_id": st["last_tx_id"],
            "high_water": st["high_water"],
            "source_max_tx": src_max,
            "pending": max(0, src_max - st["last_tx_id"]),
            "full_started_at": st["full_started_at"],
            "full_done_at": st["full_done_at"],
            "source_counts": src_counts,
            "target_counts": tgt_counts,
            "consistent": consistent,
            "auto_correct": auto.enabled.is_set(),
            "fault_armed": injector.armed,
            "last_error": syncer.last_error,
            "storage_ok": gate.storage_ok,
            "paused": gate.paused,
            "generation": gate.generation,
        })

    # ------------------------------------------------------------- 同步控制
    @app.post("/api/sync/full")
    def start_full_sync():
        try:
            with gate.read("start-full"):
                started = target.request_full_sync(config.TARGET_DB)
        except (OperationPaused, StorageUnavailable) as exc:
            return _paused_response(exc)
        if started:
            return jsonify({"ok": True, "message": "首次全量同步已启动"}), 202
        st = target.get_state(config.TARGET_DB)
        return jsonify({"ok": False,
                        "message": f"当前状态 {st['phase']} 不允许发起全量"
                                   "（首次全量只允许一次，之后为增量同步）"}), 409

    # ------------------------------------------------------------- 源端操作
    @app.post("/api/source/correct")
    def correct():
        body = request.get_json(silent=True) or {}
        try:
            with gate.read("correct"):
                if body.get("sample_no"):
                    tx_id = source.apply_correction(
                        config.SOURCE_DB,
                        sample_no=body["sample_no"],
                        new_site=body.get("site"),
                        adds=body.get("adds") or [],
                        dels=body.get("dels") or [],
                        note=body.get("note") or "manual correction",
                    )
                    return jsonify({"ok": True, "tx_id": tx_id})
                detail = source.random_correction(config.SOURCE_DB,
                                                  note="manual correction")
                return jsonify({"ok": True, **detail})
        except (OperationPaused, StorageUnavailable) as exc:
            return _paused_response(exc)
        except ValueError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400

    @app.post("/api/source/auto")
    def toggle_auto():
        body = request.get_json(silent=True) or {}
        if body.get("enabled"):
            auto.enabled.set()
        else:
            auto.enabled.clear()
        return jsonify({"ok": True, "auto_correct": auto.enabled.is_set()})

    # ------------------------------------------------------------- 故障注入
    @app.post("/api/fault/inject")
    def inject_fault():
        injector.arm(1)
        return jsonify({"ok": True,
                        "message": "已武装：下一次目标写入将失败一次（随后自动重试）"})

    # ------------------------------------------------------------- 查询
    @app.get("/api/query")
    def query():
        sample_no = (request.args.get("sample_no") or "").strip()
        if not sample_no:
            return jsonify({"ok": False, "message": "请提供样品号"}), 400
        try:
            with gate.read("query"):
                st = target.get_state(config.TARGET_DB)
                if st["phase"] != target.PHASE_READY:
                    return jsonify({"ok": False,
                                    "message": "首次全量同步未完成，关联明细查询暂未开放"}), 409
                detail = target.query_sample(config.TARGET_DB, sample_no)
        except (OperationPaused, StorageUnavailable) as exc:
            return _paused_response(exc)
        if detail is None:
            return jsonify({"ok": False,
                            "message": f"分析库中不存在样品 {sample_no}"}), 404
        return jsonify({"ok": True, "sample": detail})

    @app.get("/api/source/txs")
    def source_txs():
        try:
            with gate.read("source-txs"):
                txs = source.recent_txs(config.SOURCE_DB, limit=20)
        except (OperationPaused, StorageUnavailable) as exc:
            return _paused_response(exc)
        return jsonify({"txs": txs})

    @app.get("/api/target/samples")
    def target_samples():
        try:
            with gate.read("target-samples"):
                samples = target.list_samples(config.TARGET_DB)
        except (OperationPaused, StorageUnavailable) as exc:
            return _paused_response(exc)
        return jsonify({"samples": samples})

    # ------------------------------------------------------------- 停机备份恢复
    @app.get("/api/restore/status")
    def restore_status():
        return jsonify(recovery.status())

    @app.post("/api/restore/upload")
    def restore_upload():
        src_file = request.files.get("source_db")
        tgt_file = request.files.get("target_db")
        if src_file is None or tgt_file is None:
            return jsonify({"ok": False,
                            "message": "必须同时上传 source_db 与 target_db 两个文件"}), 400
        try:
            state = recovery.stage_upload(
                src_file.stream, tgt_file.stream,
                request.files.get("restore_json").stream
                if request.files.get("restore_json") else None,
            )
        except RestoreRejected as exc:
            return jsonify({"ok": False, "rejected": True,
                            "errors": exc.errors,
                            "message": "备份被拒绝，原库未改动"}), 422
        except RestoreConflict as exc:
            return jsonify({"ok": False, "message": str(exc)}), 409
        except Exception as exc:  # 损坏/超限等
            log.exception("上传校验异常")
            return jsonify({"ok": False, "rejected": True,
                            "errors": [f"{type(exc).__name__}: {exc}"],
                            "message": "备份校验失败，原库未改动"}), 422
        return jsonify({"ok": True, "state": state,
                        "message": "校验通过，请核对水位后确认替换"}), 201

    @app.post("/api/restore/confirm")
    def restore_confirm():
        try:
            result = recovery.confirm()
        except RestoreConflict as exc:
            return jsonify({"ok": False, "message": str(exc)}), 409
        except Exception as exc:
            log.exception("恢复确认异常")
            return jsonify({"ok": False, "message": f"恢复失败：{exc}"}), 500
        return jsonify({"ok": True, "result": result,
                        "message": "两份备份已整体启用，将按备份进度增量接续"})

    @app.post("/api/restore/cancel")
    def restore_cancel():
        try:
            state = recovery.cancel()
        except RestoreConflict as exc:
            return jsonify({"ok": False, "message": str(exc)}), 409
        return jsonify({"ok": True, "state": state, "message": "候选备份已丢弃"})

    return app
