#!/usr/bin/env python3
"""生成一组可直接用于恢复页的示例停机备份。

在临时目录中启动一份全新的作业库/分析库，完成首次全量后制造若干笔
源端更正（其中一部分尚未被分析库同步，形成“待同步更正”），然后在
“停机”时刻复制出 source.db / target.db，并随附 restore.json 清单。

用法：
    python scripts/make_sample_backup.py [输出目录]   # 默认 samples/backup-<时间>
"""
import json
import os
import random
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import source, target  # noqa: E402
from app.recovery import _sha256, _copy_db  # noqa: E402


def _build(src_path: str, tgt_path: str):
    source.init_db(src_path)
    source.seed_if_empty(src_path, 20)
    target.init_db(tgt_path)

    # 全量前先产生 2 笔更正，使全量快照水位 high_water > 0
    for i in range(2):
        source.random_correction(src_path, rng=random.Random(6100 + i),
                                 note=f"全量前更正 #{i + 1}")

    # 首次全量：快照 + 装载 + 追平 + ready（与 syncer 相同的顺序）
    high, samples, results = source.read_snapshot(src_path)
    target.request_full_sync(tgt_path)
    target.full_load(tgt_path, high, samples, results)
    for tx in source.read_txs_after(src_path, target.get_state(tgt_path)["last_tx_id"]):
        target.apply_tx(tgt_path, tx)
    target.mark_ready(tgt_path)


def _pending_corrections(src_path: str, tgt_path: str, n: int):
    """制造 n 笔停机前更正，但只把前 n-2 笔装入分析库，留下 2 笔待同步。"""
    made = []
    for i in range(n):
        made.append(source.random_correction(
            src_path, rng=random.Random(7000 + i), note=f"停机前更正 #{i + 1}"))
    keep_pending = {x["tx_id"] for x in made[-2:]}
    for tx in source.read_txs_after(src_path,
                                    target.get_state(tgt_path)["last_tx_id"]):
        if tx["tx_id"] in keep_pending:
            continue
        target.apply_tx(tgt_path, tx)
    return made


def build(output_dir: str) -> dict:
    work = tempfile.mkdtemp(prefix="geosync-sample-")
    try:
        src_work = os.path.join(work, "source.db")
        tgt_work = os.path.join(work, "target.db")
        _build(src_work, tgt_work)
        made = _pending_corrections(src_work, tgt_work, 6)

        os.makedirs(output_dir, exist_ok=True)
        src_out = os.path.join(output_dir, "source.db")
        tgt_out = os.path.join(output_dir, "target.db")
        _copy_db(src_work, src_out)
        _copy_db(tgt_work, tgt_out)

        st = target.get_state(tgt_out)
        max_tx = source.max_tx(src_out)
        sc = source.counts(src_out)
        tc = target.counts(tgt_out)
        manifest = {
            "kind": "geosync-offline-backup",
            "created_at": "2026-11-15T02:30:00+08:00",
            "note": "演示用停机备份：作业库与分析库同一次停机保全；含待同步更正",
            "files": {
                "source": {"name": "source.db", "sha256": _sha256(src_out),
                           "size": os.path.getsize(src_out)},
                "target": {"name": "target.db", "sha256": _sha256(tgt_out),
                           "size": os.path.getsize(tgt_out)},
            },
            "watermarks": {
                "source_max_tx": max_tx,
                "target_last_tx_id": st["last_tx_id"],
                "target_high_water": st["high_water"],
            },
            "counts": {"source": sc, "target": tc},
            "pending_corrections": [
                {"tx_id": x["tx_id"], "sample_no": x["sample_no"],
                 "new_site": x["new_site"], "adds": x["adds"], "dels": x["dels"]}
                for x in made if x["tx_id"] > st["last_tx_id"]
            ],
        }
        with open(os.path.join(output_dir, "restore.json"), "w",
                  encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        return manifest
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        ROOT, "samples", "backup-20261115")
    m = build(out)
    print(f"已生成示例备份到 {out}")
    print(f"  源端最新 tx = {m['watermarks']['source_max_tx']}, "
          f"目标已应用 = {m['watermarks']['target_last_tx_id']}, "
          f"待同步 = {m['watermarks']['source_max_tx'] - m['watermarks']['target_last_tx_id']} 笔")
