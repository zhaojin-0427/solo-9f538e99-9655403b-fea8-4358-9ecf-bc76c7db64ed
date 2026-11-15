"""集中配置：路径与运行参数均可通过环境变量覆盖，便于容器化与测试。"""
import os

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(_BASE, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

# 两库独立存放：作业库（源）与分析库（目标）是两个互不相干的 SQLite 文件。
SOURCE_DB = os.environ.get("SOURCE_DB", os.path.join(DATA_DIR, "source.db"))
TARGET_DB = os.environ.get("TARGET_DB", os.path.join(DATA_DIR, "target.db"))
for _p in (SOURCE_DB, TARGET_DB):
    os.makedirs(os.path.dirname(os.path.abspath(_p)), exist_ok=True)

PORT = int(os.environ.get("PORT", "5000"))

# 增量轮询间隔（秒）
POLL_INTERVAL = float(os.environ.get("SYNC_POLL_INTERVAL", "0.5"))

# 自动更正发生器间隔（秒）
AUTO_CORRECT_INTERVAL = float(os.environ.get("AUTO_CORRECT_INTERVAL", "2.5"))

# 示例源初始样品数
SEED_SAMPLES = int(os.environ.get("SEED_SAMPLES", "20"))

# 测试钩子：全量同步在“读取一致性快照之后、写入分析库之前”人为停顿的毫秒数，
# 用于在自测中稳定复现“全量期间源端发生更正”的竞态窗口。生产环境保持 0。
SNAPSHOT_DELAY_MS = int(os.environ.get("SYNC_SNAPSHOT_DELAY_MS", "0"))

# ---------------------------------------------------------------- 停机恢复
# 上传的恢复备份与切换中间状态存放在 DATA_DIR 下的独立子目录（不放进两库卷）。
RESTORE_DIR = os.environ.get("RESTORE_DIR", os.path.join(DATA_DIR, "restore"))
os.makedirs(RESTORE_DIR, exist_ok=True)

# 单次上传的库文件上限（字节），默认 256 MiB。
RESTORE_MAX_BYTES = int(os.environ.get("RESTORE_MAX_BYTES", str(256 * 1024 * 1024)))

# 等待已受理操作结束的最长秒数；超时则放弃切换并恢复原库继续服务。
RESTORE_DRAIN_TIMEOUT = float(os.environ.get("RESTORE_DRAIN_TIMEOUT", "30"))

# 测试钩子：在校验阶段人为停顿的毫秒数，用于自测“校验期间原系统可用”
# （业务请求不应被校验阻塞）。生产保持 0。
RESTORE_VERIFY_DELAY_MS = int(os.environ.get("RESTORE_VERIFY_DELAY_MS", "0"))

# 测试钩子：切换过程中在指定步骤之后人为注入一次写盘失败（如
# "after_source_swap"），用于验证“切换失败整体回滚完整旧配对”。生产为空。
RESTORE_FAIL_AFTER = os.environ.get("RESTORE_FAIL_AFTER", "")

# 测试钩子：在指定步骤之后停顿的毫秒数（如 "after_source_swap"），
# 用于自测稳定复现“两库切换途中中断”。生产为空。
RESTORE_PAUSE_AFTER = os.environ.get("RESTORE_PAUSE_AFTER", "")
RESTORE_PAUSE_MS = int(os.environ.get("RESTORE_PAUSE_MS", "0"))
