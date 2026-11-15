# 地质化验数据同步平台（作业库 → 分析库）

Flask + SQLite 演示系统：把作业库中的样品与化验结果持续、可靠地装入分析库。
两库为**相互独立的 SQLite 文件**，通过"变更日志 + 有序重放"实现
**首次全量 + 持续增量**同步，满足事务级一致与断点续传。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 浏览器打开 http://localhost:5000
```

- 两库分别存放在两个独立卷：`source-data`（作业库）、`target-data`（分析库）。
- 验证断点续传：执行若干更正后 `docker compose restart`，页面可见
  `last_tx_id` 与数据保持不变，新更正继续增量同步，**不会清库或重做全量**。
- 停机恢复演示：打开 <http://localhost:5000/restore>，上传
  `samples/backup-20261115/` 下的 `source.db`、`target.db`、`restore.json`。

### 本地运行

```bash
pip install -r requirements.txt
python -m app            # http://localhost:5000 ，数据在 ./data/
```

### 端到端自测

```bash
pip install requests
python scripts/selftest.py
```

自测会真实启动服务进程，覆盖：查询门禁、全量期间源端并发更正、
增量增删改、删除不复活、故障注入（失败不推进/重试不重复）、
进程重启接续、禁止重复全量、自动更正持续装入，并在各阶段做
**源/目标两库逐行精确对账**。

## 演示路径（页面操作）

1. 打开首页，初始为"待发起全量"，关联明细查询返回 409（未开放）。
2. 点击 **① 发起首次全量同步** → 完成后进入"增量同步中"，查询开放。
3. 点击 **② 模拟一笔更正事务**：源端在一笔事务内改样品采样点并增删多条结果；
   约 1 秒内分析库可见，且两库对账标识保持"一致"。
4. **自动更正**开关：源端每 2.5 秒产生一笔更正，演示持续装入。
5. **注入下一次目标写入失败**：观察 `last_tx_id` 停滞、异常信息出现，
   随后自动重试成功——进度不丢、数据不重。

## 一致性设计要点

| 需求 | 实现 |
| --- | --- |
| 一笔事务更正样品并增删多条结果 | 源端 `apply_correction`：业务写入与变更日志（`tx_log`/`tx_ops`）同一 SQLite 事务提交 |
| 按源端提交顺序同步 | `tx_id` 为 rowid（非 AUTOINCREMENT），SQLite 单写者串行 ⇒ rowid 序即提交序；增量严格按 `tx_id` 升序重放 |
| 全量期间源端更正仍能完成 | 源库 WAL 模式，快照读事务不阻塞写事务 |
| 切换增量不漏更正 | 全量快照与 `high_water=MAX(tx_id)` 取自**同一读事务**；装载后先追平 `tx_id > high_water` 再开放查询 |
| 不重复结果 | 目标表主键约束 + `INSERT OR REPLACE`；`tx_id <= last_tx_id` 的事务跳过 |
| 删除记录不复活 | 删除作为日志操作整笔重放；目标永不出现源端不存在的记录（自测不变式断言） |
| 整笔生效、查询不见半笔 | 每笔源事务在分析库**一个事务**内应用全部操作并推进进度；查询在 `ready` 后开放 |
| 写入失败不推进进度 | 数据写入与 `last_tx_id` 更新同事务，失败整体回滚 |
| 重试不重复生效 | 幂等重放（upsert/delete + 已应用跳过），自测含重复重放验证 |
| 中断重启按持久进度接续 | `sync_state` 持久化于分析库；重启后 `ready` 直接续传；仅"首次全量从未成功"时才重做全量 |

## 停机备份恢复（管理员误装数据后的回退）

管理员误装数据后，可用**同一次停机保存的** `source.db`、`target.db` 整体替换
作业库与分析库。入口：首页右上角或 **`/restore`**。

1. 上传 `source.db`、`target.db`（可选 `restore.json` 清单文件，其中的校验和与水位
   会一并核对）。文件先落到暂存目录**只读校验，原库保持不变，期间业务照常可用**。
2. 页面展示两份备份的大小、SHA-256、样品/结果/事务数，以及**水位对比**：
   源端最新事务 `source_max_tx`、目标已应用 `last_tx_id`、快照水位 `high_water`、
   待接续事务数。
3. 核对无误点 **确认替换**。系统先暂停新更正/查询/同步，等待已受理操作结束后，
   在几十毫秒内整体换入两份备份，随后同步引擎按**备份的进度**增量接续
   （不清库、不重做全量）。旧世代的同步/更正执行者不会写入恢复后的库。

### 接收条件（任一不满足即拒绝，原库不变）

| 条件 | 说明 |
| --- | --- |
| 目标已完成首次全量 | `target.db` 的 `phase=ready`，否则无法从备份进度接续 |
| 目标水位不超过源水位 | `last_tx_id ≤ source_max_tx`，否则两库并非同次停机配对 |
| 待同步日志完整 | `tx_log` 为 1..max 连续、每笔事务的操作流水 `seq` 连续、无缺流水 |
| 结构兼容 | 两库必需表与字段齐全（缺表/缺字段即视为不兼容） |
| 内容同源（深校验） | 在目标副本上重放待同步事务后与源库逐行对账一致 |
| 文件完好 | 非空、SQLite 魔数正确、`PRAGMA integrity_check` 通过、外键/孤儿行检查通过 |

损坏文件、非 SQLite 文件、只上传一个库、超过大小上限、清单校验和不符等都会被拒。

### 写盘失败与切换途中中断

- 切换前先把**完整旧配对**备份到两库各自卷内，并写入 `meta.json` 提交标记；
  任何一步失败都会**整体回滚为完整旧配对**，状态记为 `failed`，可直接重试。
- 进程在切换途中被强杀：重启时自愈——
  - 两库都已是备份 ⇒ 补记为 `applied`；
  - 有完整旧配对 ⇒ 整体回滚为旧配对（`failed`，可重试）；
  - 否则核对当前两库确为一致配对后才放行，否则进入 `incomplete` 并暂停业务。
  任何情况下都不会出现“一旧一新”的混用配对。
- 恢复状态持久化在恢复页可查（`none/verified/switching/applied/failed/incomplete/cancelled`）。

### 示例备份（含待同步更正）

`samples/backup-20261115/` 提供一组可直接上传的示例：源端 8 笔事务、分析库已应用 6 笔，
**2 笔更正尚未同步**（`restore.json` 中列明）。上传后确认，即可看到分析库从
`last_tx_id=6` 接续到 8，并保持两库逐行一致。容器内该目录位于 `/srv/samples`。

生成/重建示例：`python scripts/make_sample_backup.py samples/backup-20261115`。

### 恢复自测

```bash
python scripts/selftest.py            # 原有同步功能回归
python scripts/restore_selftest.py    # 恢复功能端到端自测
```

恢复自测会真实启动服务，覆盖：校验期间业务可用（延迟校验下测量请求延迟）、
8 类不合格备份被拒且原库不变、正常恢复并增量接续、写盘失败回滚与重试、
`after_source_swap` 强杀后重启回滚、`after_target_swap` 强杀后补记 `applied`、
两库独立目录断言。

## 架构

```
浏览器 ──► Flask (app/web.py) ──┬── 作业库 source.db（app/source.py）
                                │     samples / results / tx_log / tx_ops
                                └── 分析库 target.db（app/target.py）
                                      samples / results / sync_state
            后台同步引擎 app/syncer.py：idle → full_running → ready
            恢复：app/recovery.py + 业务闸门 app/gate.py
```

- `app/source.py`：示例源。建库、播种、更正事务（手动/随机）、一致性快照、日志读取。
- `app/target.py`：分析库。进度状态机、全量装载（单事务）、整笔重放（幂等）。
- `app/syncer.py`：后台线程驱动全量→追平→增量；故障注入点；失败重试不推进。
- `app/web.py`：状态/操作/查询 API 与页面；自动更正发生器。
- `app/gate.py`：业务读写闸门。正常操作持共享读区段；恢复确认时取独占写区段，
  先暂停新操作、再等在途操作结束，然后才替换两库。
- `app/recovery.py`：恢复备份的上传、只读校验、深对账、原子切换与中断自愈。

## API 摘要

| 方法/路径 | 说明 |
| --- | --- |
| `GET /` | 演示页面 |
| `GET /restore` | 停机备份恢复页 |
| `GET /api/status` | 阶段、进度、对账、异常、存储闸门状态 |
| `POST /api/sync/full` | 发起首次全量（仅 `idle`，重复发起返回 409） |
| `POST /api/source/correct` | 源端更正事务（空 body 随机；可指定 `sample_no/site/adds/dels`） |
| `POST /api/source/auto` | 自动更正开关 `{enabled: bool}` |
| `POST /api/fault/inject` | 武装下一次目标写入失败 |
| `GET /api/query?sample_no=` | 关联明细查询（`ready` 前 409；恢复窗口内 503 可重试） |
| `GET /api/source/txs` / `GET /api/target/samples` | 源端事务日志 / 分析库样品一览 |
| `GET /api/restore/status` | 恢复状态、校验报告、在线水位、闸门状态 |
| `POST /api/restore/upload` | 上传 `source_db`、`target_db`、可选 `restore_json` 并校验 |
| `POST /api/restore/confirm` | 确认替换（暂停业务→等在途结束→整体换入→增量接续） |
| `POST /api/restore/cancel` | 丢弃候选备份 |

## 环境变量

`PORT`（默认 5000）、`DATA_DIR`（默认 `./data`）、`SOURCE_DB`/`TARGET_DB`（覆盖库文件路径）、
`SYNC_POLL_INTERVAL`（增量轮询秒，默认 0.5）、`AUTO_CORRECT_INTERVAL`（默认 2.5）、
`SEED_SAMPLES`（默认 20）、`SYNC_SNAPSHOT_DELAY_MS`（**测试钩子**，默认 0：
在全量快照后人为停顿，用于稳定复现"全量期间源端更正"的竞态窗口，生产保持 0）。

恢复相关：`RESTORE_DIR`（上传暂存目录，默认 `<DATA_DIR>/restore`）、
`RESTORE_MAX_BYTES`（单文件上限，默认 256 MiB）、`RESTORE_DRAIN_TIMEOUT`
（等待在途操作的上限秒数，默认 30）。测试钩子（生产保持默认）：
`RESTORE_VERIFY_DELAY_MS`（放大校验耗时）、`RESTORE_FAIL_AFTER`（在
`before_source_swap/after_source_swap/after_target_swap` 注入写盘失败）、
`RESTORE_PAUSE_AFTER` + `RESTORE_PAUSE_MS`（切换途中停顿以复现强杀）。

## 已知限制（演示级实现）

- 变更日志只增不减，未做归档/清理；数据规模为演示量级（全量装载为单事务批量写入）。
- 深校验上限 `deep_verify_max_txs`（默认 50000 笔待同步事务），超过时退化为结构校验。
- 同步引擎为 Web 进程内单线程；若改用 gunicorn 部署必须 `--workers 1`。
- 恢复切换假定两库目录位于同一台主机、各自卷；若两卷位于不同物理主机（非本地
  文件系统），`os.replace` 与 fsync 的原子性无法保证，演示实现不覆盖该部署。
- 演示源为本地 SQLite 文件直读；接入真实外部源时，把 `source.py` 的
  快照/日志读取替换为对应源端 API 即可，目标端重放逻辑不变。
