# `trading.db` 拆分后专项审计（2026-08-01）

## 范围与结论

本报告审计当前仓库中的连接路由、表归属、并发写入、健康检查、自动恢复和容器配置。仓库没有生产数据库、故障日志、宿主机内核日志或损坏文件，因此**不能仅凭代码认定上次损坏的唯一根因**。可以确认的是：拆分本身只是把少数表改写到新文件，不会按 SQLite 的正常行为损坏旧文件；若日志确实是 `database disk image is malformed`，仍应优先调查存储、非一致备份/覆盖、异常终止或旧版本恢复程序。

当前风险结论如下：

| 等级 | 结论 | 状态 |
| --- | --- | --- |
| P0 | 没有生产故障文件与宿主机证据，根因尚未闭环 | 需现场采证 |
| P1 | 拆分没有数据迁移步骤；旧库中的核心历史表不会自动复制到 `trading_core.db` | 未解决，部署前需人工迁移/核验 |
| P1 | 健康巡检过去会把 `locked`/`busy` 当成损坏并隔离整个库 | 本次已修复为延后巡检 |
| P1 | `trading.db` 仍承载多数高频风控明细，并非“轻量剩余库” | 需容量与保留策略 |
| P2 | Compose 和 `.env.example` 过去没有显式传递 `TRADING_CORE_DB_PATH` | 本次已补齐 |
| P2 | 运行期反复调用 `init_tables()`，造成不必要的 schema lock 与元数据查询 | 建议后续改为每进程一次初始化 |
| P2 | 自动恢复按整库隔离；`trading.db` 损坏会同时丢失多个独立风控模块的在线状态 | 需备份、告警和恢复优先级设计 |

## 1. 拆分实际上拆走了什么

生产路径相等时，`trading_core_path()` 才把访问路由到 `TRADING_CORE_DB_PATH`；测试或自定义路径仍将所有表放在同一个文件。当前迁往核心库的是：

- `trading_experiment_trades`
- `trading_experiment_position_snapshots`
- `zombie_force_liquidation_checks`
- `zombie_force_liquidation_records`

`trading_experiment_error_records` 明确保留在 `trading.db`。此外，持仓评分、保本止损、分批止盈、动态利润保护、移动止盈、移动减仓、动态加仓阈值和交易动作锁等表仍写入 `trading.db`。因此这次拆分减少了部分核心交易写入，但没有把交易域完整迁走，也不能据此预期 `trading.db` 不再发生写竞争或快速增长。

### 关键部署缺口：没有历史数据迁移

初始化逻辑只执行 `CREATE TABLE IF NOT EXISTS` 和列迁移，没有从旧 `trading.db` 向新 `trading_core.db` 执行 `INSERT ... SELECT`、备份或校验。升级已有环境后，新核心库会被创建为空库，而旧核心表仍留在旧文件中成为孤立历史数据。直接上线前至少应停写并核对：

```sql
ATTACH DATABASE 'data/trading_core.db' AS core;
SELECT 'old_trades', count(*) FROM main.trading_experiment_trades;
SELECT 'new_trades', count(*) FROM core.trading_experiment_trades;
SELECT 'old_positions', count(*) FROM main.trading_experiment_position_snapshots;
SELECT 'new_positions', count(*) FROM core.trading_experiment_position_snapshots;
```

若旧表有数据而新表为空，应在停掉 worker、web 和所有外部 SQLite 工具并完成整组备份后，使用显式列清单迁移；不要使用未经核对的 `SELECT *`。迁移完成后分别执行 `PRAGMA integrity_check`，并校验行数、主键范围及未平仓记录。当前仓库没有足以安全自动判定“首次迁移还是已迁移”的版本表，所以不建议运行期静默搬运。

## 2. 已确认的代码风险

### 2.1 巡检可能误删健康但繁忙的库（本次已修复）

`quick_check_sqlite_database()` 将 SQLite 异常转换成 `(False, detail)`。旧的 worker 巡检对所有 `False` 都创建 fence，并隔离主库及 WAL/SHM；因此一次超过 30 秒的锁等待也可能触发健康库替换。这不产生最初的 `malformed`，但会把普通拥塞升级为整库丢失，并容易被误判为“数据库崩溃”。本次修改明确跳过 `database is locked` 和 `database is busy`，保留数据库供下轮检查。

仍建议后续把巡检结果分为 `healthy / corrupt / transient / io_error`，只有明确的完整性错误才允许自动隔离；I/O 错误、权限错误和磁盘满应告警并停止自动替换。

### 2.2 表初始化位于热路径

多个 `run`、`summary`、刷新和锁获取方法会再次调用 `init_tables()`。虽然 schema file lock 避免并发 `ALTER`，但它不能降低调用频率；每次仍会串行获取文件锁、连接数据库、查询 `PRAGMA table_info` 并执行 `CREATE IF NOT EXISTS`。这通常导致延迟或 `busy`，不是页面损坏的直接原因，但会放大交易轮次和 Web 请求之间的竞争。

建议将 schema 初始化移到进程启动阶段，用 schema version 做幂等迁移；热路径只做数据事务。需要保留“数据库被恢复后重建 schema”的能力时，可由恢复器显式重置进程内初始化状态。

### 2.3 `trading.db` 的故障域仍然过大

当前库同时保存多类检查明细、执行记录、持仓风险摘要和互斥锁。自动恢复以文件为粒度，任一页面损坏都会隔离整个文件并创建空 schema。这意味着一个非关键历史检查表的问题，会连带清空交易动作锁、止损/止盈生命周期证据及其他模块状态。对交易系统而言，恢复后的“库可写”不等于“业务状态已恢复”。

建议按以下顺序治理：

1. 为执行记录和在线状态定义 RPO/RTO；恢复后先从交易所重新对账，再允许新交易动作。
2. 对只追加的检查明细设置按时间归档/删除和 `VACUUM` 维护窗口，监控页数、WAL 大小和磁盘余量。
3. 将可重算的观察明细与不可重算的订单生命周期/锁状态进一步分库。
4. 自动恢复时发出强告警并记录隔离文件、首次错误、旧文件 inode 和校验结果，不能只打印成功消息。

## 3. 已有防护及其边界

- 所有受管连接使用 30 秒 busy timeout、WAL、`synchronous=FULL` 和自动 checkpoint；这些设置增强掉电安全并缓解竞争，但不能修复底层 I/O 或错误备份。
- access lock 与 recovery marker 能阻止新受管连接，并等待既有受管连接关闭后再换库。外部程序或任何绕开 `connect_sqlite()` 的生产代码不受该协议保护。
- 健康线程每五分钟执行 `quick_check`；它能较快发现损坏，但仅检查当时可读到的数据库，恢复后新库的 `ok` 不能证明旧文件曾经健康。
- 拆分后诊断工具会从 `DB_LABELS` 检查五个库；Compose 现在也显式向所有服务传入核心库路径，避免自定义路径部署时 worker/web 对文件位置理解不一致。

## 4. 下次故障的证据链

首次错误出现后、重启或自动恢复前，立即执行：

```bash
python sqlite_diagnostics.py --output "logs/sqlite-diagnostic-$(date -u +%Y%m%dT%H%M%SZ).json"
docker inspect trade trade-web sqlite-web
docker ps -a --no-trunc
df -hT "${HOST_DATA_DIR:-./data}" && df -i "${HOST_DATA_DIR:-./data}"
findmnt -T "${HOST_DATA_DIR:-./data}" -o TARGET,SOURCE,FSTYPE,OPTIONS
dmesg -T | tail -n 300
journalctl -k --since '-2 hours'
```

同时保存 `trading.db*` 和 `trading_core.db*` 的主文件、`-wal`、`-shm`、`.recovering`、大小、mtime 与 inode。重点对齐首次错误前后：容器重启/OOM、磁盘或 inode 耗尽、I/O/文件系统错误、备份/同步任务、人工 SQLite 工具，以及是否存在多个部署实例挂载同一目录。

判读原则：

- `locked`/`busy` 是并发拥塞，不是损坏证据。
- `database disk image is malformed`、`file is not a database` 或 `integrity_check` 页/B-tree 错误才是内容损坏证据。
- 只有 `trading.db` 损坏而同目录其他库健康，会降低“整个磁盘同时故障”的概率，但不能排除局部坏块、只覆盖该文件的备份任务、该库独有的高写入量，或旧恢复逻辑只替换该库。
- 事件恰好发生在拆分后只说明时间相关性；必须用部署版本、文件 inode/mtime、首次异常和 quarantine 时间证明因果。

## 5. 建议验收清单

1. 确认 worker 与 web 中五个数据库环境变量完全一致，并都落在同一本地可靠挂载中。
2. 在停写快照上对五库运行 `PRAGMA integrity_check`，记录文件哈希和行数基线。
3. 核验旧核心表迁移前后行数、未平仓交易和最新 position snapshot。
4. 压测长写事务时，健康巡检只能延后，不能生成 `.corrupt-*`。
5. 模拟核心库和普通交易库分别损坏，确认只恢复目标文件，且恢复后会先进行交易所状态对账。
6. 增加监控：数据库/WAL 大小、磁盘与 inode、`busy` 次数、quick-check 分类、quarantine 次数、容器退出码和 OOM。
