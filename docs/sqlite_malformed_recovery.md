# SQLite `database disk image is malformed` 排障与恢复

## 现象

页面提示“部分模块加载失败，页面已降级展示”，并且多个评分规则同时报：

- `评分规则13 rule13：database disk image is malformed`
- `评分规则14 rule14：database disk image is malformed`
- `评分规则15 rule15：database disk image is malformed`
- `评分规则16 rule16：database disk image is malformed`

## 判断

这不是单条评分规则的业务逻辑错误，而是 SQLite 文件或 WAL/SHM sidecar 已损坏。规则 13-16 都会读取 15m/1h K 线与成交量窗口；如果它们同时失败，优先怀疑底层数据库文件损坏，而不是四条规则同时写错。

## 常见原因

1. 容器或宿主机异常断电、强制 kill，导致 SQLite 主库与 `-wal`/`-shm` 未正常收敛。
2. 多进程同时做 schema 初始化或大量写入时被中断。
3. 数据卷磁盘空间不足、I/O 错误，或宿主机文件系统异常。
4. SQLite 文件被外部工具以非 WAL 兼容方式复制、截断或覆盖。

## 已有隔离策略

系统已经将基础数据、评分、交易、市场行情拆成多个 SQLite 文件，避免单个模块库损坏拖垮整页。Web 页面加载单个模块失败时会降级展示其余模块。

当 Web 页面或 worker 捕获到 `database disk image is malformed` 或 `file is not a database` 时，会先为故障库创建 recovery marker。所有主库连接和 `ATTACH` 连接都要持有跨进程共享访问锁；marker 出现后新业务连接会被拒绝。worker 再等待已有连接关闭、取得独占锁，将主库与 WAL/SHM 隔离，重建 schema 并通过 `quick_check` 后才移除 marker。这样既可在线自动恢复，又不会让旧 inode 和新数据库同时被业务访问。

## 本次系统化排查结论

目前代码层面最高风险不是 `sqlite-web`，而是旧版“自动恢复”：worker 每 30 分钟巡检、业务异常处理器及 Web 请求异常处理器都可能在其他线程或容器仍持有连接时，把主库和 sidecar 逐个重命名。这三个文件的移动也不是一个原子操作。一天多次、且停止 `sqlite-web` 后仍出现，和这个并发窗口高度吻合。

评分库改为批量写后稳定、而基础库开始暴露问题，并不表示损坏从一个文件“转移”到了另一个文件。两者的写入模型此前并不对称：评分轮次已将每个 symbol、每条规则各自连接/提交改为每 20 个 symbol 一次提交；基础库的指标处理仍对每个 symbol 分别提交 MA20、EMA 和 MACD。若 universe 有 N 个 symbol，15 分钟边界仅指标链路就会产生最多 `3N` 次连接与 WAL 提交，并与 K 线聚合、持仓量、ATR、资金费率写入叠加。`db_write_lock` 只覆盖 collector 模块内的部分写入，不能串行化独立的 data-processor 线程，更不能跨容器保护 `sqlite-web`。

因此本次将 processor 的一个 interval 结果合并为单连接、单事务、三次 `executemany`。这会显著减少 WAL 提交、fsync 和 checkpoint 竞争，是与评分库优化效果相同的第一优先级降载措施；但 SQLite 的正常并发只应造成 `locked/busy`，不应造成 `malformed`。如果降载后仍损坏，必须继续按下述宿主机证据链排查，不能把高并发直接当作数据页损坏的根因。

建议在下一次故障时保留并对齐以下时间线：

1. 应用日志中首次 `malformed`（不要使用随后大量级联报错的时间）。
2. 同一分钟的 `dmesg -T`、`journalctl -k`、OOM 与容器 restart/exit code。
3. `df -h`、`df -i`、数据目录文件系统类型和 mount 参数。
4. 故障前后的 `.db`、`-wal`、`-shm` 大小、mtime、inode，以及恢复程序生成的 quarantine 文件名。
5. 所有挂载该目录的容器、备份/同步任务和打开数据库的进程。

本次防护调整包括：

1. 运行时巡检会先 fence 故障库，等待其他连接退出后再自动隔离和重建。
2. Web 遇到 malformed 时立即写入 marker，后续请求不再访问故障库，由 worker 在 30 秒巡检中恢复。
3. WAL 连接从 `synchronous=NORMAL` 提升为 `synchronous=FULL`，降低宿主机掉电、内核崩溃或存储写缓存失序时的风险。

仍需从服务器侧确认以下外因；SQLite 自身正常并发不会频繁产生页面损坏：

- `data` 是否位于 NFS/CIFS、云盘 FUSE、Docker overlay，而不是本地可靠文件系统；WAL 要求所有进程共享可靠的文件锁与共享内存语义。
- 宿主机是否有 OOM kill、强制重启、磁盘满、inode 用尽、I/O error、文件系统报错或底层盘健康异常。
- 是否有备份/同步/杀毒/运维脚本直接复制、覆盖、截断 `.db`，或遗漏同一时刻的 `-wal`/`-shm`。
- worker 和 web 是否确实挂载同一个宿主目录，以及是否存在旧容器仍在写相同路径。

## 手工恢复建议

1. 先备份当前 `data/` 目录。
2. 停止所有会访问该目录的容器后，再将主库、`-wal`、`-shm` 作为一组复制留证；不要只移动其中一个文件。
3. 如果基础 K 线库损坏，重启采集任务让系统重新补齐 K 线与指标。
4. 如果评分库损坏，重启评分任务，让下一轮评分重新生成规则明细与总分。
5. 若需要抢救历史数据，可在离线环境对 `.corrupt-*` 文件尝试 `.recover`，不要直接覆盖线上库。

## 预防措施

- 保证宿主机磁盘空间充足，并监控 I/O 错误。
- 停机维护前优雅停止容器，避免直接 kill 写库进程。
- 备份 SQLite 时同时处理主库与 `-wal`/`-shm`，或先 checkpoint 后再复制。
- 对核心数据库开启定期 `PRAGMA quick_check` 巡检，并告警异常结果。
