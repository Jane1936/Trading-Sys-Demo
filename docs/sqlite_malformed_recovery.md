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

当 Web 页面捕获到 `database disk image is malformed` 或 `file is not a database` 时，会对所有模块数据库执行 `PRAGMA quick_check`，定位损坏的库，并将损坏库及其 `-wal`/`-shm` 文件重命名为 `.corrupt-<UTC时间戳>` 后缀。随后刷新页面时，模块会重新初始化空库或等待采集任务补数。

## 手工恢复建议

1. 先备份当前 `data/` 目录。
2. 查看是否已生成 `.corrupt-*` 文件，保留这些文件用于离线恢复。
3. 如果基础 K 线库损坏，重启采集任务让系统重新补齐 K 线与指标。
4. 如果评分库损坏，重启评分任务，让下一轮评分重新生成规则明细与总分。
5. 若需要抢救历史数据，可在离线环境对 `.corrupt-*` 文件尝试 `.recover`，不要直接覆盖线上库。

## 预防措施

- 保证宿主机磁盘空间充足，并监控 I/O 错误。
- 停机维护前优雅停止容器，避免直接 kill 写库进程。
- 备份 SQLite 时同时处理主库与 `-wal`/`-shm`，或先 checkpoint 后再复制。
- 对核心数据库开启定期 `PRAGMA quick_check` 巡检，并告警异常结果。
