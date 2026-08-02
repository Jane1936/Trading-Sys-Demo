# `trading.db` 与 `trading_core.db` 连接数优化空间

本文只从“减少 SQLite 物理连接打开次数、缩短连接持有时间、降低跨库重复连接”角度审视现状。

## 当前已经具备的基础

- `db_config.connect_sqlite()` 支持在当前上下文内借用已打开连接；如果同一路径已经进入 `sqlite_connection_scope()`，后续 `connect_sqlite()` 返回 `BorrowedSQLiteConnection`，不会再次打开物理连接。
- `sqlite_connection_scope()` 适合包住一个逻辑轮次，让轮次内多个模块复用同一个数据库连接。
- `trading_core_path()` 已经把生产默认的 `trading.db` 核心表路由到 `trading_core.db`，同时保持自定义/测试数据库仍然单库运行。

## 本次新增的小优化

- 新增 `sqlite_connection_scopes()`，用于同时为多个数据库建立轮次级连接作用域。
- 该 helper 会按真实路径去重；当 `trading_core_path()` 在测试或自定义库场景回落到同一个 DB 文件时，只会打开一次物理连接。
- `app.py` 中持仓评分轮次改用这个 helper 包住 `trading.db` 与 `trading_core.db`，避免调用点手写重复嵌套 scope，也降低后续遗漏去重的概率。

## 进一步优化建议

1. **把高频轮次都显式纳入 scope**：优先检查 1 分钟/5 分钟任务中是否存在“同一轮次内多次调用模块方法、每个方法各开一次连接”的路径；能合并的用 `sqlite_connection_scope()` 或 `sqlite_connection_scopes()` 包住整轮。
2. **减少初始化期重复连接**：多个模块的 `init_tables()` 在启动时会独立打开连接。可设计一个启动期 schema 初始化编排器，把同库 DDL 合并到同一个 scope 中执行。
3. **跨库读尽量用主连接 ATTACH**：只读依赖库时优先 `attach_databases()`，避免为每个读取来源单独打开连接；但写入不同 DB 时仍建议保留各自连接和清晰事务边界。
4. **把“可重算高频流水”和“核心状态”继续拆清楚**：连接数优化不应牺牲故障隔离。若后续继续拆表，应同步补齐轮次级 scope，否则 DB 文件变多会带来更多连接打开成本。
5. **为关键任务加连接计数回归测试**：当前已有 scope 单元测试；建议继续覆盖 break-even、partial take-profit、holding scoring 等高频任务的一轮调用，防止重构后回退到每个方法单独开物理连接。

## 注意事项

- 不建议做全局长连接池。SQLite 连接跨线程复用风险高，且本项目已有恢复 fencing、WAL 初始化和访问锁逻辑；轮次级短生命周期复用更安全。
- `BorrowedSQLiteConnection.__exit__()` 会在每个嵌套单元结束时提交/回滚，因此 scope 主要优化连接打开次数，并不等价于把整个轮次强行合成一个大事务。
