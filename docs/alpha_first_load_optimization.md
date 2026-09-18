# Alpha 币观测页：首次加载优化评估

## 结论

当前首次请求不是“表格太多”本身造成的，主要瓶颈是服务端在渲染 HTML **之前**同步计算三个分析模块。建议优先采用“首屏只返回快照 + 其余模块懒加载”的方案，改动小、风险低，而且不会改变已有筛选语义。

## 已确认的耗时路径

`/market/alpha` 每次非 AJAX 请求都会执行：

1. `latest_snapshot` 读取 Alpha 数据库；
2. `daily_trends`：逐 token 查询最近 30 根日线；
3. `_alpha_oi_changes`：对每个 token 多次查询 OI、历史 OI 和 1h K 线；
4. `_alpha_funding_changes`：对每个 token 查询 1h 资金费率和 4h K 线；
5. 最后才渲染包含四张表的完整 HTML。

其中 OI 和 funding 都是典型的 N+1 查询；token 数量增加时，数据库往返次数近似线性增长。模板虽然只显示前 10 行，但后端仍计算并传输了完整结果，因此“折叠列表”不能降低首次响应时间。

## 性价比排序

### P0（建议先做）

* **懒加载三个非首屏模块**：初始请求只取 `latest_snapshot` 和数据库状态；浏览器显示快照表后，再并行请求 `/market/alpha/data?module=oi|funding|trend`，将结果插入对应面板。保留带筛选参数的独立请求，避免破坏分享链接和无 JS 降级。
* **首屏限制传输量**：服务端默认只返回快照前 10 行（或分页），点击“展开全部”再取完整列表。即使暂时不做懒加载，也应避免把数百行 HTML 发送到浏览器。

### P1（后端查询优化）

* 将 OI/funding 的逐 symbol 查询改成批量 SQL（`symbol IN (...)` + 窗口函数或聚合），把每个模块的查询次数从 `O(symbols)` 降到常数级。
* 确认 `open_interest_1m(symbol, snapshot_time)`、`klines_1h(symbol, open_time)`、`klines_4h(symbol, open_time)` 均有联合索引；OI 表已有索引时，不要重复创建。
* 对结果增加 30–60 秒进程内缓存，并以最新 Alpha snapshot 时间作为 cache key。这样刷新和多个用户访问不会重复扫描历史数据；缓存失效不影响页面正确性。

### P2（体验与基础设施）

* 用 `Server-Timing` 记录 snapshot/trend/OI/funding/render 各阶段耗时，配合浏览器 Network 面板建立基线（TTFB、响应字节数、LCP）。
* 对数据接口启用 gzip/br；表格接口返回 JSON，避免重复传输 HTML 标签。
* 数据量继续增长后，再考虑定时任务预计算 OI/funding/trend 结果并写入 Alpha DB；不要在没有基线前引入复杂缓存或后台任务。

## 推荐实施顺序

1. 先加耗时日志和响应大小基线。
2. 实施 P0 懒加载，确认首屏 TTFB 和 LCP 改善。
3. 批量化 OI/funding 查询并补齐索引，比较数据库查询次数。
4. 最后增加短 TTL 缓存和压缩；以数据新鲜度指标验证缓存 TTL（建议不超过一个采集周期）。

## 验收指标

在相同数据库和网络条件下，对比改造前后：

* 首字节时间（TTFB）和 Largest Contentful Paint（LCP）至少下降 50%；
* 初始 HTML 字节数下降 60% 以上；
* 首次请求只执行快照查询，不触发 OI/funding/trend 查询；
* 切换模块、筛选和无 JavaScript 访问仍返回与当前页面一致的数据；
* 数据库查询错误时，快照仍可展示，分析模块独立显示降级提示。

这份评估不建议直接删除任何模块或降低数据精度；优先把非关键计算移出关键渲染路径，收益最大且回滚简单。
