# Docker 云服务器部署指南

你当前的部署目标是 3 个容器：
- `trade`：运行项目主进程（`worker`）
- `trade-web`：运行交易数据 Web 平台（`web`）
- `sqlite-web`：运行 SQLite 可视化 Web

下面给你两种方案：**推荐用 Docker Compose（一条命令管理 3 个容器）**；也保留优化后的 `docker run` 版本。

## 1) 拉取最新代码

```bash
git clone <你的仓库地址>
cd Trading-Sys-Demo
```

如果仓库已存在：

```bash
git pull --rebase
```

## 2) 目录准备

```bash
mkdir -p ./data ./logs
sudo chown -R "$(id -u):$(id -g)" ./data ./logs
sudo chmod -R u+rwX ./data ./logs
```

> 如果出现 `sqlite3.OperationalError: attempt to write a readonly database`，
> 基本都是宿主机挂载目录权限导致容器内 `appuser` 无法写入 `/app/data`。

---



## 3) 数据库配置（服务器没有旧库时直接用默认值）

项目现在会在挂载的 `./data` 目录下自动创建 4 个相互独立的 SQLite 数据库，不再依赖旧的 `klines.db`：

| 数据库 | 默认文件 | 对应模块 |
| --- | --- | --- |
| 基础数据库 | `data/base_data.db` | 数据收集、K线、OI、funding、MA/EMA/MACD、ATR 等基础行情数据 |
| 评分系统数据库 | `data/scoring.db` | 评分系统、异常插针记录、本轮可开仓 symbol、冷却 symbol、动态开仓门槛 |
| 交易数据库 | `data/trading.db` | 交易实验、持仓评分、账户/权益记录、止盈止损、移动追踪、僵尸强平 |
| 市场行情数据库 | `data/market.db` | 市场行情过滤模块 |

如果你的服务器已经删除了旧数据库文件，**不需要手动创建这些 `.db` 文件**；只要 `./data` 目录可写，`worker` 和 `web` 会在启动/首次访问时自动建表。

> 注意：如果服务器 shell、旧 `.env` 或云平台环境变量里还保留 `DB_PATH=/app/data/klines.db` / `data/klines.db`，请删除它，或明确设置下面 4 个新变量。`docker-compose.yml` 已默认注入 4 个新路径，不需要旧 `DB_PATH`。

推荐复制 `.env.example` 后按需修改：

```bash
cp .env.example .env
# 按需编辑 .env 里的 API key、HOST_DATA_DIR、HOST_LOGS_DIR
```

默认数据库路径如下，通常保持不变即可：

```bash
BASE_DB_PATH=data/base_data.db
SCORING_DB_PATH=data/scoring.db
TRADING_DB_PATH=data/trading.db
MARKET_DB_PATH=data/market.db
```

`sqlite-web` 默认打开基础数据库。如需查看其他库，启动前设置：

```bash
# 查看评分库
export SQLITE_WEB_DB_PATH=/data/scoring.db
# 查看交易库
# export SQLITE_WEB_DB_PATH=/data/trading.db
# 查看市场行情库
# export SQLITE_WEB_DB_PATH=/data/market.db
docker compose up -d sqlite-web
```

---

## 4) 评分权重配置

本项目的评分规则权重现在由仓库根目录的 `scoring_rule_weights.json` 管理，Docker 镜像构建时会把该文件复制到 `/app/scoring_rule_weights.json`。

本次新增的 `cooldown_module.py` 属于 Python 源码文件，`Dockerfile` 的 `COPY *.py ./` 会自动把它打进镜像；因此服务器升级时必须重新构建镜像，不能只重启旧容器。

如果你修改了评分权重或 Python 源码，请在服务器上重新构建并重启容器，让新镜像包含最新配置和代码：

```bash
docker compose up -d --build
```


## 5) Binance Demo/Testnet 账户配置

账户余额查询功能默认使用 Binance USDⓈ-M Futures Demo/Testnet：

```python
BINANCE_TESTNET = True

TESTNET_API_KEY = "YOUR_TESTNET_API_KEY"
TESTNET_SECRET_KEY = "YOUR_TESTNET_SECRET_KEY"
REAL_API_KEY = "YOUR_REAL_API_KEY"
REAL_API_SECRET = "YOUR_REAL_API_SECRET"

if BINANCE_TESTNET:
    BASE_URL = "https://demo-fapi.binance.com"
    API_KEY = TESTNET_API_KEY
    SECRET_KEY = TESTNET_SECRET_KEY
else:
    BASE_URL = "https://fapi.binance.com"
    API_KEY = REAL_API_KEY
    SECRET_KEY = REAL_API_SECRET
```

请不要把真实密钥提交到仓库。推荐在服务器或本地 shell 使用环境变量覆盖占位符：

```bash
export BINANCE_TESTNET=true
export BINANCE_TESTNET_API_KEY="你的 demo API key"
export BINANCE_TESTNET_SECRET_KEY="你的 demo secret key"
# 如切换实盘：
# export BINANCE_TESTNET=false
# export BINANCE_REAL_API_KEY="你的 real API key"
# export BINANCE_REAL_API_SECRET="你的 real secret key"
```

Docker Compose 启动前在同一个 shell 中导出这些变量即可；`docker-compose.yml` 会把这些变量同时注入 `worker` 和 `web` 容器：
- `worker` 容器会在每轮“本轮可开仓symbol情况”完成计算后立即执行第一组交易实验，因此必须能读取交易 API key/secret。
- `web` 容器会通过 `/api/account/balance` 在你点击网页“账户情况”的查询按钮时实时请求余额，也会复用这些变量。

---

## 方案 A（推荐）：Docker Compose

### 启动（构建并后台运行）

```bash
export PUID=$(id -u)
export PGID=$(id -g)
# 默认使用当前目录下的 ./data 和 ./logs；如需沿用 /root/trade，可取消下面两行注释：
# export HOST_DATA_DIR=/root/trade/data
# export HOST_LOGS_DIR=/root/trade/logs
docker compose up -d --build
```

`docker-compose.yml` 已支持：
- `user: "${PUID:-1000}:${PGID:-1000}"`：让 `worker/web` 进程直接使用宿主机当前用户 UID/GID，避免挂载数据目录时出现只读权限问题。
- `HOST_DATA_DIR` / `HOST_LOGS_DIR`：可切换宿主机挂载目录；不设置时默认使用仓库当前目录的 `./data`、`./logs`。
- `worker` 和 `web` 都声明同一个 `build: .` / `image: trading-sys-demo:latest`，避免新增 Python 文件后只启动 web 时仍使用旧镜像。
- `worker` 和 `web` 都注入 Binance 环境变量和 4 个数据库路径；第一组交易实验现在由 `worker` 在每轮可开仓 symbol 计算完成后自动触发。
- 健康检查只配置在 `web` 服务上，避免 `worker` 因不提供 HTTP 服务而在云平台部署时被误判。

### 查看状态与日志

```bash
docker compose ps
docker compose logs -f worker
docker compose logs -f web
docker compose logs -f sqlite-web
```

### 访问地址

- 交易数据平台：`http://YOUR_PUBLIC_IP:5000/`
- SQLite 可视化：`http://YOUR_PUBLIC_IP:8080/`

---

## 方案 B：优化后的 docker run（对应你给的 3 条命令）

> 先构建镜像：

```bash
docker build -t trading-sys-demo:latest .
```

### 1) 运行项目（worker）

```bash
docker run -d \
  --name trade \
  -e BINANCE_TESTNET="${BINANCE_TESTNET:-true}" \
  -e BINANCE_TESTNET_API_KEY="${BINANCE_TESTNET_API_KEY}" \
  -e BINANCE_TESTNET_SECRET_KEY="${BINANCE_TESTNET_SECRET_KEY}" \
  -e BINANCE_REAL_API_KEY="${BINANCE_REAL_API_KEY:-YOUR_REAL_API_KEY}" \
  -e BINANCE_REAL_API_SECRET="${BINANCE_REAL_API_SECRET:-YOUR_REAL_API_SECRET}" \
  -e BASE_DB_PATH="${BASE_DB_PATH:-data/base_data.db}" \
  -e SCORING_DB_PATH="${SCORING_DB_PATH:-data/scoring.db}" \
  -e TRADING_DB_PATH="${TRADING_DB_PATH:-data/trading.db}" \
  -e MARKET_DB_PATH="${MARKET_DB_PATH:-data/market.db}" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  --restart=always \
  trading-sys-demo:latest worker
```

### 2) 运行可视化数据库 web（sqlite-web）

```bash
docker run -d \
  --name sqlite-web \
  -p 8080:8080 \
  -v "$(pwd)/data:/data" \
  --restart=always \
  coleifer/sqlite-web:latest \
  sqlite_web "${SQLITE_WEB_DB_PATH:-/data/base_data.db}" --host 0.0.0.0
```

### 3) 运行交易数据 web 平台（trade-web）

```bash
docker run -d \
  --name trade-web \
  -e BINANCE_TESTNET="${BINANCE_TESTNET:-true}" \
  -e BINANCE_TESTNET_API_KEY="${BINANCE_TESTNET_API_KEY}" \
  -e BINANCE_TESTNET_SECRET_KEY="${BINANCE_TESTNET_SECRET_KEY}" \
  -e BINANCE_REAL_API_KEY="${BINANCE_REAL_API_KEY:-YOUR_REAL_API_KEY}" \
  -e BINANCE_REAL_API_SECRET="${BINANCE_REAL_API_SECRET:-YOUR_REAL_API_SECRET}" \
  -e BASE_DB_PATH="${BASE_DB_PATH:-data/base_data.db}" \
  -e SCORING_DB_PATH="${SCORING_DB_PATH:-data/scoring.db}" \
  -e TRADING_DB_PATH="${TRADING_DB_PATH:-data/trading.db}" \
  -e MARKET_DB_PATH="${MARKET_DB_PATH:-data/market.db}" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/logs:/app/logs" \
  -p 5000:5000 \
  --restart=always \
  trading-sys-demo:latest web
```

---

## 升级流程

```bash
git pull --rebase
export PUID=$(id -u)
export PGID=$(id -g)
docker compose up -d --build
```

## 防火墙与安全组

请在云平台安全组和服务器防火墙放行：
- TCP `5000`（交易数据平台）
- TCP `8080`（sqlite-web）
