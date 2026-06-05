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


## 3) 评分权重配置

本项目的评分规则权重现在由仓库根目录的 `scoring_rule_weights.json` 管理，Docker 镜像构建时会把该文件复制到 `/app/scoring_rule_weights.json`。

本次新增的 `cooldown_module.py` 属于 Python 源码文件，`Dockerfile` 的 `COPY *.py ./` 会自动把它打进镜像；因此服务器升级时必须重新构建镜像，不能只重启旧容器。

如果你修改了评分权重或 Python 源码，请在服务器上重新构建并重启容器，让新镜像包含最新配置和代码：

```bash
docker compose up -d --build
```


## 4) Binance Demo/Testnet 账户配置

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

Docker Compose 启动前在同一个 shell 中导出这些变量即可；`docker-compose.yml` 会把这些变量注入 `web` 容器；`web` 容器会通过 `/api/account/balance` 在你点击网页“账户情况”的查询按钮时实时请求余额。

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
  sqlite_web /data/klines.db --host 0.0.0.0
```

### 3) 运行交易数据 web 平台（trade-web）

```bash
docker run -d \
  --name trade-web \
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
