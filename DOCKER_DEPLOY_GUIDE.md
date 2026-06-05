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
sudo chown -R 1000:1000 ./data ./logs
sudo chmod -R u+rwX ./data ./logs
```

> 如果出现 `sqlite3.OperationalError: attempt to write a readonly database`，
> 基本都是宿主机挂载目录权限导致容器内 `appuser` 无法写入 `/app/data`。

---


## 3) 评分权重配置

本项目的评分规则权重现在由仓库根目录的 `scoring_rule_weights.json` 管理，Docker 镜像构建时会把该文件复制到 `/app/scoring_rule_weights.json`。

如果你只修改评分权重配置，请在服务器上重新构建并重启容器，让新镜像包含最新配置：

```bash
docker compose up -d --build
```

## 方案 A（推荐）：Docker Compose

### 启动（构建并后台运行）

```bash
export PUID=$(id -u)
export PGID=$(id -g)
docker compose up -d --build
```

`docker-compose.yml` 已支持 `user: "${PUID:-1000}:${PGID:-1000}"`，会让 `worker/web` 进程直接使用宿主机当前用户 UID/GID，避免挂载 `./data`、`./logs` 时出现只读权限问题。

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
  -v /root/trade/data:/app/data \
  -v /root/trade/logs:/app/logs \
  --restart=always \
  trading-sys-demo:latest worker
```

### 2) 运行可视化数据库 web（sqlite-web）

```bash
docker run -d \
  --name sqlite-web \
  -p 8080:8080 \
  -v /root/trade/data:/data \
  --restart=always \
  coleifer/sqlite-web:latest \
  sqlite_web /data/klines.db --host 0.0.0.0
```

### 3) 运行交易数据 web 平台（trade-web）

```bash
docker run -d \
  --name trade-web \
  -v /root/trade/data:/app/data \
  -v /root/trade/logs:/app/logs \
  -p 5000:5000 \
  --restart=always \
  trading-sys-demo:latest web
```

---

## 升级流程

```bash
git pull --rebase
docker compose up -d --build
```

## 防火墙与安全组

请在云平台安全组和服务器防火墙放行：
- TCP `5000`（交易数据平台）
- TCP `8080`（sqlite-web）
