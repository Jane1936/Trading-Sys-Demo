# Docker 公网 IP 部署（无 HTTPS）

你当前项目建议拆成两个容器：
- `worker`：运行 `app.py`（collector + pre_safety + processor）
- `web`：运行 `web_app.py`（Flask 页面）

两个容器挂载同一个 `data` 卷，保证网页能读到检测结果。

## 1) 构建镜像

先更新代码（在你的服务器项目目录）：

```bash
git pull
```

> 如果当前分支没有 tracking（例如你本地是 `work` 分支），请先切到有远端跟踪的分支再 pull：
> `git checkout main && git pull`

然后构建镜像：

```bash
docker build -t trading-sys-demo:latest .
```

## 2) 启动 worker（后台任务）

```bash
docker run -d \
  --name trading-worker \
  -v $(pwd)/data:/app/data \
  --restart unless-stopped \
  trading-sys-demo:latest worker
```

## 3) 启动 web（网页）

```bash
docker run -d \
  --name trading-web \
  -v $(pwd)/data:/app/data \
  -p 5000:5000 \
  --restart unless-stopped \
  trading-sys-demo:latest web
```

## 4) 访问地址

- `http://YOUR_PUBLIC_IP:5000/`
- `http://YOUR_PUBLIC_IP:5000/safety/abnormal-wicks`
- `http://YOUR_PUBLIC_IP:5000/safety/abnormal-wicks?limit=200`

## 5) 常用排障

```bash
docker ps
docker logs -f trading-worker
docker logs -f trading-web
```

## 6) 停止与重启

```bash
docker restart trading-worker trading-web
docker stop trading-worker trading-web
docker rm trading-worker trading-web
```
