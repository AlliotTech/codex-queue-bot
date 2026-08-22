# Codex Queue Bot

Codex API 排队与保活服务，提供 Gin + React 管理控制台。多个 target 共用同一个任务队列、可配置并发上限和运行状态；OpenILink 与 Telegram Bot 都可作为消息入口，并可同时启用。

## 快速部署

要求 Docker Compose v2。

```bash
cp .env.example .env
openssl rand -base64 32
# 将上一步输出填入 .env 的 CODEX_QUEUE_MASTER_KEY
docker compose up -d
docker compose logs -f codex-queue-bot
```

打开 <http://127.0.0.1:8080/>。第一次访问时创建唯一管理员，密码至少 5 个字符；初始化前请确保页面只对可信网络开放。

Compose 默认使用 GHCR 镜像，并持久化 SQLite 数据库：

- 数据库：命名卷 `codex-queue-data` → `/app/data/codex-queue-bot.db`
- Prompt：首次读取 `./prompts.txt`；在控制台保存非空列表后改用 SQLite 中的 prompt 列表
- 端口：`127.0.0.1:8080:8080`（仅本机访问）

如果需要让局域网直接访问，将 `compose.yaml` 中的映射改为 `8080:8080`，但生产环境建议保持本机绑定并通过 HTTPS 反向代理暴露。代理需要关闭 SSE 缓冲（例如 `proxy_buffering off`），并把读取超时设为至少一小时。

查看 Compose 最终配置：

```bash
docker compose config
```

## 配置与数据

SQLite 是运行时的唯一配置源。登录控制台后可以维护 target、Codex 参数、OpenILink、Telegram 和 Web 设置；密钥写入数据库前会加密，API 不会返回原文。

必须设置：

- `CODEX_QUEUE_MASTER_KEY`：Base64 编码的 32 字节随机值。首次生成后必须长期保存；丢失或更换会导致数据库密钥无法解密。

可选环境变量见 [.env.example](.env.example)：镜像名、日志级别和出站代理。Codex API Key、OpenILink Token 和 Telegram Bot Token 通常在控制台配置，不需要写入 `.env`。

常用启动参数：

```text
-db <path>       SQLite 路径（默认 data/codex-queue-bot.db）
-config <path>   废弃兼容参数；不再读取 JSON
-check           执行配置、Codex 和 Prompt 预检后退出
-version         输出版本
```

旧版 `config.json` 导入链路已移除。请同时备份 `codex-queue-data` 卷和 `CODEX_QUEUE_MASTER_KEY`。会话与任务状态保存在内存中，服务重启后会清空。

## 控制台与任务

控制台展示 SSE、OpenILink、Telegram 状态、并发概览和每个 target 的排队/保活状态，并支持单个或批量“开始挤队列”“开始保活”“停止当前任务”。最大并发可在“任务与保活”中修改并立即生效；降低并发不会中断已经运行的请求。

任务规则：

- 排队失败后立即重新竞争全局并发槽，成功后停止该 target；旧重试区间仅为回滚兼容保留。
- 保活启动后立即请求，之后在随机区间内继续请求；失败只记录状态。
- 同一 target 只有一个活动模式；切换模式时先取消并等待当前请求退出。
- 编辑或删除运行中的 target 会先停止任务并最多等待 10 秒；超时则返回冲突且不写数据库。

## 消息入口（可选）

OpenILink 和 Telegram 默认关闭，可在控制台分别配置并启用；地址、Token、超时、白名单或启用状态保存后立即热重载。网络错误指数退避，Token 被拒绝后显示 `unauthorized` 并等待配置修改。Telegram 使用 Bot API 长轮询，无需公网 Webhook。

两个入口使用相同的中英文命令：

| 中文 | 英文别名 | 作用 |
|---|---|---|
| `/开挤` | `/start` | 开始排队 |
| `/状态` | `/status` | 查看排队状态 |
| `/停止` | `/stop` | 停止当前任务 |
| `/保活` | `/keepalive` | 开启保活 |
| `/保活状态` | `/keepalive-status` | 查看保活状态 |
| `/停止保活` | `/stop-keepalive` | 停止当前任务（兼容命令） |
| `/列表` | `/list` | 查看 target 列表 |
| `/帮助` | `/help` | 查看帮助 |

多个 target 可用空格、逗号或分号分隔；`all`/`全部` 表示全部目标。

Telegram 中无参数的 `/start` 用于显示帮助，避免首次打开机器人时意外启动全部任务；启动全部目标可使用 `/开挤`、`/go` 或 `/start all`。

## 本机开发

要求 Go 1.24+、Node 22+ 和可用的 Codex CLI：

```bash
npm --prefix frontend ci
npm --prefix frontend test
npm --prefix frontend run build

export CODEX_QUEUE_MASTER_KEY="$(openssl rand -base64 32)"
go run ./cmd/codex-queue-bot -check
go run ./cmd/codex-queue-bot
```

前端构建产物会嵌入 Go 二进制；Docker 构建会自动执行前端构建。提交前建议运行：

```bash
go test -race ./...
go vet ./...
npm --prefix frontend test
npm --prefix frontend run build
```

健康检查地址：<http://127.0.0.1:8080/healthz>。

出站代理只从启动环境变量读取，修改后需要重启。支持大小写形式的 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY`、`NO_PROXY`，以及 HTTP、HTTPS、SOCKS5、SOCKS5H；同一解析结果用于 Telegram、OpenILink HTTP/WebSocket 和 Codex 子进程。日志只记录是否启用，不打印代理 URL。
