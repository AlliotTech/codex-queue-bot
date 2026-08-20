# Codex Queue Bot

这是一个由 OpenILink Hub 消息控制的 Codex API 排队与保活服务。收到 `/开挤` 后，它会使用原生 `codex exec` 随机发送轻量请求；失败后按随机间隔继续，直到请求成功，然后通过 OpenILink 回复“开蹬”并停止该目标。收到 `/保活` 后，它会立即请求一次，并在每次请求完成后按配置的随机间隔持续请求。

主要能力：

- 一个进程配置多个 Codex Key / API 地址 / 模型，每个配置称为一个“目标”。
- 排队命令 `/开挤`、`/状态`、`/停止`，保活命令 `/保活`、`/保活状态`、`/停止保活`，以及 `/列表`、`/帮助`。
- 多目标可并行运行，并通过 `max_parallel` 控制本机同时存在的 Codex 进程数。
- 排队和保活复用同一套 Prompt、Codex Runner、密钥隔离与全局并发限制；同一目标不会同时发起两类请求。
- 每次从 `prompts.txt` 随机选择 Prompt，使用新的空临时目录和临时会话。
- 当前目标的 API Key 仅通过最小化的子进程环境传给 Codex，不出现在命令参数或日志中；OpenILink Token、其他目标 Key 和无关环境变量不会被 Codex 继承。
- OpenILink WebSocket 自动重连；成功通知发送失败时会持续退避重试，直到发送成功或进程退出。
- 提供 Dockerfile 和 Compose 配置。

## 工作流程

```text
微信 /开挤
    ↓
OpenILink Hub WebSocket
    ↓
Go 服务启动一个或多个目标
    ↓
原生 codex exec → 失败 → 随机等待 → 再试
                  ↓成功
OpenILink POST /bot/v1/message/send
    ↓
微信收到：✅ primary：开蹬（第 N 次，耗时 ...）
```

每个目标成功后停止。以后若再次掉出队列，重新发送 `/开挤` 即可开始新一轮。

保活默认关闭，需要通过 `/保活` 显式启动。每个目标拥有独立计时器：启动后立即请求，无论成功还是失败，都会在请求结束后重新生成下一次随机间隔。保活失败只写日志和状态，不发送聊天通知，也不会自动停止。

同一目标同时启用排队与保活时，排队任务拥有整轮优先权。已经执行中的保活请求可以完成，随后排队任务会连续重试直至成功或停止；这期间到期的保活会等待，并在排队结束后立即执行。`/停止` 只停止排队，`/停止保活` 只停止保活。

## OpenILink Hub 配置

本项目对接的是你示例中使用的 Hub API：

```text
POST /bot/v1/message/send
GET  /bot/v1/ws?token={app_token}
```

在 OpenILink Hub 中创建或安装一个 WebSocket App，并确保安装拥有：

- Events：`message`
- Scopes：`message:read`、`message:write`
- App Token：用于 WebSocket 和 Bot API

建议将 [openilink-tools.example.json](openilink-tools.example.json) 中的 Tools 配到 App，这样 `/开挤` 等命令会被确定性路由到本服务。即使收到的是 `message.text` 而不是 `command` 事件，服务也能识别同样的文本命令。

你提供的 `openilink-sdk-go` 是微信原始 iLink Bot API SDK，而 `/bot/v1/...` 是 OpenILink Hub 的 App API，两者是不同协议层。因此当前实现直接使用 Hub 的 WebSocket + REST API，能够复用现有 Hub URL 和 Bearer App Token，不需要再直连微信 iLink。

相关文档：

- [OpenILink API](https://openilink.com/docs/api)
- [OpenILink Hub WebSocket](https://openilink.com/docs/hub/websocket)
- [OpenILink Hub App 开发](https://github.com/openilink/openilink-hub/blob/main/docs/app-development.md)

## 配置多个 Key / API

复制配置模板：

```bash
cp config.example.json config.json
cp .env.example .env
```

一个目标对应一组 API 地址、Key 和模型：

```json
{
  "name": "primary",
  "api_base_url": "https://codex-a.example.com/v1",
  "api_key_env": "CODEX_KEY_PRIMARY",
  "model": "gpt-5.2-codex",
  "wire_api": "responses"
}
```

字段说明：

| 字段 | 说明 |
|---|---|
| `name` | 命令中使用的目标名，不能含空格或逗号 |
| `api_base_url` | Codex 兼容 Responses API 的基地址，程序不会自动追加 `/v1` |
| `api_key_env` | 保存 Key 的环境变量名，推荐方式 |
| `api_key` | 也可直接写 Key，但不建议将密钥放进配置文件 |
| `model` | 该站点支持的模型名 |
| `wire_api` | 当前固定为 `responses` |
| `config_overrides` | 可选，追加给 Codex 的 `-c key=value` 配置 |

全局 Codex 配置：

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `request_timeout_seconds` | `180` | 单次 Codex 请求超时 |
| `retry_min_seconds` | `3` | 失败后最短等待时间，最低 1 秒 |
| `retry_max_seconds` | `8` | 失败后最长等待时间 |
| `keepalive_min_seconds` | `2700` | 保活请求完成后的最短等待时间，最低 1 秒 |
| `keepalive_max_seconds` | `3300` | 保活请求完成后的最长等待时间，不得小于最短时间 |
| `max_parallel` | `2` | 同时运行的 Codex 子进程上限 |
| `reasoning_effort` | `low` | 健康请求使用的推理强度 |
| `success_message` | `开蹬` | 成功通知正文 |

`allowed_user_ids` 建议配置为你的 OpenILink 用户 ID。留空表示任何能向该 App 发消息的人都可以启动和停止任务。

## 本机运行

要求：

- Go 1.24+
- 当前最新版或兼容版本的原生 Codex CLI
- 一个 OpenILink Hub App Token

安装或确认 Codex：

```bash
npm install -g @openai/codex
codex --version
```

这里采用 OpenAI 官方文档中的无版本号安装方式，因此 npm 会安装当前 `latest` 版本。

导出密钥后先做静态检查：

```bash
export OPENILINK_APP_TOKEN='...'
export CODEX_KEY_PRIMARY='...'
export CODEX_KEY_BACKUP='...'

go run ./cmd/codex-queue-bot -config ./config.json -check
```

启动：

```bash
go run ./cmd/codex-queue-bot -config ./config.json
```

日志级别可通过 `LOG_LEVEL=debug|info|warn|error` 设置。日志会显示目标名、API Host、次数和错误摘要，不会记录 Key。

## 微信命令

| 命令 | 作用 |
|---|---|
| `/开挤` | 启动全部目标 |
| `/开挤 primary` | 只启动 `primary` |
| `/开挤 primary,backup` | 同时启动多个目标 |
| `/状态` | 查看全部目标状态 |
| `/状态 primary` | 查看一个目标 |
| `/停止` | 停止全部正在运行的目标 |
| `/停止 primary` | 停止一个目标 |
| `/保活` | 启动全部目标的保活；每个目标立即请求一次 |
| `/保活 primary,backup` | 启动指定目标的保活 |
| `/保活状态 [目标]` | 查看保活阶段、总请求次数、下次请求和最近失败 |
| `/停止保活 [目标]` | 停止保活，并取消该目标正在执行的保活请求 |
| `/列表` | 查看目标、模型和 API Host，不显示 Key |
| `/帮助` | 显示命令帮助 |

如果目标已经运行，另一位获授权用户再次发送 `/开挤 <目标>` 不会启动重复进程，而是订阅该轮成功通知。

重复发送 `/保活 <目标>` 只会报告该目标“正在保活”，不会重置请求次数或计时。

同时支持英文别名：`/start`、`/status`、`/stop`、`/keepalive`、`/keepalive-status`、`/stop-keepalive`、`/list`、`/help`。

## Docker 部署

编辑 `.env` 和 `config.json` 后，Compose 会直接拉取 GHCR 上的多架构镜像：

```bash
docker compose pull
docker compose up -d
docker compose logs -f codex-queue-bot
```

默认镜像是 `ghcr.io/alliottech/codex-queue-bot:latest`。可通过环境变量选择固定标签：

```bash
CODEX_QUEUE_BOT_IMAGE=ghcr.io/alliottech/codex-queue-bot:v1.0.0 \
  docker compose up -d
```

如果 GHCR 包为私有，需要先使用具有 `read:packages` 权限的 GitHub PAT 登录：

```bash
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u AlliotTech --password-stdin
```

镜像构建阶段默认安装官方 `@openai/codex@latest`，并提取平台对应的原生 Codex 二进制；最终运行层不携带 Node/npm。需要复现或验证特定版本时，可本地显式覆盖：

```bash
docker build \
  --build-arg CODEX_VERSION=0.148.0 \
  -t codex-queue-bot:local .
```

Compose 通过只读挂载提供 `config.json` 和 `prompts.txt`，Key 通过环境变量注入，不会写入镜像。

### GitHub Actions / GHCR

推送到 `main`、版本标签或手动触发工作流时，GitHub Actions 会构建 `linux/amd64` 和 `linux/arm64` 镜像并发布到 GHCR：

- 默认分支：`latest`、`main`、`sha-<commit>`
- Git 标签：同名标签、`sha-<commit>`
- Pull Request：只验证构建，不推送镜像

工作流仅授予 `contents: read` 和 `packages: write`，发布认证使用 GitHub 自动提供的短期 `GITHUB_TOKEN`，仓库中不保存 GHCR 密钥。

### 出站代理

容器运行时会统一读取以下标准代理环境变量：

- `HTTP_PROXY` / `http_proxy`
- `HTTPS_PROXY` / `https_proxy`
- `ALL_PROXY` / `all_proxy`
- `NO_PROXY` / `no_proxy`

只配置一个代理变量即可覆盖全部出站请求。例如在 `.env` 中配置：

```dotenv
HTTP_PROXY=socks5://host.docker.internal:1080
NO_PROXY=localhost,127.0.0.1
```

该代理会同时用于 OpenILink Hub 的 HTTP、WebSocket 连接和 Codex API 请求。缺失的 `HTTPS_PROXY`、`ALL_PROXY` 会自动沿用已配置的代理；如果分别配置了 HTTP 和 HTTPS 代理，则保留各自的值。代理值推荐使用 `http://` 或 `socks5://` URL，也兼容 `socks5:host:port` 简写，但建议使用带 `//` 的标准形式。`NO_PROXY` 中的地址会直连。

## Codex 调用方式

每次尝试都会直接执行 `codex exec`，不依赖 `c=codex --yolo` 等 shell alias。主要隔离参数包括：

```text
--ignore-user-config
--ephemeral
--skip-git-repo-check
--ignore-rules
--sandbox read-only
-c shell_environment_policy.inherit="none"
--output-last-message <临时文件>
```

程序为每个目标动态配置独立的 custom model provider：`base_url`、`env_key`、`wire_api="responses"`。这与 OpenAI Docs 中的非交互 `codex exec` 和自定义 provider 配置一致：

- [Codex CLI reference](https://developers.openai.com/codex/cli/reference/#codex-exec)
- [Codex advanced configuration](https://developers.openai.com/codex/config-advanced/#custom-model-providers)

成功条件是 Codex 退出码为 `0`，并且 `--output-last-message` 产生非空最终响应。

Codex 进程只会继承运行所需的环境白名单，包括 `PATH`、`HOME`、临时目录、语言区域、代理和 CA 证书设置，再额外加入当前目标 Key。`shell_environment_policy.inherit="none"` 会阻止 Codex 启动的命令继承该 Key；这项隔离配置在用户自定义 `config_overrides` 之后强制追加，不能被覆盖。

## Prompt

默认使用 [prompts.txt](prompts.txt)：

- 每行一个 Prompt。
- 空行和去掉行首空白后以 `#` 开头的行会被忽略。
- 每次请求前重新读取，因此修改文件后无需重启。
- 每次追加唯一请求 ID、尝试次数，以及“不读取文件、不调用工具、简短回答”的约束。

## 测试

```bash
go test ./...
go test -race ./...
go vet ./...
```

使用本机原生 Codex 二进制验证 Responses SSE 调用链：

```bash
RUN_NATIVE_CODEX_INTEGRATION=1 \
CODEX_INTEGRATION_BINARY=codex \
go test -tags=integration ./internal/codex \
  -run TestRunnerWithNativeCodexBinary -v
```

已有测试覆盖配置和环境变量密钥解析、Codex 子进程参数隔离、失败重试/取消/成功通知、保活循环与停止、排队/保活互斥和共享并发限制，以及 OpenILink REST 发送与 WebSocket 事件接收。

## 运行边界

失败重试会持续到成功、收到 `/停止` 或进程退出。请根据目标站点的服务条款和限流规则设置合理的重试区间；默认使用 3～8 秒随机间隔，并强制最低 1 秒，避免无间隔紧循环。

任务状态保存在内存中，容器重启后不会自动恢复此前正在运行的排队或保活任务，需要重新发送 `/开挤` 或 `/保活`。

原来的 [codex-healthcheck.sh](codex-healthcheck.sh) 仍保留，可继续用于本机低频健康检查；新的 Go 服务和它互不依赖。
