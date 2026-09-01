import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import App from "./App"

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

class MockEventSource {
  static instances: MockEventSource[] = []
  listeners = new Map<string, (event: MessageEvent) => void>()
  onerror: (() => void) | null = null
  constructor(public url: string) { MockEventSource.instances.push(this) }
  addEventListener(name: string, listener: EventListenerOrEventListenerObject) { this.listeners.set(name, listener as (event: MessageEvent) => void) }
  close() {}
  emit(name: string, data: unknown) { this.listeners.get(name)?.(new MessageEvent(name, { data: JSON.stringify(data) })) }
}

const dashboard = {
  version: "test",
  generated_at: "2026-08-20T12:00:00Z",
  config_revision: 3,
  restart_required: false,
  restart_fields: [],
  openilink: { state: "disabled", updated_at: null },
  telegram: { state: "disabled", updated_at: null },
  concurrency: { current: 0, max: 2 },
  targets: [{ id: 1, name: "main", model: "gpt-test", api_host: "api.example.test", busy: false, adhoc_running: false, queue: { state: "idle", attempts: 0, started_at: null, last_attempt: null, next_attempt: null, finished_at: null }, keepalive: { state: "stopped", requests: 0, started_at: null, last_request: null, last_success: null, last_failure: null, next_request: null, stopped_at: null } }],
}

const configuration = {
  revision: 3,
  loaded_startup_revision: 3,
  restart_required: false,
  restart_fields: [],
  codex: { binary: "codex", prompts_file: "prompts.txt", prompts: ["health check"], request_timeout_seconds: 180, retry_min_seconds: 3, retry_max_seconds: 8, keepalive_min_seconds: 2700, keepalive_max_seconds: 3300, max_parallel: 2, success_message: "开蹬", reasoning_effort: "low", config_overrides: [] },
  openilink: { enabled: false, base_url: "http://127.0.0.1:9800", token_set: false, allowed_user_ids: [], http_timeout_seconds: 15 },
  telegram: { enabled: false, base_url: "https://api.telegram.org", token_set: false, allowed_user_ids: [], http_timeout_seconds: 45, poll_timeout_seconds: 30 },
  web: { listen_address: ":8080", cookie_secure: false, trusted_proxies: [], activity_limit: 200 },
  account: { username: "admin" },
  targets: [{ id: 1, sort_order: 0, name: "main", api_base_url: "https://api.example.test/v1", api_key_set: true, model: "gpt-test", wire_api: "responses", config_overrides: [], busy: false }],
}

function publicStatus(required = false) {
  return new Response(JSON.stringify({ required, suggested_username: "admin" }), { status: 200 })
}

describe("console", () => {
  it("runs the first-time administrator setup", async () => {
    MockEventSource.instances = []
    vi.stubGlobal("EventSource", MockEventSource)
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(true)
      if (url.endsWith("/setup")) return new Response(JSON.stringify({ authenticated: true, username: "owner", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify({ ...dashboard, targets: [] }), { status: 200 })
      throw new Error(`unexpected request ${url}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    expect(await screen.findByText("初始化管理控制台")).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText("管理员用户名"), { target: { value: "owner" } })
    fireEvent.change(screen.getByLabelText("管理员密码"), { target: { value: "abcde" } })
    fireEvent.change(screen.getByLabelText("确认密码"), { target: { value: "abcde" } })
    fireEvent.click(screen.getByRole("button", { name: "创建管理员并登录" }))
    expect(await screen.findByText("还没有 target")).toBeInTheDocument()
  })

  it("shows login errors when there is no session", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(false)
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ error: "未登录" }), { status: 401 })
      if (url.endsWith("/auth/login")) return new Response(JSON.stringify({ error: "用户名或密码错误" }), { status: 401 })
      throw new Error(`unexpected request ${url}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    expect(await screen.findByText("Codex 运行控制台")).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText("密码"), { target: { value: "wrong-password" } })
    fireEvent.click(screen.getByRole("button", { name: "登录" }))
    expect(await screen.findByRole("alert")).toHaveTextContent("用户名或密码错误")
  })

  it("renders state, sends actions, and applies SSE updates", async () => {
    MockEventSource.instances = []
    vi.stubGlobal("EventSource", MockEventSource)
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(false)
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ authenticated: true, username: "admin", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify(dashboard), { status: 200 })
      if (url.endsWith("/actions")) return new Response(JSON.stringify({ changed: ["main"], unchanged: [], unknown: [] }), { status: 200 })
      throw new Error(`unexpected request ${url} ${init?.method}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    expect(await screen.findByText("main")).toBeInTheDocument()
    const batchButton = screen.getAllByRole("button", { name: /开始挤队列/ })[0]
    expect(batchButton).toBeDisabled()
    fireEvent.click(screen.getByRole("checkbox", { name: "选择 main" }))
    expect(batchButton).toBeEnabled()
    fireEvent.click(batchButton)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/v1/actions", expect.objectContaining({ method: "POST", body: JSON.stringify({ action: "queue.start", targets: ["main"] }) })))
    MockEventSource.instances[0].emit("state", { ...dashboard, targets: [{ ...dashboard.targets[0], busy: true, queue: { ...dashboard.targets[0].queue, state: "running", attempts: 1 } }] })
    expect(await screen.findByText("排队中")).toBeInTheDocument()
  })

  it("runs an adhoc prompt and displays output and exit code", async () => {
    MockEventSource.instances = []
    vi.stubGlobal("EventSource", MockEventSource)
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(false)
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ authenticated: true, username: "admin", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify(dashboard), { status: 200 })
      if (url.endsWith("/targets/1/adhoc") && init?.method === "POST") return new Response(JSON.stringify({ target_id: 1, target: "main", success: true, output: "manual answer", process_output: "cli trace", exit_code: 0, duration_ms: 1234 }), { status: 200 })
      throw new Error(`unexpected request ${url} ${init?.method}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    await screen.findByText("main")
    fireEvent.click(screen.getByRole("button", { name: "手动请求" }))
    expect(await screen.findByText("手动请求 · main")).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText("Prompt"), { target: { value: "custom prompt" } })
    fireEvent.click(screen.getByRole("button", { name: "执行请求" }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/v1/targets/1/adhoc", expect.objectContaining({ method: "POST", body: JSON.stringify({ prompt: "custom prompt" }) })))
    expect(await screen.findByText("manual answer")).toBeInTheDocument()
    expect(screen.getByText((_, element) => element?.textContent === "退出码 0")).toBeInTheDocument()
    expect(screen.getByText("cli trace")).toBeInTheDocument()
  })

  it("creates a target from the configuration view without echoing a key", async () => {
    MockEventSource.instances = []
    vi.stubGlobal("EventSource", MockEventSource)
    const created = { ...configuration, revision: 4, targets: [...configuration.targets, { id: 2, sort_order: 1, name: "backup", api_base_url: "https://backup.example/v1", api_key_set: true, model: "gpt-backup", wire_api: "responses", config_overrides: [], busy: false }] }
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(false)
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ authenticated: true, username: "admin", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify(dashboard), { status: 200 })
      if (url.endsWith("/config") && !init?.method) return new Response(JSON.stringify(configuration), { status: 200 })
      if (url.endsWith("/targets") && init?.method === "POST") return new Response(JSON.stringify(created), { status: 201 })
      throw new Error(`unexpected request ${url} ${init?.method}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    await screen.findByText("main")
    fireEvent.click(screen.getByRole("button", { name: "配置" }))
    expect(await screen.findByRole("heading", { name: "Targets" })).toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "添加 target" }))
    fireEvent.change(screen.getByLabelText("名称"), { target: { value: "backup" } })
    fireEvent.change(screen.getByLabelText("API Base URL"), { target: { value: "https://backup.example/v1" } })
    fireEvent.change(screen.getByLabelText("模型"), { target: { value: "gpt-backup" } })
    fireEvent.change(screen.getByLabelText(/^API Key/), { target: { value: "new-secret" } })
    fireEvent.click(screen.getByRole("button", { name: "保存 target" }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/v1/targets", expect.objectContaining({ method: "POST" })))
    const createCall = fetchMock.mock.calls.find(([input, init]) => String(input).endsWith("/targets") && init?.method === "POST")
    expect(JSON.parse(String(createCall?.[1]?.body))).not.toHaveProperty("sort_order")
    expect(await screen.findByText("backup")).toBeInTheDocument()
    expect(screen.queryByText("new-secret")).not.toBeInTheDocument()
  })

  it("reveals a saved target key only after clicking the visibility control", async () => {
    MockEventSource.instances = []
    vi.stubGlobal("EventSource", MockEventSource)
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(false)
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ authenticated: true, username: "admin", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify(dashboard), { status: 200 })
      if (url.endsWith("/config") && !init?.method) return new Response(JSON.stringify(configuration), { status: 200 })
      if (url.endsWith("/config/secrets")) return new Response(JSON.stringify({ openilink_token: "openilink-secret", telegram_token: "telegram-secret", targets: [{ id: 1, name: "main", api_key: "target-secret" }] }), { status: 200 })
      throw new Error(`unexpected request ${url} ${init?.method}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    await screen.findByText("main")
    fireEvent.click(screen.getByRole("button", { name: "配置" }))
    fireEvent.click(await screen.findByRole("button", { name: "编辑" }))
    const reveal = screen.getByRole("button", { name: "显示API Key" })
    expect(screen.getByLabelText("API Key")).toHaveAttribute("type", "password")
    fireEvent.click(reveal)
    await waitFor(() => expect(fetchMock.mock.calls.some(([input]) => String(input).endsWith("/config/secrets"))).toBe(true))
    await waitFor(() => expect(screen.getByLabelText("API Key")).toHaveValue("target-secret"))
    expect(screen.getByLabelText("API Key")).toHaveAttribute("type", "text")
    fireEvent.click(screen.getByRole("button", { name: "隐藏API Key" }))
    expect(screen.getByLabelText("API Key")).toHaveAttribute("type", "password")
  })

  it("validates random intervals and shows restart and server errors", async () => {
    MockEventSource.instances = []
    vi.stubGlobal("EventSource", MockEventSource)
    const pendingRestart = { ...configuration, restart_required: true, restart_fields: ["web.listen_address"] }
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(false)
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ authenticated: true, username: "admin", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify(dashboard), { status: 200 })
      if (url.endsWith("/config") && !init?.method) return new Response(JSON.stringify(pendingRestart), { status: 200 })
      if (url.endsWith("/config/codex") && init?.method === "PUT") return new Response(JSON.stringify({ error: "服务器拒绝了配置" }), { status: 400 })
      throw new Error(`unexpected request ${url} ${init?.method}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    await screen.findByText("main")
    fireEvent.click(screen.getByRole("button", { name: "配置" }))
    expect(await screen.findByText("部分配置等待服务重启后生效")).toBeInTheDocument()
    expect(screen.getByText("web.listen_address")).toBeInTheDocument()
    fireEvent.click(screen.getByRole("button", { name: "任务与保活" }))
    fireEvent.change(screen.getByLabelText("保活最小秒数"), { target: { value: "4000" } })
    fireEvent.click(screen.getByRole("button", { name: "保存任务参数" }))
    expect(await screen.findByRole("alert")).toHaveTextContent("保活最小秒数不能大于最大秒数")
    expect(fetchMock.mock.calls.some(([input, init]) => String(input).endsWith("/config/codex") && init?.method === "PUT")).toBe(false)
    fireEvent.change(screen.getByLabelText("保活最小秒数"), { target: { value: "2700" } })
    fireEvent.click(screen.getByRole("button", { name: "保存任务参数" }))
    expect(await screen.findByRole("alert")).toHaveTextContent("服务器拒绝了配置")
  })

  it("saves Telegram bot configuration without echoing its token", async () => {
    MockEventSource.instances = []
    vi.stubGlobal("EventSource", MockEventSource)
    const saved = { ...configuration, revision: 4, telegram: { ...configuration.telegram, enabled: true, token_set: true, allowed_user_ids: ["123"] }, restart_required: true, restart_fields: ["telegram.enabled", "telegram.token"] }
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input)
      if (url.endsWith("/setup/status")) return publicStatus(false)
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ authenticated: true, username: "admin", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify(dashboard), { status: 200 })
      if (url.endsWith("/config") && !init?.method) return new Response(JSON.stringify(configuration), { status: 200 })
      if (url.endsWith("/config/telegram") && init?.method === "PUT") return new Response(JSON.stringify(saved), { status: 200 })
      throw new Error(`unexpected request ${url} ${init?.method}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    await screen.findByText("main")
    fireEvent.click(screen.getByRole("button", { name: "配置" }))
    fireEvent.click(await screen.findByRole("button", { name: "消息入口" }))
    fireEvent.click(screen.getByLabelText("启用 Telegram Bot"))
    fireEvent.change(screen.getByLabelText("Bot Token"), { target: { value: "telegram-secret" } })
    fireEvent.change(screen.getByLabelText("允许的 Telegram 用户 ID"), { target: { value: "123" } })
    fireEvent.click(screen.getByRole("button", { name: "保存 Telegram" }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/v1/config/telegram", expect.objectContaining({ method: "PUT" })))
    expect(screen.queryByText("telegram-secret")).not.toBeInTheDocument()
    expect(await screen.findByText("Telegram 配置已保存并热重载")).toBeInTheDocument()
  })
})
