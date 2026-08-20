import { describe, expect, it, vi } from "vitest"
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react"
import App from "./App"
import { afterEach } from "vitest"

afterEach(() => cleanup())

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
  openilink: { state: "disabled", updated_at: null },
  concurrency: { current: 0, max: 2 },
  targets: [{ name: "main", model: "gpt-test", api_host: "api.example.test", queue: { state: "idle", attempts: 0, started_at: null, last_attempt: null, next_attempt: null, finished_at: null }, keepalive: { state: "stopped", requests: 0, started_at: null, last_request: null, last_success: null, last_failure: null, next_request: null, stopped_at: null } }],
  activities: [],
}

describe("console", () => {
  it("shows the administrator login when there is no session", async () => {
    const fetchMock = vi.fn()
      .mockRejectedValueOnce(new Error("not logged in"))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: "用户名或密码错误" }), { status: 401 }))
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
      if (url.endsWith("/auth/session")) return new Response(JSON.stringify({ authenticated: true, username: "admin", csrf_token: "csrf", expires_at: "2026-08-21T00:00:00Z" }), { status: 200 })
      if (url.endsWith("/dashboard")) return new Response(JSON.stringify(dashboard), { status: 200 })
      if (url.endsWith("/actions")) return new Response(JSON.stringify({ changed: ["main"], unchanged: [], unknown: [] }), { status: 200 })
      throw new Error(`unexpected request ${url} ${init?.method}`)
    })
    vi.stubGlobal("fetch", fetchMock)
    render(<App />)
    expect(await screen.findByText("main")).toBeInTheDocument()
    const batchButton = screen.getAllByRole("button", { name: /开始排队/ })[0]
    expect(batchButton).toBeDisabled()
    fireEvent.click(screen.getByRole("checkbox", { name: "选择 main" }))
    expect(batchButton).toBeEnabled()
    fireEvent.click(batchButton)
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/api/v1/actions", expect.objectContaining({ method: "POST", body: JSON.stringify({ action: "queue.start", targets: ["main"] }) })))
    MockEventSource.instances[0].emit("activity", { id: 1, type: "queue.start", target: "main", source: "web", actor: "admin", attempts: 0, at: "2026-08-20T12:00:01Z" })
    expect(await screen.findByText(/开始排队 · web/)).toBeInTheDocument()
  })
})
