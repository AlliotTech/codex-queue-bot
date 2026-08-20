export type QueueState = "idle" | "running" | "succeeded" | "stopped"
export type KeepaliveState = "requesting" | "waiting_queue" | "waiting_next" | "stopped"

export interface Target {
  name: string
  model: string
  api_host: string
  queue: {
    state: QueueState
    attempts: number
    started_at: string | null
    last_attempt: string | null
    next_attempt: string | null
    finished_at: string | null
    last_error?: string
  }
  keepalive: {
    state: KeepaliveState
    requests: number
    started_at: string | null
    last_request: string | null
    last_success: string | null
    last_failure: string | null
    next_request: string | null
    stopped_at: string | null
    last_error?: string
  }
}

export interface Activity {
  id: number
  type: string
  target: string
  source: "web" | "openilink" | "system"
  actor: string
  attempts: number
  at: string
  error?: string
}

export interface Dashboard {
  version: string
  generated_at: string
  openilink: { state: string; error?: string; updated_at: string | null }
  concurrency: { current: number; max: number }
  targets: Target[]
  activities: Activity[]
}

export interface ActionResult {
  changed: string[]
  unchanged: string[]
  unknown: string[]
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, { credentials: "same-origin", ...init })
  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(body.error || `请求失败 (${response.status})`)
  return body as T
}

export async function session() {
  return request<{ authenticated: boolean; username: string; csrf_token: string; expires_at: string }>("/api/v1/auth/session")
}

export async function login(username: string, password: string) {
  return request<{ authenticated: boolean; username: string; csrf_token: string; expires_at: string }>("/api/v1/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  })
}

export async function logout(csrf: string) {
  return request<{ logged_out: boolean }>("/api/v1/auth/logout", { method: "POST", headers: { ["X-CSRF-Token"]: csrf } })
}

export async function dashboard() {
  return request<Dashboard>("/api/v1/dashboard")
}

export async function action(csrf: string, name: string, targets: string[]) {
  return request<ActionResult>("/api/v1/actions", {
    method: "POST",
    headers: { "Content-Type": "application/json", ["X-CSRF-Token"]: csrf },
    body: JSON.stringify({ action: name, targets }),
  })
}
