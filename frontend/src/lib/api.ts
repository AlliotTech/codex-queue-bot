export type QueueState = "idle" | "running" | "succeeded" | "stopped"
export type KeepaliveState = "requesting" | "waiting_queue" | "waiting_next" | "stopped"

export interface Target {
  id: number
  name: string
  model: string
  api_host: string
  busy: boolean
  adhoc_running: boolean
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

export interface Dashboard {
  version: string
  generated_at: string
  config_revision: number
  restart_required: boolean
  restart_fields: string[]
  openilink: { state: string; error?: string; updated_at: string | null }
  telegram: { state: string; error?: string; updated_at: string | null }
  concurrency: { current: number; max: number }
  targets: Target[]
}

export interface ActionResult {
  changed: string[]
  unchanged: string[]
  unknown: string[]
}

export interface AdhocRunResult {
  target_id: number
  target: string
  success: boolean
  output: string
  process_output: string
  exit_code: number
  error?: string
  duration_ms: number
}

export interface ConfigurationTarget {
  id: number
  sort_order: number
  name: string
  api_base_url: string
  api_key_set: boolean
  model: string
  wire_api: string
  config_overrides: string[]
  busy: boolean
}

export interface Configuration {
  revision: number
  loaded_startup_revision: number
  restart_required: boolean
  restart_fields: string[]
  codex: {
    binary: string
    prompts_file: string
    prompts: string[]
    request_timeout_seconds: number
    retry_min_seconds: number
    retry_max_seconds: number
    keepalive_min_seconds: number
    keepalive_max_seconds: number
    max_parallel: number
    success_message: string
    reasoning_effort: string
    config_overrides: string[]
  }
  openilink: {
    enabled: boolean
    base_url: string
    token_set: boolean
    allowed_user_ids: string[]
    http_timeout_seconds: number
  }
  telegram: {
    enabled: boolean
    base_url: string
    token_set: boolean
    allowed_user_ids: string[]
    http_timeout_seconds: number
    poll_timeout_seconds: number
  }
  web: {
    listen_address: string
    cookie_secure: boolean
    trusted_proxies: string[]
    activity_limit: number
  }
  account: { username: string }
  targets: ConfigurationTarget[]
}

export interface ConfigurationSecrets {
  openilink_token: string
  telegram_token: string
  targets: Array<{ id: number; name: string; api_key: string }>
}

export interface TargetInput {
  sort_order?: number
  name: string
  api_base_url: string
  api_key: string
  model: string
  wire_api: string
  config_overrides: string[]
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, { credentials: "same-origin", ...init })
  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(body.error || `请求失败 (${response.status})`)
  return body as T
}

function jsonRequest<T>(url: string, method: string, body: unknown) {
  return request<T>(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })
}

export function setupStatus() {
  return request<{ required: boolean; suggested_username: string }>("/api/v1/setup/status")
}

export function setup(username: string, password: string) {
  return request<{ authenticated: boolean; username: string; expires_at: string }>("/api/v1/setup", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  })
}

export function session() {
  return request<{ authenticated: boolean; username: string; expires_at: string }>("/api/v1/auth/session")
}

export function login(username: string, password: string) {
  return request<{ authenticated: boolean; username: string; expires_at: string }>("/api/v1/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  })
}

export function logout() {
  return request<{ logged_out: boolean }>("/api/v1/auth/logout", { method: "POST" })
}

export function dashboard() {
  return request<Dashboard>("/api/v1/dashboard")
}

export function action(name: string, targets: string[]) {
  return jsonRequest<ActionResult>("/api/v1/actions", "POST", { action: name, targets })
}

export function runAdhoc(id: number, prompt: string, signal?: AbortSignal) {
  return request<AdhocRunResult>(`/api/v1/targets/${id}/adhoc`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prompt }),
    signal,
  })
}

export function getConfiguration() {
  return request<Configuration>("/api/v1/config")
}

export function getConfigurationSecrets() {
  return request<ConfigurationSecrets>("/api/v1/config/secrets")
}

export function updateCodex(value: Configuration["codex"]) {
  return jsonRequest<Configuration>("/api/v1/config/codex", "PUT", value)
}

export function updateOpenILink(value: Omit<Configuration["openilink"], "token_set"> & { token: string; clear_token: boolean }) {
  return jsonRequest<Configuration>("/api/v1/config/openilink", "PUT", value)
}

export function updateTelegram(value: Omit<Configuration["telegram"], "token_set"> & { token: string; clear_token: boolean }) {
  return jsonRequest<Configuration>("/api/v1/config/telegram", "PUT", value)
}

export function updateWeb(value: Configuration["web"]) {
  return jsonRequest<Configuration>("/api/v1/config/web", "PUT", value)
}

export function updateAccount(value: { username: string; current_password: string; new_password: string }) {
  return jsonRequest<{ reauthentication_required: boolean; revision: number }>("/api/v1/account", "PUT", value)
}

export function createTarget(value: TargetInput) {
  return jsonRequest<Configuration>("/api/v1/targets", "POST", value)
}

export function updateTarget(id: number, value: TargetInput) {
  return jsonRequest<Configuration>(`/api/v1/targets/${id}`, "PUT", value)
}

export function deleteTarget(id: number) {
  return request<Configuration>(`/api/v1/targets/${id}`, { method: "DELETE" })
}
