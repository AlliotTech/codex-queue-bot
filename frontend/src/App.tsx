import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react"
import {
  Activity, AlertTriangle, ArrowLeft, ArrowUp, CheckCircle2, CircleStop,
  Database, Link2, LogOut, Pause, Pencil, Play, Plus, Radio, RefreshCw,
  Save, Server, Settings, ShieldCheck, SlidersHorizontal, Timer, Trash2,
  UserCog, Wifi, XCircle,
} from "lucide-react"
import {
  action, createTarget, dashboard, deleteTarget, getConfiguration, login, logout,
  session, setup, setupStatus, updateAccount, updateCodex, updateOpenILink,
  updateTarget, updateWeb, type Activity as ActivityItem, type Configuration,
  type ConfigurationTarget, type Dashboard, type Target, type TargetInput,
} from "./lib/api"
import { Button } from "./components/ui/button"
import { Badge } from "./components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "./components/ui/card"
import { Checkbox } from "./components/ui/checkbox"
import { Input } from "./components/ui/input"
import { AlertDialog } from "./components/ui/alert-dialog"
import { countdown, formatTime } from "./lib/utils"
import "./styles.css"

const minimumAdminPasswordLength = 5

type Auth = { csrf: string; username: string }
type Connection = "connecting" | "connected" | "reconnecting" | "closed"
type ConfigSection = "targets" | "tasks" | "openilink" | "runtime" | "account"

function SetupPage({ suggested, onComplete }: { suggested: string; onComplete: (auth: Auth) => void }) {
  const [username, setUsername] = useState(suggested || "admin")
  const [password, setPassword] = useState("")
  const [confirm, setConfirm] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState("")
  async function submit(event: FormEvent) {
    event.preventDefault()
    if (Array.from(password).length < minimumAdminPasswordLength) return setError(`密码至少需要 ${minimumAdminPasswordLength} 个字符`)
    if (password !== confirm) return setError("两次输入的密码不一致")
    setBusy(true); setError("")
    try {
      const result = await setup(username, password)
      onComplete({ csrf: result.csrf_token, username: result.username })
    } catch (err) {
      setError(err instanceof Error ? err.message : "初始化失败")
    } finally { setBusy(false) }
  }
  return <main className="login-shell"><Card className="login-card setup-card"><CardHeader><div className="brand-mark"><Database size={22} /></div><CardTitle>初始化管理控制台</CardTitle><p className="muted">创建唯一管理员。完成后可在界面中添加 target 与维护全部运行配置。</p></CardHeader><CardContent><form onSubmit={submit} className="stack"><label>管理员用户名<Input value={username} onChange={event => setUsername(event.target.value)} autoComplete="username" /></label><label>管理员密码<Input value={password} onChange={event => setPassword(event.target.value)} type="password" autoComplete="new-password" placeholder={`至少 ${minimumAdminPasswordLength} 个字符`} /></label><label>确认密码<Input value={confirm} onChange={event => setConfirm(event.target.value)} type="password" autoComplete="new-password" /></label><div className="info-box"><ShieldCheck size={16} /><span>首个访问者可以完成初始化；请先确认当前页面只暴露在可信网络。</span></div>{error && <p className="error-text" role="alert">{error}</p>}<Button type="submit" disabled={busy || !username || !password || !confirm}>{busy ? "正在初始化…" : "创建管理员并登录"}</Button></form></CardContent></Card></main>
}

function LoginPage({ onLoggedIn }: { onLoggedIn: (auth: Auth) => void }) {
  const [username, setUsername] = useState("admin")
  const [password, setPassword] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState("")
  async function submit(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError("")
    try {
      const result = await login(username, password)
      onLoggedIn({ csrf: result.csrf_token, username: result.username })
    } catch (err) { setError(err instanceof Error ? err.message : "登录失败") } finally { setBusy(false) }
  }
  return <main className="login-shell"><Card className="login-card"><CardHeader><div className="brand-mark"><ShieldCheck size={22} /></div><CardTitle>Codex 运行控制台</CardTitle><p className="muted">管理员登录后控制任务并维护持久化配置</p></CardHeader><CardContent><form onSubmit={submit} className="stack"><label>用户名<Input value={username} onChange={event => setUsername(event.target.value)} autoComplete="username" /></label><label>密码<Input value={password} onChange={event => setPassword(event.target.value)} type="password" autoComplete="current-password" /></label>{error && <p className="error-text" role="alert">{error}</p>}<Button type="submit" disabled={busy || !username || !password}>{busy ? "登录中…" : "登录"}</Button></form></CardContent></Card></main>
}

function statusLabel(state: string) {
  const labels: Record<string, string> = { idle: "未启动", running: "排队中", succeeded: "已成功", stopped: "已停止", requesting: "请求中", waiting_queue: "等待排队", waiting_next: "等待下次", disabled: "未启用", connecting: "连接中", connected: "已连接", reconnecting: "重连中", unauthorized: "鉴权失败", closed: "已断开" }
  return labels[state] || state
}

function activityLabel(type: string) {
  const labels: Record<string, string> = { "queue.start": "开始排队", "queue.stop": "停止排队", "queue.request.success": "排队请求成功", "queue.request.failure": "排队请求失败", "keepalive.start": "开启保活", "keepalive.stop": "停止保活", "keepalive.request.success": "保活请求成功", "keepalive.request.failure": "保活请求失败" }
  return labels[type] || type
}

function stateVariant(state: string): "default" | "secondary" | "outline" | "destructive" {
  if (["running", "requesting", "connected"].includes(state)) return "default"
  if (state === "succeeded") return "secondary"
  if (state === "unauthorized") return "destructive"
  return "outline"
}

function RestartBanner({ fields }: { fields: string[] }) {
  if (!fields.length) return null
  return <div className="restart-banner" role="status"><AlertTriangle size={18} /><div><strong>部分配置等待服务重启后生效</strong><span>{fields.join("、")}</span></div></div>
}

function ConsoleHeader({ username, view, setView, onLogout }: { username: string; view: "dashboard" | "config"; setView: (view: "dashboard" | "config") => void; onLogout: () => void }) {
  return <header className="topbar"><div><div className="eyebrow">CODEX OPERATIONS</div><h1>{view === "dashboard" ? "运行控制台" : "配置管理"}</h1></div><div className="topbar-actions"><Button variant={view === "dashboard" ? "outline" : "ghost"} size="sm" onClick={() => setView("dashboard")}><Activity size={15} />运行状态</Button><Button variant={view === "config" ? "outline" : "ghost"} size="sm" onClick={() => setView("config")}><Settings size={15} />配置</Button><span className="user-chip"><ShieldCheck size={15} />{username}</span><Button variant="ghost" size="sm" onClick={onLogout}><LogOut size={15} />退出</Button></div></header>
}

function TargetCard({ target, selected, toggle, runAction }: { target: Target; selected: boolean; toggle: () => void; runAction: (actionName: string, target: string) => void }) {
  const queueBusy = target.queue.state === "running"
  const keepaliveBusy = target.keepalive.state !== "stopped"
  return <Card className="target-card"><CardHeader className="target-header"><div className="target-title"><Checkbox aria-label={`选择 ${target.name}`} checked={selected} onChange={toggle} /><div><CardTitle>{target.name}</CardTitle><p className="muted">{target.model} · {target.api_host}</p></div></div><div className="target-actions"><Button size="sm" variant={queueBusy ? "destructive" : "default"} onClick={() => runAction(queueBusy ? "queue.stop" : "queue.start", target.name)}>{queueBusy ? <><Pause size={14} />停止排队</> : <><Play size={14} />开始排队</>}</Button><Button size="sm" variant={keepaliveBusy ? "destructive" : "outline"} onClick={() => runAction(keepaliveBusy ? "keepalive.stop" : "keepalive.start", target.name)}>{keepaliveBusy ? <><CircleStop size={14} />停止保活</> : <><Radio size={14} />开启保活</>}</Button></div></CardHeader><CardContent><div className="status-grid"><div className="status-block"><div className="status-heading"><span>排队任务</span><Badge variant={stateVariant(target.queue.state)}>{statusLabel(target.queue.state)}</Badge></div><div className="metrics"><span>尝试 {target.queue.attempts} 次</span><span>下次 {countdown(target.queue.next_attempt)}</span></div><p className="muted small">最近尝试：{formatTime(target.queue.last_attempt)} · 完成：{formatTime(target.queue.finished_at)}</p>{target.queue.last_error && <p className="error-text small"><AlertTriangle size={13} />{target.queue.last_error}</p>}</div><div className="status-block"><div className="status-heading"><span>保活</span><Badge variant={stateVariant(target.keepalive.state)}>{statusLabel(target.keepalive.state)}</Badge></div><div className="metrics"><span>请求 {target.keepalive.requests} 次</span><span>下次 {countdown(target.keepalive.next_request)}</span></div><p className="muted small">最近成功：{formatTime(target.keepalive.last_success)} · 失败：{formatTime(target.keepalive.last_failure)}</p>{target.keepalive.last_error && <p className="error-text small"><AlertTriangle size={13} />{target.keepalive.last_error}</p>}</div></div></CardContent></Card>
}

function ActivityList({ activities }: { activities: ActivityItem[] }) {
  const [targetFilter, setTargetFilter] = useState("all")
  const [kindFilter, setKindFilter] = useState("all")
  const targets = Array.from(new Set(activities.map(item => item.target)))
  const filtered = activities.filter(item => (targetFilter === "all" || item.target === targetFilter) && (kindFilter === "all" || item.type.startsWith(kindFilter)))
  return <Card><CardHeader><div className="section-heading"><CardTitle><Activity size={18} />近期活动</CardTitle><div className="filters"><select value={targetFilter} onChange={event => setTargetFilter(event.target.value)}><option value="all">全部目标</option>{targets.map(target => <option key={target}>{target}</option>)}</select><select value={kindFilter} onChange={event => setKindFilter(event.target.value)}><option value="all">全部类型</option><option value="queue">排队</option><option value="keepalive">保活</option></select></div></div></CardHeader><CardContent><div className="activity-list">{filtered.length === 0 && <p className="muted">暂无活动记录</p>}{filtered.map(item => <div className="activity-row" key={item.id}><div className={`activity-icon ${item.type.includes("failure") ? "danger" : item.type.includes("success") ? "success" : ""}`}>{item.type.includes("failure") ? <XCircle size={15} /> : item.type.includes("success") ? <CheckCircle2 size={15} /> : <Timer size={15} />}</div><div className="activity-copy"><strong>{item.target}</strong><span>{activityLabel(item.type)} · {item.source} · {item.actor}</span>{item.error && <span className="error-text">{item.error}</span>}</div><time>{formatTime(item.at)}</time></div>)}</div></CardContent></Card>
}

function DashboardPage({ csrf, username, onLogout, setView }: { csrf: string; username: string; onLogout: () => void; setView: (view: "dashboard" | "config") => void }) {
  const [data, setData] = useState<Dashboard | null>(null)
  const [connection, setConnection] = useState<Connection>("connecting")
  const [selected, setSelected] = useState<string[]>([])
  const [notice, setNotice] = useState("")
  const [busy, setBusy] = useState(false)
  const [confirmStop, setConfirmStop] = useState(false)
  const [, setClock] = useState(() => Date.now())
  const load = useCallback(async () => {
    try { setData(await dashboard()) }
    catch (err) { if (err instanceof Error && err.message.includes("未登录")) onLogout(); else setNotice(err instanceof Error ? err.message : "加载失败") }
  }, [onLogout])
  useEffect(() => { void load() }, [load])
  useEffect(() => { const timer = window.setInterval(() => setClock(Date.now()), 1000); return () => window.clearInterval(timer) }, [])
  useEffect(() => {
    const source = new EventSource("/api/v1/events")
    setConnection("connecting")
    source.addEventListener("snapshot", event => { setData(JSON.parse((event as MessageEvent).data)); setConnection("connected") })
    source.addEventListener("state", event => { const next = JSON.parse((event as MessageEvent).data); setData(previous => previous ? { ...previous, ...next } : next); setConnection("connected") })
    source.addEventListener("activity", event => { const activity = JSON.parse((event as MessageEvent).data); setData(previous => previous ? { ...previous, activities: [activity, ...previous.activities.filter(item => item.id !== activity.id)].slice(0, 200) } : previous) })
    source.addEventListener("auth_expired", onLogout)
    source.onerror = () => setConnection("reconnecting")
    return () => { source.close(); setConnection("closed") }
  }, [onLogout])
  useEffect(() => { if (data) setSelected(items => items.filter(name => data.targets.some(target => target.name === name))) }, [data?.targets])
  const runAction = async (actionName: string, targets: string[]) => {
    setBusy(true); setNotice("")
    try {
      const result = await action(csrf, actionName, targets)
      setNotice([`已变更 ${result.changed.length}`, `未变更 ${result.unchanged.length}`, `未知 ${result.unknown.length}`].join(" · "))
      await load()
    } catch (err) { setNotice(err instanceof Error ? err.message : "操作失败") } finally { setBusy(false) }
  }
  const selectedAll = Boolean(data && data.targets.length > 0 && selected.length === data.targets.length)
  const selectAll = () => setSelected(selectedAll ? [] : data?.targets.map(target => target.name) || [])
  const visibleTargets = useMemo(() => data?.targets || [], [data])
  if (!data) return <main className="loading-shell"><RefreshCw className="spin" />正在加载控制台…</main>
  return <main className="app-shell"><ConsoleHeader username={username} view="dashboard" setView={setView} onLogout={onLogout} /><RestartBanner fields={data.restart_fields || []} /><section className="overview-grid"><Card><CardContent className="overview-card"><div className="overview-icon"><Wifi size={19} /></div><div><span className="muted">SSE 连接</span><strong>{statusLabel(connection)}</strong></div></CardContent></Card><Card><CardContent className="overview-card"><div className="overview-icon"><Radio size={19} /></div><div><span className="muted">OpenILink</span><strong>{statusLabel(data.openilink.state)}</strong>{data.openilink.error && <small className="error-text">{data.openilink.error}</small>}</div></CardContent></Card><Card><CardContent className="overview-card"><div className="overview-icon"><ArrowUp size={19} /></div><div><span className="muted">并发进程</span><strong>{data.concurrency.current} / {data.concurrency.max}</strong></div></CardContent></Card><Card><CardContent className="overview-card"><div className="overview-icon"><Activity size={19} /></div><div><span className="muted">服务 / 配置版本</span><strong>{data.version} · r{data.config_revision}</strong></div></CardContent></Card></section><section className="toolbar"><div><h2>目标</h2><p className="muted">实时查看状态并控制排队与保活任务。</p></div>{data.targets.length > 0 && <div className="toolbar-actions"><Button variant="outline" size="sm" onClick={selectAll}>{selectedAll ? "取消全选" : "全选"}</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("queue.start", selected)}><Play size={14} />开始排队</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("queue.stop", selected)}><Pause size={14} />停止排队</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("keepalive.start", selected)}><Radio size={14} />开启保活</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("keepalive.stop", selected)}><CircleStop size={14} />停止保活</Button><AlertDialog open={confirmStop} onOpenChange={setConfirmStop} title="停止所有目标？" description="将停止全部目标的排队和保活。此操作不会修改配置。" onConfirm={() => { void (async () => { await runAction("queue.stop", []); await runAction("keepalive.stop", []) })() }}><Button variant="destructive" size="sm" disabled={busy}><CircleStop size={14} />停止全部</Button></AlertDialog></div>}</section>{notice && <div className="notice" role="status">{notice}</div>}{visibleTargets.length === 0 ? <Card className="empty-state"><CardContent><Database size={34} /><h2>还没有 target</h2><p className="muted">先添加一个 API 目标，保存后会立即出现在这里，无需重启。</p><Button onClick={() => setView("config")}><Plus size={15} />前往配置 target</Button></CardContent></Card> : <section className="target-list">{visibleTargets.map(target => <TargetCard key={target.id || target.name} target={target} selected={selected.includes(target.name)} toggle={() => setSelected(items => items.includes(target.name) ? items.filter(item => item !== target.name) : [...items, target.name])} runAction={(name, targetName) => void runAction(name, [targetName])} />)}</section>}<ActivityList activities={data.activities} /><footer><span>最后更新：{formatTime(data.generated_at)}</span><span>运行状态只展示 API 主机名；完整 URL 仅在鉴权后的配置页可见。</span></footer></main>
}

const emptyTarget: TargetInput = { name: "", api_base_url: "", api_key: "", model: "", wire_api: "responses", config_overrides: [] }

function TargetEditor({ initial, onCancel, onSave, busy }: { initial?: ConfigurationTarget; onCancel: () => void; onSave: (value: TargetInput) => Promise<void>; busy: boolean }) {
  const [value, setValue] = useState<TargetInput>(() => initial ? { sort_order: initial.sort_order, name: initial.name, api_base_url: initial.api_base_url, api_key: "", model: initial.model, wire_api: initial.wire_api, config_overrides: initial.config_overrides } : { ...emptyTarget })
  const [overrides, setOverrides] = useState((initial?.config_overrides || []).join("\n"))
  const [error, setError] = useState("")
  async function submit(event: FormEvent) {
    event.preventDefault()
    if (!initial && !value.api_key.trim()) return setError("创建 target 时必须提供 API Key")
    setError("")
    await onSave({ ...value, config_overrides: splitLines(overrides) })
  }
  return <Card className="editor-card"><CardHeader><CardTitle>{initial ? `编辑 ${initial.name}` : "添加 target"}</CardTitle><p className="muted">名称大小写不敏感唯一；运行中的 target 暂不能编辑。</p></CardHeader><CardContent><form className="form-grid" onSubmit={submit}><label>名称<Input value={value.name} onChange={event => setValue({ ...value, name: event.target.value })} /></label><label>排序<Input type="number" min={0} value={value.sort_order ?? ""} placeholder="自动追加" onChange={event => setValue({ ...value, sort_order: event.target.value === "" ? undefined : Number(event.target.value) })} /></label><label className="span-2">API Base URL<Input value={value.api_base_url} onChange={event => setValue({ ...value, api_base_url: event.target.value })} placeholder="https://api.example.com/v1" /></label><label>模型<Input value={value.model} onChange={event => setValue({ ...value, model: event.target.value })} /></label><label>Wire API<select value={value.wire_api} onChange={event => setValue({ ...value, wire_api: event.target.value })}><option value="responses">responses</option></select></label><label className="span-2">API Key<Input type="password" value={value.api_key} onChange={event => setValue({ ...value, api_key: event.target.value })} placeholder={initial?.api_key_set ? "已配置；留空保持不变" : "创建时必填"} /><small>{initial?.api_key_set ? "密钥已配置，服务器不会回显。" : "保存时将使用 AES-256-GCM 加密。"}</small></label><label className="span-2">Target Codex overrides<textarea value={overrides} onChange={event => setOverrides(event.target.value)} placeholder="每行一个 key=value" /></label>{error && <p className="error-text span-2" role="alert">{error}</p>}<div className="form-actions span-2"><Button type="button" variant="outline" onClick={onCancel}>取消</Button><Button type="submit" disabled={busy}><Save size={15} />{busy ? "保存中…" : "保存 target"}</Button></div></form></CardContent></Card>
}

function ConfigNavigation({ section, setSection }: { section: ConfigSection; setSection: (section: ConfigSection) => void }) {
  const items: Array<[ConfigSection, string, typeof Database]> = [["targets", "Targets", Database], ["tasks", "任务与保活", SlidersHorizontal], ["openilink", "OpenILink", Link2], ["runtime", "Web / 运行文件", Server], ["account", "管理员账号", UserCog]]
  return <nav className="config-nav">{items.map(([key, label, Icon]) => <button key={key} className={section === key ? "active" : ""} onClick={() => setSection(key)}><Icon size={17} />{label}</button>)}</nav>
}

function ConfigurationPage({ csrf, username, onLogout, setView }: { csrf: string; username: string; onLogout: () => void; setView: (view: "dashboard" | "config") => void }) {
  const [data, setData] = useState<Configuration | null>(null)
  const [section, setSection] = useState<ConfigSection>("targets")
  const [notice, setNotice] = useState("")
  const [error, setError] = useState("")
  const [busy, setBusy] = useState(false)
  const [editing, setEditing] = useState<ConfigurationTarget | "new" | null>(null)
  const [token, setToken] = useState("")
  const [clearToken, setClearToken] = useState(false)
  const [account, setAccount] = useState({ username, current: "", password: "", confirm: "" })
  const load = useCallback(async () => {
    try { const result = await getConfiguration(); setData(result); setAccount(value => ({ ...value, username: result.account.username })) }
    catch (err) { setError(err instanceof Error ? err.message : "加载配置失败") }
  }, [])
  useEffect(() => { void load() }, [load])
  const applyResult = (result: Configuration, message: string) => { setData(result); setNotice(message); setError("") }
  const runSave = async (operation: () => Promise<Configuration>, message: string) => {
    setBusy(true); setNotice(""); setError("")
    try { applyResult(await operation(), message) } catch (err) { setError(err instanceof Error ? err.message : "保存失败") } finally { setBusy(false) }
  }
  if (!data) return <main className="loading-shell"><RefreshCw className="spin" />正在加载配置…{error && <span className="error-text">{error}</span>}</main>
  const saveCodex = () => {
    if (data.codex.retry_min_seconds > data.codex.retry_max_seconds) return setError("重试最小秒数不能大于最大秒数")
    if (data.codex.keepalive_min_seconds > data.codex.keepalive_max_seconds) return setError("保活最小秒数不能大于最大秒数")
    void runSave(() => updateCodex(csrf, data.codex), "Codex 配置已保存")
  }
  const codexField = <K extends keyof Configuration["codex"]>(key: K, value: Configuration["codex"][K]) => setData({ ...data, codex: { ...data.codex, [key]: value } })
  const webField = <K extends keyof Configuration["web"]>(key: K, value: Configuration["web"][K]) => setData({ ...data, web: { ...data.web, [key]: value } })
  const openField = <K extends keyof Configuration["openilink"]>(key: K, value: Configuration["openilink"][K]) => setData({ ...data, openilink: { ...data.openilink, [key]: value } })
  const saveTarget = async (value: TargetInput) => {
    setBusy(true); setError("")
    try {
      const result = editing === "new" ? await createTarget(csrf, value) : await updateTarget(csrf, (editing as ConfigurationTarget).id, value)
      applyResult(result, editing === "new" ? "target 已添加并立即生效" : "target 已更新并立即生效")
      setEditing(null)
    } catch (err) { setError(err instanceof Error ? err.message : "保存 target 失败") } finally { setBusy(false) }
  }
  return <main className="app-shell"><ConsoleHeader username={username} view="config" setView={setView} onLogout={onLogout} /><RestartBanner fields={data.restart_fields} /><div className="config-layout"><ConfigNavigation section={section} setSection={value => { setSection(value); setEditing(null); setNotice(""); setError("") }} /><section className="config-content"><div className="config-content-header"><Button variant="ghost" size="sm" onClick={() => setView("dashboard")}><ArrowLeft size={15} />返回运行状态</Button><span className="muted">配置版本 r{data.revision}</span></div>{notice && <div className="notice" role="status">{notice}</div>}{error && <div className="error-banner" role="alert"><AlertTriangle size={16} />{error}</div>}
    {section === "targets" && <div className="stack-lg"><div className="section-heading"><div><h2>Targets</h2><p className="muted">空闲 target 的新增、修改、重命名与删除会立即生效。</p></div><Button onClick={() => setEditing("new")} disabled={editing !== null}><Plus size={15} />添加 target</Button></div>{editing && <TargetEditor initial={editing === "new" ? undefined : editing} onCancel={() => setEditing(null)} onSave={saveTarget} busy={busy} />}{data.targets.length === 0 ? <Card className="empty-state compact"><CardContent><Database size={30} /><h2>暂无 target</h2><p className="muted">可以先完成其他配置，稍后再添加 API 目标。</p></CardContent></Card> : <div className="config-target-list">{data.targets.map(target => <Card key={target.id}><CardContent className="config-target-row"><div><div className="target-name-line"><strong>{target.name}</strong>{target.busy && <Badge variant="outline">运行中</Badge>}<Badge variant={target.api_key_set ? "secondary" : "destructive"}>{target.api_key_set ? "密钥已配置" : "缺少密钥"}</Badge></div><span>{target.model} · {target.api_base_url}</span><small>排序 {target.sort_order} · {target.wire_api}</small></div><div className="target-actions"><Button size="sm" variant="outline" disabled={target.busy || editing !== null} onClick={() => setEditing(target)}><Pencil size={14} />编辑</Button><Button size="sm" variant="destructive" disabled={target.busy || busy} onClick={() => { if (window.confirm(`删除 target ${target.name}？`)) void runSave(() => deleteTarget(csrf, target.id), "target 已删除") }}><Trash2 size={14} />删除</Button></div></CardContent></Card>)}</div>}</div>}
    {section === "tasks" && <div className="stack-lg"><div><h2>任务与保活</h2><p className="muted">保存后，新请求立即采用新参数；等待中的重试和保活会从保存时刻重新随机倒计时。</p></div><Card><CardContent className="form-grid padded"><label>请求超时（秒）<Input type="number" min={1} value={data.codex.request_timeout_seconds} onChange={event => codexField("request_timeout_seconds", Number(event.target.value))} /></label><label>最大并发<Input type="number" min={1} value={data.codex.max_parallel} onChange={event => codexField("max_parallel", Number(event.target.value))} /></label><label>重试最小秒数<Input type="number" min={1} value={data.codex.retry_min_seconds} onChange={event => codexField("retry_min_seconds", Number(event.target.value))} /></label><label>重试最大秒数<Input type="number" min={1} value={data.codex.retry_max_seconds} onChange={event => codexField("retry_max_seconds", Number(event.target.value))} /></label><label>保活最小秒数<Input type="number" min={1} value={data.codex.keepalive_min_seconds} onChange={event => codexField("keepalive_min_seconds", Number(event.target.value))} /></label><label>保活最大秒数<Input type="number" min={1} value={data.codex.keepalive_max_seconds} onChange={event => codexField("keepalive_max_seconds", Number(event.target.value))} /></label><label>推理强度<select value={data.codex.reasoning_effort} onChange={event => codexField("reasoning_effort", event.target.value)}><option value="low">low</option><option value="medium">medium</option><option value="high">high</option><option value="xhigh">xhigh</option></select></label><label>成功消息<Input value={data.codex.success_message} onChange={event => codexField("success_message", event.target.value)} /></label><label className="span-2">全局 Codex overrides<textarea value={data.codex.config_overrides.join("\n")} onChange={event => codexField("config_overrides", splitLines(event.target.value))} placeholder="每行一个 key=value" /></label><div className="form-actions span-2"><Button onClick={saveCodex} disabled={busy}><Save size={15} />保存任务参数</Button></div></CardContent></Card></div>}
    {section === "openilink" && <div className="stack-lg"><div><h2>OpenILink</h2><p className="muted">连接配置属于启动级设置，保存后需要重启服务。</p></div><Card><CardContent className="form-grid padded"><label className="checkbox-label span-2"><Checkbox checked={data.openilink.enabled} onChange={event => { openField("enabled", event.target.checked); if (event.target.checked) setClearToken(false) }} />启用 OpenILink</label><label className="span-2">Base URL<Input value={data.openilink.base_url} onChange={event => openField("base_url", event.target.value)} /></label><label>HTTP 超时（秒）<Input type="number" min={1} value={data.openilink.http_timeout_seconds} onChange={event => openField("http_timeout_seconds", Number(event.target.value))} /></label><label>Token<Input type="password" value={token} onChange={event => setToken(event.target.value)} placeholder={data.openilink.token_set ? "已配置；留空保持不变" : "启用前必须配置"} /><small>{data.openilink.token_set ? "Token 已加密保存" : "尚未配置 Token"}</small></label><label className="span-2">允许的用户 ID<textarea value={data.openilink.allowed_user_ids.join("\n")} onChange={event => openField("allowed_user_ids", splitLines(event.target.value))} placeholder="每行一个用户 ID" /></label><label className="checkbox-label span-2"><Checkbox checked={clearToken} disabled={data.openilink.enabled} onChange={event => setClearToken(event.target.checked)} />显式清除已保存 Token（仅关闭状态可用）</label><div className="form-actions span-2"><Button disabled={busy} onClick={() => void runSave(async () => { const result = await updateOpenILink(csrf, { enabled: data.openilink.enabled, base_url: data.openilink.base_url, token, clear_token: clearToken, allowed_user_ids: data.openilink.allowed_user_ids, http_timeout_seconds: data.openilink.http_timeout_seconds }); setToken(""); setClearToken(false); return result }, "OpenILink 配置已保存，重启后生效")}><Save size={15} />保存 OpenILink</Button></div></CardContent></Card></div>}
    {section === "runtime" && <div className="stack-lg"><div><h2>Web / 运行文件</h2><p className="muted">监听、Cookie、代理、Codex binary 与 prompts 路径均在重启后应用；活动记录上限立即生效。</p></div><Card><CardHeader><CardTitle>Codex 运行文件</CardTitle></CardHeader><CardContent className="form-grid"><label>Codex binary<Input value={data.codex.binary} onChange={event => codexField("binary", event.target.value)} /></label><label>Prompts 文件<Input value={data.codex.prompts_file} onChange={event => codexField("prompts_file", event.target.value)} /></label><div className="form-actions span-2"><Button disabled={busy} onClick={saveCodex}><Save size={15} />保存运行文件</Button></div></CardContent></Card><Card><CardHeader><CardTitle>Web 服务</CardTitle></CardHeader><CardContent className="form-grid"><label>监听地址<Input value={data.web.listen_address} onChange={event => webField("listen_address", event.target.value)} /></label><label>活动记录上限<Input type="number" min={0} value={data.web.activity_limit} onChange={event => webField("activity_limit", Number(event.target.value))} /></label><label className="checkbox-label span-2"><Checkbox checked={data.web.cookie_secure} onChange={event => webField("cookie_secure", event.target.checked)} />仅通过 HTTPS 发送登录 Cookie</label><label className="span-2">Trusted proxies<textarea value={data.web.trusted_proxies.join("\n")} onChange={event => webField("trusted_proxies", splitLines(event.target.value))} placeholder="每行一个 IP 或 CIDR" /></label><div className="form-actions span-2"><Button disabled={busy} onClick={() => void runSave(() => updateWeb(csrf, data.web), "Web 配置已保存")}><Save size={15} />保存 Web 配置</Button></div></CardContent></Card></div>}
    {section === "account" && <div className="stack-lg"><div><h2>管理员账号</h2><p className="muted">修改用户名或密码会撤销全部会话，保存后需要重新登录。</p></div><Card><CardContent className="form-grid padded"><label className="span-2">用户名<Input value={account.username} onChange={event => setAccount({ ...account, username: event.target.value })} /></label><label>当前密码（可选确认）<Input type="password" value={account.current} onChange={event => setAccount({ ...account, current: event.target.value })} autoComplete="current-password" /></label><span /><label>新密码<Input type="password" value={account.password} onChange={event => setAccount({ ...account, password: event.target.value })} autoComplete="new-password" placeholder="留空表示不修改" /></label><label>确认新密码<Input type="password" value={account.confirm} onChange={event => setAccount({ ...account, confirm: event.target.value })} autoComplete="new-password" /></label><div className="form-actions span-2"><Button disabled={busy} onClick={() => { if (account.password && Array.from(account.password).length < minimumAdminPasswordLength) return setError(`新密码至少需要 ${minimumAdminPasswordLength} 个字符`); if (account.password !== account.confirm) return setError("两次输入的新密码不一致"); setBusy(true); setError(""); updateAccount(csrf, { username: account.username, current_password: account.current, new_password: account.password }).then(() => onLogout()).catch(err => setError(err instanceof Error ? err.message : "账号保存失败")).finally(() => setBusy(false)) }}><Save size={15} />保存并重新登录</Button></div></CardContent></Card></div>}
  </section></div></main>
}

function splitLines(value: string) {
  return value.split(/\r?\n/).map(item => item.trim()).filter(Boolean)
}

function AuthenticatedApp({ auth, onLogout }: { auth: Auth; onLogout: () => void }) {
  const [view, setView] = useState<"dashboard" | "config">("dashboard")
  return view === "dashboard" ? <DashboardPage csrf={auth.csrf} username={auth.username} onLogout={onLogout} setView={setView} /> : <ConfigurationPage csrf={auth.csrf} username={auth.username} onLogout={onLogout} setView={setView} />
}

export default function App() {
  const [booting, setBooting] = useState(true)
  const [setupRequired, setSetupRequired] = useState(false)
  const [suggestedUsername, setSuggestedUsername] = useState("admin")
  const [auth, setAuth] = useState<Auth | null>(null)
  useEffect(() => {
    setupStatus().then(status => {
      setSetupRequired(status.required)
      setSuggestedUsername(status.suggested_username || "admin")
      if (status.required) return
      return session().then(result => setAuth({ csrf: result.csrf_token, username: result.username })).catch(() => undefined)
    }).catch(() => undefined).finally(() => setBooting(false))
  }, [])
  const doLogout = useCallback(async () => { if (auth) await logout(auth.csrf).catch(() => undefined); setAuth(null) }, [auth])
  if (booting) return <main className="loading-shell"><RefreshCw className="spin" />正在检查初始化状态…</main>
  if (setupRequired && !auth) return <SetupPage suggested={suggestedUsername} onComplete={next => { setSetupRequired(false); setAuth(next) }} />
  if (!auth) return <LoginPage onLoggedIn={setAuth} />
  return <AuthenticatedApp auth={auth} onLogout={doLogout} />
}
