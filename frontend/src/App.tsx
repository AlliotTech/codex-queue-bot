import { useCallback, useEffect, useMemo, useState } from "react"
import { Activity, AlertTriangle, ArrowUp, CheckCircle2, CircleStop, LogOut, Pause, Play, Radio, RefreshCw, ShieldCheck, Timer, Wifi, XCircle } from "lucide-react"
import { action, dashboard, login, logout, session, type Activity as ActivityItem, type Dashboard, type Target } from "./lib/api"
import { Button } from "./components/ui/button"
import { Badge } from "./components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "./components/ui/card"
import { Checkbox } from "./components/ui/checkbox"
import { Input } from "./components/ui/input"
import { AlertDialog } from "./components/ui/alert-dialog"
import { countdown, formatTime } from "./lib/utils"
import "./styles.css"

type Connection = "connecting" | "connected" | "reconnecting" | "closed"

function LoginPage({ onLoggedIn }: { onLoggedIn: (csrf: string, username: string) => void }) {
  const [username, setUsername] = useState("admin")
  const [password, setPassword] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState("")
  async function submit(event: React.FormEvent) {
    event.preventDefault(); setBusy(true); setError("")
    try { const result = await login(username, password); onLoggedIn(result.csrf_token, result.username) } catch (err) { setError(err instanceof Error ? err.message : "登录失败") } finally { setBusy(false) }
  }
  return <main className="login-shell"><Card className="login-card"><CardHeader><div className="brand-mark"><ShieldCheck size={22} /></div><CardTitle>Codex 运行控制台</CardTitle><p className="muted">管理员登录后控制排队与保活任务</p></CardHeader><CardContent><form onSubmit={submit} className="stack"><label>用户名<Input value={username} onChange={e => setUsername(e.target.value)} autoComplete="username" /></label><label>密码<Input value={password} onChange={e => setPassword(e.target.value)} type="password" autoComplete="current-password" /></label>{error && <p className="error-text" role="alert">{error}</p>}<Button type="submit" disabled={busy || !username || !password}>{busy ? "登录中…" : "登录"}</Button></form></CardContent></Card></main>
}

function statusLabel(state: string) {
  const labels: Record<string, string> = { idle: "未启动", running: "排队中", succeeded: "已成功", stopped: "已停止", requesting: "请求中", waiting_queue: "等待排队", waiting_next: "等待下次", disabled: "未启用", connecting: "连接中", connected: "已连接", reconnecting: "重连中", unauthorized: "鉴权失败", closed: "已断开" }
  return labels[state] || state
}

function activityLabel(type: string) {
  const labels: Record<string, string> = { "queue.start": "开始排队", "queue.stop": "停止排队", "queue.request.success": "排队请求成功", "queue.request.failure": "排队请求失败", "keepalive.start": "开启保活", "keepalive.stop": "停止保活", "keepalive.request.success": "保活请求成功", "keepalive.request.failure": "保活请求失败" }
  return labels[type] || type
}

function stateVariant(state: string): "default" | "secondary" | "outline" | "destructive" { if (["running", "requesting", "connected"].includes(state)) return "default"; if (["succeeded"].includes(state)) return "secondary"; if (["unauthorized"].includes(state)) return "destructive"; return "outline" }

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
  return <Card><CardHeader><div className="section-heading"><CardTitle><Activity size={18} />近期活动</CardTitle><div className="filters"><select value={targetFilter} onChange={e => setTargetFilter(e.target.value)}><option value="all">全部目标</option>{targets.map(target => <option key={target}>{target}</option>)}</select><select value={kindFilter} onChange={e => setKindFilter(e.target.value)}><option value="all">全部类型</option><option value="queue">排队</option><option value="keepalive">保活</option></select></div></div></CardHeader><CardContent><div className="activity-list">{filtered.length === 0 && <p className="muted">暂无活动记录</p>}{filtered.map(item => <div className="activity-row" key={item.id}><div className={`activity-icon ${item.type.includes("failure") ? "danger" : item.type.includes("success") ? "success" : ""}`}>{item.type.includes("failure") ? <XCircle size={15} /> : item.type.includes("success") ? <CheckCircle2 size={15} /> : <Timer size={15} />}</div><div className="activity-copy"><strong>{item.target}</strong><span>{activityLabel(item.type)} · {item.source} · {item.actor}</span>{item.error && <span className="error-text">{item.error}</span>}</div><time>{formatTime(item.at)}</time></div>)}</div></CardContent></Card>
}

function DashboardPage({ csrf, username, onLogout }: { csrf: string; username: string; onLogout: () => void }) {
  const [data, setData] = useState<Dashboard | null>(null)
  const [connection, setConnection] = useState<Connection>("connecting")
  const [selected, setSelected] = useState<string[]>([])
  const [notice, setNotice] = useState("")
  const [busy, setBusy] = useState(false)
  const [confirmStop, setConfirmStop] = useState(false)
  const [, setClock] = useState(() => Date.now())
  const load = useCallback(async () => { try { setData(await dashboard()) } catch (err) { if (err instanceof Error && err.message.includes("未登录")) onLogout(); else setNotice(err instanceof Error ? err.message : "加载失败") } }, [onLogout])
  useEffect(() => { void load() }, [load])
  useEffect(() => { const timer = window.setInterval(() => setClock(Date.now()), 1000); return () => window.clearInterval(timer) }, [])
  useEffect(() => {
    const source = new EventSource("/api/v1/events")
    setConnection("connecting")
    source.addEventListener("snapshot", event => { setData(JSON.parse((event as MessageEvent).data)); setConnection("connected") })
    source.addEventListener("state", event => { const next = JSON.parse((event as MessageEvent).data); setData(prev => prev ? { ...prev, ...next } : next); setConnection("connected") })
    source.addEventListener("activity", event => { const activity = JSON.parse((event as MessageEvent).data); setData(prev => prev ? { ...prev, activities: [activity, ...prev.activities.filter(item => item.id !== activity.id)].slice(0, 200) } : prev) })
    source.addEventListener("auth_expired", () => onLogout())
    source.onerror = () => setConnection("reconnecting")
    return () => { source.close(); setConnection("closed") }
  }, [])
  const runAction = async (actionName: string, targets: string[]) => { setBusy(true); setNotice(""); try { const result = await action(csrf, actionName, targets); const text = [`已变更 ${result.changed.length}`, `未变更 ${result.unchanged.length}`, `未知 ${result.unknown.length}`].join(" · "); setNotice(text); await load() } catch (err) { setNotice(err instanceof Error ? err.message : "操作失败") } finally { setBusy(false) } }
  const selectedAll = data && data.targets.length > 0 && selected.length === data.targets.length
  const selectAll = () => setSelected(selectedAll ? [] : data?.targets.map(target => target.name) || [])
  const visibleTargets = useMemo(() => data?.targets || [], [data])
  if (!data) return <main className="loading-shell"><RefreshCw className="spin" />正在加载控制台…</main>
  return <main className="app-shell"><header className="topbar"><div><div className="eyebrow">CODEX OPERATIONS</div><h1>运行控制台</h1></div><div className="topbar-actions"><span className="user-chip"><ShieldCheck size={15} />{username}</span><Button variant="ghost" size="sm" onClick={onLogout}><LogOut size={15} />退出</Button></div></header><section className="overview-grid"><Card><CardContent className="overview-card"><div className="overview-icon"><Wifi size={19} /></div><div><span className="muted">SSE 连接</span><strong>{statusLabel(connection)}</strong></div></CardContent></Card><Card><CardContent className="overview-card"><div className="overview-icon"><Radio size={19} /></div><div><span className="muted">OpenILink</span><strong>{statusLabel(data.openilink.state)}</strong>{data.openilink.error && <small className="error-text">{data.openilink.error}</small>}</div></CardContent></Card><Card><CardContent className="overview-card"><div className="overview-icon"><ArrowUp size={19} /></div><div><span className="muted">并发进程</span><strong>{data.concurrency.current} / {data.concurrency.max}</strong></div></CardContent></Card><Card><CardContent className="overview-card"><div className="overview-icon"><Activity size={19} /></div><div><span className="muted">服务版本</span><strong>{data.version}</strong></div></CardContent></Card></section><section className="toolbar"><div><h2>目标</h2><p className="muted">实时查看状态并控制任务；配置和密钥仍由服务端维护。</p></div><div className="toolbar-actions"><Button variant="outline" size="sm" onClick={selectAll}>{selectedAll ? "取消全选" : "全选"}</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("queue.start", selected)}><Play size={14} />开始排队</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("queue.stop", selected)}><Pause size={14} />停止排队</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("keepalive.start", selected)}><Radio size={14} />开启保活</Button><Button variant="outline" size="sm" disabled={!selected.length || busy} onClick={() => runAction("keepalive.stop", selected)}><CircleStop size={14} />停止保活</Button><AlertDialog open={confirmStop} onOpenChange={setConfirmStop} title="停止所有目标？" description="将停止全部目标的排队和保活。此操作不会修改配置。" onConfirm={() => { void (async () => { await runAction("queue.stop", []); await runAction("keepalive.stop", []) })() }}><Button variant="destructive" size="sm" disabled={busy}><CircleStop size={14} />停止全部</Button></AlertDialog></div></section>{notice && <div className="notice" role="status">{notice}</div>}<section className="target-list">{visibleTargets.map(target => <TargetCard key={target.name} target={target} selected={selected.includes(target.name)} toggle={() => setSelected(items => items.includes(target.name) ? items.filter(item => item !== target.name) : [...items, target.name])} runAction={(name, targetName) => void runAction(name, [targetName])} />)}</section><ActivityList activities={data.activities} /><footer><span>最后更新：{formatTime(data.generated_at)}</span><span>控制台仅提供运行控制，不显示密钥或完整 API URL。</span></footer></main>
}

export default function App() {
  const [auth, setAuth] = useState<{ csrf: string; username: string } | null>(null)
  useEffect(() => { session().then(result => setAuth({ csrf: result.csrf_token, username: result.username })).catch(() => undefined) }, [])
  const doLogout = useCallback(async () => { if (auth) await logout(auth.csrf).catch(() => undefined); setAuth(null) }, [auth])
  if (!auth) return <LoginPage onLoggedIn={(csrf, username) => setAuth({ csrf, username })} />
  return <DashboardPage csrf={auth.csrf} username={auth.username} onLogout={doLogout} />
}
