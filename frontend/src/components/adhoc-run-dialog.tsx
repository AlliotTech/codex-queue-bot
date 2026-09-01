import * as DialogPrimitive from "@radix-ui/react-dialog"
import { useEffect, useRef, useState, type FormEvent } from "react"
import { CircleStop, RefreshCw, Send, SquareTerminal } from "lucide-react"
import { runAdhoc, type AdhocRunResult, type Target } from "../lib/api"
import { Badge } from "./ui/badge"
import { Button } from "./ui/button"

export function AdhocRunDialog({ target, open, onOpenChange }: {
  target: Target | null
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const [prompt, setPrompt] = useState("")
  const [result, setResult] = useState<AdhocRunResult | null>(null)
  const [error, setError] = useState("")
  const [busy, setBusy] = useState(false)
  const controller = useRef<AbortController | null>(null)

  useEffect(() => {
    if (!open) return
    setPrompt("")
    setResult(null)
    setError("")
    setBusy(false)
    controller.current?.abort()
    controller.current = null
  }, [open, target?.id])

  useEffect(() => () => controller.current?.abort(), [])

  if (!target) return null

  const changeOpen = (next: boolean) => {
    if (!next) controller.current?.abort()
    onOpenChange(next)
  }

  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (!prompt.trim()) {
      setError("Prompt 不能为空")
      return
    }
    const nextController = new AbortController()
    controller.current = nextController
    setBusy(true)
    setError("")
    setResult(null)
    try {
      setResult(await runAdhoc(target.id, prompt, nextController.signal))
    } catch (err) {
      if (err instanceof DOMException && err.name === "AbortError") setError("请求已取消")
      else setError(err instanceof Error ? err.message : "手动请求失败")
    } finally {
      if (controller.current === nextController) controller.current = null
      setBusy(false)
    }
  }

  return <DialogPrimitive.Root open={open} onOpenChange={changeOpen}>
    <DialogPrimitive.Portal>
      <DialogPrimitive.Overlay className="dialog-overlay" />
      <DialogPrimitive.Content className="dialog-content adhoc-dialog">
        <div className="adhoc-dialog-heading">
          <div className="adhoc-dialog-icon"><SquareTerminal size={19} /></div>
          <div>
            <DialogPrimitive.Title className="dialog-title">手动请求 · {target.name}</DialogPrimitive.Title>
            <DialogPrimitive.Description className="dialog-description">输入一次性 Prompt。请求使用该 target 的模型与 API 配置，并计入全局并发上限。</DialogPrimitive.Description>
          </div>
        </div>
        <form className="adhoc-form" onSubmit={submit}>
          <label htmlFor="adhoc-prompt">Prompt</label>
          <textarea id="adhoc-prompt" aria-label="Prompt" value={prompt} onChange={event => setPrompt(event.target.value)} placeholder="输入要发送给 Codex 的 Prompt" maxLength={32768} disabled={busy} autoFocus />
          {error && <div className="error-banner" role="alert">{error}</div>}
          {result && <section className="adhoc-result" aria-label="手动请求结果">
            <div className="adhoc-result-summary">
              <Badge variant={result.success ? "secondary" : "destructive"}>{result.success ? "成功" : "失败"}</Badge>
              <span>退出码 <strong>{result.exit_code}</strong></span>
              <span>耗时 <strong>{(result.duration_ms / 1000).toFixed(2)}s</strong></span>
            </div>
            <div className="adhoc-output-block">
              <h3>最终输出</h3>
              <pre>{result.output || "（无最终输出）"}</pre>
            </div>
            {result.error && <div className="adhoc-output-block error-output"><h3>错误</h3><pre>{result.error}</pre></div>}
            {result.process_output && <details className="adhoc-process-output"><summary>Codex CLI 输出</summary><pre>{result.process_output}</pre></details>}
          </section>}
          <div className="dialog-actions">
            <DialogPrimitive.Close asChild><Button type="button" variant="outline">关闭</Button></DialogPrimitive.Close>
            {busy ? <Button type="button" variant="destructive" onClick={() => controller.current?.abort()}><CircleStop size={14} />终止请求</Button> : <Button type="submit" disabled={!prompt.trim()}><Send size={14} />执行请求</Button>}
          </div>
          {busy && <p className="adhoc-running" role="status"><RefreshCw className="spin" size={14} />Codex 正在运行，请等待结果…</p>}
        </form>
      </DialogPrimitive.Content>
    </DialogPrimitive.Portal>
  </DialogPrimitive.Root>
}
