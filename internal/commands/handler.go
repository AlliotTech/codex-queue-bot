package commands

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"

	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
)

type Handler struct {
	manager *jobs.Manager
	sender  jobs.Messenger
	logger  *slog.Logger
	allowed map[string]struct{}
}

func New(manager *jobs.Manager, sender jobs.Messenger, logger *slog.Logger, allowedUserIDs []string) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	allowed := make(map[string]struct{}, len(allowedUserIDs))
	for _, id := range allowedUserIDs {
		if id = strings.TrimSpace(id); id != "" {
			allowed[id] = struct{}{}
		}
	}
	return &Handler{manager: manager, sender: sender, logger: logger, allowed: allowed}
}

func (h *Handler) Handle(ctx context.Context, incoming hub.Incoming) {
	command, args, ok := parseCommand(incoming.Text)
	if !ok {
		return
	}
	if !h.isAllowed(incoming.SenderID) {
		h.logger.Warn("ignored command from unauthorized OpenILink user", "sender", incoming.SenderID, "command", command)
		go h.reply(ctx, incoming, "无权限执行此命令。")
		return
	}

	targets := splitTargets(args)
	var response string
	switch command {
	case "开挤", "start", "go":
		response = h.start(targets, incoming)
	case "停止", "停挤", "stop":
		response = h.stopFrom(targets, incoming)
	case "状态", "status":
		response = h.status(targets)
	case "保活", "keepalive":
		response = h.startKeepaliveFrom(targets, incoming)
	case "停止保活", "stop-keepalive":
		response = h.stopKeepaliveFrom(targets, incoming)
	case "保活状态", "keepalive-status":
		response = h.keepaliveStatus(targets)
	case "列表", "目标", "list", "targets":
		response = h.list()
	case "帮助", "help":
		response = helpText()
	default:
		return
	}
	go h.reply(ctx, incoming, response)
}

func (h *Handler) start(targets []string, incoming hub.Incoming) string {
	result := h.manager.StartWithOperation(
		targets,
		jobs.Subscriber{Recipient: incoming.SenderID, TraceID: incoming.TraceID},
		jobs.Operation{Source: jobs.SourceOpenILink, Actor: incoming.SenderID},
	)
	parts := make([]string, 0, 3)
	if len(result.Started) > 0 {
		parts = append(parts, "已开始："+strings.Join(result.Started, "、"))
	}
	if len(result.Already) > 0 {
		parts = append(parts, "正在进行并已订阅通知："+strings.Join(result.Already, "、"))
	}
	if len(result.Unknown) > 0 {
		parts = append(parts, "未知目标："+strings.Join(result.Unknown, "、"))
	}
	if len(parts) == 0 {
		return "没有可启动的目标。"
	}
	return strings.Join(parts, "\n")
}

func (h *Handler) stopFrom(targets []string, incoming hub.Incoming) string {
	result := h.manager.StopWithOperation(targets, jobs.Operation{Source: jobs.SourceOpenILink, Actor: incoming.SenderID})
	return formatStopResult(result)
}

func (h *Handler) stop(targets []string) string {
	return formatStopResult(h.manager.Stop(targets))
}

func formatStopResult(result jobs.StopResult) string {
	parts := make([]string, 0, 3)
	if len(result.Stopped) > 0 {
		parts = append(parts, "已停止："+strings.Join(result.Stopped, "、"))
	}
	if len(result.Inactive) > 0 {
		parts = append(parts, "未在运行："+strings.Join(result.Inactive, "、"))
	}
	if len(result.Unknown) > 0 {
		parts = append(parts, "未知目标："+strings.Join(result.Unknown, "、"))
	}
	if len(parts) == 0 {
		return "没有可停止的目标。"
	}
	return strings.Join(parts, "\n")
}

func (h *Handler) startKeepaliveFrom(targets []string, incoming hub.Incoming) string {
	result := h.manager.StartKeepaliveWithOperation(targets, jobs.Operation{Source: jobs.SourceOpenILink, Actor: incoming.SenderID})
	return formatKeepaliveStartResult(result)
}

func (h *Handler) startKeepalive(targets []string) string {
	return formatKeepaliveStartResult(h.manager.StartKeepalive(targets))
}

func formatKeepaliveStartResult(result jobs.KeepaliveStartResult) string {
	parts := make([]string, 0, 3)
	if len(result.Started) > 0 {
		parts = append(parts, "已开始保活："+strings.Join(result.Started, "、"))
	}
	if len(result.Already) > 0 {
		parts = append(parts, "正在保活："+strings.Join(result.Already, "、"))
	}
	if len(result.Unknown) > 0 {
		parts = append(parts, "未知目标："+strings.Join(result.Unknown, "、"))
	}
	if len(parts) == 0 {
		return "没有可启动保活的目标。"
	}
	return strings.Join(parts, "\n")
}

func (h *Handler) stopKeepaliveFrom(targets []string, incoming hub.Incoming) string {
	result := h.manager.StopKeepaliveWithOperation(targets, jobs.Operation{Source: jobs.SourceOpenILink, Actor: incoming.SenderID})
	return formatKeepaliveStopResult(result)
}

func (h *Handler) stopKeepalive(targets []string) string {
	return formatKeepaliveStopResult(h.manager.StopKeepalive(targets))
}

func formatKeepaliveStopResult(result jobs.KeepaliveStopResult) string {
	parts := make([]string, 0, 3)
	if len(result.Stopped) > 0 {
		parts = append(parts, "已停止保活："+strings.Join(result.Stopped, "、"))
	}
	if len(result.Inactive) > 0 {
		parts = append(parts, "未在保活："+strings.Join(result.Inactive, "、"))
	}
	if len(result.Unknown) > 0 {
		parts = append(parts, "未知目标："+strings.Join(result.Unknown, "、"))
	}
	if len(parts) == 0 {
		return "没有可停止保活的目标。"
	}
	return strings.Join(parts, "\n")
}

func (h *Handler) status(targets []string) string {
	snapshots, unknown := h.manager.Snapshots(targets)
	lines := make([]string, 0, len(snapshots)+1)
	now := time.Now()
	for _, snapshot := range snapshots {
		lines = append(lines, formatSnapshot(snapshot, now))
	}
	if len(unknown) > 0 {
		lines = append(lines, "未知目标："+strings.Join(unknown, "、"))
	}
	if len(lines) == 0 {
		return "没有匹配的目标。"
	}
	return strings.Join(lines, "\n\n")
}

func (h *Handler) keepaliveStatus(targets []string) string {
	snapshots, unknown := h.manager.KeepaliveSnapshots(targets)
	lines := make([]string, 0, len(snapshots)+1)
	now := time.Now()
	for _, snapshot := range snapshots {
		lines = append(lines, formatKeepaliveSnapshot(snapshot, now))
	}
	if len(unknown) > 0 {
		lines = append(lines, "未知目标："+strings.Join(unknown, "、"))
	}
	if len(lines) == 0 {
		return "没有匹配的目标。"
	}
	return strings.Join(lines, "\n\n")
}

func (h *Handler) list() string {
	snapshots, _ := h.manager.Snapshots(nil)
	lines := []string{"可用目标："}
	for _, snapshot := range snapshots {
		lines = append(lines, fmt.Sprintf("- %s（%s @ %s）", snapshot.Name, snapshot.Model, snapshot.APIHost))
	}
	return strings.Join(lines, "\n")
}

func (h *Handler) reply(parent context.Context, incoming hub.Incoming, content string) {
	if strings.TrimSpace(content) == "" {
		return
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(parent, 20*time.Second)
		lastErr = h.sender.Send(ctx, incoming.SenderID, content, incoming.TraceID)
		cancel()
		if lastErr == nil {
			return
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
			select {
			case <-parent.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
	h.logger.Error("failed to reply to OpenILink command", "sender", incoming.SenderID, "error", lastErr)
}

func (h *Handler) isAllowed(sender string) bool {
	if len(h.allowed) == 0 {
		return true
	}
	_, ok := h.allowed[sender]
	return ok
}

func parseCommand(text string) (string, string, bool) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "/") {
		return "", "", false
	}
	text = strings.TrimSpace(strings.TrimPrefix(text, "/"))
	if text == "" {
		return "", "", false
	}
	command, args := splitFirstField(text)
	if at := strings.IndexByte(command, '@'); at >= 0 {
		command = command[:at]
	}
	return strings.ToLower(command), strings.TrimSpace(args), command != ""
}

func splitFirstField(value string) (string, string) {
	for index, r := range value {
		if unicode.IsSpace(r) {
			return value[:index], strings.TrimSpace(value[index:])
		}
	}
	return value, ""
}

func splitTargets(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || r == ',' || r == '，' || r == ';' || r == '；'
	})
}

func formatSnapshot(snapshot jobs.Snapshot, now time.Time) string {
	prefix := snapshot.Name + "："
	switch snapshot.State {
	case jobs.StateRunning:
		value := fmt.Sprintf("挤队中，第 %d 次", snapshot.Attempts)
		if !snapshot.NextAttempt.IsZero() {
			value += "，下次约 " + relativeFuture(snapshot.NextAttempt, now)
		}
		if snapshot.LastError != "" {
			value += "\n\n最近失败：" + truncate(snapshot.LastError, 100)
		}
		return prefix + value
	case jobs.StateSucceeded:
		return fmt.Sprintf("%s已成功，第 %d 次，耗时 %s", prefix, snapshot.Attempts, humanDuration(snapshot.FinishedAt.Sub(snapshot.StartedAt)))
	case jobs.StateStopped:
		return fmt.Sprintf("%s已停止（共尝试 %d 次）", prefix, snapshot.Attempts)
	default:
		return prefix + "未启动"
	}
}

func formatKeepaliveSnapshot(snapshot jobs.KeepaliveSnapshot, now time.Time) string {
	prefix := snapshot.Name + "："
	var value string
	switch snapshot.State {
	case jobs.KeepaliveStateRequesting:
		value = fmt.Sprintf("请求中（共请求 %d 次）", snapshot.Requests)
	case jobs.KeepaliveStateWaitingQueue:
		value = fmt.Sprintf("等待排队任务结束（共请求 %d 次）", snapshot.Requests)
	case jobs.KeepaliveStateWaitingNext:
		value = fmt.Sprintf("等待下次请求（共请求 %d 次）", snapshot.Requests)
		if !snapshot.NextRequest.IsZero() {
			value += "，下次约 " + relativeFuture(snapshot.NextRequest, now)
		}
	default:
		value = fmt.Sprintf("已停止（共请求 %d 次）", snapshot.Requests)
	}
	if !snapshot.LastFailure.IsZero() {
		errorText := snapshot.LastError
		if errorText == "" {
			errorText = "无错误详情"
		}
		value += "\n\n最近失败：" + truncate(errorText, 100)
	}
	return prefix + value
}

func relativeFuture(at, now time.Time) string {
	remaining := at.Sub(now).Round(time.Second)
	if remaining <= 0 {
		return "马上"
	}
	return remaining.String() + "后"
}

func humanDuration(value time.Duration) string {
	value = value.Round(time.Second)
	if value < time.Second {
		return "不足 1 秒"
	}
	return value.String()
}

func truncate(value string, maximum int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum]) + "…"
}

func helpText() string {
	return strings.Join([]string{
		"可用命令：\n",
		"/开挤 [目标|all] — 开始请求；不写目标则全部启动\n",
		"/状态 [目标] — 查看尝试次数、最近错误和下次重试\n",
		"/停止 [目标|all] — 停止任务\n",
		"/保活 [目标|all] — 启动随机间隔保活\n",
		"/保活状态 [目标] — 查看保活次数、阶段和最近失败\n",
		"/停止保活 [目标|all] — 停止保活\n",
		"/列表 — 查看已配置的 Key/API 目标\n",
		"/帮助 — 显示本帮助\n",
		"多个目标可用空格或逗号分隔。\n",
		"英文别名：/start、/status、/stop、/keepalive、/keepalive-status、/stop-keepalive、/list、/help。",
	}, "\n")
}
