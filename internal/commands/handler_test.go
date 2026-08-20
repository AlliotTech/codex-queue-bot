package commands

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
)

type commandBlockingRunner struct{}

func (commandBlockingRunner) Run(ctx context.Context, _ config.Target, _ int) codex.Result {
	<-ctx.Done()
	return codex.Result{Error: "cancelled"}
}

type replyMessage struct {
	to      string
	content string
	traceID string
}

type replyRecorder struct {
	messages chan replyMessage
}

func (r *replyRecorder) Send(_ context.Context, to, content, traceID string) error {
	r.messages <- replyMessage{to: to, content: content, traceID: traceID}
	return nil
}

func receiveReply(t *testing.T, recorder *replyRecorder) replyMessage {
	t.Helper()
	select {
	case message := <-recorder.messages:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for command reply")
		return replyMessage{}
	}
}

func newCommandTestManager(ctx context.Context, targets []config.Target) *jobs.Manager {
	return jobs.New(
		ctx,
		targets,
		commandBlockingRunner{},
		nil,
		nil,
		time.Second,
		time.Second,
		time.Hour,
		time.Hour,
		1,
		"开蹬",
	)
}

func TestParseCommandAndTargets(t *testing.T) {
	command, args, ok := parseCommand("  /开挤@bot  a,b，c  ")
	if !ok || command != "开挤" || args != "a,b，c" {
		t.Fatalf("parseCommand = %q %q %v", command, args, ok)
	}
	want := []string{"a", "b", "c"}
	if got := splitTargets(args); !reflect.DeepEqual(got, want) {
		t.Fatalf("splitTargets = %#v, want %#v", got, want)
	}
}

func TestFormatSnapshotSeparatesFailureDetails(t *testing.T) {
	snapshot := jobs.Snapshot{
		Name:      "backup",
		State:     jobs.StateRunning,
		Attempts:  53,
		LastError: "codex exit failed: exit status 1",
	}

	want := "backup：挤队中，第 53 次\n\n最近失败：codex exit failed: exit status 1"
	if got := formatSnapshot(snapshot, time.Now()); got != want {
		t.Fatalf("formatSnapshot = %q, want %q", got, want)
	}
}

func TestStatusSeparatesTargetsWithBlankLine(t *testing.T) {
	manager := jobs.New(
		context.Background(),
		[]config.Target{{Name: "primary"}, {Name: "backup"}},
		nil,
		nil,
		nil,
		time.Second,
		time.Second,
		time.Second,
		time.Second,
		1,
		"开蹬",
	)
	handler := New(manager, nil, nil, nil)

	want := "primary：未启动\n\nbackup：未启动"
	if got := handler.status(nil); got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}

func TestFormatKeepaliveSnapshotStates(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		snapshot jobs.KeepaliveSnapshot
		want     string
	}{
		{
			name:     "requesting",
			snapshot: jobs.KeepaliveSnapshot{Name: "main", State: jobs.KeepaliveStateRequesting, Requests: 2},
			want:     "main：请求中（共请求 2 次）",
		},
		{
			name:     "waiting queue",
			snapshot: jobs.KeepaliveSnapshot{Name: "main", State: jobs.KeepaliveStateWaitingQueue, Requests: 2},
			want:     "main：等待排队任务结束（共请求 2 次）",
		},
		{
			name:     "waiting next",
			snapshot: jobs.KeepaliveSnapshot{Name: "main", State: jobs.KeepaliveStateWaitingNext, Requests: 2, NextRequest: now.Add(time.Minute)},
			want:     "main：等待下次请求（共请求 2 次），下次约 1m0s后",
		},
		{
			name: "stopped with failure",
			snapshot: jobs.KeepaliveSnapshot{
				Name:        "main",
				State:       jobs.KeepaliveStateStopped,
				Requests:    3,
				LastFailure: now.Add(-time.Minute),
				LastError:   "request failed",
			},
			want: "main：已停止（共请求 3 次）\n\n最近失败：request failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatKeepaliveSnapshot(tt.snapshot, now); got != tt.want {
				t.Fatalf("formatKeepaliveSnapshot = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestKeepaliveCommandResultsAndIndependentStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targets := []config.Target{{Name: "primary"}, {Name: "backup"}}
	manager := newCommandTestManager(ctx, targets)
	handler := New(manager, nil, nil, nil)

	if got := handler.startKeepalive([]string{"primary", "missing"}); got != "已开始保活：primary\n未知目标：missing" {
		t.Fatalf("startKeepalive = %q", got)
	}
	if got := handler.startKeepalive([]string{"PRIMARY"}); got != "正在保活：primary" {
		t.Fatalf("duplicate startKeepalive = %q", got)
	}
	manager.Start([]string{"backup"}, jobs.Subscriber{})
	if got := handler.stopKeepalive([]string{"primary", "missing"}); got != "已停止保活：primary\n未知目标：missing" {
		t.Fatalf("stopKeepalive = %q", got)
	}
	queueSnapshots, _ := manager.Snapshots([]string{"backup"})
	if queueSnapshots[0].State != jobs.StateRunning {
		t.Fatalf("stopping keepalive changed queue state: %+v", queueSnapshots[0])
	}
	manager.Stop(nil)
}

func TestKeepaliveEnglishAliases(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := newCommandTestManager(ctx, []config.Target{{Name: "primary"}})
	recorder := &replyRecorder{messages: make(chan replyMessage, 3)}
	handler := New(manager, recorder, nil, nil)
	incoming := hub.Incoming{SenderID: "user-1", TraceID: "trace-1"}

	incoming.Text = "/keepalive primary"
	handler.Handle(ctx, incoming)
	if message := receiveReply(t, recorder); message.content != "已开始保活：primary" || message.to != "user-1" || message.traceID != "trace-1" {
		t.Fatalf("keepalive reply = %+v", message)
	}
	incoming.Text = "/keepalive-status primary"
	handler.Handle(ctx, incoming)
	if message := receiveReply(t, recorder); !strings.Contains(message.content, "primary：") || !strings.Contains(message.content, "共请求") {
		t.Fatalf("keepalive status reply = %+v", message)
	}
	incoming.Text = "/stop-keepalive primary"
	handler.Handle(ctx, incoming)
	if message := receiveReply(t, recorder); message.content != "已停止保活：primary" {
		t.Fatalf("stop keepalive reply = %+v", message)
	}
}

func TestKeepaliveStatusUnknownTargetAndHelp(t *testing.T) {
	manager := newCommandTestManager(context.Background(), []config.Target{{Name: "primary"}})
	handler := New(manager, nil, nil, nil)
	if got := handler.keepaliveStatus([]string{"missing"}); got != "未知目标：missing" {
		t.Fatalf("keepaliveStatus = %q", got)
	}
	help := helpText()
	for _, command := range []string{"/保活", "/停止保活", "/保活状态", "/keepalive", "/stop-keepalive", "/keepalive-status"} {
		if !strings.Contains(help, command) {
			t.Fatalf("help does not contain %q: %s", command, help)
		}
	}
}
