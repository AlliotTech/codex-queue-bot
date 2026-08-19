package jobs

import (
	"context"
	"sync"
	"testing"
	"time"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/config"
)

type sequenceRunner struct {
	mu    sync.Mutex
	calls int
}

func (r *sequenceRunner) Run(context.Context, config.Target, int) codex.Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls == 1 {
		return codex.Result{Error: "not admitted"}
	}
	return codex.Result{Success: true, Response: "ok"}
}

type sentMessage struct {
	to      string
	content string
	traceID string
}

type channelMessenger struct {
	messages chan sentMessage
}

func (m *channelMessenger) Send(_ context.Context, to, content, traceID string) error {
	m.messages <- sentMessage{to: to, content: content, traceID: traceID}
	return nil
}

func TestManagerRetriesUntilSuccessAndNotifiesSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &sequenceRunner{}
	messenger := &channelMessenger{messages: make(chan sentMessage, 1)}
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, messenger, nil, 5*time.Millisecond, 5*time.Millisecond, 1, "开蹬")

	started := manager.Start(nil, Subscriber{Recipient: "user-1", TraceID: "trace-1"})
	if len(started.Started) != 1 || started.Started[0] != "main" {
		t.Fatalf("start result = %+v", started)
	}

	select {
	case message := <-messenger.messages:
		if message.to != "user-1" || message.traceID != "trace-1" {
			t.Fatalf("message = %+v", message)
		}
		if message.content == "" {
			t.Fatal("success notification is empty")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for success notification")
	}

	snapshots, _ := manager.Snapshots(nil)
	if snapshots[0].State != StateSucceeded || snapshots[0].Attempts != 2 {
		t.Fatalf("snapshot = %+v", snapshots[0])
	}
}

type blockingRunner struct {
	started chan struct{}
	once    sync.Once
}

func (r *blockingRunner) Run(ctx context.Context, _ config.Target, _ int) codex.Result {
	r.once.Do(func() { close(r.started) })
	<-ctx.Done()
	return codex.Result{Error: "cancelled"}
}

func TestManagerSubscribesToRunningJobAndStopsIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &blockingRunner{started: make(chan struct{})}
	messenger := &channelMessenger{messages: make(chan sentMessage, 1)}
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, messenger, nil, time.Second, time.Second, 1, "开蹬")
	manager.Start(nil, Subscriber{Recipient: "user-1"})
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}

	second := manager.Start([]string{"MAIN"}, Subscriber{Recipient: "user-2"})
	if len(second.Already) != 1 {
		t.Fatalf("second start = %+v", second)
	}
	stopped := manager.Stop(nil)
	if len(stopped.Stopped) != 1 {
		t.Fatalf("stop result = %+v", stopped)
	}
	snapshots, _ := manager.Snapshots(nil)
	if snapshots[0].State != StateStopped {
		t.Fatalf("snapshot = %+v", snapshots[0])
	}
}
