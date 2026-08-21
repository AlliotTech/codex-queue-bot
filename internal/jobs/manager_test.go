package jobs

import (
	"context"
	"strings"
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
	manager := New(ctx, []config.Target{target}, runner, messenger, nil, 5*time.Millisecond, 5*time.Millisecond, time.Second, time.Second, 1, "开蹬")

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

type observedCall struct {
	target  string
	attempt int
	at      time.Time
}

type immediateSequenceRunner struct {
	mu      sync.Mutex
	results []codex.Result
	calls   chan observedCall
	index   int
}

func (r *immediateSequenceRunner) Run(_ context.Context, target config.Target, attempt int) codex.Result {
	r.calls <- observedCall{target: target.Name, attempt: attempt, at: time.Now()}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.index >= len(r.results) {
		return codex.Result{Success: true, Response: "ok"}
	}
	result := r.results[r.index]
	r.index++
	return result
}

type controlledCall struct {
	target  string
	attempt int
	result  chan codex.Result
	done    chan struct{}
}

type controlledRunner struct {
	calls chan *controlledCall

	mu        sync.Mutex
	active    int
	maxActive int
}

func newControlledRunner() *controlledRunner {
	return &controlledRunner{calls: make(chan *controlledCall, 32)}
}

func (r *controlledRunner) Run(ctx context.Context, target config.Target, attempt int) codex.Result {
	call := &controlledCall{
		target:  target.Name,
		attempt: attempt,
		result:  make(chan codex.Result, 1),
		done:    make(chan struct{}),
	}
	r.mu.Lock()
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.mu.Unlock()
	r.calls <- call
	defer func() {
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
		close(call.done)
	}()

	select {
	case result := <-call.result:
		return result
	case <-ctx.Done():
		return codex.Result{Error: "cancelled"}
	}
}

func (r *controlledRunner) maximumActive() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxActive
}

func receiveObservedCall(t *testing.T, calls <-chan observedCall) observedCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runner call")
		return observedCall{}
	}
}

func receiveControlledCall(t *testing.T, runner *controlledRunner) *controlledCall {
	t.Helper()
	select {
	case call := <-runner.calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for controlled runner call")
		return nil
	}
}

func assertNoControlledCall(t *testing.T, runner *controlledRunner, duration time.Duration) {
	t.Helper()
	select {
	case call := <-runner.calls:
		t.Fatalf("unexpected runner call: target=%s attempt=%d", call.target, call.attempt)
	case <-time.After(duration):
	}
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
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
	manager := New(ctx, []config.Target{target}, runner, messenger, nil, time.Second, time.Second, time.Second, time.Second, 1, "开蹬")
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

func TestKeepaliveRunsImmediatelyAndContinuesAfterSuccessAndFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &immediateSequenceRunner{
		results: []codex.Result{
			{Error: "first failure"},
			{Success: true, Response: "ok"},
			{Error: "third failure"},
		},
		calls: make(chan observedCall, 8),
	}
	messenger := &channelMessenger{messages: make(chan sentMessage, 1)}
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, messenger, nil, time.Second, time.Second, 25*time.Millisecond, 25*time.Millisecond, 1, "开蹬")

	startedAt := time.Now()
	result := manager.StartKeepalive(nil)
	if len(result.Started) != 1 || result.Started[0] != "main" {
		t.Fatalf("start keepalive result = %+v", result)
	}
	first := receiveObservedCall(t, runner.calls)
	second := receiveObservedCall(t, runner.calls)
	third := receiveObservedCall(t, runner.calls)
	if first.at.Sub(startedAt) > 200*time.Millisecond {
		t.Fatalf("first keepalive request was not immediate: %s", first.at.Sub(startedAt))
	}
	if second.at.Sub(first.at) < 20*time.Millisecond || third.at.Sub(second.at) < 20*time.Millisecond {
		t.Fatalf("keepalive intervals were too short: first=%s second=%s", second.at.Sub(first.at), third.at.Sub(second.at))
	}

	waitForCondition(t, func() bool {
		snapshots, _ := manager.KeepaliveSnapshots(nil)
		return snapshots[0].Requests >= 3 && !snapshots[0].LastSuccess.IsZero() && !snapshots[0].LastFailure.IsZero() && snapshots[0].LastError == "third failure"
	})
	select {
	case message := <-messenger.messages:
		t.Fatalf("keepalive unexpectedly sent a chat notification: %+v", message)
	default:
	}
	stopped := manager.StopKeepalive(nil)
	if len(stopped.Stopped) != 1 {
		t.Fatalf("stop keepalive result = %+v", stopped)
	}
}

func TestKeepaliveTargetsRunIndependently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	targets := []config.Target{
		{Name: "primary", APIBaseURL: "https://primary.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
		{Name: "backup", APIBaseURL: "https://backup.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
	}
	manager := New(ctx, targets, runner, nil, nil, time.Second, time.Second, time.Hour, time.Hour, 2, "开蹬")

	result := manager.StartKeepalive(nil)
	if len(result.Started) != 2 {
		t.Fatalf("start keepalive result = %+v", result)
	}
	first := receiveControlledCall(t, runner)
	second := receiveControlledCall(t, runner)
	if first.target == second.target {
		t.Fatalf("expected independent targets, got %q twice", first.target)
	}
	if runner.maximumActive() != 2 {
		t.Fatalf("maximum active requests = %d, want 2", runner.maximumActive())
	}

	manager.StopKeepalive(nil)
	select {
	case <-first.done:
	case <-time.After(time.Second):
		t.Fatal("first target was not cancelled")
	}
	select {
	case <-second.done:
	case <-time.After(time.Second):
		t.Fatal("second target was not cancelled")
	}
}

func TestStopKeepaliveCancelsCurrentRequestWithoutStoppingQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, nil, nil, time.Second, time.Second, time.Hour, time.Hour, 1, "开蹬")

	manager.StartKeepalive(nil)
	call := receiveControlledCall(t, runner)
	secondStart := manager.StartKeepalive([]string{"MAIN"})
	if len(secondStart.Already) != 1 || len(secondStart.Started) != 0 {
		t.Fatalf("duplicate start result = %+v", secondStart)
	}
	before, _ := manager.KeepaliveSnapshots(nil)
	stopped := manager.StopKeepalive(nil)
	if len(stopped.Stopped) != 1 {
		t.Fatalf("stop result = %+v", stopped)
	}
	select {
	case <-call.done:
	case <-time.After(time.Second):
		t.Fatal("keepalive request was not cancelled")
	}
	after, _ := manager.KeepaliveSnapshots(nil)
	if after[0].State != KeepaliveStateStopped || after[0].Requests != before[0].Requests || after[0].StartedAt != before[0].StartedAt {
		t.Fatalf("keepalive snapshot after stop = %+v, before = %+v", after[0], before[0])
	}
	queueSnapshots, _ := manager.Snapshots(nil)
	if queueSnapshots[0].State != StateIdle {
		t.Fatalf("queue state changed when stopping keepalive: %+v", queueSnapshots[0])
	}
}

func TestStopQueueDoesNotStopKeepalive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	targets := []config.Target{
		{Name: "primary", APIBaseURL: "https://primary.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
		{Name: "backup", APIBaseURL: "https://backup.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
	}
	manager := New(ctx, targets, runner, nil, nil, time.Second, time.Second, time.Hour, time.Hour, 2, "开蹬")

	manager.StartKeepalive([]string{"primary"})
	keepaliveCall := receiveControlledCall(t, runner)
	manager.Start([]string{"backup"}, Subscriber{})
	queueCall := receiveControlledCall(t, runner)
	stopped := manager.Stop([]string{"backup"})
	if len(stopped.Stopped) != 1 || stopped.Stopped[0] != "backup" {
		t.Fatalf("stop queue result = %+v", stopped)
	}
	select {
	case <-queueCall.done:
	case <-time.After(time.Second):
		t.Fatal("queue request was not cancelled")
	}
	keepaliveSnapshots, _ := manager.KeepaliveSnapshots([]string{"primary"})
	if keepaliveSnapshots[0].State != KeepaliveStateRequesting {
		t.Fatalf("stopping queue changed keepalive state: %+v", keepaliveSnapshots[0])
	}
	select {
	case <-keepaliveCall.done:
		t.Fatal("stopping queue cancelled keepalive request")
	default:
	}
	manager.StopKeepalive(nil)
	select {
	case <-keepaliveCall.done:
	case <-time.After(time.Second):
		t.Fatal("keepalive request was not cancelled during cleanup")
	}
}

func TestQueueAndKeepaliveArbitrationKeepsQueueRetriesTogether(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, nil, nil, 15*time.Millisecond, 15*time.Millisecond, 15*time.Millisecond, 15*time.Millisecond, 1, "开蹬")

	manager.StartKeepalive(nil)
	keepaliveFirst := receiveControlledCall(t, runner)
	manager.Start(nil, Subscriber{})
	assertNoControlledCall(t, runner, 25*time.Millisecond)

	keepaliveFirst.result <- codex.Result{Success: true, Response: "ok"}
	queueFirst := receiveControlledCall(t, runner)
	if queueFirst.attempt != 1 {
		t.Fatalf("first queue attempt = %d", queueFirst.attempt)
	}
	waitForCondition(t, func() bool {
		snapshots, _ := manager.KeepaliveSnapshots(nil)
		return snapshots[0].State == KeepaliveStateWaitingQueue
	})

	queueFirst.result <- codex.Result{Error: "still queued"}
	queueSecond := receiveControlledCall(t, runner)
	queueSnapshots, _ := manager.Snapshots(nil)
	keepaliveSnapshots, _ := manager.KeepaliveSnapshots(nil)
	if queueSnapshots[0].Attempts != 2 || keepaliveSnapshots[0].Requests != 1 {
		t.Fatalf("keepalive inserted between queue retries: queue=%+v keepalive=%+v", queueSnapshots[0], keepaliveSnapshots[0])
	}
	queueSecond.result <- codex.Result{Success: true, Response: "admitted"}

	keepaliveSecond := receiveControlledCall(t, runner)
	waitForCondition(t, func() bool {
		queueState, _ := manager.Snapshots(nil)
		keepaliveState, _ := manager.KeepaliveSnapshots(nil)
		return queueState[0].State == StateSucceeded && keepaliveState[0].Requests == 2
	})
	manager.StopKeepalive(nil)
	select {
	case <-keepaliveSecond.done:
	case <-time.After(time.Second):
		t.Fatal("second keepalive request was not cancelled")
	}
}

func TestQueueAndKeepaliveShareMaxParallel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	targets := []config.Target{
		{Name: "primary", APIBaseURL: "https://primary.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
		{Name: "backup", APIBaseURL: "https://backup.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
	}
	manager := New(ctx, targets, runner, nil, nil, time.Second, time.Second, time.Hour, time.Hour, 1, "开蹬")

	manager.StartKeepalive([]string{"primary"})
	keepaliveCall := receiveControlledCall(t, runner)
	manager.Start([]string{"backup"}, Subscriber{})
	assertNoControlledCall(t, runner, 30*time.Millisecond)
	if runner.maximumActive() != 1 {
		t.Fatalf("maximum active requests = %d, want 1", runner.maximumActive())
	}

	keepaliveCall.result <- codex.Result{Success: true, Response: "ok"}
	queueCall := receiveControlledCall(t, runner)
	if queueCall.target != "backup" {
		t.Fatalf("next request target = %q, want backup", queueCall.target)
	}
	queueCall.result <- codex.Result{Success: true, Response: "ok"}
	manager.StopKeepalive(nil)
	if runner.maximumActive() != 1 {
		t.Fatalf("shared semaphore allowed %d concurrent requests", runner.maximumActive())
	}
}

func TestComprehensiveSnapshotIncludesSharedProcessCount(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "secret", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, nil, nil, time.Second, time.Second, time.Hour, time.Hour, 2, "开蹬")

	manager.StartWithOperation(nil, Subscriber{}, Operation{Source: SourceWeb, Actor: "admin"})
	call := receiveControlledCall(t, runner)
	snapshot := manager.ComprehensiveSnapshot()
	if snapshot.CurrentProcesses != 1 || snapshot.MaxParallel != 2 {
		t.Fatalf("concurrency snapshot = %+v", snapshot)
	}
	if len(snapshot.Targets) != 1 || snapshot.Targets[0].Queue.State != StateRunning || snapshot.Targets[0].Queue.Attempts != 1 {
		t.Fatalf("target snapshot = %+v", snapshot.Targets)
	}
	call.result <- codex.Result{Success: true, Response: "ok"}
	waitForCondition(t, func() bool { return manager.ComprehensiveSnapshot().CurrentProcesses == 0 })
}

func TestActivityLimitOrderingAndOperationMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &blockingRunner{started: make(chan struct{})}
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "secret", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, nil, nil, time.Hour, time.Hour, time.Hour, time.Hour, 1, "开蹬", 3)

	for i := 0; i < 3; i++ {
		manager.StartWithOperation(nil, Subscriber{}, Operation{Source: SourceWeb, Actor: "admin"})
		manager.StopWithOperation(nil, Operation{Source: SourceWeb, Actor: "admin"})
	}
	activities := manager.Activities()
	if len(activities) != 3 {
		t.Fatalf("activity count = %d, want 3", len(activities))
	}
	for i, activity := range activities {
		if activity.Source != SourceWeb || activity.Actor != "admin" {
			t.Fatalf("activity metadata = %+v", activity)
		}
		if i > 0 && activities[i-1].ID <= activity.ID {
			t.Fatalf("activities not newest first: %+v", activities)
		}
	}
}

func TestObserverEventsAreOrderedAndSlowObserverIsDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &blockingRunner{started: make(chan struct{})}
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "secret", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, nil, nil, time.Hour, time.Hour, time.Hour, time.Hour, 1, "开蹬")
	_, _, subscription := manager.Observe(1)
	defer subscription.Close()

	manager.StartWithOperation(nil, Subscriber{}, Operation{Source: SourceWeb, Actor: "admin"})
	first, ok := <-subscription.Events
	if !ok || first.Kind != EventActivity || first.Activity == nil || first.Activity.Type != "queue.start" {
		t.Fatalf("first observer event = %+v, ok=%v", first, ok)
	}
	if _, ok := <-subscription.Events; ok {
		t.Fatal("slow observer channel should be closed after its buffer fills")
	}
}

func TestWebStartDoesNotNotifyButLaterOpenILinkSubscriptionDoes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	messenger := &channelMessenger{messages: make(chan sentMessage, 2)}
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "secret", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, messenger, nil, time.Hour, time.Hour, time.Hour, time.Hour, 1, "开蹬")

	manager.StartWithOperation(nil, Subscriber{}, Operation{Source: SourceWeb, Actor: "admin"})
	call := receiveControlledCall(t, runner)
	result := manager.StartWithOperation(nil, Subscriber{Recipient: "user-1", TraceID: "trace-1"}, Operation{Source: SourceOpenILink, Actor: "user-1"})
	if len(result.Already) != 1 {
		t.Fatalf("subscription result = %+v", result)
	}
	call.result <- codex.Result{Success: true, Response: "ok"}
	select {
	case message := <-messenger.messages:
		if message.to != "user-1" || message.traceID != "trace-1" {
			t.Fatalf("notification = %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("OpenILink subscriber did not receive notification")
	}
	select {
	case extra := <-messenger.messages:
		t.Fatalf("unexpected notification for Web start: %+v", extra)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestTargetErrorsAreSanitizedBeforeSnapshotsAndActivities(t *testing.T) {
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1/private", APIKey: "secret-key", APIKeyEnv: "SECRET_KEY_ENV", Model: "m", WireAPI: "responses"}
	value := sanitizeTargetError(target, "request https://api.example/v1/private/responses with secret-key from SECRET_KEY_ENV failed")
	for _, forbidden := range []string{target.APIBaseURL, target.APIKey, target.APIKeyEnv} {
		if strings.Contains(value, forbidden) {
			t.Fatalf("sanitized error leaked %q: %s", forbidden, value)
		}
	}
	if !strings.Contains(value, "[URL]") || !strings.Contains(value, "[REDACTED]") {
		t.Fatalf("sanitized error = %q", value)
	}
}

func TestBeginShutdownCancelsRunsAndWaitsForCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	target := config.Target{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
	manager := New(ctx, []config.Target{target}, runner, nil, nil, time.Hour, time.Hour, time.Hour, time.Hour, 1, "开蹬")
	manager.Start(nil, Subscriber{})
	call := receiveControlledCall(t, runner)
	manager.BeginShutdown()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := manager.Wait(waitCtx); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	select {
	case <-call.done:
	default:
		t.Fatal("runner was not cleaned up")
	}
	if result := manager.Start(nil, Subscriber{}); len(result.Started) != 0 {
		t.Fatalf("start after shutdown = %+v", result)
	}
}

func TestIntervalUpdatesResampleWaitingQueueAndKeepaliveFromSaveTime(t *testing.T) {
	t.Run("queue retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runner := &immediateSequenceRunner{
			results: []codex.Result{{Error: "queued"}, {Success: true, Response: "ok"}},
			calls:   make(chan observedCall, 4),
		}
		target := config.Target{ID: 1, Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
		manager := New(ctx, []config.Target{target}, runner, nil, nil, time.Hour, time.Hour, time.Hour, time.Hour, 1, "ok")
		manager.Start(nil, Subscriber{})
		receiveObservedCall(t, runner.calls)
		waitForCondition(t, func() bool {
			snapshots, _ := manager.Snapshots(nil)
			return !snapshots[0].NextAttempt.IsZero()
		})
		savedAt := time.Now()
		manager.UpdateSettings(20*time.Millisecond, 30*time.Millisecond, time.Hour, time.Hour, 1, "ok", 200)
		second := receiveObservedCall(t, runner.calls)
		if delay := second.at.Sub(savedAt); delay < 15*time.Millisecond || delay > 200*time.Millisecond {
			t.Fatalf("resampled retry delay = %s", delay)
		}
	})

	t.Run("keepalive", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runner := &immediateSequenceRunner{results: []codex.Result{{Success: true}, {Success: true}}, calls: make(chan observedCall, 4)}
		target := config.Target{ID: 1, Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}
		manager := New(ctx, []config.Target{target}, runner, nil, nil, time.Hour, time.Hour, time.Hour, time.Hour, 1, "ok")
		manager.StartKeepalive(nil)
		receiveObservedCall(t, runner.calls)
		waitForCondition(t, func() bool {
			snapshots, _ := manager.KeepaliveSnapshots(nil)
			return !snapshots[0].NextRequest.IsZero()
		})
		savedAt := time.Now()
		manager.UpdateSettings(time.Hour, time.Hour, 20*time.Millisecond, 30*time.Millisecond, 1, "ok", 200)
		second := receiveObservedCall(t, runner.calls)
		if delay := second.at.Sub(savedAt); delay < 15*time.Millisecond || delay > 200*time.Millisecond {
			t.Fatalf("resampled keepalive delay = %s", delay)
		}
		manager.StopKeepalive(nil)
	})
}

func TestDynamicConcurrencyDecreaseAndIncreaseDoNotCancelRunningProcesses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := newControlledRunner()
	targets := []config.Target{
		{ID: 1, Name: "one", APIBaseURL: "https://one.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
		{ID: 2, Name: "two", APIBaseURL: "https://two.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
		{ID: 3, Name: "three", APIBaseURL: "https://three.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
	}
	manager := New(ctx, targets, runner, nil, nil, time.Hour, time.Hour, time.Hour, time.Hour, 2, "ok")
	manager.Start(nil, Subscriber{})
	first := receiveControlledCall(t, runner)
	second := receiveControlledCall(t, runner)
	manager.UpdateSettings(time.Hour, time.Hour, time.Hour, time.Hour, 1, "ok", 200)
	first.result <- codex.Result{Success: true, Response: "ok"}
	assertNoControlledCall(t, runner, 30*time.Millisecond)
	select {
	case <-second.done:
		t.Fatal("decreasing max_parallel cancelled an existing process")
	default:
	}
	second.result <- codex.Result{Success: true, Response: "ok"}
	third := receiveControlledCall(t, runner)

	manager.CreateTarget(&config.Target{ID: 4, SortOrder: 4, Name: "four", APIBaseURL: "https://four.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}, nil)
	manager.Start([]string{"four"}, Subscriber{})
	assertNoControlledCall(t, runner, 20*time.Millisecond)
	manager.UpdateSettings(time.Hour, time.Hour, time.Hour, time.Hour, 2, "ok", 200)
	fourth := receiveControlledCall(t, runner)
	if third.target == fourth.target {
		t.Fatalf("raising max_parallel did not release a different waiting target: %s", third.target)
	}
	third.result <- codex.Result{Success: true, Response: "ok"}
	fourth.result <- codex.Result{Success: true, Response: "ok"}
}
