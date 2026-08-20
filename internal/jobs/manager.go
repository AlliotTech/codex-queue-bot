package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/config"
)

type State string

var absoluteURLPattern = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s]+`)

const (
	StateIdle      State = "idle"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateStopped   State = "stopped"
)

type KeepaliveState string

const (
	KeepaliveStateRequesting   KeepaliveState = "requesting"
	KeepaliveStateWaitingQueue KeepaliveState = "waiting_queue"
	KeepaliveStateWaitingNext  KeepaliveState = "waiting_next"
	KeepaliveStateStopped      KeepaliveState = "stopped"
)

type AttemptRunner interface {
	Run(ctx context.Context, target config.Target, attempt int) codex.Result
}

type Messenger interface {
	Send(ctx context.Context, to, content, traceID string) error
}

type Subscriber struct {
	Recipient string
	TraceID   string
}

type Source string

const (
	SourceWeb       Source = "web"
	SourceOpenILink Source = "openilink"
	SourceSystem    Source = "system"
)

type Operation struct {
	Source Source
	Actor  string
}

type StartResult struct {
	Started []string
	Already []string
	Unknown []string
}

type StopResult struct {
	Stopped  []string
	Inactive []string
	Unknown  []string
}

type KeepaliveStartResult struct {
	Started []string
	Already []string
	Unknown []string
}

type KeepaliveStopResult struct {
	Stopped  []string
	Inactive []string
	Unknown  []string
}

type Snapshot struct {
	Name        string
	Model       string
	APIHost     string
	State       State
	Attempts    int
	StartedAt   time.Time
	LastAttempt time.Time
	NextAttempt time.Time
	FinishedAt  time.Time
	LastError   string
}

type KeepaliveSnapshot struct {
	Name        string
	Model       string
	APIHost     string
	State       KeepaliveState
	Requests    int
	StartedAt   time.Time
	LastRequest time.Time
	LastSuccess time.Time
	LastFailure time.Time
	NextRequest time.Time
	StoppedAt   time.Time
	LastError   string
}

type TargetSnapshot struct {
	Name      string
	Model     string
	APIHost   string
	Queue     Snapshot
	Keepalive KeepaliveSnapshot
}

type ManagerSnapshot struct {
	Targets          []TargetSnapshot
	CurrentProcesses int
	MaxParallel      int
}

type Activity struct {
	ID       uint64
	Type     string
	Target   string
	Source   Source
	Actor    string
	Attempts int
	At       time.Time
	Error    string
}

type EventKind string

const (
	EventState    EventKind = "state"
	EventActivity EventKind = "activity"
)

type Event struct {
	ID       uint64
	Kind     EventKind
	Snapshot *ManagerSnapshot
	Activity *Activity
}

type Subscription struct {
	Events <-chan Event
	cancel func()
}

func (s *Subscription) Close() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

type Manager struct {
	root           context.Context
	runner         AttemptRunner
	messenger      Messenger
	logger         *slog.Logger
	retryMin       time.Duration
	retryMax       time.Duration
	keepaliveMin   time.Duration
	keepaliveMax   time.Duration
	successMessage string
	sem            chan struct{}
	maxParallel    int
	activityLimit  int

	mu               sync.Mutex
	targets          map[string]config.Target
	order            []string
	jobs             map[string]*job
	keepalives       map[string]*keepaliveJob
	arbiters         map[string]*targetArbiter
	currentProcesses int
	activities       []Activity
	nextEventID      uint64
	nextObserverID   uint64
	observers        map[uint64]chan Event
	runWG            sync.WaitGroup
	shuttingDown     bool
}

type job struct {
	state       State
	attempts    int
	startedAt   time.Time
	lastAttempt time.Time
	nextAttempt time.Time
	finishedAt  time.Time
	lastError   string
	runID       uint64
	cancel      context.CancelFunc
	subscribers map[string]Subscriber
	operation   Operation
}

type keepaliveJob struct {
	state       KeepaliveState
	requests    int
	startedAt   time.Time
	lastRequest time.Time
	lastSuccess time.Time
	lastFailure time.Time
	nextRequest time.Time
	stoppedAt   time.Time
	lastError   string
	runID       uint64
	cancel      context.CancelFunc
	operation   Operation
}

type requestOwner uint8

const (
	requestOwnerNone requestOwner = iota
	requestOwnerQueue
	requestOwnerKeepalive
)

type targetArbiter struct {
	owner      requestOwner
	ownerRunID uint64
	changed    chan struct{}
}

func New(
	root context.Context,
	targets []config.Target,
	runner AttemptRunner,
	messenger Messenger,
	logger *slog.Logger,
	retryMin, retryMax time.Duration,
	keepaliveMin, keepaliveMax time.Duration,
	maxParallel int,
	successMessage string,
	activityLimit ...int,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	limit := 200
	if len(activityLimit) > 0 && activityLimit[0] > 0 {
		limit = activityLimit[0]
	}
	m := &Manager{
		root:           root,
		runner:         runner,
		messenger:      messenger,
		logger:         logger,
		retryMin:       retryMin,
		retryMax:       retryMax,
		keepaliveMin:   keepaliveMin,
		keepaliveMax:   keepaliveMax,
		successMessage: successMessage,
		sem:            make(chan struct{}, maxParallel),
		maxParallel:    maxParallel,
		activityLimit:  limit,
		targets:        make(map[string]config.Target, len(targets)),
		jobs:           make(map[string]*job, len(targets)),
		keepalives:     make(map[string]*keepaliveJob, len(targets)),
		arbiters:       make(map[string]*targetArbiter, len(targets)),
		activities:     make([]Activity, 0, limit),
		observers:      make(map[uint64]chan Event),
	}
	for _, target := range targets {
		key := normalizeName(target.Name)
		m.targets[key] = target
		m.order = append(m.order, key)
		m.jobs[key] = &job{state: StateIdle, subscribers: make(map[string]Subscriber)}
		m.keepalives[key] = &keepaliveJob{state: KeepaliveStateStopped}
		m.arbiters[key] = &targetArbiter{changed: make(chan struct{})}
	}
	return m
}

func (m *Manager) Start(names []string, subscriber Subscriber) StartResult {
	return m.StartWithOperation(names, subscriber, Operation{Source: SourceSystem, Actor: "system"})
}

func (m *Manager) StartWithOperation(names []string, subscriber Subscriber, operation Operation) StartResult {
	keys, unknown := m.resolve(names)
	result := StartResult{Unknown: unknown}
	operation = normalizeOperation(operation)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shuttingDown {
		for _, key := range keys {
			result.Already = append(result.Already, m.targets[key].Name)
		}
		return result
	}
	changed := false
	for _, key := range keys {
		target := m.targets[key]
		current := m.jobs[key]
		if subscriber.Recipient != "" {
			current.subscribers[subscriber.Recipient] = subscriber
		}
		if current.state == StateRunning {
			result.Already = append(result.Already, target.Name)
			continue
		}

		current.runID++
		runID := current.runID
		jobCtx, cancel := context.WithCancel(m.root)
		current.cancel = cancel
		current.state = StateRunning
		current.attempts = 0
		current.startedAt = time.Now()
		current.lastAttempt = time.Time{}
		current.nextAttempt = time.Time{}
		current.finishedAt = time.Time{}
		current.lastError = ""
		current.operation = operation
		result.Started = append(result.Started, target.Name)
		m.recordActivityLocked("queue.start", target.Name, operation, 0, "")
		m.signalTargetLocked(key)
		changed = true
		m.runWG.Add(1)
		go func() {
			defer m.runWG.Done()
			m.run(jobCtx, key, runID)
		}()
	}
	if changed {
		m.broadcastStateLocked()
	}

	return result
}

func (m *Manager) Stop(names []string) StopResult {
	return m.StopWithOperation(names, Operation{Source: SourceSystem, Actor: "system"})
}

func (m *Manager) StopWithOperation(names []string, operation Operation) StopResult {
	keys, unknown := m.resolve(names)
	result := StopResult{Unknown: unknown}
	operation = normalizeOperation(operation)

	m.mu.Lock()
	changed := false
	for _, key := range keys {
		target := m.targets[key]
		current := m.jobs[key]
		if current.state != StateRunning {
			result.Inactive = append(result.Inactive, target.Name)
			continue
		}
		cancel := current.cancel
		current.runID++
		current.cancel = nil
		current.state = StateStopped
		current.finishedAt = time.Now()
		current.nextAttempt = time.Time{}
		current.subscribers = make(map[string]Subscriber)
		result.Stopped = append(result.Stopped, target.Name)
		m.recordActivityLocked("queue.stop", target.Name, operation, current.attempts, "")
		m.signalTargetLocked(key)
		changed = true
		if cancel != nil {
			cancel()
		}
	}
	if changed {
		m.broadcastStateLocked()
	}
	m.mu.Unlock()
	return result
}

func (m *Manager) StartKeepalive(names []string) KeepaliveStartResult {
	return m.StartKeepaliveWithOperation(names, Operation{Source: SourceSystem, Actor: "system"})
}

func (m *Manager) StartKeepaliveWithOperation(names []string, operation Operation) KeepaliveStartResult {
	keys, unknown := m.resolve(names)
	result := KeepaliveStartResult{Unknown: unknown}
	operation = normalizeOperation(operation)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shuttingDown {
		for _, key := range keys {
			result.Already = append(result.Already, m.targets[key].Name)
		}
		return result
	}
	changed := false
	for _, key := range keys {
		target := m.targets[key]
		current := m.keepalives[key]
		if current.state != KeepaliveStateStopped {
			result.Already = append(result.Already, target.Name)
			continue
		}

		current.runID++
		runID := current.runID
		keepaliveCtx, cancel := context.WithCancel(m.root)
		current.cancel = cancel
		current.state = KeepaliveStateRequesting
		current.requests = 0
		current.startedAt = time.Now()
		current.lastRequest = time.Time{}
		current.lastSuccess = time.Time{}
		current.lastFailure = time.Time{}
		current.nextRequest = time.Time{}
		current.stoppedAt = time.Time{}
		current.lastError = ""
		current.operation = operation
		result.Started = append(result.Started, target.Name)
		m.recordActivityLocked("keepalive.start", target.Name, operation, 0, "")
		m.signalTargetLocked(key)
		changed = true
		m.runWG.Add(1)
		go func() {
			defer m.runWG.Done()
			m.runKeepalive(keepaliveCtx, key, runID)
		}()
	}
	if changed {
		m.broadcastStateLocked()
	}

	return result
}

func (m *Manager) StopKeepalive(names []string) KeepaliveStopResult {
	return m.StopKeepaliveWithOperation(names, Operation{Source: SourceSystem, Actor: "system"})
}

func (m *Manager) StopKeepaliveWithOperation(names []string, operation Operation) KeepaliveStopResult {
	keys, unknown := m.resolve(names)
	result := KeepaliveStopResult{Unknown: unknown}
	operation = normalizeOperation(operation)

	m.mu.Lock()
	changed := false
	for _, key := range keys {
		target := m.targets[key]
		current := m.keepalives[key]
		if current.state == KeepaliveStateStopped {
			result.Inactive = append(result.Inactive, target.Name)
			continue
		}
		cancel := current.cancel
		current.runID++
		current.cancel = nil
		current.state = KeepaliveStateStopped
		current.stoppedAt = time.Now()
		current.nextRequest = time.Time{}
		result.Stopped = append(result.Stopped, target.Name)
		m.recordActivityLocked("keepalive.stop", target.Name, operation, current.requests, "")
		m.signalTargetLocked(key)
		changed = true
		if cancel != nil {
			cancel()
		}
	}
	if changed {
		m.broadcastStateLocked()
	}
	m.mu.Unlock()
	return result
}

func (m *Manager) Snapshots(names []string) ([]Snapshot, []string) {
	keys, unknown := m.resolve(names)
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]Snapshot, 0, len(keys))
	for _, key := range keys {
		result = append(result, m.queueSnapshotLocked(key))
	}
	return result, unknown
}

func (m *Manager) KeepaliveSnapshots(names []string) ([]KeepaliveSnapshot, []string) {
	keys, unknown := m.resolve(names)
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]KeepaliveSnapshot, 0, len(keys))
	for _, key := range keys {
		result = append(result, m.keepaliveSnapshotLocked(key))
	}
	return result, unknown
}

// ComprehensiveSnapshot returns all target and process state while holding a
// single lock, so the Web and OpenILink views cannot observe mixed moments.
func (m *Manager) ComprehensiveSnapshot() ManagerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *Manager) DashboardSnapshot() (ManagerSnapshot, []Activity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked(), m.activitiesLocked()
}

// Activities returns newest-first copies of the in-memory activity log.
func (m *Manager) Activities() []Activity {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activitiesLocked()
}

// Observe atomically registers an observer and returns the initial state and
// activity log. Producers never block: an observer whose buffer fills is
// removed and its channel is closed, allowing an SSE client to reconnect.
func (m *Manager) Observe(buffer int) (ManagerSnapshot, []Activity, *Subscription) {
	if buffer <= 0 {
		buffer = 32
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextObserverID++
	id := m.nextObserverID
	channel := make(chan Event, buffer)
	m.observers[id] = channel
	subscription := &Subscription{
		Events: channel,
		cancel: func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			if current, ok := m.observers[id]; ok {
				delete(m.observers, id)
				close(current)
			}
		},
	}
	return m.snapshotLocked(), m.activitiesLocked(), subscription
}

func (m *Manager) ObserverCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.observers)
}

// BeginShutdown prevents new jobs and cancels every active run. Call Wait
// after request ingress has stopped to wait for Codex subprocess cleanup.
func (m *Manager) BeginShutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shuttingDown {
		return
	}
	m.shuttingDown = true
	for _, current := range m.jobs {
		if current.cancel != nil {
			current.cancel()
		}
	}
	for _, current := range m.keepalives {
		if current.cancel != nil {
			current.cancel()
		}
	}
}

func (m *Manager) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		m.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) TargetNames() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.order))
	for _, key := range m.order {
		names = append(names, m.targets[key].Name)
	}
	return names
}

func (m *Manager) queueSnapshotLocked(key string) Snapshot {
	target := m.targets[key]
	current := m.jobs[key]
	return Snapshot{
		Name:        target.Name,
		Model:       target.Model,
		APIHost:     codex.TargetHost(target),
		State:       current.state,
		Attempts:    current.attempts,
		StartedAt:   current.startedAt,
		LastAttempt: current.lastAttempt,
		NextAttempt: current.nextAttempt,
		FinishedAt:  current.finishedAt,
		LastError:   current.lastError,
	}
}

func (m *Manager) keepaliveSnapshotLocked(key string) KeepaliveSnapshot {
	target := m.targets[key]
	current := m.keepalives[key]
	return KeepaliveSnapshot{
		Name:        target.Name,
		Model:       target.Model,
		APIHost:     codex.TargetHost(target),
		State:       current.state,
		Requests:    current.requests,
		StartedAt:   current.startedAt,
		LastRequest: current.lastRequest,
		LastSuccess: current.lastSuccess,
		LastFailure: current.lastFailure,
		NextRequest: current.nextRequest,
		StoppedAt:   current.stoppedAt,
		LastError:   current.lastError,
	}
}

func (m *Manager) snapshotLocked() ManagerSnapshot {
	snapshot := ManagerSnapshot{
		Targets:          make([]TargetSnapshot, 0, len(m.order)),
		CurrentProcesses: m.currentProcesses,
		MaxParallel:      m.maxParallel,
	}
	for _, key := range m.order {
		queue := m.queueSnapshotLocked(key)
		keepalive := m.keepaliveSnapshotLocked(key)
		snapshot.Targets = append(snapshot.Targets, TargetSnapshot{
			Name:      queue.Name,
			Model:     queue.Model,
			APIHost:   queue.APIHost,
			Queue:     queue,
			Keepalive: keepalive,
		})
	}
	return snapshot
}

func (m *Manager) activitiesLocked() []Activity {
	result := make([]Activity, len(m.activities))
	for i := range m.activities {
		result[len(m.activities)-1-i] = m.activities[i]
	}
	return result
}

func (m *Manager) recordActivityLocked(activityType, target string, operation Operation, attempts int, errorText string) {
	operation = normalizeOperation(operation)
	if targetConfig, ok := m.targets[normalizeName(target)]; ok {
		errorText = sanitizeTargetError(targetConfig, errorText)
	}
	m.nextEventID++
	activity := Activity{
		ID:       m.nextEventID,
		Type:     activityType,
		Target:   target,
		Source:   operation.Source,
		Actor:    operation.Actor,
		Attempts: attempts,
		At:       time.Now(),
		Error:    truncate(errorText, 600),
	}
	m.activities = append(m.activities, activity)
	if len(m.activities) > m.activityLimit {
		copy(m.activities, m.activities[len(m.activities)-m.activityLimit:])
		m.activities = m.activities[:m.activityLimit]
	}
	activityCopy := activity
	m.publishLocked(Event{ID: activity.ID, Kind: EventActivity, Activity: &activityCopy})
}

func (m *Manager) broadcastStateLocked() {
	m.nextEventID++
	snapshot := m.snapshotLocked()
	m.publishLocked(Event{ID: m.nextEventID, Kind: EventState, Snapshot: &snapshot})
}

func (m *Manager) publishLocked(event Event) {
	for id, observer := range m.observers {
		select {
		case observer <- event:
		default:
			delete(m.observers, id)
			close(observer)
		}
	}
}

func (m *Manager) run(ctx context.Context, key string, runID uint64) {
	for {
		if !m.acquireQueueTarget(ctx, key, runID) {
			m.markQueueCancelled(key, runID)
			return
		}
		if !m.acquire(ctx) {
			m.markQueueCancelled(key, runID)
			return
		}

		attempt, target, ok := m.beginAttempt(key, runID)
		if !ok {
			m.releaseSlot()
			m.releaseTarget(key, requestOwnerQueue, runID)
			return
		}
		m.logger.Info("starting Codex queue attempt", "target", target.Name, "attempt", attempt, "api_host", codex.TargetHost(target))
		result := m.runner.Run(ctx, target, attempt)
		m.finishProcess()

		if result.Success {
			subscribers, elapsed, ok := m.markSuccess(key, runID)
			if !ok {
				return
			}
			m.logger.Info("Codex queue target succeeded", "target", target.Name, "attempt", attempt, "elapsed", elapsed)
			m.notifySuccess(target.Name, attempt, elapsed, subscribers)
			return
		}
		if ctx.Err() != nil {
			m.markQueueCancelled(key, runID)
			return
		}

		delay := randomDuration(m.retryMin, m.retryMax)
		if !m.markFailure(key, runID, result, delay) {
			return
		}
		m.logger.Warn("Codex queue attempt failed", "target", target.Name, "attempt", attempt, "error", sanitizeTargetError(target, result.Error), "retry_in", delay)
		if !wait(ctx, delay) {
			m.markQueueCancelled(key, runID)
			return
		}
	}
}

func (m *Manager) runKeepalive(ctx context.Context, key string, runID uint64) {
	for {
		request, target, ok := m.beginKeepaliveRequest(ctx, key, runID)
		if !ok {
			m.markKeepaliveCancelled(key, runID)
			return
		}
		m.logger.Info("starting Codex keepalive request", "target", target.Name, "request", request, "api_host", codex.TargetHost(target))
		result := m.runner.Run(ctx, target, request)
		m.finishProcess()

		if ctx.Err() != nil {
			m.markKeepaliveCancelled(key, runID)
			return
		}
		delay := randomDuration(m.keepaliveMin, m.keepaliveMax)
		if !m.finishKeepaliveRequest(key, runID, result, delay) {
			return
		}
		if result.Success {
			m.logger.Info("Codex keepalive request succeeded", "target", target.Name, "request", request, "next_in", delay)
		} else {
			m.logger.Warn("Codex keepalive request failed", "target", target.Name, "request", request, "error", sanitizeTargetError(target, result.Error), "next_in", delay)
		}
		if !wait(ctx, delay) {
			m.markKeepaliveCancelled(key, runID)
			return
		}
	}
}

func (m *Manager) acquireQueueTarget(ctx context.Context, key string, runID uint64) bool {
	for {
		m.mu.Lock()
		current := m.jobs[key]
		arbiter := m.arbiters[key]
		if current.runID != runID || current.state != StateRunning {
			m.mu.Unlock()
			return false
		}
		if arbiter.owner == requestOwnerNone {
			arbiter.owner = requestOwnerQueue
			arbiter.ownerRunID = runID
			m.mu.Unlock()
			return true
		}
		changed := arbiter.changed
		m.mu.Unlock()
		if !waitForChange(ctx, changed) {
			return false
		}
	}
}

func (m *Manager) beginKeepaliveRequest(ctx context.Context, key string, runID uint64) (int, config.Target, bool) {
	for {
		m.mu.Lock()
		current := m.keepalives[key]
		queue := m.jobs[key]
		arbiter := m.arbiters[key]
		if current.runID != runID || current.state == KeepaliveStateStopped {
			m.mu.Unlock()
			return 0, config.Target{}, false
		}
		if queue.state == StateRunning || arbiter.owner != requestOwnerNone {
			previousState := current.state
			if queue.state == StateRunning || arbiter.owner == requestOwnerQueue {
				current.state = KeepaliveStateWaitingQueue
			} else {
				current.state = KeepaliveStateRequesting
			}
			if current.state != previousState {
				m.broadcastStateLocked()
			}
			changed := arbiter.changed
			m.mu.Unlock()
			if !waitForChange(ctx, changed) {
				return 0, config.Target{}, false
			}
			continue
		}
		if current.state != KeepaliveStateRequesting {
			current.state = KeepaliveStateRequesting
			m.broadcastStateLocked()
		}
		m.mu.Unlock()

		if !m.acquire(ctx) {
			return 0, config.Target{}, false
		}

		m.mu.Lock()
		current = m.keepalives[key]
		queue = m.jobs[key]
		arbiter = m.arbiters[key]
		if current.runID != runID || current.state == KeepaliveStateStopped {
			m.mu.Unlock()
			m.releaseSlot()
			return 0, config.Target{}, false
		}
		if queue.state == StateRunning || arbiter.owner != requestOwnerNone {
			previousState := current.state
			if queue.state == StateRunning || arbiter.owner == requestOwnerQueue {
				current.state = KeepaliveStateWaitingQueue
			}
			if current.state != previousState {
				m.broadcastStateLocked()
			}
			changed := arbiter.changed
			m.mu.Unlock()
			m.releaseSlot()
			if !waitForChange(ctx, changed) {
				return 0, config.Target{}, false
			}
			continue
		}

		arbiter.owner = requestOwnerKeepalive
		arbiter.ownerRunID = runID
		current.state = KeepaliveStateRequesting
		current.requests++
		current.lastRequest = time.Now()
		current.nextRequest = time.Time{}
		m.currentProcesses++
		request := current.requests
		target := m.targets[key]
		m.broadcastStateLocked()
		m.mu.Unlock()
		return request, target, true
	}
}

func (m *Manager) beginAttempt(key string, runID uint64) (int, config.Target, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	arbiter := m.arbiters[key]
	if current.runID != runID || current.state != StateRunning || arbiter.owner != requestOwnerQueue || arbiter.ownerRunID != runID {
		return 0, config.Target{}, false
	}
	current.attempts++
	current.lastAttempt = time.Now()
	current.nextAttempt = time.Time{}
	m.currentProcesses++
	m.broadcastStateLocked()
	return current.attempts, m.targets[key], true
}

func (m *Manager) markFailure(key string, runID uint64, result codex.Result, delay time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	valid := current.runID == runID && current.state == StateRunning
	if valid {
		target := m.targets[key]
		current.lastError = sanitizeTargetError(target, result.Error)
		current.nextAttempt = time.Now().Add(delay)
		m.recordActivityLocked("queue.request.failure", target.Name, current.operation, current.attempts, current.lastError)
	}
	m.releaseTargetLocked(key, requestOwnerQueue, runID)
	if valid {
		m.broadcastStateLocked()
	}
	return valid
}

func (m *Manager) markSuccess(key string, runID uint64) ([]Subscriber, time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	if current.runID != runID || current.state != StateRunning {
		m.releaseTargetLocked(key, requestOwnerQueue, runID)
		return nil, 0, false
	}
	current.state = StateSucceeded
	current.finishedAt = time.Now()
	current.nextAttempt = time.Time{}
	current.lastError = ""
	if current.cancel != nil {
		current.cancel()
		current.cancel = nil
	}
	subscribers := make([]Subscriber, 0, len(current.subscribers))
	for _, subscriber := range current.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	current.subscribers = make(map[string]Subscriber)
	target := m.targets[key]
	m.recordActivityLocked("queue.request.success", target.Name, current.operation, current.attempts, "")
	m.releaseTargetLocked(key, requestOwnerQueue, runID)
	m.broadcastStateLocked()
	return subscribers, current.finishedAt.Sub(current.startedAt), true
}

func (m *Manager) finishKeepaliveRequest(key string, runID uint64, result codex.Result, delay time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.keepalives[key]
	if current.runID != runID || current.state == KeepaliveStateStopped {
		m.releaseTargetLocked(key, requestOwnerKeepalive, runID)
		return false
	}
	now := time.Now()
	if result.Success {
		current.lastSuccess = now
	} else {
		current.lastFailure = now
		current.lastError = sanitizeTargetError(m.targets[key], result.Error)
	}
	current.state = KeepaliveStateWaitingNext
	current.nextRequest = now.Add(delay)
	target := m.targets[key]
	activityType := "keepalive.request.failure"
	activityError := current.lastError
	if result.Success {
		activityType = "keepalive.request.success"
		activityError = ""
	}
	m.recordActivityLocked(activityType, target.Name, current.operation, current.requests, activityError)
	m.releaseTargetLocked(key, requestOwnerKeepalive, runID)
	m.broadcastStateLocked()
	return true
}

func (m *Manager) markQueueCancelled(key string, runID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	m.releaseTargetLocked(key, requestOwnerQueue, runID)
	if current.runID != runID || current.state != StateRunning {
		return
	}
	current.state = StateStopped
	current.finishedAt = time.Now()
	current.nextAttempt = time.Time{}
	current.cancel = nil
	current.subscribers = make(map[string]Subscriber)
	target := m.targets[key]
	m.recordActivityLocked("queue.stop", target.Name, Operation{Source: SourceSystem, Actor: "shutdown"}, current.attempts, "")
	m.signalTargetLocked(key)
	m.broadcastStateLocked()
}

func (m *Manager) markKeepaliveCancelled(key string, runID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.keepalives[key]
	m.releaseTargetLocked(key, requestOwnerKeepalive, runID)
	if current.runID != runID || current.state == KeepaliveStateStopped {
		return
	}
	current.state = KeepaliveStateStopped
	current.stoppedAt = time.Now()
	current.nextRequest = time.Time{}
	current.cancel = nil
	target := m.targets[key]
	m.recordActivityLocked("keepalive.stop", target.Name, Operation{Source: SourceSystem, Actor: "shutdown"}, current.requests, "")
	m.signalTargetLocked(key)
	m.broadcastStateLocked()
}

func (m *Manager) releaseTarget(key string, owner requestOwner, runID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseTargetLocked(key, owner, runID)
}

func (m *Manager) releaseTargetLocked(key string, owner requestOwner, runID uint64) {
	arbiter := m.arbiters[key]
	if arbiter.owner != owner || arbiter.ownerRunID != runID {
		return
	}
	arbiter.owner = requestOwnerNone
	arbiter.ownerRunID = 0
	m.signalTargetLocked(key)
}

func (m *Manager) signalTargetLocked(key string) {
	arbiter := m.arbiters[key]
	close(arbiter.changed)
	arbiter.changed = make(chan struct{})
}

func (m *Manager) notifySuccess(target string, attempt int, elapsed time.Duration, subscribers []Subscriber) {
	if m.messenger == nil {
		return
	}
	message := fmt.Sprintf("✅ %s：%s（第 %d 次，耗时 %s）", target, m.successMessage, attempt, formatDuration(elapsed))
	for _, subscriber := range subscribers {
		subscriber := subscriber
		go m.notifySubscriber(target, message, subscriber)
	}
}

func (m *Manager) notifySubscriber(target, message string, subscriber Subscriber) {
	delay := 2 * time.Second
	for {
		ctx, cancel := context.WithTimeout(m.root, 20*time.Second)
		err := m.messenger.Send(ctx, subscriber.Recipient, message, subscriber.TraceID)
		cancel()
		if err == nil {
			return
		}
		m.logger.Error("failed to send success notification; will retry", "target", target, "recipient", subscriber.Recipient, "error", err, "retry_in", delay)
		if !wait(m.root, delay) {
			return
		}
		delay = minDuration(delay*2, time.Minute)
	}
}

func (m *Manager) resolve(names []string) ([]string, []string) {
	if len(names) == 0 || containsAll(names) {
		return append([]string(nil), m.order...), nil
	}
	seen := make(map[string]struct{}, len(names))
	var keys, unknown []string
	for _, name := range names {
		key := normalizeName(name)
		if key == "" {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		if _, ok := m.targets[key]; !ok {
			unknown = append(unknown, name)
			continue
		}
		keys = append(keys, key)
	}
	return keys, unknown
}

func (m *Manager) acquire(ctx context.Context) bool {
	select {
	case m.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *Manager) finishProcess() {
	m.mu.Lock()
	if m.currentProcesses > 0 {
		m.currentProcesses--
	}
	m.broadcastStateLocked()
	m.mu.Unlock()
	<-m.sem
}

func (m *Manager) releaseSlot() {
	<-m.sem
}

func normalizeName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeOperation(operation Operation) Operation {
	switch operation.Source {
	case SourceWeb, SourceOpenILink, SourceSystem:
	default:
		operation.Source = SourceSystem
	}
	operation.Actor = strings.TrimSpace(operation.Actor)
	if operation.Actor == "" {
		operation.Actor = string(operation.Source)
	}
	return operation
}

func containsAll(names []string) bool {
	for _, name := range names {
		switch normalizeName(name) {
		case "all", "全部", "*":
			return true
		}
	}
	return false
}

func randomDuration(minimum, maximum time.Duration) time.Duration {
	if maximum <= minimum {
		return minimum
	}
	return minimum + time.Duration(rand.Int63n(int64(maximum-minimum)+1))
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func waitForChange(ctx context.Context, changed <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-changed:
		return true
	}
}

func truncate(value string, maximum int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum]) + "…"
}

func sanitizeTargetError(target config.Target, value string) string {
	value = strings.TrimSpace(value)
	value = absoluteURLPattern.ReplaceAllString(value, "[URL]")
	for _, secret := range []string{target.APIKey, url.QueryEscape(target.APIKey), target.APIKeyEnv} {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return truncate(value, 600)
}

func formatDuration(duration time.Duration) string {
	duration = duration.Round(time.Second)
	if duration < time.Second {
		return "不足 1 秒"
	}
	return duration.String()
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
