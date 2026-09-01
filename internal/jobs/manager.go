package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"regexp"
	"sort"
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

type PromptRunner interface {
	RunPrompt(ctx context.Context, target config.Target, prompt string) codex.Result
}

type Messenger interface {
	Send(ctx context.Context, to, content, traceID string) error
}

type Subscriber struct {
	Recipient string
	TraceID   string
	Key       string
	Messenger Messenger
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

// KeepaliveStopResult is retained as an alias for source compatibility.  A
// stop operation now applies to whichever mode is active on each target.
type KeepaliveStopResult = StopResult

// TaskStopResult is the result returned by the unified task.stop action.  It
// intentionally has the same wire shape as the historical queue/keepalive
// stop results so callers can migrate without a data-format change.
type TaskStopResult = StopResult

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
	ID           int64
	SortOrder    int
	Name         string
	Model        string
	APIHost      string
	Busy         bool
	AdhocRunning bool
	Queue        Snapshot
	Keepalive    KeepaliveSnapshot
}

type AdhocRunResult struct {
	Target config.Target
	Result codex.Result
}

type ManagerSnapshot struct {
	Targets          []TargetSnapshot
	CurrentProcesses int
	MaxParallel      int
}

type Event struct {
	ID       uint64
	Snapshot *ManagerSnapshot
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
	keepaliveMin   time.Duration
	keepaliveMax   time.Duration
	successMessage string
	maxParallel    int

	mu               sync.Mutex
	targets          map[string]config.Target
	order            []string
	jobs             map[string]*job
	keepalives       map[string]*keepaliveJob
	arbiters         map[string]*targetArbiter
	currentProcesses int
	slotsInUse       int
	nextEventID      uint64
	nextObserverID   uint64
	nextAdhocRunID   uint64
	observers        map[uint64]chan Event
	runWG            sync.WaitGroup
	shuttingDown     bool
	parallelChanged  chan struct{}
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
	workers     int
}

type keepaliveJob struct {
	state           KeepaliveState
	requests        int
	startedAt       time.Time
	lastRequest     time.Time
	lastSuccess     time.Time
	lastFailure     time.Time
	nextRequest     time.Time
	stoppedAt       time.Time
	lastError       string
	runID           uint64
	cancel          context.CancelFunc
	scheduleChanged chan struct{}
	workers         int
}

type requestOwner uint8

const (
	requestOwnerNone requestOwner = iota
	requestOwnerQueue
	requestOwnerKeepalive
	requestOwnerAdhoc
)

type targetArbiter struct {
	owner      requestOwner
	ownerRunID uint64
	cancel     context.CancelFunc
	changed    chan struct{}
}

var (
	ErrTargetBusy       = errors.New("target has an active task")
	ErrTargetNotFound   = errors.New("target not found")
	ErrTargetConflict   = errors.New("target name already exists")
	ErrAdhocUnavailable = errors.New("runner does not support adhoc prompts")
	ErrShuttingDown     = errors.New("manager is shutting down")
)

func New(
	root context.Context,
	targets []config.Target,
	runner AttemptRunner,
	messenger Messenger,
	logger *slog.Logger,
	_, _ time.Duration,
	keepaliveMin, keepaliveMax time.Duration,
	maxParallel int,
	successMessage string,
	_ ...int,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		root:            root,
		runner:          runner,
		messenger:       messenger,
		logger:          logger,
		keepaliveMin:    keepaliveMin,
		keepaliveMax:    keepaliveMax,
		successMessage:  successMessage,
		maxParallel:     maxParallel,
		targets:         make(map[string]config.Target, len(targets)),
		jobs:            make(map[string]*job, len(targets)),
		keepalives:      make(map[string]*keepaliveJob, len(targets)),
		arbiters:        make(map[string]*targetArbiter, len(targets)),
		observers:       make(map[uint64]chan Event),
		parallelChanged: make(chan struct{}),
	}
	for _, target := range targets {
		key := normalizeName(target.Name)
		m.targets[key] = target
		m.order = append(m.order, key)
		m.jobs[key] = &job{state: StateIdle, subscribers: make(map[string]Subscriber)}
		m.keepalives[key] = &keepaliveJob{state: KeepaliveStateStopped, scheduleChanged: make(chan struct{})}
		m.arbiters[key] = &targetArbiter{changed: make(chan struct{})}
	}
	return m
}

func (m *Manager) Start(names []string, subscriber Subscriber) StartResult {
	m.mu.Lock()
	keys, unknown := m.resolveLocked(names)
	m.mu.Unlock()
	result := StartResult{Unknown: unknown}
	for _, key := range keys {
		name, started := m.startQueue(key, subscriber)
		if name == "" {
			continue
		}
		if started {
			result.Started = append(result.Started, name)
		} else {
			result.Already = append(result.Already, name)
		}
	}
	return result
}

func (m *Manager) startQueue(key string, subscriber Subscriber) (string, bool) {
	for {
		// A target has one active mode. Starting queue mode explicitly
		// switches off keepalive mode and waits for its worker to exit.
		if !m.prepareMode(key, requestOwnerQueue) {
			m.mu.Lock()
			defer m.mu.Unlock()
			target, ok := m.targets[key]
			if !ok {
				return "", false
			}
			if subscriber.Recipient != "" && m.jobs[key].state == StateRunning {
				m.addSubscriberLocked(m.jobs[key], subscriber)
			}
			return target.Name, false
		}

		m.mu.Lock()
		if m.shuttingDown {
			target, ok := m.targets[key]
			m.mu.Unlock()
			if !ok {
				return "", false
			}
			return target.Name, false
		}
		target, exists := m.targets[key]
		if !exists {
			m.mu.Unlock()
			return "", false
		}
		current := m.jobs[key]
		if current.state == StateRunning {
			m.addSubscriberLocked(current, subscriber)
			m.mu.Unlock()
			return target.Name, false
		}
		// Another caller may have started keepalive after prepareMode released
		// the manager lock. Retry the transition instead of starting both modes.
		keepalive := m.keepalives[key]
		if keepalive.state != KeepaliveStateStopped || keepalive.workers > 0 {
			m.mu.Unlock()
			continue
		}

		m.addSubscriberLocked(current, subscriber)
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
		m.broadcastStateLocked()
		m.signalTargetLocked(key)
		m.runWG.Add(1)
		current.workers++
		go func() {
			defer m.runWG.Done()
			defer m.finishQueueWorker(key)
			m.run(jobCtx, key, runID)
		}()
		m.mu.Unlock()
		return target.Name, true
	}
}

func (m *Manager) addSubscriberLocked(current *job, subscriber Subscriber) {
	if subscriber.Recipient == "" {
		return
	}
	subscriberKey := strings.TrimSpace(subscriber.Key)
	if subscriberKey == "" {
		subscriberKey = subscriber.Recipient
	}
	current.subscribers[subscriberKey] = subscriber
}

func (m *Manager) Stop(names []string) StopResult {
	return m.stop(names)
}

// StopTask is the canonical unified stop operation.  Stop and the historical
// queue.stop/keepalive.stop methods delegate to the same implementation.
func (m *Manager) StopTask(names []string) TaskStopResult {
	return m.stop(names)
}

func (m *Manager) stop(names []string) StopResult {
	m.mu.Lock()
	keys, unknown := m.resolveLocked(names)
	result := StopResult{Unknown: unknown}
	changed := false
	for _, key := range keys {
		target := m.targets[key]
		current := m.jobs[key]
		keepalive := m.keepalives[key]
		stopped := false
		if current.state == StateRunning || current.workers > 0 {
			m.stopQueueLocked(key)
			stopped = true
		}
		if keepalive.state != KeepaliveStateStopped || keepalive.workers > 0 {
			m.stopKeepaliveLocked(key)
			stopped = true
		}
		if m.arbiters[key].owner == requestOwnerAdhoc {
			m.stopAdhocLocked(key)
			stopped = true
		}
		if stopped {
			result.Stopped = append(result.Stopped, target.Name)
			changed = true
		} else {
			result.Inactive = append(result.Inactive, target.Name)
		}
	}
	if changed {
		m.broadcastStateLocked()
	}
	m.mu.Unlock()
	return result
}

func (m *Manager) StartKeepalive(names []string) KeepaliveStartResult {
	m.mu.Lock()
	keys, unknown := m.resolveLocked(names)
	m.mu.Unlock()
	result := KeepaliveStartResult{Unknown: unknown}
	for _, key := range keys {
		name, started := m.startKeepalive(key)
		if name == "" {
			continue
		}
		if started {
			result.Started = append(result.Started, name)
		} else {
			result.Already = append(result.Already, name)
		}
	}
	return result
}

func (m *Manager) startKeepalive(key string) (string, bool) {
	for {
		// Keepalive and queue are mutually exclusive on a target. Switching
		// modes cancels the old worker and waits for its runner call to exit.
		if !m.prepareMode(key, requestOwnerKeepalive) {
			m.mu.Lock()
			defer m.mu.Unlock()
			if target, ok := m.targets[key]; ok {
				return target.Name, false
			}
			return "", false
		}
		m.mu.Lock()
		if m.shuttingDown {
			target, ok := m.targets[key]
			m.mu.Unlock()
			if !ok {
				return "", false
			}
			return target.Name, false
		}
		target, exists := m.targets[key]
		if !exists {
			m.mu.Unlock()
			return "", false
		}
		current := m.keepalives[key]
		if current.state != KeepaliveStateStopped {
			m.mu.Unlock()
			return target.Name, false
		}
		// Another caller may have started queue mode after prepareMode released
		// the manager lock. Retry the transition instead of starting both modes.
		queue := m.jobs[key]
		if queue.state == StateRunning || queue.workers > 0 {
			m.mu.Unlock()
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
		m.broadcastStateLocked()
		m.signalTargetLocked(key)
		m.runWG.Add(1)
		current.workers++
		go func() {
			defer m.runWG.Done()
			defer m.finishKeepaliveWorker(key)
			m.runKeepalive(keepaliveCtx, key, runID)
		}()
		m.mu.Unlock()
		return target.Name, true
	}
}

func (m *Manager) StopKeepalive(names []string) KeepaliveStopResult {
	// Historically this method only stopped keepalive.  Keep it as a
	// compatibility name, but make it obey the unified task.stop semantics so
	// `/停止保活` cannot leave a queue task running on the same target.
	return m.stop(names)
}

func (m *Manager) Snapshots(names []string) ([]Snapshot, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys, unknown := m.resolveLocked(names)

	result := make([]Snapshot, 0, len(keys))
	for _, key := range keys {
		result = append(result, m.queueSnapshotLocked(key))
	}
	return result, unknown
}

func (m *Manager) KeepaliveSnapshots(names []string) ([]KeepaliveSnapshot, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys, unknown := m.resolveLocked(names)

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

// Observe atomically registers a state observer and returns the initial
// snapshot. Producers never block: an observer whose buffer fills is removed
// and its channel is closed, allowing an SSE client to reconnect.
func (m *Manager) Observe(buffer int) (ManagerSnapshot, *Subscription) {
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
	return m.snapshotLocked(), subscription
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
	for _, arbiter := range m.arbiters {
		if arbiter.owner == requestOwnerAdhoc && arbiter.cancel != nil {
			arbiter.cancel()
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

// SetMessenger replaces the default success-notification route.  A stable
// proxy can be installed here while its underlying message client is hot
// reloaded independently.
func (m *Manager) SetMessenger(messenger Messenger) {
	m.mu.Lock()
	m.messenger = messenger
	m.mu.Unlock()
}

// UpdateSettings applies request-level configuration without interrupting
// running Codex processes. Waiting retry and keepalive timers are resampled
// from the update instant when their interval changes.
func (m *Manager) UpdateSettings(
	_, _ time.Duration,
	keepaliveMin, keepaliveMax time.Duration,
	maxParallel int,
	successMessage string,
	_ ...int,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if keepaliveMin != m.keepaliveMin || keepaliveMax != m.keepaliveMax {
		m.keepaliveMin, m.keepaliveMax = keepaliveMin, keepaliveMax
		for key, current := range m.keepalives {
			if current.state == KeepaliveStateWaitingNext && !current.nextRequest.IsZero() {
				current.nextRequest = now.Add(randomDuration(m.keepaliveMin, m.keepaliveMax))
				m.signalKeepaliveScheduleLocked(key)
			}
		}
	}
	if maxParallel > 0 && maxParallel != m.maxParallel {
		m.maxParallel = maxParallel
		m.signalParallelLocked()
	}
	m.successMessage = successMessage
	m.broadcastStateLocked()
}

func (m *Manager) TargetByID(id int64) (config.Target, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok := m.targetKeyByIDLocked(id)
	if !ok {
		return config.Target{}, false
	}
	return m.targets[key], true
}

// RunAdhoc reserves a target and one global process slot for a single
// caller-supplied prompt. The request is exclusive with queue and keepalive
// work on the same target and is included in shutdown/process accounting.
func (m *Manager) RunAdhoc(ctx context.Context, id int64, prompt string) (AdhocRunResult, error) {
	runner, ok := m.runner.(PromptRunner)
	if !ok {
		return AdhocRunResult{}, ErrAdhocUnavailable
	}

	started := time.Now()
	m.mu.Lock()
	key, ok := m.targetKeyByIDLocked(id)
	if !ok {
		m.mu.Unlock()
		return AdhocRunResult{}, ErrTargetNotFound
	}
	if m.shuttingDown || m.root.Err() != nil {
		m.mu.Unlock()
		return AdhocRunResult{}, ErrShuttingDown
	}
	if m.targetBusyLocked(key) {
		m.mu.Unlock()
		return AdhocRunResult{}, ErrTargetBusy
	}

	runCtx, cancel := context.WithCancel(ctx)
	stopRootCancel := context.AfterFunc(m.root, cancel)
	m.nextAdhocRunID++
	runID := m.nextAdhocRunID
	target := m.targets[key]
	arbiter := m.arbiters[key]
	arbiter.owner = requestOwnerAdhoc
	arbiter.ownerRunID = runID
	arbiter.cancel = cancel
	m.runWG.Add(1)
	m.broadcastStateLocked()
	m.mu.Unlock()

	slotAcquired := false
	processStarted := false
	defer func() {
		cancel()
		stopRootCancel()
		m.finishAdhocRequest(key, runID, slotAcquired, processStarted)
	}()

	cancelled := func() (AdhocRunResult, error) {
		return AdhocRunResult{
			Target: target,
			Result: codex.Result{
				ExitCode: codex.ExitCodeUnavailable,
				Error:    "request cancelled",
				Duration: time.Since(started),
			},
		}, nil
	}
	if !m.acquire(runCtx) {
		return cancelled()
	}
	slotAcquired = true
	if runCtx.Err() != nil {
		return cancelled()
	}

	m.mu.Lock()
	arbiter = m.arbiters[key]
	if arbiter == nil || arbiter.owner != requestOwnerAdhoc || arbiter.ownerRunID != runID {
		m.mu.Unlock()
		return cancelled()
	}
	m.currentProcesses++
	processStarted = true
	m.broadcastStateLocked()
	m.mu.Unlock()

	result := runner.RunPrompt(runCtx, target, prompt)
	return AdhocRunResult{Target: target, Result: result}, nil
}

// CreateTarget serializes persistence with target resolution so commands
// cannot start a just-created target in a partially applied state.
func (m *Manager) CreateTarget(target *config.Target, persist func() error) error {
	if target == nil {
		return ErrTargetNotFound
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := normalizeName(target.Name)
	if _, exists := m.targets[key]; exists {
		return ErrTargetConflict
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	key = normalizeName(target.Name)
	if _, exists := m.targets[key]; exists {
		return ErrTargetConflict
	}
	m.targets[key] = *target
	m.jobs[key] = &job{state: StateIdle, subscribers: make(map[string]Subscriber)}
	m.keepalives[key] = &keepaliveJob{state: KeepaliveStateStopped, scheduleChanged: make(chan struct{})}
	m.arbiters[key] = &targetArbiter{changed: make(chan struct{})}
	m.order = append(m.order, key)
	m.sortTargetsLocked()
	m.broadcastStateLocked()
	return nil
}

func (m *Manager) UpdateTarget(id int64, target *config.Target, persist func() error) error {
	if target == nil {
		return ErrTargetNotFound
	}
	m.mu.Lock()
	oldKey, ok := m.targetKeyByIDLocked(id)
	if !ok {
		m.mu.Unlock()
		return ErrTargetNotFound
	}
	if m.targetBusyLocked(oldKey) {
		m.stopQueueLocked(oldKey)
		m.stopKeepaliveLocked(oldKey)
		m.stopAdhocLocked(oldKey)
		m.broadcastStateLocked()
	}
	m.mu.Unlock()
	if !m.waitTargetIdle(oldKey, 10*time.Second) {
		return ErrTargetBusy
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check after waiting.  Holding the lock across persistence prevents a
	// concurrent start from observing a half-applied target update.
	oldKey, ok = m.targetKeyByIDLocked(id)
	if !ok {
		return ErrTargetNotFound
	}
	if m.targetBusyLocked(oldKey) {
		return ErrTargetBusy
	}
	newKey := normalizeName(target.Name)
	if existing, exists := m.targets[newKey]; exists && existing.ID != id {
		return ErrTargetConflict
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	newKey = normalizeName(target.Name)
	if existing, exists := m.targets[newKey]; exists && existing.ID != id {
		return ErrTargetConflict
	}
	if newKey != oldKey {
		currentJob := m.jobs[oldKey]
		currentKeepalive := m.keepalives[oldKey]
		currentArbiter := m.arbiters[oldKey]
		delete(m.targets, oldKey)
		delete(m.jobs, oldKey)
		delete(m.keepalives, oldKey)
		delete(m.arbiters, oldKey)
		m.jobs[newKey] = currentJob
		m.keepalives[newKey] = currentKeepalive
		m.arbiters[newKey] = currentArbiter
		for index := range m.order {
			if m.order[index] == oldKey {
				m.order[index] = newKey
				break
			}
		}
	}
	m.targets[newKey] = *target
	m.sortTargetsLocked()
	m.broadcastStateLocked()
	return nil
}

func (m *Manager) DeleteTarget(id int64, persist func() error) error {
	m.mu.Lock()
	key, ok := m.targetKeyByIDLocked(id)
	if !ok {
		m.mu.Unlock()
		return ErrTargetNotFound
	}
	if m.targetBusyLocked(key) {
		m.stopQueueLocked(key)
		m.stopKeepaliveLocked(key)
		m.stopAdhocLocked(key)
		m.broadcastStateLocked()
	}
	m.mu.Unlock()
	if !m.waitTargetIdle(key, 10*time.Second) {
		return ErrTargetBusy
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	key, ok = m.targetKeyByIDLocked(id)
	if !ok {
		return ErrTargetNotFound
	}
	if m.targetBusyLocked(key) {
		return ErrTargetBusy
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	delete(m.targets, key)
	delete(m.jobs, key)
	delete(m.keepalives, key)
	delete(m.arbiters, key)
	for index := range m.order {
		if m.order[index] == key {
			m.order = append(m.order[:index], m.order[index+1:]...)
			break
		}
	}
	m.broadcastStateLocked()
	return nil
}

func (m *Manager) targetKeyByIDLocked(id int64) (string, bool) {
	for _, key := range m.order {
		if m.targets[key].ID == id {
			return key, true
		}
	}
	return "", false
}

func (m *Manager) targetBusyLocked(key string) bool {
	return m.jobs[key].state == StateRunning || m.jobs[key].workers > 0 ||
		m.keepalives[key].state != KeepaliveStateStopped || m.keepalives[key].workers > 0 ||
		m.arbiters[key].owner != requestOwnerNone
}

// prepareMode enforces the one-mode-per-target invariant.  It is deliberately
// outside the manager's main lock while waiting: Codex runners must be able to
// finish and run their cleanup paths (which take the same lock) before a new
// mode is started.
func (m *Manager) prepareMode(key string, desired requestOwner) bool {
	for {
		m.mu.Lock()
		if _, ok := m.targets[key]; !ok || m.shuttingDown {
			m.mu.Unlock()
			return false
		}
		queue := m.jobs[key]
		keepalive := m.keepalives[key]
		arbiter := m.arbiters[key]
		if desired == requestOwnerQueue && queue.state == StateRunning {
			m.mu.Unlock()
			return false
		}
		if desired == requestOwnerKeepalive && keepalive.state != KeepaliveStateStopped {
			m.mu.Unlock()
			return false
		}

		changed := false
		if arbiter.owner == requestOwnerAdhoc {
			m.stopAdhocLocked(key)
			changed = true
		}
		if desired == requestOwnerQueue && (keepalive.state != KeepaliveStateStopped || keepalive.workers > 0) {
			m.stopKeepaliveLocked(key)
			changed = true
		}
		if desired == requestOwnerKeepalive && (queue.state == StateRunning || queue.workers > 0) {
			m.stopQueueLocked(key)
			changed = true
		}
		// A previous stop may still be unwinding its runner.  Do not let a new
		// worker acquire the target arbiter until that cleanup has completed.
		busy := m.targetBusyLocked(key)
		if changed {
			m.broadcastStateLocked()
		}
		m.mu.Unlock()

		if busy {
			if !m.waitTargetIdle(key, 10*time.Second) {
				return false
			}
			continue
		}
		return true
	}
}

// stopQueueLocked and stopKeepaliveLocked only mutate state and issue the
// cancellation signal.  The caller owns the manager lock and may batch a
// single state broadcast for several targets.
func (m *Manager) stopQueueLocked(key string) {
	current, ok := m.jobs[key]
	if !ok || (current.state != StateRunning && current.workers == 0) {
		return
	}
	cancel := current.cancel
	current.runID++
	current.cancel = nil
	if current.state == StateRunning {
		current.state = StateStopped
		current.finishedAt = time.Now()
		current.nextAttempt = time.Time{}
		current.subscribers = make(map[string]Subscriber)
	}
	m.signalTargetLocked(key)
	if cancel != nil {
		cancel()
	}
}

func (m *Manager) stopKeepaliveLocked(key string) {
	current, ok := m.keepalives[key]
	if !ok || (current.state == KeepaliveStateStopped && current.workers == 0) {
		return
	}
	cancel := current.cancel
	current.runID++
	current.cancel = nil
	if current.state != KeepaliveStateStopped {
		current.state = KeepaliveStateStopped
		current.stoppedAt = time.Now()
		current.nextRequest = time.Time{}
	}
	m.signalTargetLocked(key)
	if cancel != nil {
		cancel()
	}
}

func (m *Manager) stopAdhocLocked(key string) {
	arbiter, ok := m.arbiters[key]
	if !ok || arbiter.owner != requestOwnerAdhoc {
		return
	}
	cancel := arbiter.cancel
	arbiter.cancel = nil
	if cancel != nil {
		cancel()
	}
}

// waitTargetIdle waits until no worker or request owner remains for a target.
// It is exported for the Web CRUD layer, which must guarantee that a target is
// quiescent before persisting a replacement or deleting it.
func (m *Manager) WaitTargetIdle(id int64, timeout time.Duration) bool {
	m.mu.Lock()
	key, ok := m.targetKeyByIDLocked(id)
	m.mu.Unlock()
	if !ok {
		return false
	}
	return m.waitTargetIdle(key, timeout)
}

func (m *Manager) waitTargetIdle(key string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		m.mu.Lock()
		_, exists := m.targets[key]
		idle := exists && !m.targetBusyLocked(key) && m.arbiters[key].owner == requestOwnerNone
		m.mu.Unlock()
		if !exists || idle {
			return exists
		}
		if time.Now().After(deadline) {
			return false
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-m.root.Done():
			stopTimer(timer)
			return false
		case <-timer.C:
		}
	}
}

func (m *Manager) finishQueueWorker(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.jobs[key]; ok && current.workers > 0 {
		current.workers--
		m.broadcastStateLocked()
	}
}

func (m *Manager) finishKeepaliveWorker(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, ok := m.keepalives[key]; ok && current.workers > 0 {
		current.workers--
		m.broadcastStateLocked()
	}
}

func (m *Manager) sortTargetsLocked() {
	sort.SliceStable(m.order, func(i, j int) bool {
		left, right := m.targets[m.order[i]], m.targets[m.order[j]]
		if left.SortOrder != right.SortOrder {
			return left.SortOrder < right.SortOrder
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		return strings.ToLower(left.Name) < strings.ToLower(right.Name)
	})
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
		target := m.targets[key]
		queue := m.queueSnapshotLocked(key)
		keepalive := m.keepaliveSnapshotLocked(key)
		snapshot.Targets = append(snapshot.Targets, TargetSnapshot{
			ID:           target.ID,
			SortOrder:    target.SortOrder,
			Name:         queue.Name,
			Model:        queue.Model,
			APIHost:      queue.APIHost,
			Busy:         m.targetBusyLocked(key),
			AdhocRunning: m.arbiters[key].owner == requestOwnerAdhoc,
			Queue:        queue,
			Keepalive:    keepalive,
		})
	}
	return snapshot
}

func (m *Manager) broadcastStateLocked() {
	m.nextEventID++
	snapshot := m.snapshotLocked()
	m.publishLocked(Event{ID: m.nextEventID, Snapshot: &snapshot})
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

		delay, ok := m.markFailure(key, runID, result)
		if !ok {
			return
		}
		m.logger.Warn("Codex queue attempt failed", "target", target.Name, "attempt", attempt, "error", sanitizeTargetError(target, result.Error), "retry_in", delay)
		// Queue admission is intentionally aggressive: after a failed request
		// the worker immediately competes for the global slot again.  Retry
		// interval fields remain persisted for rollback compatibility, but are
		// no longer used by the runtime.
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
		delay, ok := m.finishKeepaliveRequest(key, runID, result)
		if !ok {
			return
		}
		if result.Success {
			m.logger.Info("Codex keepalive request succeeded", "target", target.Name, "request", request, "next_in", delay)
		} else {
			m.logger.Warn("Codex keepalive request failed", "target", target.Name, "request", request, "error", sanitizeTargetError(target, result.Error), "next_in", delay)
		}
		if !m.waitKeepaliveNext(ctx, key, runID) {
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

func (m *Manager) markFailure(key string, runID uint64, result codex.Result) (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	valid := current.runID == runID && current.state == StateRunning
	// Keep the return value for callers and old diagnostics, but do not use it
	// to schedule queue retries.  Only keepalive requests are randomized.
	delay := time.Duration(0)
	if valid {
		target := m.targets[key]
		current.lastError = sanitizeTargetError(target, result.Error)
		current.nextAttempt = time.Time{}
	}
	m.releaseTargetLocked(key, requestOwnerQueue, runID)
	if valid {
		m.broadcastStateLocked()
	}
	return delay, valid
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
	m.releaseTargetLocked(key, requestOwnerQueue, runID)
	m.broadcastStateLocked()
	return subscribers, current.finishedAt.Sub(current.startedAt), true
}

func (m *Manager) finishKeepaliveRequest(key string, runID uint64, result codex.Result) (time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.keepalives[key]
	if current.runID != runID || current.state == KeepaliveStateStopped {
		m.releaseTargetLocked(key, requestOwnerKeepalive, runID)
		return 0, false
	}
	now := time.Now()
	delay := randomDuration(m.keepaliveMin, m.keepaliveMax)
	if result.Success {
		current.lastSuccess = now
	} else {
		current.lastFailure = now
		current.lastError = sanitizeTargetError(m.targets[key], result.Error)
	}
	current.state = KeepaliveStateWaitingNext
	current.nextRequest = now.Add(delay)
	m.releaseTargetLocked(key, requestOwnerKeepalive, runID)
	m.broadcastStateLocked()
	return delay, true
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
	arbiter.cancel = nil
	m.signalTargetLocked(key)
}

func (m *Manager) signalTargetLocked(key string) {
	arbiter := m.arbiters[key]
	close(arbiter.changed)
	arbiter.changed = make(chan struct{})
}

func (m *Manager) notifySuccess(target string, attempt int, elapsed time.Duration, subscribers []Subscriber) {
	if len(subscribers) == 0 {
		return
	}
	m.mu.Lock()
	successMessage := m.successMessage
	m.mu.Unlock()
	message := fmt.Sprintf("✅ %s：%s（第 %d 次，耗时 %s）", target, successMessage, attempt, formatDuration(elapsed))
	for _, subscriber := range subscribers {
		subscriber := subscriber
		go m.notifySubscriber(target, message, subscriber)
	}
}

func (m *Manager) notifySubscriber(target, message string, subscriber Subscriber) {
	messenger := subscriber.Messenger
	if messenger == nil {
		messenger = m.messenger
	}
	if messenger == nil {
		return
	}
	delay := 2 * time.Second
	for {
		ctx, cancel := context.WithTimeout(m.root, 20*time.Second)
		err := messenger.Send(ctx, subscriber.Recipient, message, subscriber.TraceID)
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

func (m *Manager) resolveLocked(names []string) ([]string, []string) {
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
	for {
		m.mu.Lock()
		if m.slotsInUse < m.maxParallel {
			m.slotsInUse++
			m.mu.Unlock()
			return true
		}
		changed := m.parallelChanged
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

func (m *Manager) finishProcess() {
	m.mu.Lock()
	if m.currentProcesses > 0 {
		m.currentProcesses--
	}
	if m.slotsInUse > 0 {
		m.slotsInUse--
	}
	m.signalParallelLocked()
	m.broadcastStateLocked()
	m.mu.Unlock()
}

func (m *Manager) finishAdhocRequest(key string, runID uint64, slotAcquired, processStarted bool) {
	m.mu.Lock()
	if processStarted && m.currentProcesses > 0 {
		m.currentProcesses--
	}
	if slotAcquired && m.slotsInUse > 0 {
		m.slotsInUse--
		m.signalParallelLocked()
	}
	if arbiter, ok := m.arbiters[key]; ok && arbiter.owner == requestOwnerAdhoc && arbiter.ownerRunID == runID {
		arbiter.owner = requestOwnerNone
		arbiter.ownerRunID = 0
		arbiter.cancel = nil
		m.signalTargetLocked(key)
	}
	m.broadcastStateLocked()
	m.mu.Unlock()
	m.runWG.Done()
}

func (m *Manager) releaseSlot() {
	m.mu.Lock()
	if m.slotsInUse > 0 {
		m.slotsInUse--
	}
	m.signalParallelLocked()
	m.mu.Unlock()
}

func (m *Manager) signalParallelLocked() {
	close(m.parallelChanged)
	m.parallelChanged = make(chan struct{})
}

func (m *Manager) waitKeepaliveNext(ctx context.Context, key string, runID uint64) bool {
	for {
		m.mu.Lock()
		current, ok := m.keepalives[key]
		if !ok || current.runID != runID || current.state == KeepaliveStateStopped {
			m.mu.Unlock()
			return false
		}
		at := current.nextRequest
		changed := current.scheduleChanged
		m.mu.Unlock()
		remaining := time.Until(at)
		if remaining <= 0 {
			return true
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return false
		case <-changed:
			stopTimer(timer)
			continue
		case <-timer.C:
			return true
		}
	}
}

func (m *Manager) signalKeepaliveScheduleLocked(key string) {
	current := m.keepalives[key]
	close(current.scheduleChanged)
	current.scheduleChanged = make(chan struct{})
}

func stopTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

func normalizeName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
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
