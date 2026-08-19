package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/config"
)

type State string

const (
	StateIdle      State = "idle"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateStopped   State = "stopped"
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

type Manager struct {
	root           context.Context
	runner         AttemptRunner
	messenger      Messenger
	logger         *slog.Logger
	retryMin       time.Duration
	retryMax       time.Duration
	successMessage string
	sem            chan struct{}

	mu      sync.Mutex
	targets map[string]config.Target
	order   []string
	jobs    map[string]*job
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
}

func New(
	root context.Context,
	targets []config.Target,
	runner AttemptRunner,
	messenger Messenger,
	logger *slog.Logger,
	retryMin, retryMax time.Duration,
	maxParallel int,
	successMessage string,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		root:           root,
		runner:         runner,
		messenger:      messenger,
		logger:         logger,
		retryMin:       retryMin,
		retryMax:       retryMax,
		successMessage: successMessage,
		sem:            make(chan struct{}, maxParallel),
		targets:        make(map[string]config.Target, len(targets)),
		jobs:           make(map[string]*job, len(targets)),
	}
	for _, target := range targets {
		key := normalizeName(target.Name)
		m.targets[key] = target
		m.order = append(m.order, key)
		m.jobs[key] = &job{state: StateIdle, subscribers: make(map[string]Subscriber)}
	}
	return m
}

func (m *Manager) Start(names []string, subscriber Subscriber) StartResult {
	keys, unknown := m.resolve(names)
	result := StartResult{Unknown: unknown}

	m.mu.Lock()
	defer m.mu.Unlock()
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
		result.Started = append(result.Started, target.Name)
		go m.run(jobCtx, key, runID)
	}

	return result
}

func (m *Manager) Stop(names []string) StopResult {
	keys, unknown := m.resolve(names)
	result := StopResult{Unknown: unknown}

	m.mu.Lock()
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
		if cancel != nil {
			cancel()
		}
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
		target := m.targets[key]
		current := m.jobs[key]
		result = append(result, Snapshot{
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
		})
	}
	return result, unknown
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

func (m *Manager) run(ctx context.Context, key string, runID uint64) {
	for {
		if !m.acquire(ctx) {
			m.markCancelled(key, runID)
			return
		}

		attempt, target, ok := m.beginAttempt(key, runID)
		if !ok {
			m.release()
			return
		}
		m.logger.Info("starting Codex queue attempt", "target", target.Name, "attempt", attempt, "api_host", codex.TargetHost(target))
		result := m.runner.Run(ctx, target, attempt)
		m.release()

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
			m.markCancelled(key, runID)
			return
		}

		delay := randomDuration(m.retryMin, m.retryMax)
		if !m.markFailure(key, runID, result, delay) {
			return
		}
		m.logger.Warn("Codex queue attempt failed", "target", target.Name, "attempt", attempt, "error", result.Error, "retry_in", delay)
		if !wait(ctx, delay) {
			m.markCancelled(key, runID)
			return
		}
	}
}

func (m *Manager) beginAttempt(key string, runID uint64) (int, config.Target, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	if current.runID != runID || current.state != StateRunning {
		return 0, config.Target{}, false
	}
	current.attempts++
	current.lastAttempt = time.Now()
	current.nextAttempt = time.Time{}
	return current.attempts, m.targets[key], true
}

func (m *Manager) markFailure(key string, runID uint64, result codex.Result, delay time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	if current.runID != runID || current.state != StateRunning {
		return false
	}
	current.lastError = truncate(result.Error, 600)
	current.nextAttempt = time.Now().Add(delay)
	return true
}

func (m *Manager) markSuccess(key string, runID uint64) ([]Subscriber, time.Duration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	if current.runID != runID || current.state != StateRunning {
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
	return subscribers, current.finishedAt.Sub(current.startedAt), true
}

func (m *Manager) markCancelled(key string, runID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.jobs[key]
	if current.runID != runID || current.state != StateRunning {
		return
	}
	current.state = StateStopped
	current.finishedAt = time.Now()
	current.nextAttempt = time.Time{}
	current.cancel = nil
	current.subscribers = make(map[string]Subscriber)
}

func (m *Manager) notifySuccess(target string, attempt int, elapsed time.Duration, subscribers []Subscriber) {
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

func (m *Manager) release() {
	<-m.sem
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

func truncate(value string, maximum int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum]) + "…"
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
