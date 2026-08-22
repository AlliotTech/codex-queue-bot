package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"codex-queue-bot/internal/proxyenv"

	"github.com/gorilla/websocket"
)

const (
	maxEventSize       = 1 << 20
	maxResponseSize    = 1 << 20
	maxRememberedEvent = 2048
)

var ErrUnauthorized = errors.New("OpenILink Hub rejected the app token")
var absoluteURLPattern = regexp.MustCompile(`(?i)\b(?:https?|wss?)://[^\s]+`)

type Incoming struct {
	EventID  string
	TraceID  string
	SenderID string
	ReplyTo  string
	Text     string
}

type Status string

const (
	StatusDisabled     Status = "disabled"
	StatusConnecting   Status = "connecting"
	StatusConnected    Status = "connected"
	StatusReconnecting Status = "reconnecting"
	StatusUnauthorized Status = "unauthorized"
)

type StatusSnapshot struct {
	State     Status
	Error     string
	UpdatedAt time.Time
}

type StatusSubscription struct {
	Updates <-chan StatusSnapshot
	cancel  func()
}

func (s *StatusSubscription) Close() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

type StatusStore struct {
	mu             sync.Mutex
	snapshot       StatusSnapshot
	nextObserverID uint64
	observers      map[uint64]chan StatusSnapshot
}

func NewStatusStore(initial Status) *StatusStore {
	return &StatusStore{
		snapshot:  StatusSnapshot{State: initial, UpdatedAt: time.Now()},
		observers: make(map[uint64]chan StatusSnapshot),
	}
}

func (s *StatusStore) Snapshot() StatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *StatusStore) Observe(buffer int) (StatusSnapshot, *StatusSubscription) {
	if buffer <= 0 {
		buffer = 8
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextObserverID++
	id := s.nextObserverID
	updates := make(chan StatusSnapshot, buffer)
	s.observers[id] = updates
	return s.snapshot, &StatusSubscription{
		Updates: updates,
		cancel: func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if current, ok := s.observers[id]; ok {
				delete(s.observers, id)
				close(current)
			}
		},
	}
}

func (s *StatusStore) Set(state Status, errorText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	errorText = strings.TrimSpace(errorText)
	if len([]rune(errorText)) > 600 {
		errorText = string([]rune(errorText)[:600]) + "…"
	}
	s.snapshot = StatusSnapshot{State: state, Error: errorText, UpdatedAt: time.Now()}
	for id, observer := range s.observers {
		select {
		case observer <- s.snapshot:
		default:
			delete(s.observers, id)
			close(observer)
		}
	}
}

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	dialer     *websocket.Dialer
	logger     *slog.Logger
	seenMu     sync.Mutex
	seen       map[string]struct{}
	seenOrder  []string
	status     *StatusStore
	runMu      sync.Mutex
	runCancel  context.CancelFunc
}

type wsEnvelope struct {
	Type    string `json:"type"`
	TraceID string `json:"trace_id"`
	Event   struct {
		Type string          `json:"type"`
		ID   string          `json:"id"`
		Data json.RawMessage `json:"data"`
	} `json:"event"`
}

type eventData struct {
	Content string `json:"content"`
	Command string `json:"command"`
	Text    string `json:"text"`
	Sender  struct {
		ID   string `json:"id"`
		Role string `json:"role"`
	} `json:"sender"`
}

func New(baseURL, token string, timeout time.Duration, logger *slog.Logger, resolvers ...*proxyenv.Resolver) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	var resolver *proxyenv.Resolver
	if len(resolvers) > 0 {
		resolver = resolvers[0]
	}
	baseURL = strings.TrimRight(baseURL, "/")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if resolver != nil {
		transport = resolver.HTTPTransport(transport)
	}
	dialer := *websocket.DefaultDialer
	dialer.Proxy = nil
	if resolver != nil {
		if endpoint, err := url.Parse(baseURL); err == nil {
			if endpoint.Scheme == "https" {
				endpoint.Scheme = "wss"
			} else {
				endpoint.Scheme = "ws"
			}
			dialer.NetDialContext = resolver.WebSocketDialContext(endpoint, nil)
		}
	}
	return &Client{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		dialer:    &dialer,
		logger:    logger,
		seen:      make(map[string]struct{}, maxRememberedEvent),
		seenOrder: make([]string, 0, maxRememberedEvent),
		status:    NewStatusStore(StatusConnecting),
	}
}

func NewWithProxy(baseURL, token string, timeout time.Duration, logger *slog.Logger, resolver *proxyenv.Resolver) *Client {
	return New(baseURL, token, timeout, logger, resolver)
}

// Close stops the long-running listener and is safe to call repeatedly.  It
// is used by configuration hot reloads before swapping in a new client.
func (c *Client) Close() { c.stopAdapter() }

func (c *Client) StatusStore() *StatusStore { return c.status }

func (c *Client) Run(ctx context.Context, handler func(context.Context, Incoming)) error {
	adapterCtx, cancel := context.WithCancel(ctx)
	c.runMu.Lock()
	c.runCancel = cancel
	c.runMu.Unlock()
	defer func() {
		cancel()
		c.runMu.Lock()
		c.runCancel = nil
		c.runMu.Unlock()
	}()
	ctx = adapterCtx
	c.status.Set(StatusConnecting, "")
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}

		conn, err := c.connect(ctx)
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				c.status.Set(StatusUnauthorized, err.Error())
				return err
			}
			safeError := c.safeError(err)
			c.status.Set(StatusReconnecting, safeError)
			c.logger.Warn("OpenILink WebSocket connection failed", "error", safeError, "retry_in", backoff)
			if !waitContext(ctx, withJitter(backoff)) {
				return nil
			}
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}

		backoff = time.Second
		c.status.Set(StatusConnected, "")
		c.logger.Info("OpenILink WebSocket connected", "base_url", c.baseURL)
		err = c.readLoop(ctx, conn, handler)
		_ = conn.Close()
		if c.status.Snapshot().State == StatusUnauthorized {
			return ErrUnauthorized
		}
		if ctx.Err() != nil {
			return nil
		}
		safeError := c.safeError(err)
		c.status.Set(StatusReconnecting, safeError)
		c.logger.Warn("OpenILink WebSocket disconnected", "error", safeError, "retry_in", backoff)
		if !waitContext(ctx, withJitter(backoff)) {
			return nil
		}
		backoff = minDuration(backoff*2, 30*time.Second)
	}
}

func (c *Client) Send(ctx context.Context, to, content, traceID string) error {
	payload := map[string]string{
		"type":    "text",
		"content": content,
	}
	if to != "" {
		payload["to"] = to
	}
	if traceID != "" {
		payload["trace_id"] = traceID
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode OpenILink message: %w", err)
	}

	endpoint, err := url.JoinPath(c.baseURL, "bot/v1/message/send")
	if err != nil {
		return fmt.Errorf("build OpenILink send URL: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create OpenILink send request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send OpenILink message: %s", c.safeError(err))
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		c.status.Set(StatusUnauthorized, ErrUnauthorized.Error())
		c.stopAdapter()
		return ErrUnauthorized
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("send OpenILink message: HTTP %d: %s", resp.StatusCode, c.safeText(compactBody(respBody)))
	}

	var result map[string]any
	if len(respBody) > 0 && json.Unmarshal(respBody, &result) == nil {
		if ok, exists := result["ok"].(bool); exists && !ok {
			return fmt.Errorf("send OpenILink message: %s", c.safeText(fmt.Sprint(result["error"])))
		}
		if ok, exists := result["success"].(bool); exists && !ok {
			return fmt.Errorf("send OpenILink message failed: %s", c.safeText(compactBody(respBody)))
		}
	}
	return nil
}

func (c *Client) stopAdapter() {
	c.runMu.Lock()
	cancel := c.runCancel
	c.runMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Client) connect(ctx context.Context) (*websocket.Conn, error) {
	wsURL, err := url.JoinPath(c.baseURL, "bot/v1/ws")
	if err != nil {
		return nil, fmt.Errorf("build OpenILink WebSocket URL: %w", err)
	}
	u, err := url.Parse(wsURL)
	if err != nil {
		return nil, fmt.Errorf("parse OpenILink WebSocket URL: %w", err)
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	query := u.Query()
	query.Set("token", c.token)
	u.RawQuery = query.Encode()

	conn, resp, err := c.dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				return nil, ErrUnauthorized
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return nil, fmt.Errorf("WebSocket handshake HTTP %d: %s", resp.StatusCode, compactBody(body))
		}
		return nil, err
	}
	return conn, nil
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, handler func(context.Context, Incoming)) error {
	conn.SetReadLimit(maxEventSize)
	_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})

	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"), time.Now().Add(time.Second))
			_ = conn.Close()
		case <-closed:
		}
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))

		var envelope wsEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			c.logger.Warn("ignored invalid OpenILink WebSocket message", "error", err)
			continue
		}
		switch envelope.Type {
		case "init":
			continue
		case "event":
		default:
			continue
		}
		if envelope.Event.ID != "" {
			if !c.rememberEvent(envelope.Event.ID) {
				continue
			}
		}

		incoming, ok := decodeIncoming(envelope)
		if ok {
			handler(ctx, incoming)
		}
	}
}

func (c *Client) rememberEvent(eventID string) bool {
	c.seenMu.Lock()
	defer c.seenMu.Unlock()
	if _, exists := c.seen[eventID]; exists {
		return false
	}
	c.seen[eventID] = struct{}{}
	c.seenOrder = append(c.seenOrder, eventID)
	if len(c.seenOrder) > maxRememberedEvent {
		delete(c.seen, c.seenOrder[0])
		c.seenOrder = c.seenOrder[1:]
	}
	return true
}

func decodeIncoming(envelope wsEnvelope) (Incoming, bool) {
	if envelope.Event.Type != "message.text" && envelope.Event.Type != "command" {
		return Incoming{}, false
	}
	var data eventData
	if err := json.Unmarshal(envelope.Event.Data, &data); err != nil {
		return Incoming{}, false
	}
	if data.Sender.ID == "" || (data.Sender.Role != "" && data.Sender.Role != "user") {
		return Incoming{}, false
	}

	text := strings.TrimSpace(data.Content)
	if envelope.Event.Type == "command" {
		command := strings.TrimSpace(data.Command)
		if command == "" {
			return Incoming{}, false
		}
		text = "/" + command
		if args := strings.TrimSpace(data.Text); args != "" {
			text += " " + args
		}
	}
	if text == "" {
		return Incoming{}, false
	}
	return Incoming{
		EventID:  envelope.Event.ID,
		TraceID:  envelope.TraceID,
		SenderID: data.Sender.ID,
		Text:     text,
	}, true
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func withJitter(duration time.Duration) time.Duration {
	if duration <= 0 {
		return 0
	}
	return duration + time.Duration(rand.Int63n(int64(duration/2)+1))
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func compactBody(body []byte) string {
	value := strings.Join(strings.Fields(string(body)), " ")
	runes := []rune(value)
	if len(runes) > 512 {
		return string(runes[:512])
	}
	return value
}

func (c *Client) safeError(err error) string {
	if err == nil {
		return "connection closed"
	}
	return c.safeText(err.Error())
}

func (c *Client) safeText(value string) string {
	value = absoluteURLPattern.ReplaceAllString(value, "[URL]")
	for _, secret := range []string{c.token, url.QueryEscape(c.token)} {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}
