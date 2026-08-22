package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/proxyenv"
)

const maxResponseSize = 1 << 20

var ErrUnauthorized = errors.New("Telegram Bot API rejected the bot token")

type Client struct {
	baseURL     string
	token       string
	pollTimeout time.Duration
	httpClient  *http.Client
	logger      *slog.Logger
	status      *hub.StatusStore
	runMu       sync.Mutex
	runCancel   context.CancelFunc
}

type apiResponse[T any] struct {
	OK          bool   `json:"ok"`
	Result      T      `json:"result"`
	Description string `json:"description"`
	ErrorCode   int    `json:"error_code"`
}

type update struct {
	UpdateID int64    `json:"update_id"`
	Message  *message `json:"message"`
}

type message struct {
	MessageID int64  `json:"message_id"`
	From      *user  `json:"from"`
	Chat      chat   `json:"chat"`
	Text      string `json:"text"`
}

type user struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type chat struct {
	ID int64 `json:"id"`
}

func New(baseURL, token string, timeout, pollTimeout time.Duration, logger *slog.Logger, resolvers ...*proxyenv.Resolver) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	var resolver *proxyenv.Resolver
	if len(resolvers) > 0 {
		resolver = resolvers[0]
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if resolver != nil {
		transport = resolver.HTTPTransport(transport)
	}
	return &Client{
		baseURL:     strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:       strings.TrimSpace(token),
		pollTimeout: pollTimeout,
		httpClient:  &http.Client{Timeout: timeout, Transport: transport},
		logger:      logger,
		status:      hub.NewStatusStore(hub.StatusConnecting),
	}
}

func NewWithProxy(baseURL, token string, timeout, pollTimeout time.Duration, logger *slog.Logger, resolver *proxyenv.Resolver) *Client {
	return New(baseURL, token, timeout, pollTimeout, logger, resolver)
}

func (c *Client) StatusStore() *hub.StatusStore { return c.status }

func (c *Client) Run(ctx context.Context, handler func(context.Context, hub.Incoming)) error {
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
	c.status.Set(hub.StatusConnecting, "")
	var offset int64
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		updates, err := c.getUpdates(ctx, offset)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, ErrUnauthorized) {
				c.status.Set(hub.StatusUnauthorized, err.Error())
				return err
			}
			safeError := c.safeError(err)
			c.status.Set(hub.StatusReconnecting, safeError)
			c.logger.Warn("Telegram long poll failed", "error", safeError, "retry_in", backoff)
			if !wait(ctx, backoff) {
				return nil
			}
			backoff = minDuration(backoff*2, 30*time.Second)
			continue
		}
		if c.status.Snapshot().State != hub.StatusConnected {
			c.logger.Info("Telegram Bot API connected", "base_url", c.baseURL)
			c.status.Set(hub.StatusConnected, "")
		}
		backoff = time.Second
		for _, current := range updates {
			if current.UpdateID >= offset {
				offset = current.UpdateID + 1
			}
			if current.Message == nil || current.Message.From == nil || strings.TrimSpace(current.Message.Text) == "" {
				continue
			}
			incoming := hub.Incoming{
				EventID:  strconv.FormatInt(current.UpdateID, 10),
				TraceID:  strconv.FormatInt(current.Message.MessageID, 10),
				SenderID: strconv.FormatInt(current.Message.From.ID, 10),
				ReplyTo:  strconv.FormatInt(current.Message.Chat.ID, 10),
				Text:     current.Message.Text,
			}
			handler(ctx, incoming)
		}
	}
}

// Close interrupts a long poll immediately so a replacement client can be
// started without waiting for the configured poll timeout.
func (c *Client) Close() {
	c.runMu.Lock()
	cancel := c.runCancel
	c.runMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Client) Send(ctx context.Context, to, content, _ string) error {
	values := url.Values{}
	values.Set("chat_id", strings.TrimSpace(to))
	values.Set("text", content)
	_, err := call[json.RawMessage](c, ctx, "sendMessage", values)
	if errors.Is(err, ErrUnauthorized) {
		c.status.Set(hub.StatusUnauthorized, ErrUnauthorized.Error())
		c.Close()
	}
	return err
}

func (c *Client) getUpdates(ctx context.Context, offset int64) ([]update, error) {
	values := url.Values{}
	values.Set("timeout", strconv.Itoa(int(c.pollTimeout.Seconds())))
	values.Set("allowed_updates", `["message"]`)
	if offset > 0 {
		values.Set("offset", strconv.FormatInt(offset, 10))
	}
	return call[[]update](c, ctx, "getUpdates", values)
}

func call[T any](c *Client, ctx context.Context, method string, values url.Values) (T, error) {
	var zero T
	endpoint := fmt.Sprintf("%s/bot%s/%s", c.baseURL, c.token, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(values.Encode()))
	if err != nil {
		return zero, fmt.Errorf("create Telegram %s request: %s", method, c.safeError(err))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		return zero, fmt.Errorf("Telegram %s request: %s", method, c.safeError(err))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return zero, fmt.Errorf("read Telegram %s response: %w", method, err)
	}
	if len(body) > maxResponseSize {
		return zero, fmt.Errorf("Telegram %s response is too large", method)
	}
	var decoded apiResponse[T]
	if err := json.Unmarshal(body, &decoded); err != nil {
		return zero, fmt.Errorf("decode Telegram %s response: %w", method, err)
	}
	if resp.StatusCode == http.StatusUnauthorized || decoded.ErrorCode == http.StatusUnauthorized ||
		(resp.StatusCode == http.StatusNotFound && decoded.ErrorCode == http.StatusNotFound) {
		return zero, ErrUnauthorized
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !decoded.OK {
		description := truncate(decoded.Description, 300)
		if description == "" {
			description = http.StatusText(resp.StatusCode)
		}
		return zero, fmt.Errorf("Telegram %s failed: HTTP %d: %s", method, resp.StatusCode, description)
	}
	return decoded.Result, nil
}

func (c *Client) safeError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if c.token != "" {
		value = strings.ReplaceAll(value, c.token, "[REDACTED]")
	}
	return truncate(value, 600)
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

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func truncate(value string, maximum int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum]) + "…"
}
