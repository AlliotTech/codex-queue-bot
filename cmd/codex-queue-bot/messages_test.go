package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/storage"

	"github.com/gorilla/websocket"
)

type messageTestServer struct {
	server *httptest.Server
	sends  atomic.Int32
}

func newMessageTestServer(t *testing.T) *messageTestServer {
	t.Helper()
	result := &messageTestServer{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	result.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/bot/v1/ws":
			connection, err := upgrader.Upgrade(writer, request, nil)
			if err != nil {
				return
			}
			defer connection.Close()
			for {
				if _, _, err := connection.ReadMessage(); err != nil {
					return
				}
			}
		case "/bot/v1/message/send":
			result.sends.Add(1)
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"ok":true}`)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(result.server.Close)
	return result
}

func TestMessageRuntimeReloadUsesCurrentClientAndIgnoresStaleStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := jobs.New(ctx, nil, nil, nil, logger, time.Second, time.Second, time.Second, time.Second, 1, "ok")
	openStatus := hub.NewStatusStore(hub.StatusDisabled)
	telegramStatus := hub.NewStatusStore(hub.StatusDisabled)
	runtime := newMessageRuntime(ctx, manager, nil, logger, openStatus, telegramStatus)
	t.Cleanup(runtime.Close)

	first := newMessageTestServer(t)
	second := newMessageTestServer(t)
	firstSnapshot := openILinkSnapshot(first.server.URL, "first-token")
	if err := runtime.Reload(firstSnapshot); err != nil {
		t.Fatalf("first Reload: %v", err)
	}
	waitForMessageStatus(t, openStatus, hub.StatusConnected)
	runtime.mu.Lock()
	oldClient := runtime.openClient
	runtime.mu.Unlock()
	stableMessenger := runtime.OpenMessenger()
	if err := stableMessenger.Send(ctx, "user", "first", "trace"); err != nil {
		t.Fatalf("send through first client: %v", err)
	}

	if err := runtime.Reload(openILinkSnapshot(second.server.URL, "second-token")); err != nil {
		t.Fatalf("second Reload: %v", err)
	}
	if stableMessenger != runtime.OpenMessenger() {
		t.Fatal("reload replaced the stable OpenILink messenger proxy")
	}
	waitForMessageStatus(t, openStatus, hub.StatusConnected)
	if err := stableMessenger.Send(ctx, "user", "second", "trace"); err != nil {
		t.Fatalf("send through reloaded client: %v", err)
	}

	oldClient.StatusStore().Set(hub.StatusUnauthorized, "stale client")
	time.Sleep(50 * time.Millisecond)
	if status := openStatus.Snapshot(); status.State != hub.StatusConnected || status.Error != "" {
		t.Fatalf("stale client overwrote current status: %+v", status)
	}
	if got := first.sends.Load(); got != 1 {
		t.Fatalf("first server sends = %d, want 1", got)
	}
	if got := second.sends.Load(); got != 1 {
		t.Fatalf("second server sends = %d, want 1", got)
	}
}

func openILinkSnapshot(baseURL, token string) storage.Snapshot {
	cfg := config.Config{
		OpenILink: config.OpenILinkConfig{
			Enabled:           true,
			BaseURL:           baseURL,
			Token:             token,
			HTTPTimeoutSecond: 2,
		},
	}
	return storage.Snapshot{Config: cfg}
}

func waitForMessageStatus(t *testing.T, store *hub.StatusStore, expected hub.Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if store.Snapshot().State == expected {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("message status = %+v, want %s", store.Snapshot(), expected)
}
