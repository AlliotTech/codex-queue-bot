package telegram

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"codex-queue-bot/internal/hub"
)

func TestClientSend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bottest-token/sendMessage" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if r.Form.Get("chat_id") != "-1001" || r.Form.Get("text") != "开蹬" {
			t.Errorf("form = %#v", r.Form)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	defer server.Close()

	client := New(server.URL, "test-token", time.Second, 100*time.Millisecond, nil)
	if err := client.Send(context.Background(), "-1001", "开蹬", ""); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func TestClientRunReceivesTextMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bottest-token/getUpdates" {
			t.Errorf("path = %q", r.URL.Path)
		}
		body, _ := url.ParseQuery(readBody(t, r))
		if body.Get("timeout") == "" || !strings.Contains(body.Get("allowed_updates"), "message") {
			t.Errorf("form = %#v", body)
		}
		if calls.Add(1) == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []any{map[string]any{
				"update_id": 7,
				"message":   map[string]any{"message_id": 9, "from": map[string]any{"id": 123}, "chat": map[string]any{"id": -456}, "text": "/状态"},
			}}})
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	client := New(server.URL, "test-token", 2*time.Second, 100*time.Millisecond, nil)
	incoming := make(chan hub.Incoming, 1)
	done := make(chan error, 1)
	go func() {
		done <- client.Run(ctx, func(_ context.Context, value hub.Incoming) { incoming <- value; cancel() })
	}()
	select {
	case value := <-incoming:
		if value.EventID != "7" || value.TraceID != "9" || value.SenderID != "123" || value.ReplyTo != "-456" || value.Text != "/状态" {
			t.Fatalf("incoming = %+v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Telegram update")
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestClientUnauthorizedDoesNotLeakToken(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": status, "description": http.StatusText(status)})
			}))
			defer server.Close()
			client := New(server.URL, "secret-token", time.Second, 100*time.Millisecond, nil)
			if _, err := client.getUpdates(context.Background(), 0); err != ErrUnauthorized {
				t.Fatalf("getUpdates error = %v", err)
			}
		})
	}
}

func readBody(t *testing.T, r *http.Request) string {
	t.Helper()
	buffer, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(buffer)
}
