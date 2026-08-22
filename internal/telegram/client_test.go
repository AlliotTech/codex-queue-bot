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
	"codex-queue-bot/internal/proxyenv"
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

func TestClientSendAndLongPollUseInjectedHTTPProxy(t *testing.T) {
	var seen atomic.Int32
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		if r.URL.Host != "telegram.invalid" {
			t.Errorf("proxy request host = %q", r.URL.Host)
		}
		method := strings.TrimPrefix(r.URL.Path, "/bottest-token/")
		if method == "getUpdates" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []any{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	defer proxyServer.Close()
	resolver, err := proxyenv.Resolve([]string{"HTTP_PROXY=" + proxyServer.URL, "HTTPS_PROXY=" + proxyServer.URL})
	if err != nil {
		t.Fatal(err)
	}
	client := New("http://telegram.invalid", "test-token", time.Second, 100*time.Millisecond, nil, resolver)
	if _, err := client.getUpdates(context.Background(), 0); err != nil {
		t.Fatalf("getUpdates: %v", err)
	}
	if err := client.Send(context.Background(), "1", "ok", ""); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if seen.Load() != 2 {
		t.Fatalf("proxy saw %d requests, want 2", seen.Load())
	}
}

func TestClientNoProxyBypassesInjectedProxy(t *testing.T) {
	var proxyCalls atomic.Int32
	proxyServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { proxyCalls.Add(1) }))
	defer proxyServer.Close()
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	}))
	defer apiServer.Close()
	u, _ := url.Parse(apiServer.URL)
	resolver, err := proxyenv.Resolve([]string{"HTTP_PROXY=" + proxyServer.URL, "NO_PROXY=" + u.Hostname()})
	if err != nil {
		t.Fatal(err)
	}
	client := New(apiServer.URL, "test-token", time.Second, 100*time.Millisecond, nil, resolver)
	if err := client.Send(context.Background(), "1", "ok", ""); err != nil {
		t.Fatal(err)
	}
	if proxyCalls.Load() != 0 {
		t.Fatalf("NO_PROXY still used proxy %d times", proxyCalls.Load())
	}
}

func TestClientWithoutResolverDoesNotReadProxyEnvironment(t *testing.T) {
	client := New("https://api.telegram.org", "token", time.Second, time.Second, nil)
	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil {
		t.Fatalf("HTTP transport unexpectedly reads proxy environment: %#v", client.httpClient.Transport)
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

func TestClientSendUnauthorizedStopsAdapterAndUpdatesStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": http.StatusUnauthorized, "description": "Unauthorized"})
	}))
	defer server.Close()
	client := New(server.URL, "secret-token", time.Second, 100*time.Millisecond, nil)
	client.runMu.Lock()
	cancelled := make(chan struct{})
	client.runCancel = func() { close(cancelled) }
	client.runMu.Unlock()

	if err := client.Send(context.Background(), "1", "message", ""); err != ErrUnauthorized {
		t.Fatalf("Send error = %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("unauthorized send did not stop the adapter")
	}
	if status := client.StatusStore().Snapshot(); status.State != hub.StatusUnauthorized || status.Error == "" {
		t.Fatalf("status = %+v", status)
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
