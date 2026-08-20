package hub

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestClientSend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bot/v1/message/send" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer app-token" {
			t.Errorf("authorization = %q", got)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["to"] != "user-1" || body["content"] != "开蹬" || body["trace_id"] != "trace-1" {
			t.Errorf("body = %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := New(server.URL, "app-token", time.Second, nil)
	if err := client.Send(context.Background(), "user-1", "开蹬", "trace-1"); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func TestClientConfiguresProxyAwareHTTPAndWebSocketTransports(t *testing.T) {
	client := New("https://hub.example.com", "app-token", time.Second, nil)
	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil {
		t.Fatalf("HTTP transport is not proxy-aware: %#v", client.httpClient.Transport)
	}
	if client.dialer == websocket.DefaultDialer {
		t.Fatal("client must not mutate or reuse the global WebSocket dialer")
	}
	if client.dialer.Proxy == nil {
		t.Fatal("WebSocket dialer is not proxy-aware")
	}
}

func TestClientRunReceivesMessageAndCommandEvents(t *testing.T) {
	upgrader := websocket.Upgrader{}
	events := []string{
		`{"type":"event","trace_id":"tr-1","event":{"type":"message.text","id":"evt-1","data":{"content":"/状态","sender":{"id":"u1","role":"user"}}}}`,
		`{"type":"event","trace_id":"tr-2","event":{"type":"command","id":"evt-2","data":{"command":"开挤","text":"main","sender":{"id":"u1","role":"user"}}}}`,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bot/v1/ws" || r.URL.Query().Get("token") != "app-token" {
			http.Error(w, "bad request", http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "init"})
		for _, event := range events {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	client := New(server.URL, "app-token", time.Second, nil)
	received := make(chan Incoming, 2)
	done := make(chan error, 1)
	go func() {
		count := 0
		done <- client.Run(ctx, func(_ context.Context, message Incoming) {
			received <- message
			count++
			if count == 2 {
				cancel()
			}
		})
	}()

	var got []Incoming
	for len(got) < 2 {
		select {
		case message := <-received:
			got = append(got, message)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for events")
		}
	}
	if got[0].Text != "/状态" || got[1].Text != "/开挤 main" {
		t.Fatalf("messages = %+v", got)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not stop")
	}
}

func TestRememberEventSurvivesReconnectBoundary(t *testing.T) {
	client := New("https://hub.example.com", "token", time.Second, nil)
	if !client.rememberEvent("evt-1") {
		t.Fatal("first event should be accepted")
	}
	if client.rememberEvent("evt-1") {
		t.Fatal("duplicate event should be rejected")
	}
}

func TestClientStatusBecomesUnauthorizedAndAdapterStops(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	client := New(server.URL, "bad-token", time.Second, nil)
	err := client.Run(context.Background(), func(context.Context, Incoming) {})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Run error = %v", err)
	}
	status := client.StatusStore().Snapshot()
	if status.State != StatusUnauthorized || status.Error == "" {
		t.Fatalf("status = %+v", status)
	}
}

func TestStatusStoreSlowObserverIsDisconnected(t *testing.T) {
	store := NewStatusStore(StatusDisabled)
	_, observer := store.Observe(1)
	defer observer.Close()
	store.Set(StatusConnecting, "")
	store.Set(StatusConnected, "")
	first, ok := <-observer.Updates
	if !ok || first.State != StatusConnecting {
		t.Fatalf("first status = %+v, ok=%v", first, ok)
	}
	if _, ok := <-observer.Updates; ok {
		t.Fatal("slow status observer should be closed")
	}
}

func TestClientSafeErrorRedactsTokenAndURLs(t *testing.T) {
	client := New("https://hub.example.com", "token/value", time.Second, nil)
	value := client.safeError(errors.New("dial wss://hub.example.com/bot/v1/ws?token=token%2Fvalue failed for token/value"))
	for _, forbidden := range []string{"token/value", "token%2Fvalue", "wss://hub.example.com"} {
		if strings.Contains(value, forbidden) {
			t.Fatalf("safe error leaked %q: %s", forbidden, value)
		}
	}
}
