//go:build integration

package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codex-queue-bot/internal/config"
)

func TestRunnerWithNativeCodexBinary(t *testing.T) {
	if os.Getenv("RUN_NATIVE_CODEX_INTEGRATION") != "1" {
		t.Skip("set RUN_NATIVE_CODEX_INTEGRATION=1 to run")
	}
	codexBinary := os.Getenv("CODEX_INTEGRATION_BINARY")
	if codexBinary == "" {
		codexBinary = "codex"
	}

	dir := t.TempDir()
	prompts := filepath.Join(dir, "prompts.txt")
	if err := os.WriteFile(prompts, []byte("Reply only ok.\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer integration-key" {
			t.Errorf("authorization = %q", got)
		}
		if !strings.HasSuffix(r.URL.Path, "/responses") {
			t.Errorf("path = %q", r.URL.Path)
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if stream, _ := request["stream"].(bool); !stream {
			t.Errorf("stream = %#v", request["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("x-request-id", "req_integration")
		_, _ = fmt.Fprint(w, "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}}\n\n")
		_, _ = fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_integration\",\"usage\":{\"input_tokens\":1,\"input_tokens_details\":null,\"output_tokens\":1,\"output_tokens_details\":null,\"total_tokens\":2}}}\n\n")
	})}
	go func() { _ = server.Serve(listener) }()
	defer server.Shutdown(context.Background())

	runner := &Runner{
		Binary:          codexBinary,
		PromptsFile:     prompts,
		Timeout:         20 * time.Second,
		ReasoningEffort: "low",
	}
	if err := runner.Check(); err != nil {
		t.Fatal(err)
	}
	result := runner.Run(context.Background(), config.Target{
		Name:       "integration",
		APIBaseURL: "http://" + listener.Addr().String() + "/v1",
		APIKey:     "integration-key",
		Model:      "gpt-test",
		WireAPI:    "responses",
	}, 1)
	if !result.Success || result.Response != "ok" {
		t.Fatalf("result = %+v", result)
	}
}
