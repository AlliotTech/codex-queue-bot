package codex

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codex-queue-bot/internal/config"
)

func TestRunnerInvokesNativeExecutableWithIsolatedProvider(t *testing.T) {
	dir := t.TempDir()
	prompts := filepath.Join(dir, "prompts.txt")
	capture := filepath.Join(dir, "args.txt")
	envCapture := filepath.Join(dir, "env.txt")
	keyCapture := filepath.Join(dir, "key.txt")
	script := filepath.Join(dir, "fake-codex")
	if err := os.WriteFile(prompts, []byte("What is 2 + 2?\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fake := `#!/bin/sh
set -eu
script_dir=${0%/*}
printf '%s\n' "$CODEX_QUEUE_TARGET_API_KEY" > "$script_dir/key.txt"
printf '%s\n' "$@" > "$script_dir/args.txt"
env | sort > "$script_dir/env.txt"
out=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "--output-last-message" ]; then out="$arg"; fi
  previous="$arg"
done
[ -n "$out" ]
printf 'four' > "$out"
`
	if err := os.WriteFile(script, []byte(fake), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENILINK_APP_TOKEN", "hub-secret")
	t.Setenv("CODEX_KEY_OTHER", "other-target-secret")
	t.Setenv("UNRELATED_SECRET", "unrelated-secret")
	t.Setenv(apiKeyEnvName, "stale-target-secret")
	t.Setenv("HTTP_PROXY", "socks5:proxy.example:1080")
	t.Setenv("HTTPS_PROXY", "")
	t.Setenv("ALL_PROXY", "")
	t.Setenv("http_proxy", "")
	t.Setenv("https_proxy", "")
	t.Setenv("all_proxy", "")

	runner := &Runner{
		Binary:          script,
		PromptsFile:     prompts,
		Timeout:         2 * time.Second,
		ReasoningEffort: "low",
		Overrides:       []string{`shell_environment_policy.inherit="all"`},
	}
	target := config.Target{
		Name:            "primary",
		APIBaseURL:      "https://queue.example/v1",
		APIKey:          "super-secret-key",
		Model:           "gpt-test",
		WireAPI:         "responses",
		ConfigOverrides: []string{`shell_environment_policy.inherit="core"`},
	}
	result := runner.Run(context.Background(), target, 3)
	if !result.Success || result.Response != "four" {
		t.Fatalf("result = %+v", result)
	}

	args, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	gotArgs := string(args)
	for _, want := range []string{"exec", "--ignore-user-config", "--ephemeral", "model_provider=\"queue_proxy\"", "https://queue.example/v1", `shell_environment_policy.inherit="none"`, "Attempt: 3"} {
		if !strings.Contains(gotArgs, want) {
			t.Errorf("args missing %q:\n%s", want, gotArgs)
		}
	}
	if got, want := strings.LastIndex(gotArgs, "shell_environment_policy.inherit="), strings.LastIndex(gotArgs, `shell_environment_policy.inherit="none"`); got != want {
		t.Fatalf("environment isolation override was not enforced last:\n%s", gotArgs)
	}
	if strings.Contains(gotArgs, target.APIKey) {
		t.Fatal("API key leaked into command arguments")
	}
	key, err := os.ReadFile(keyCapture)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(key)) != target.APIKey {
		t.Fatalf("key env = %q", key)
	}
	environ, err := os.ReadFile(envCapture)
	if err != nil {
		t.Fatal(err)
	}
	gotEnv := string(environ)
	for _, want := range []string{
		apiKeyEnvName + "=" + target.APIKey,
		"HTTP_PROXY=socks5://proxy.example:1080",
		"HTTPS_PROXY=socks5://proxy.example:1080",
		"ALL_PROXY=socks5://proxy.example:1080",
	} {
		if !strings.Contains(gotEnv, want) {
			t.Errorf("environment missing %q:\n%s", want, gotEnv)
		}
	}
	for _, forbidden := range []string{
		"OPENILINK_APP_TOKEN=",
		"CODEX_KEY_OTHER=",
		"UNRELATED_SECRET=",
		"hub-secret",
		"other-target-secret",
		"unrelated-secret",
		"stale-target-secret",
	} {
		if strings.Contains(gotEnv, forbidden) {
			t.Errorf("unrelated secret inherited by Codex environment: %q\n%s", forbidden, gotEnv)
		}
	}
}

func TestRunnerReportsExitFailureAndTimeout(t *testing.T) {
	dir := t.TempDir()
	prompts := filepath.Join(dir, "prompts.txt")
	if err := os.WriteFile(prompts, []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := config.Target{Name: "t", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"}

	t.Run("failure", func(t *testing.T) {
		script := filepath.Join(dir, "fail-codex")
		if err := os.WriteFile(script, []byte("#!/bin/sh\necho 'queue unavailable key=x' >&2\nexit 7\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		runner := &Runner{Binary: script, PromptsFile: prompts, Timeout: time.Second, ReasoningEffort: "low"}
		result := runner.Run(context.Background(), target, 1)
		if result.Success || !strings.Contains(result.Error, "queue unavailable") {
			t.Fatalf("result = %+v", result)
		}
		if strings.Contains(result.Error, "key=x") || !strings.Contains(result.Error, "[REDACTED]") {
			t.Fatalf("secret was not redacted: %q", result.Error)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		script := filepath.Join(dir, "slow-codex")
		if err := os.WriteFile(script, []byte("#!/bin/sh\nexec sleep 5\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		runner := &Runner{Binary: script, PromptsFile: prompts, Timeout: 50 * time.Millisecond, ReasoningEffort: "low"}
		result := runner.Run(context.Background(), target, 1)
		if result.Success || !strings.Contains(result.Error, "timed out") {
			t.Fatalf("result = %+v", result)
		}
	})
}

func TestTailBufferKeepsOnlyTheNewestBytes(t *testing.T) {
	buffer := newTailBuffer(5)
	_, _ = buffer.Write([]byte("abc"))
	_, _ = buffer.Write([]byte("defg"))
	if got := buffer.String(); got != "cdefg" {
		t.Fatalf("buffer = %q", got)
	}
	_, _ = buffer.Write([]byte("123456"))
	if got := buffer.String(); got != "23456" {
		t.Fatalf("buffer after large write = %q", got)
	}
}
