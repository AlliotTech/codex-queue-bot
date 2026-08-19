package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAppliesDefaultsAndResolvesSecrets(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(filepath.Join(dir, "prompts.txt"), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_HUB_TOKEN", "hub-secret")
	t.Setenv("TEST_CODEX_KEY", "codex-secret")
	data := `{
  "openilink": {"base_url": "https://hub.example.com/", "token_env": "TEST_HUB_TOKEN"},
  "codex": {
    "targets": [
      {"name": "main", "api_base_url": "https://api.example.com/v1/", "api_key_env": "TEST_CODEX_KEY", "model": "gpt-test"}
    ]
  }
}`
	if err := os.WriteFile(configPath, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OpenILink.Token != "hub-secret" {
		t.Fatalf("OpenILink token = %q", cfg.OpenILink.Token)
	}
	if cfg.Codex.Targets[0].APIKey != "codex-secret" {
		t.Fatalf("Codex key = %q", cfg.Codex.Targets[0].APIKey)
	}
	if cfg.OpenILink.BaseURL != "https://hub.example.com" {
		t.Fatalf("base URL = %q", cfg.OpenILink.BaseURL)
	}
	if cfg.Codex.Targets[0].APIBaseURL != "https://api.example.com/v1" {
		t.Fatalf("target URL = %q", cfg.Codex.Targets[0].APIBaseURL)
	}
	if cfg.Codex.Targets[0].WireAPI != "responses" {
		t.Fatalf("wire API = %q", cfg.Codex.Targets[0].WireAPI)
	}
	if cfg.Codex.PromptsFile != filepath.Join(dir, "prompts.txt") {
		t.Fatalf("prompts path = %q", cfg.Codex.PromptsFile)
	}
	if cfg.Codex.RetryMinSecond != defaultRetryMin || cfg.Codex.RetryMaxSecond != defaultRetryMax {
		t.Fatalf("retry defaults = %d-%d", cfg.Codex.RetryMinSecond, cfg.Codex.RetryMaxSecond)
	}
}

func TestLoadRejectsUnknownFieldAndTrailingJSON(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "unknown",
			body: `{"unknown": true}`,
			want: "unknown field",
		},
		{
			name: "trailing",
			body: `{}` + "\n{}",
			want: "multiple JSON values",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(tt.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestValidateRejectsDuplicateAndUnaddressableTargetNames(t *testing.T) {
	base := Config{
		OpenILink: OpenILinkConfig{BaseURL: "https://hub.example.com", Token: "x", HTTPTimeoutSecond: 1},
		Codex: CodexConfig{
			Binary:               "codex",
			RequestTimeoutSecond: 1,
			RetryMinSecond:       1,
			RetryMaxSecond:       1,
			MaxParallel:          1,
		},
	}

	t.Run("duplicate", func(t *testing.T) {
		cfg := base
		cfg.Codex.Targets = []Target{
			{Name: "A", APIBaseURL: "https://a.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
			{Name: "a", APIBaseURL: "https://b.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
		}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("space", func(t *testing.T) {
		cfg := base
		cfg.Codex.Targets = []Target{
			{Name: "bad name", APIBaseURL: "https://a.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
		}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must not contain") {
			t.Fatalf("error = %v", err)
		}
	})
}
