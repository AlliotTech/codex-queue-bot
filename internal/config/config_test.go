package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	if cfg.Codex.KeepaliveMinSecond != defaultKeepaliveMin || cfg.Codex.KeepaliveMaxSecond != defaultKeepaliveMax {
		t.Fatalf("keepalive defaults = %d-%d", cfg.Codex.KeepaliveMinSecond, cfg.Codex.KeepaliveMaxSecond)
	}
	if cfg.KeepaliveMin() != 2700*time.Second || cfg.KeepaliveMax() != 3300*time.Second {
		t.Fatalf("keepalive durations = %s-%s", cfg.KeepaliveMin(), cfg.KeepaliveMax())
	}
	if !cfg.OpenILinkEnabled() {
		t.Fatal("legacy token_env configuration should enable OpenILink")
	}
	if cfg.Web.ListenAddress != ":8080" || cfg.Web.AdminUsername != "admin" || cfg.Web.AdminPasswordEnv != "WEB_ADMIN_PASSWORD" || cfg.Web.ActivityLimit != 200 || !cfg.Web.CookieSecure {
		t.Fatalf("web defaults = %+v", cfg.Web)
	}
}

func TestLoadAllowsWebOnlyConfigurationAndExplicitlyDisabledOpenILink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	t.Setenv("TEST_CODEX_KEY", "codex-secret")
	data := `{
  "openilink": {"enabled": false, "token_env": "MISSING_TOKEN"},
  "web": {"listen_address": "127.0.0.1:9090", "cookie_secure": false},
  "codex": {"targets": [{"name":"main","api_base_url":"https://api.example/v1","api_key_env":"TEST_CODEX_KEY","model":"gpt-test"}]}
}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OpenILinkEnabled() {
		t.Fatal("OpenILink should be disabled")
	}
	if cfg.Web.CookieSecure || cfg.Web.ListenAddress != "127.0.0.1:9090" {
		t.Fatalf("web configuration = %+v", cfg.Web)
	}
}

func TestExplicitlyEnabledOpenILinkStillRequiresToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	t.Setenv("TEST_CODEX_KEY", "codex-secret")
	data := `{
  "openilink": {"enabled": true, "base_url": "https://hub.example.com"},
  "codex": {"targets": [{"name":"main","api_base_url":"https://api.example/v1","api_key_env":"TEST_CODEX_KEY","model":"gpt-test"}]}
}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "openilink token is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestAdminPasswordComesOnlyFromEnvironmentAndHasMinimumLength(t *testing.T) {
	cfg := Config{Web: WebConfig{AdminPasswordEnv: "TEST_WEB_PASSWORD"}}
	t.Setenv("TEST_WEB_PASSWORD", "short")
	if _, err := cfg.AdminPassword(); err == nil || !strings.Contains(err.Error(), "at least 12") {
		t.Fatalf("short password error = %v", err)
	}
	t.Setenv("TEST_WEB_PASSWORD", "long-enough-password")
	password, err := cfg.AdminPassword()
	if err != nil || password != "long-enough-password" {
		t.Fatalf("AdminPassword = %q, %v", password, err)
	}
}

func TestLoadRejectsNegativeActivityLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	t.Setenv("TEST_CODEX_KEY", "codex-secret")
	data := `{
  "openilink": {"enabled": false},
  "web": {"activity_limit": -1},
  "codex": {"targets": [{"name":"main","api_base_url":"https://api.example/v1","api_key_env":"TEST_CODEX_KEY","model":"gpt-test"}]}
}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "activity_limit") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadKeepaliveInterval(t *testing.T) {
	t.Setenv("TEST_HUB_TOKEN", "hub-secret")
	t.Setenv("TEST_CODEX_KEY", "codex-secret")
	tests := []struct {
		name    string
		fields  string
		wantMin int
		wantMax int
	}{
		{name: "explicit", fields: `"keepalive_min_seconds": 12, "keepalive_max_seconds": 34,`, wantMin: 12, wantMax: 34},
		{name: "explicit zero uses defaults", fields: `"keepalive_min_seconds": 0, "keepalive_max_seconds": 0,`, wantMin: defaultKeepaliveMin, wantMax: defaultKeepaliveMax},
		{name: "zero minimum only", fields: `"keepalive_min_seconds": 0, "keepalive_max_seconds": 3000,`, wantMin: defaultKeepaliveMin, wantMax: 3000},
		{name: "zero maximum only", fields: `"keepalive_min_seconds": 3000, "keepalive_max_seconds": 0,`, wantMin: 3000, wantMax: defaultKeepaliveMax},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := `{
  "openilink": {"base_url": "https://hub.example.com", "token_env": "TEST_HUB_TOKEN"},
  "codex": {
    ` + tt.fields + `
    "targets": [
      {"name": "main", "api_base_url": "https://api.example.com/v1", "api_key_env": "TEST_CODEX_KEY", "model": "gpt-test"}
    ]
  }
}`
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Codex.KeepaliveMinSecond != tt.wantMin || cfg.Codex.KeepaliveMaxSecond != tt.wantMax {
				t.Fatalf("keepalive interval = %d-%d, want %d-%d", cfg.Codex.KeepaliveMinSecond, cfg.Codex.KeepaliveMaxSecond, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestValidateRejectsInvalidKeepaliveInterval(t *testing.T) {
	base := Config{
		OpenILink: OpenILinkConfig{BaseURL: "https://hub.example.com", Token: "x", HTTPTimeoutSecond: 1},
		Codex: CodexConfig{
			Binary:               "codex",
			RequestTimeoutSecond: 1,
			RetryMinSecond:       1,
			RetryMaxSecond:       1,
			KeepaliveMinSecond:   1,
			KeepaliveMaxSecond:   1,
			MaxParallel:          1,
			Targets: []Target{
				{Name: "main", APIBaseURL: "https://api.example/v1", APIKey: "x", Model: "m", WireAPI: "responses"},
			},
		},
	}
	tests := []struct {
		name string
		min  int
		max  int
		want string
	}{
		{name: "minimum below one", min: -1, max: 1, want: "at least 1 second"},
		{name: "maximum below one", min: 1, max: -1, want: "at least 1 second"},
		{name: "reversed", min: 2, max: 1, want: "keepalive_min_seconds must be <= keepalive_max_seconds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			cfg.Codex.KeepaliveMinSecond = tt.min
			cfg.Codex.KeepaliveMaxSecond = tt.max
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want substring %q", err, tt.want)
			}
		})
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
			KeepaliveMinSecond:   1,
			KeepaliveMaxSecond:   1,
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
