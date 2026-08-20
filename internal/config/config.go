package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

const (
	defaultHubBaseURL       = "http://127.0.0.1:9800"
	defaultCodexBinary      = "codex"
	defaultPromptsFile      = "prompts.txt"
	defaultRequestTimeout   = 180
	defaultRetryMin         = 3
	defaultRetryMax         = 8
	defaultKeepaliveMin     = 2700
	defaultKeepaliveMax     = 3300
	defaultMaxParallel      = 2
	defaultSuccessMessage   = "开蹬"
	defaultReasoningEffort  = "low"
	defaultWireAPI          = "responses"
	defaultHTTPTimeout      = 15
	minimumRetryIntervalSec = 1
)

type Config struct {
	OpenILink OpenILinkConfig `json:"openilink"`
	Codex     CodexConfig     `json:"codex"`
}

type OpenILinkConfig struct {
	BaseURL           string   `json:"base_url"`
	Token             string   `json:"token,omitempty"`
	TokenEnv          string   `json:"token_env,omitempty"`
	AllowedUserIDs    []string `json:"allowed_user_ids,omitempty"`
	HTTPTimeoutSecond int      `json:"http_timeout_seconds,omitempty"`
}

type CodexConfig struct {
	Binary               string   `json:"binary,omitempty"`
	PromptsFile          string   `json:"prompts_file,omitempty"`
	RequestTimeoutSecond int      `json:"request_timeout_seconds,omitempty"`
	RetryMinSecond       int      `json:"retry_min_seconds,omitempty"`
	RetryMaxSecond       int      `json:"retry_max_seconds,omitempty"`
	KeepaliveMinSecond   int      `json:"keepalive_min_seconds,omitempty"`
	KeepaliveMaxSecond   int      `json:"keepalive_max_seconds,omitempty"`
	MaxParallel          int      `json:"max_parallel,omitempty"`
	SuccessMessage       string   `json:"success_message,omitempty"`
	ReasoningEffort      string   `json:"reasoning_effort,omitempty"`
	ConfigOverrides      []string `json:"config_overrides,omitempty"`
	Targets              []Target `json:"targets"`
}

type Target struct {
	Name            string   `json:"name"`
	APIBaseURL      string   `json:"api_base_url"`
	APIKey          string   `json:"api_key,omitempty"`
	APIKeyEnv       string   `json:"api_key_env,omitempty"`
	Model           string   `json:"model"`
	WireAPI         string   `json:"wire_api,omitempty"`
	ConfigOverrides []string `json:"config_overrides,omitempty"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("decode config: multiple JSON values")
		}
		return nil, fmt.Errorf("decode config: trailing data: %w", err)
	}

	cfg.applyDefaults(filepath.Dir(path))
	if err := cfg.resolveSecrets(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults(configDir string) {
	if strings.TrimSpace(c.OpenILink.BaseURL) == "" {
		c.OpenILink.BaseURL = defaultHubBaseURL
	}
	c.OpenILink.BaseURL = strings.TrimRight(strings.TrimSpace(c.OpenILink.BaseURL), "/")
	c.OpenILink.Token = strings.TrimSpace(c.OpenILink.Token)
	c.OpenILink.TokenEnv = strings.TrimSpace(c.OpenILink.TokenEnv)
	if c.OpenILink.HTTPTimeoutSecond == 0 {
		c.OpenILink.HTTPTimeoutSecond = defaultHTTPTimeout
	}

	if strings.TrimSpace(c.Codex.Binary) == "" {
		c.Codex.Binary = defaultCodexBinary
	}
	if strings.TrimSpace(c.Codex.PromptsFile) == "" {
		c.Codex.PromptsFile = defaultPromptsFile
	}
	if !filepath.IsAbs(c.Codex.PromptsFile) {
		c.Codex.PromptsFile = filepath.Join(configDir, c.Codex.PromptsFile)
	}
	if c.Codex.RequestTimeoutSecond == 0 {
		c.Codex.RequestTimeoutSecond = defaultRequestTimeout
	}
	if c.Codex.RetryMinSecond == 0 {
		c.Codex.RetryMinSecond = defaultRetryMin
	}
	if c.Codex.RetryMaxSecond == 0 {
		c.Codex.RetryMaxSecond = defaultRetryMax
	}
	if c.Codex.KeepaliveMinSecond == 0 {
		c.Codex.KeepaliveMinSecond = defaultKeepaliveMin
	}
	if c.Codex.KeepaliveMaxSecond == 0 {
		c.Codex.KeepaliveMaxSecond = defaultKeepaliveMax
	}
	if c.Codex.MaxParallel == 0 {
		c.Codex.MaxParallel = defaultMaxParallel
	}
	if strings.TrimSpace(c.Codex.SuccessMessage) == "" {
		c.Codex.SuccessMessage = defaultSuccessMessage
	}
	if strings.TrimSpace(c.Codex.ReasoningEffort) == "" {
		c.Codex.ReasoningEffort = defaultReasoningEffort
	}

	for i := range c.Codex.Targets {
		t := &c.Codex.Targets[i]
		t.Name = strings.TrimSpace(t.Name)
		t.APIBaseURL = strings.TrimRight(strings.TrimSpace(t.APIBaseURL), "/")
		t.Model = strings.TrimSpace(t.Model)
		t.APIKeyEnv = strings.TrimSpace(t.APIKeyEnv)
		t.WireAPI = strings.ToLower(strings.TrimSpace(t.WireAPI))
		if strings.TrimSpace(t.WireAPI) == "" {
			t.WireAPI = defaultWireAPI
		}
	}
}

func (c *Config) resolveSecrets() error {
	if c.OpenILink.Token == "" && c.OpenILink.TokenEnv != "" {
		c.OpenILink.Token = os.Getenv(c.OpenILink.TokenEnv)
		if c.OpenILink.Token == "" {
			return fmt.Errorf("openilink.token_env %q is not set", c.OpenILink.TokenEnv)
		}
	}
	for i := range c.Codex.Targets {
		t := &c.Codex.Targets[i]
		if t.APIKey == "" && t.APIKeyEnv != "" {
			t.APIKey = os.Getenv(t.APIKeyEnv)
			if t.APIKey == "" {
				return fmt.Errorf("target %q api_key_env %q is not set", t.Name, t.APIKeyEnv)
			}
		}
	}
	return nil
}

func (c *Config) Validate() error {
	if err := validateHTTPURL("openilink.base_url", c.OpenILink.BaseURL); err != nil {
		return err
	}
	if strings.TrimSpace(c.OpenILink.Token) == "" {
		return errors.New("openilink token is required (set token or token_env)")
	}
	if c.OpenILink.HTTPTimeoutSecond <= 0 {
		return errors.New("openilink.http_timeout_seconds must be positive")
	}
	if strings.TrimSpace(c.Codex.Binary) == "" {
		return errors.New("codex.binary is required")
	}
	if c.Codex.RequestTimeoutSecond <= 0 {
		return errors.New("codex.request_timeout_seconds must be positive")
	}
	if c.Codex.RetryMinSecond < minimumRetryIntervalSec || c.Codex.RetryMaxSecond < minimumRetryIntervalSec {
		return fmt.Errorf("codex retry interval must be at least %d second", minimumRetryIntervalSec)
	}
	if c.Codex.RetryMinSecond > c.Codex.RetryMaxSecond {
		return errors.New("codex.retry_min_seconds must be <= retry_max_seconds")
	}
	if c.Codex.KeepaliveMinSecond < minimumRetryIntervalSec || c.Codex.KeepaliveMaxSecond < minimumRetryIntervalSec {
		return fmt.Errorf("codex keepalive interval must be at least %d second", minimumRetryIntervalSec)
	}
	if c.Codex.KeepaliveMinSecond > c.Codex.KeepaliveMaxSecond {
		return errors.New("codex.keepalive_min_seconds must be <= keepalive_max_seconds")
	}
	if c.Codex.MaxParallel <= 0 {
		return errors.New("codex.max_parallel must be positive")
	}
	if len(c.Codex.Targets) == 0 {
		return errors.New("at least one codex target is required")
	}
	if err := validateOverrides("codex.config_overrides", c.Codex.ConfigOverrides); err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(c.Codex.Targets))
	for i := range c.Codex.Targets {
		t := &c.Codex.Targets[i]
		if t.Name == "" {
			return fmt.Errorf("codex.targets[%d].name is required", i)
		}
		key := strings.ToLower(t.Name)
		if key == "all" || key == "全部" {
			return fmt.Errorf("target name %q is reserved", t.Name)
		}
		if strings.IndexFunc(t.Name, func(r rune) bool {
			return unicode.IsSpace(r) || strings.ContainsRune(",，;；", r)
		}) >= 0 {
			return fmt.Errorf("target name %q must not contain spaces, commas, or semicolons", t.Name)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate target name %q", t.Name)
		}
		seen[key] = struct{}{}
		if err := validateHTTPURL(fmt.Sprintf("target %q api_base_url", t.Name), t.APIBaseURL); err != nil {
			return err
		}
		if t.APIKey == "" {
			return fmt.Errorf("target %q API key is required (set api_key or api_key_env)", t.Name)
		}
		if t.Model == "" {
			return fmt.Errorf("target %q model is required", t.Name)
		}
		if t.WireAPI != "responses" {
			return fmt.Errorf("target %q wire_api %q is unsupported; use responses", t.Name, t.WireAPI)
		}
		if err := validateOverrides(fmt.Sprintf("target %q config_overrides", t.Name), t.ConfigOverrides); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) RequestTimeout() time.Duration {
	return time.Duration(c.Codex.RequestTimeoutSecond) * time.Second
}

func (c *Config) RetryMin() time.Duration {
	return time.Duration(c.Codex.RetryMinSecond) * time.Second
}

func (c *Config) RetryMax() time.Duration {
	return time.Duration(c.Codex.RetryMaxSecond) * time.Second
}

func (c *Config) KeepaliveMin() time.Duration {
	return time.Duration(c.Codex.KeepaliveMinSecond) * time.Second
}

func (c *Config) KeepaliveMax() time.Duration {
	return time.Duration(c.Codex.KeepaliveMaxSecond) * time.Second
}

func (c *Config) HTTPTimeout() time.Duration {
	return time.Duration(c.OpenILink.HTTPTimeoutSecond) * time.Second
}

func validateHTTPURL(label, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("%s must be an absolute http(s) URL", label)
	}
	if u.User != nil {
		return fmt.Errorf("%s must not contain URL userinfo", label)
	}
	if u.Fragment != "" {
		return fmt.Errorf("%s must not contain a URL fragment", label)
	}
	if u.RawQuery != "" {
		return fmt.Errorf("%s must not contain a URL query", label)
	}
	return nil
}

func validateOverrides(label string, values []string) error {
	for i, value := range values {
		if strings.TrimSpace(value) == "" || !strings.Contains(value, "=") {
			return fmt.Errorf("%s[%d] must be a non-empty key=value Codex override", label, i)
		}
	}
	return nil
}
