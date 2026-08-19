package proxyenv

import (
	"net/url"
	"os"
	"strings"
)

var proxyVariableNames = []string{
	"HTTP_PROXY",
	"HTTPS_PROXY",
	"ALL_PROXY",
	"NO_PROXY",
	"http_proxy",
	"https_proxy",
	"all_proxy",
	"no_proxy",
}

// Config is the effective outbound proxy configuration shared by the bot and
// the Codex subprocesses it starts.
type Config struct {
	HTTPProxy  string
	HTTPSProxy string
	AllProxy   string
	NoProxy    string
}

// FromEnvironment resolves both upper- and lower-case proxy variables. A
// single configured proxy is used as the fallback for the other outbound
// protocols, so HTTP_PROXY alone also covers HTTPS and WebSocket traffic.
func FromEnvironment(environ []string) Config {
	values := environmentMap(environ)
	cfg := Config{
		HTTPProxy:  normalizeProxyURL(firstNonEmpty(values["HTTP_PROXY"], values["http_proxy"])),
		HTTPSProxy: normalizeProxyURL(firstNonEmpty(values["HTTPS_PROXY"], values["https_proxy"])),
		AllProxy:   normalizeProxyURL(firstNonEmpty(values["ALL_PROXY"], values["all_proxy"])),
		NoProxy:    firstNonEmpty(values["NO_PROXY"], values["no_proxy"]),
	}

	if cfg.HTTPProxy == "" {
		cfg.HTTPProxy = firstNonEmpty(cfg.AllProxy, cfg.HTTPSProxy)
	}
	if cfg.HTTPSProxy == "" {
		cfg.HTTPSProxy = firstNonEmpty(cfg.AllProxy, cfg.HTTPProxy)
	}
	if cfg.AllProxy == "" && cfg.HTTPProxy == cfg.HTTPSProxy {
		cfg.AllProxy = cfg.HTTPProxy
	}
	return cfg
}

// Normalize returns environ with one consistent set of upper- and lower-case
// proxy variables. Unrelated variables are preserved unchanged.
func Normalize(environ []string) []string {
	cfg := FromEnvironment(environ)
	result := make([]string, 0, len(environ)+len(proxyVariableNames))
	for _, item := range environ {
		name, _, ok := strings.Cut(item, "=")
		if ok && isProxyVariable(name) {
			continue
		}
		result = append(result, item)
	}

	result = appendProxyPair(result, "HTTP_PROXY", "http_proxy", cfg.HTTPProxy)
	result = appendProxyPair(result, "HTTPS_PROXY", "https_proxy", cfg.HTTPSProxy)
	result = appendProxyPair(result, "ALL_PROXY", "all_proxy", cfg.AllProxy)
	result = appendProxyPair(result, "NO_PROXY", "no_proxy", cfg.NoProxy)
	return result
}

// Apply normalizes the current process environment before any network client
// reads and caches the standard proxy variables.
func Apply() Config {
	normalized := Normalize(os.Environ())
	values := environmentMap(normalized)
	for _, name := range proxyVariableNames {
		value, ok := values[name]
		if !ok {
			_ = os.Unsetenv(name)
			continue
		}
		_ = os.Setenv(name, value)
	}
	return FromEnvironment(normalized)
}

func (c Config) Enabled() bool {
	return c.HTTPProxy != "" || c.HTTPSProxy != "" || c.AllProxy != ""
}

func environmentMap(environ []string) map[string]string {
	values := make(map[string]string, len(environ))
	for _, item := range environ {
		name, value, ok := strings.Cut(item, "=")
		if ok {
			values[name] = strings.TrimSpace(value)
		}
	}
	return values
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func isProxyVariable(name string) bool {
	for _, candidate := range proxyVariableNames {
		if name == candidate {
			return true
		}
	}
	return false
}

func appendProxyPair(environ []string, upper, lower, value string) []string {
	if value == "" {
		return environ
	}
	return append(environ, upper+"="+value, lower+"="+value)
}

func normalizeProxyURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host != "" || parsed.Opaque == "" {
		return raw
	}
	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "http", "https", "socks5", "socks5h":
		// Accept the commonly used shorthand socks5:host:port in addition
		// to the canonical socks5://host:port form expected by clients.
		return scheme + "://" + parsed.Opaque
	default:
		return raw
	}
}
