package proxyenv

import (
	"strings"
	"testing"
)

func TestNormalizeUsesHTTPProxyForAllOutboundTraffic(t *testing.T) {
	environ := Normalize([]string{
		"PATH=/usr/bin",
		"HTTP_PROXY=socks5:proxy.example:1080",
		"NO_PROXY=localhost,127.0.0.1",
	})
	joined := strings.Join(environ, "\n")

	for _, want := range []string{
		"HTTP_PROXY=socks5://proxy.example:1080",
		"http_proxy=socks5://proxy.example:1080",
		"HTTPS_PROXY=socks5://proxy.example:1080",
		"https_proxy=socks5://proxy.example:1080",
		"ALL_PROXY=socks5://proxy.example:1080",
		"all_proxy=socks5://proxy.example:1080",
		"NO_PROXY=localhost,127.0.0.1",
		"no_proxy=localhost,127.0.0.1",
		"PATH=/usr/bin",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("normalized environment missing %q:\n%s", want, joined)
		}
	}
}

func TestFromEnvironmentPreservesProtocolSpecificProxies(t *testing.T) {
	cfg := FromEnvironment([]string{
		"http_proxy=http://http-proxy.example:3128",
		"HTTPS_PROXY=socks5://secure-proxy.example:1080",
	})
	if cfg.HTTPProxy != "http://http-proxy.example:3128" {
		t.Fatalf("HTTPProxy = %q", cfg.HTTPProxy)
	}
	if cfg.HTTPSProxy != "socks5://secure-proxy.example:1080" {
		t.Fatalf("HTTPSProxy = %q", cfg.HTTPSProxy)
	}
	if cfg.AllProxy != "" {
		t.Fatalf("AllProxy = %q; want empty for distinct protocol proxies", cfg.AllProxy)
	}
}

func TestFromEnvironmentUsesAllProxyAsFallback(t *testing.T) {
	cfg := FromEnvironment([]string{"ALL_PROXY=socks5h://proxy.example:1080"})
	if cfg.HTTPProxy != cfg.AllProxy || cfg.HTTPSProxy != cfg.AllProxy {
		t.Fatalf("config = %+v", cfg)
	}
}
