package proxyenv

import (
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

func TestResolverSelectsProtocolProxyAndHonorsNoProxy(t *testing.T) {
	resolver, err := Resolve([]string{
		"HTTP_PROXY=http://user:secret@http-proxy.example:3128",
		"HTTPS_PROXY=https://secure-proxy.example:4443",
		"NO_PROXY=.internal.example,localhost",
	})
	if err != nil {
		t.Fatal(err)
	}
	for raw, want := range map[string]string{
		"http://public.example/path":  "http://user:secret@http-proxy.example:3128",
		"https://public.example/path": "https://secure-proxy.example:4443",
	} {
		u, _ := url.Parse(raw)
		got, err := resolver.ProxyFunc(u)
		if err != nil || got == nil || got.String() != want {
			t.Fatalf("ProxyFunc(%q) = %v, %v", raw, got, err)
		}
	}
	for _, raw := range []string{"https://service.internal.example/path", "http://localhost/path"} {
		u, _ := url.Parse(raw)
		if got, err := resolver.ProxyFunc(u); err != nil || got != nil {
			t.Fatalf("NO_PROXY %q = %v, %v", raw, got, err)
		}
	}
}

func TestResolverUsesAllProxyForHTTPAndWebSocket(t *testing.T) {
	resolver, err := Resolve([]string{"ALL_PROXY=socks5h://proxy.example:1080"})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"https://api.telegram.org", "wss://hub.example/bot/v1/ws"} {
		u, _ := url.Parse(raw)
		got, err := resolver.ProxyFunc(u)
		if err != nil || got == nil || got.Scheme != "socks5" || got.Host != "proxy.example:1080" {
			t.Fatalf("proxy for %q = %v, %v", raw, got, err)
		}
	}
	environ := strings.Join(resolver.NormalizeEnvironment([]string{"PATH=/usr/bin"}), "\n")
	if !strings.Contains(environ, "ALL_PROXY=socks5h://proxy.example:1080") || !strings.Contains(environ, "PATH=/usr/bin") {
		t.Fatalf("child environment = %s", environ)
	}
}

func TestResolverRejectsUnsupportedProxyWithoutEchoingCredentials(t *testing.T) {
	_, err := Resolve([]string{"HTTPS_PROXY=ftp://user:topsecret@proxy.example:21"})
	if err == nil {
		t.Fatal("expected invalid proxy error")
	}
	if strings.Contains(err.Error(), "topsecret") || strings.Contains(err.Error(), "user") {
		t.Fatalf("error leaked proxy credentials: %v", err)
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

func TestWebSocketDialContextSupportsAuthenticatedHTTPSProxy(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	targetDone := make(chan struct{})
	go func() {
		defer close(targetDone)
		conn, acceptErr := target.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		buffer := make([]byte, 4)
		if _, readErr := io.ReadFull(conn, buffer); readErr == nil {
			_, _ = conn.Write(buffer)
		}
	}()

	var proxyCalls atomic.Int32
	proxyServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyCalls.Add(1)
		wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
		if r.Method != http.MethodConnect || r.Header.Get("Proxy-Authorization") != wantAuth {
			http.Error(w, "bad proxy request", http.StatusProxyAuthRequired)
			return
		}
		upstream, dialErr := net.Dial("tcp", r.Host)
		if dialErr != nil {
			http.Error(w, "dial failed", http.StatusBadGateway)
			return
		}
		defer upstream.Close()
		downstream, buffered, hijackErr := w.(http.Hijacker).Hijack()
		if hijackErr != nil {
			return
		}
		defer downstream.Close()
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = buffered.Flush()
		go func() { _, _ = io.Copy(upstream, downstream) }()
		_, _ = io.Copy(downstream, upstream)
	}))
	defer proxyServer.Close()

	proxyURL, _ := url.Parse(proxyServer.URL)
	proxyURL.User = url.UserPassword("user", "secret")
	resolver, err := Resolve([]string{"HTTPS_PROXY=" + proxyURL.String()})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, _ := url.Parse("wss://service.example/socket")
	tlsConfig := proxyServer.Client().Transport.(*http.Transport).TLSClientConfig
	dial := resolver.webSocketDialContext(endpoint, nil, tlsConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := dial(ctx, "tcp", target.Addr().String())
	if err != nil {
		t.Fatalf("dial through HTTPS proxy: %v", err)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	<-targetDone
	if string(response) != "ping" || proxyCalls.Load() != 1 {
		t.Fatalf("response=%q proxy_calls=%d", response, proxyCalls.Load())
	}
}
