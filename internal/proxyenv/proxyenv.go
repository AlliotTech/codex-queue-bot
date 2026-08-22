package proxyenv

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http/httpproxy"
	xproxy "golang.org/x/net/proxy"
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

// Resolver is the single parsed outbound proxy policy used by HTTP clients,
// WebSocket dialers, and Codex child processes.  It deliberately keeps the
// original proxy strings private to avoid accidentally logging credentials.
type Resolver struct {
	cfg Config

	noProxy func(*url.URL) (*url.URL, error)
}

// Resolve parses the process environment once. Proxy URLs may use HTTP,
// HTTPS, SOCKS5, or SOCKS5H schemes; invalid configured URLs are rejected
// without echoing their credentials.
func Resolve(environ []string) (*Resolver, error) {
	cfg := FromEnvironment(environ)
	// Give httpproxy a sentinel proxy so its result distinguishes a host that
	// is not covered by NO_PROXY from a bypassed host.
	matcher := (&httpproxy.Config{HTTPProxy: "http://proxy.invalid", HTTPSProxy: "http://proxy.invalid", NoProxy: cfg.NoProxy}).ProxyFunc()
	r := &Resolver{cfg: cfg, noProxy: matcher}
	for label, raw := range map[string]string{"HTTP_PROXY": cfg.HTTPProxy, "HTTPS_PROXY": cfg.HTTPSProxy, "ALL_PROXY": cfg.AllProxy} {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		if _, err := parseProxyURL(raw); err != nil {
			return nil, fmt.Errorf("%s has an invalid or unsupported proxy URL", label)
		}
	}
	return r, nil
}

// FromConfig builds a resolver from an already normalized configuration.
func FromConfig(cfg Config) *Resolver {
	r, _ := Resolve([]string{
		"HTTP_PROXY=" + cfg.HTTPProxy,
		"HTTPS_PROXY=" + cfg.HTTPSProxy,
		"ALL_PROXY=" + cfg.AllProxy,
		"NO_PROXY=" + cfg.NoProxy,
	})
	return r
}

func (r *Resolver) Config() Config {
	if r == nil {
		return Config{}
	}
	return r.cfg
}

// NormalizeEnvironment applies this resolver's effective values to a child
// process environment without exposing any additional variables.
func (r *Resolver) NormalizeEnvironment(environ []string) []string {
	if r == nil {
		return Normalize(environ)
	}
	result := make([]string, 0, len(environ)+4)
	for _, item := range environ {
		name, _, ok := strings.Cut(item, "=")
		if ok && isProxyVariable(name) {
			continue
		}
		result = append(result, item)
	}
	result = appendProxyPair(result, "HTTP_PROXY", "http_proxy", r.cfg.HTTPProxy)
	result = appendProxyPair(result, "HTTPS_PROXY", "https_proxy", r.cfg.HTTPSProxy)
	result = appendProxyPair(result, "ALL_PROXY", "all_proxy", r.cfg.AllProxy)
	result = appendProxyPair(result, "NO_PROXY", "no_proxy", r.cfg.NoProxy)
	return result
}

func (r *Resolver) Enabled() bool { return r != nil && r.cfg.Enabled() }

// HTTPTransport returns a clone of base with the resolver's HTTP/HTTPS/SOCKS
// policy installed.  Callers can pass nil to clone http.DefaultTransport.
func (r *Resolver) HTTPTransport(base *http.Transport) *http.Transport {
	if base == nil {
		base = http.DefaultTransport.(*http.Transport)
	}
	transport := base.Clone()
	if r == nil {
		return transport
	}
	transport.Proxy = r.proxyFunc
	return transport
}

// ProxyFunc is suitable for http.Transport.Proxy and for callers that need to
// inspect the selected proxy without consulting the process environment.
func (r *Resolver) ProxyFunc(requestURL *url.URL) (*url.URL, error) {
	u, err := r.proxyURL(requestURL)
	if err != nil || u == nil {
		return u, err
	}
	if strings.EqualFold(u.Scheme, "socks5h") {
		copy := *u
		copy.Scheme = "socks5"
		u = &copy
	}
	return u, nil
}

func (r *Resolver) proxyURL(requestURL *url.URL) (*url.URL, error) {
	if r == nil || requestURL == nil {
		return nil, nil
	}
	matchURL := requestURL
	if requestURL.Scheme == "ws" || requestURL.Scheme == "wss" {
		copy := *requestURL
		if copy.Scheme == "ws" {
			copy.Scheme = "http"
		} else {
			copy.Scheme = "https"
		}
		matchURL = &copy
	}
	if r.noProxy != nil {
		if bypass, err := r.noProxy(matchURL); err == nil && bypass == nil {
			return nil, nil
		}
	}
	raw := r.proxyForScheme(requestURL.Scheme)
	if raw == "" {
		return nil, nil
	}
	u, err := parseProxyURL(raw)
	if err != nil {
		return nil, errors.New("invalid outbound proxy configuration")
	}
	return u, nil
}

// WebSocketProxy exposes the selected proxy for diagnostics and tests.
func (r *Resolver) WebSocketProxy(requestURL *url.URL) (*url.URL, error) {
	return r.ProxyFunc(requestURL)
}

// WebSocketDialContext establishes the proxy hop itself. This is required for
// HTTPS proxies, which gorilla/websocket's built-in proxy layer does not
// support, and keeps ws/wss on the same resolver policy as HTTP traffic.
func (r *Resolver) WebSocketDialContext(requestURL *url.URL, base *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return r.webSocketDialContext(requestURL, base, nil)
}

func (r *Resolver) webSocketDialContext(requestURL *url.URL, base *net.Dialer, proxyTLSConfig *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	if base == nil {
		base = &net.Dialer{}
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		proxyURL, err := r.proxyURL(requestURL)
		if err != nil {
			return nil, err
		}
		if proxyURL == nil {
			return base.DialContext(ctx, network, address)
		}
		switch strings.ToLower(proxyURL.Scheme) {
		case "socks5", "socks5h":
			dialer, err := xproxy.FromURL(proxyURL, base)
			if err != nil {
				return nil, errors.New("configure SOCKS proxy")
			}
			contextDialer, ok := dialer.(xproxy.ContextDialer)
			if !ok {
				return nil, errors.New("SOCKS proxy does not support context dialing")
			}
			return contextDialer.DialContext(ctx, network, address)
		case "http", "https":
			return dialHTTPConnect(ctx, base, proxyURL, address, proxyTLSConfig)
		default:
			return nil, errors.New("unsupported WebSocket proxy scheme")
		}
	}
}

func (r *Resolver) proxyFunc(request *http.Request) (*url.URL, error) {
	if request == nil {
		return nil, nil
	}
	return r.ProxyFunc(request.URL)
}

func (r *Resolver) proxyForScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "http", "ws":
		return firstNonEmpty(r.cfg.HTTPProxy, r.cfg.AllProxy, r.cfg.HTTPSProxy)
	case "https", "wss":
		return firstNonEmpty(r.cfg.HTTPSProxy, r.cfg.AllProxy, r.cfg.HTTPProxy)
	default:
		return firstNonEmpty(r.cfg.AllProxy, r.cfg.HTTPSProxy, r.cfg.HTTPProxy)
	}
}

func parseProxyURL(raw string) (*url.URL, error) {
	raw = normalizeProxyURL(raw)
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("empty proxy URL")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "socks5", "socks5h":
		return u, nil
	default:
		return nil, errors.New("unsupported proxy scheme")
	}
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

func dialHTTPConnect(ctx context.Context, base *net.Dialer, proxyURL *url.URL, target string, proxyTLSConfig *tls.Config) (net.Conn, error) {
	proxyAddress := proxyURL.Host
	if proxyURL.Port() == "" {
		port := "80"
		if strings.EqualFold(proxyURL.Scheme, "https") {
			port = "443"
		}
		proxyAddress = net.JoinHostPort(proxyURL.Hostname(), port)
	}
	conn, err := base.DialContext(ctx, "tcp", proxyAddress)
	if err != nil {
		return nil, fmt.Errorf("dial outbound proxy: %w", err)
	}
	closeWithError := func(err error) (net.Conn, error) {
		_ = conn.Close()
		return nil, err
	}
	if strings.EqualFold(proxyURL.Scheme, "https") {
		tlsConfig := &tls.Config{ServerName: proxyURL.Hostname()}
		if proxyTLSConfig != nil {
			tlsConfig = proxyTLSConfig.Clone()
			if tlsConfig.ServerName == "" {
				tlsConfig.ServerName = proxyURL.Hostname()
			}
		}
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return closeWithError(fmt.Errorf("establish TLS to outbound proxy: %w", err))
		}
		conn = tlsConn
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	headers := make(http.Header)
	if proxyURL.User != nil {
		password, _ := proxyURL.User.Password()
		credentials := base64.StdEncoding.EncodeToString([]byte(proxyURL.User.Username() + ":" + password))
		headers.Set("Proxy-Authorization", "Basic "+credentials)
	}
	request := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: target},
		Host:   target,
		Header: headers,
	}
	if err := request.Write(conn); err != nil {
		return closeWithError(fmt.Errorf("write outbound proxy CONNECT request: %w", err))
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		return closeWithError(fmt.Errorf("read outbound proxy CONNECT response: %w", err))
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return closeWithError(fmt.Errorf("outbound proxy CONNECT failed with HTTP %d", response.StatusCode))
	}
	_ = conn.SetDeadline(time.Time{})
	if reader.Buffered() > 0 {
		return &bufferedConn{Conn: conn, reader: reader}, nil
	}
	return conn, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(buffer []byte) (int, error) {
	return c.reader.Read(buffer)
}
