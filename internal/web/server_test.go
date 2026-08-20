package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
)

type testRunner struct {
	block bool
	once  sync.Once
	start chan struct{}
}

func (r *testRunner) Run(ctx context.Context, _ config.Target, _ int) codex.Result {
	if !r.block {
		return codex.Result{Success: true, Response: "ok"}
	}
	if r.start != nil {
		r.once.Do(func() { close(r.start) })
	}
	<-ctx.Done()
	return codex.Result{Error: "cancelled"}
}

type webFixture struct {
	server  *Server
	manager *jobs.Manager
	cancel  context.CancelFunc
}

func newWebFixture(t *testing.T, runner *testRunner, mutate func(*Options)) webFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	targets := []config.Target{
		{Name: "main", APIBaseURL: "https://api.example.test/v1/private-path", APIKey: "super-secret-api-key", APIKeyEnv: "SECRET_ENV_NAME", Model: "gpt-test", WireAPI: "responses"},
		{Name: "backup", APIBaseURL: "https://backup.example.test/v1", APIKey: "backup-secret", Model: "gpt-backup", WireAPI: "responses"},
	}
	manager := jobs.New(ctx, targets, runner, nil, nil, time.Hour, time.Hour, time.Hour, time.Hour, 2, "开蹬", 200)
	shutdown := make(chan struct{})
	options := Options{
		Manager:           manager,
		OpenILinkStatus:   hub.NewStatusStore(hub.StatusDisabled),
		Username:          "admin",
		Password:          "correct-horse-battery",
		CookieSecure:      false,
		Version:           "test-version",
		Shutdown:          shutdown,
		HeartbeatInterval: 20 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&options)
	}
	server, err := New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return webFixture{server: server, manager: manager, cancel: cancel}
}

func performRequest(server *Server, method, target, body, cookie, csrf, remote string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.RemoteAddr = remote
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if cookie != "" {
		request.Header.Set("Cookie", cookie)
	}
	if csrf != "" {
		request.Header.Set(csrfHeaderName, csrf)
	}
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func loginFixture(t *testing.T, server *Server) (string, string, *http.Cookie) {
	t.Helper()
	recorder := performRequest(server, http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"correct-horse-battery"}`, "", "", "192.0.2.1:1234")
	if recorder.Code != http.StatusOK {
		t.Fatalf("login status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response sessionResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %+v", cookies)
	}
	return cookies[0].Name + "=" + cookies[0].Value, response.CSRFToken, cookies[0]
}

func TestAuthenticationRateLimitCookieCSRFAndLogout(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{}, func(options *Options) { options.CookieSecure = true })
	if response := performRequest(fixture.server, http.MethodGet, "/api/v1/dashboard", "", "", "", "192.0.2.8:1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized dashboard status = %d", response.Code)
	}

	for attempt := 0; attempt < 5; attempt++ {
		response := performRequest(fixture.server, http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"wrong-password-value"}`, "", "", "192.0.2.9:1")
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), "用户名或密码错误") {
			t.Fatalf("failure %d status=%d body=%s", attempt, response.Code, response.Body.String())
		}
	}
	if response := performRequest(fixture.server, http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"wrong-password-value"}`, "", "", "192.0.2.9:1"); response.Code != http.StatusTooManyRequests {
		t.Fatalf("rate limited status = %d", response.Code)
	}

	cookieHeader, csrf, cookie := loginFixture(t, fixture.server)
	if !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" {
		t.Fatalf("session cookie = %+v", cookie)
	}
	if response := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.start","targets":["missing"]}`, cookieHeader, "", "192.0.2.1:1"); response.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF status = %d", response.Code)
	}
	if response := performRequest(fixture.server, http.MethodPost, "/api/v1/auth/logout", "", cookieHeader, csrf, "192.0.2.1:1"); response.Code != http.StatusOK {
		t.Fatalf("logout status = %d body=%s", response.Code, response.Body.String())
	}
	if response := performRequest(fixture.server, http.MethodGet, "/api/v1/auth/session", "", cookieHeader, "", "192.0.2.1:1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("logged out session status = %d", response.Code)
	}
}

func TestSessionExpiration(t *testing.T) {
	now := time.Now()
	fixture := newWebFixture(t, &testRunner{}, func(options *Options) { options.Now = func() time.Time { return now } })
	cookie, _, _ := loginFixture(t, fixture.server)
	now = now.Add(13 * time.Hour)
	response := performRequest(fixture.server, http.MethodGet, "/api/v1/auth/session", "", cookie, "", "192.0.2.1:1")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status = %d", response.Code)
	}
}

func TestDashboardAndAllActionsDoNotLeakSecrets(t *testing.T) {
	runner := &testRunner{block: true, start: make(chan struct{})}
	fixture := newWebFixture(t, runner, nil)
	cookie, csrf, _ := loginFixture(t, fixture.server)

	start := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.start","targets":["main","missing"]}`, cookie, csrf, "192.0.2.1:1")
	if start.Code != http.StatusOK || !strings.Contains(start.Body.String(), `"changed":["main"]`) || !strings.Contains(start.Body.String(), `"unknown":["missing"]`) {
		t.Fatalf("queue start = %d %s", start.Code, start.Body.String())
	}
	stop := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.stop","targets":["main"]}`, cookie, csrf, "192.0.2.1:1")
	if stop.Code != http.StatusOK || !strings.Contains(stop.Body.String(), `"changed":["main"]`) {
		t.Fatalf("queue stop = %d %s", stop.Code, stop.Body.String())
	}
	keepaliveStart := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"keepalive.start","targets":["main"]}`, cookie, csrf, "192.0.2.1:1")
	if keepaliveStart.Code != http.StatusOK || !strings.Contains(keepaliveStart.Body.String(), `"changed":["main"]`) {
		t.Fatalf("keepalive start = %d %s", keepaliveStart.Code, keepaliveStart.Body.String())
	}
	keepaliveStop := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"keepalive.stop","targets":[]}`, cookie, csrf, "192.0.2.1:1")
	if keepaliveStop.Code != http.StatusOK || !strings.Contains(keepaliveStop.Body.String(), `"changed":["main"]`) {
		t.Fatalf("keepalive stop = %d %s", keepaliveStop.Code, keepaliveStop.Body.String())
	}
	if invalid := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"config.write","targets":[]}`, cookie, csrf, "192.0.2.1:1"); invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid action status = %d", invalid.Code)
	}

	dashboard := performRequest(fixture.server, http.MethodGet, "/api/v1/dashboard", "", cookie, "", "192.0.2.1:1")
	if dashboard.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d", dashboard.Code)
	}
	body := dashboard.Body.String()
	for _, secret := range []string{"super-secret-api-key", "backup-secret", "SECRET_ENV_NAME", "private-path", "https://api.example.test"} {
		if strings.Contains(body, secret) {
			t.Fatalf("dashboard leaked %q: %s", secret, body)
		}
	}
	if !strings.Contains(body, `"api_host":"api.example.test"`) || !strings.Contains(body, `"source":"web"`) {
		t.Fatalf("dashboard missing public state/activity: %s", body)
	}
}

func TestActionsRejectOversizedAndUnknownJSON(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{}, nil)
	cookie, csrf, _ := loginFixture(t, fixture.server)
	unknown := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.start","targets":[],"extra":true}`, cookie, csrf, "192.0.2.1:1")
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d", unknown.Code)
	}
	oversized := `{"action":"queue.start","targets":["` + strings.Repeat("x", maxJSONBody) + `"]}`
	response := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", oversized, cookie, csrf, "192.0.2.1:1")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("oversized body status = %d", response.Code)
	}
}

func TestHealthAndStaticUIArePublic(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{}, nil)
	if response := performRequest(fixture.server, http.MethodGet, "/healthz", "", "", "", "192.0.2.1:1"); response.Code != http.StatusOK {
		t.Fatalf("health status = %d", response.Code)
	}
	response := performRequest(fixture.server, http.MethodGet, "/some/client/route", "", "", "", "192.0.2.1:1")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "<div id=\"root\">") {
		t.Fatalf("SPA status=%d body=%s", response.Code, response.Body.String())
	}
	if response := performRequest(fixture.server, http.MethodGet, "/assets/missing.js", "", "", "", "192.0.2.1:1"); response.Code != http.StatusNotFound {
		t.Fatalf("missing asset status = %d", response.Code)
	}
}

func TestSSEInitialSnapshotHeartbeatRealtimeAndDisconnectCleanup(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{block: true}, nil)
	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	loginResponse, err := client.Post(httpServer.URL+"/api/v1/auth/login", "application/json", bytes.NewBufferString(`{"username":"admin","password":"correct-horse-battery"}`))
	if err != nil {
		t.Fatal(err)
	}
	loginBody, _ := io.ReadAll(loginResponse.Body)
	_ = loginResponse.Body.Close()
	if loginResponse.StatusCode != http.StatusOK {
		t.Fatalf("login status=%d body=%s", loginResponse.StatusCode, loginBody)
	}

	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, httpServer.URL+"/api/v1/events", nil)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(response.Body)
	event, data := readSSEEvent(t, reader)
	if event != "snapshot" || !strings.Contains(data, `"version":"test-version"`) || !strings.Contains(data, `"activities":[]`) {
		t.Fatalf("initial event=%q data=%s", event, data)
	}
	event, _ = readSSEEvent(t, reader)
	if event != "heartbeat" {
		t.Fatalf("second event = %q, want heartbeat", event)
	}
	fixture.manager.StartWithOperation([]string{"main"}, jobs.Subscriber{}, jobs.Operation{Source: jobs.SourceWeb, Actor: "admin"})
	foundActivity := false
	for attempt := 0; attempt < 5; attempt++ {
		event, data = readSSEEvent(t, reader)
		if event == "activity" && strings.Contains(data, `"type":"queue.start"`) {
			foundActivity = true
			break
		}
	}
	if !foundActivity {
		t.Fatal("did not receive realtime activity event")
	}
	cancel()
	_ = response.Body.Close()
	deadline := time.Now().Add(time.Second)
	for fixture.manager.ObserverCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if fixture.manager.ObserverCount() != 0 {
		t.Fatalf("manager observers after disconnect = %d", fixture.manager.ObserverCount())
	}
}

func TestSSESessionExpirationEvent(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{}, func(options *Options) { options.SessionTTL = 40 * time.Millisecond })
	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	response, err := client.Post(httpServer.URL+"/api/v1/auth/login", "application/json", bytes.NewBufferString(`{"username":"admin","password":"correct-horse-battery"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	eventsResponse, err := client.Get(httpServer.URL + "/api/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer eventsResponse.Body.Close()
	reader := bufio.NewReader(eventsResponse.Body)
	readSSEEvent(t, reader)
	found := false
	for attempt := 0; attempt < 5; attempt++ {
		event, _ := readSSEEvent(t, reader)
		if event == "auth_expired" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("did not receive auth_expired event")
	}
}

func readSSEEvent(t *testing.T, reader *bufio.Reader) (string, string) {
	t.Helper()
	var event, data string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if event == "" && data == "" {
				continue
			}
			return event, data
		}
		if strings.HasPrefix(line, "event: ") {
			event = strings.TrimPrefix(line, "event: ")
		}
		if strings.HasPrefix(line, "data: ") {
			data = strings.TrimPrefix(line, "data: ")
		}
	}
}
