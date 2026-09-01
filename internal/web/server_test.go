package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/storage"
)

type testRunner struct {
	block       bool
	once        sync.Once
	start       chan struct{}
	adhocMu     sync.Mutex
	adhocPrompt string
	adhocResult codex.Result
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

func (r *testRunner) RunPrompt(_ context.Context, _ config.Target, prompt string) codex.Result {
	r.adhocMu.Lock()
	defer r.adhocMu.Unlock()
	r.adhocPrompt = prompt
	if r.adhocResult == (codex.Result{}) {
		return codex.Result{Success: true, Response: "ok", ExitCode: 0}
	}
	return r.adhocResult
}

func (r *testRunner) capturedAdhocPrompt() string {
	r.adhocMu.Lock()
	defer r.adhocMu.Unlock()
	return r.adhocPrompt
}

type webFixture struct {
	server  *Server
	manager *jobs.Manager
	cancel  context.CancelFunc
}

type databaseWebFixture struct {
	webFixture
	store *storage.Store
}

func newWebFixture(t *testing.T, runner *testRunner, mutate func(*Options)) webFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	targets := []config.Target{
		{ID: 1, Name: "main", APIBaseURL: "https://api.example.test/v1/private-path", APIKey: "super-secret-api-key", APIKeyEnv: "SECRET_ENV_NAME", Model: "gpt-test", WireAPI: "responses"},
		{ID: 2, Name: "backup", APIBaseURL: "https://backup.example.test/v1", APIKey: "backup-secret", Model: "gpt-backup", WireAPI: "responses"},
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

func newDatabaseWebFixture(t *testing.T, runner *testRunner) databaseWebFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	store, err := storage.Open(ctx, storage.Options{Path: filepath.Join(t.TempDir(), "data", "config.db"), MasterKeyBase64: key})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	snapshot, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.New(ctx, snapshot.Config.Codex.Targets, runner, nil, nil, snapshot.Config.RetryMin(), snapshot.Config.RetryMax(), snapshot.Config.KeepaliveMin(), snapshot.Config.KeepaliveMax(), snapshot.Config.Codex.MaxParallel, snapshot.Config.Codex.SuccessMessage, snapshot.Config.Web.ActivityLimit)
	server, err := New(Options{
		Manager: manager, OpenILinkStatus: hub.NewStatusStore(hub.StatusDisabled),
		CookieSecure: false, Version: "test-version", Shutdown: make(chan struct{}),
		HeartbeatInterval: 20 * time.Millisecond, ConfigStore: store, InitialConfig: snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	return databaseWebFixture{webFixture: webFixture{server: server, manager: manager, cancel: cancel}, store: store}
}

func setupDatabaseFixture(t *testing.T, server *Server, username, password string) string {
	t.Helper()
	body := fmt.Sprintf(`{"username":%q,"password":%q}`, username, password)
	recorder := performRequest(server, http.MethodPost, "/api/v1/setup", body, "", "192.0.2.20:1")
	if recorder.Code != http.StatusOK {
		t.Fatalf("setup status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response sessionResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("setup cookies = %+v", cookies)
	}
	return cookies[0].Name + "=" + cookies[0].Value
}

func performRequest(server *Server, method, target, body, cookie, remote string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.RemoteAddr = remote
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if cookie != "" {
		request.Header.Set("Cookie", cookie)
	}
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func loginFixture(t *testing.T, server *Server) (string, *http.Cookie) {
	t.Helper()
	recorder := performRequest(server, http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"correct-horse-battery"}`, "", "192.0.2.1:1234")
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
	return cookies[0].Name + "=" + cookies[0].Value, cookies[0]
}

func TestAuthenticationCookieSessionAndLogout(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{}, func(options *Options) { options.CookieSecure = true })
	if response := performRequest(fixture.server, http.MethodGet, "/api/v1/dashboard", "", "", "192.0.2.8:1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized dashboard status = %d", response.Code)
	}

	for attempt := 0; attempt < 5; attempt++ {
		response := performRequest(fixture.server, http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"wrong-password-value"}`, "", "192.0.2.9:1")
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), "用户名或密码错误") {
			t.Fatalf("failure %d status=%d body=%s", attempt, response.Code, response.Body.String())
		}
	}
	if response := performRequest(fixture.server, http.MethodPost, "/api/v1/auth/login", `{"username":"admin","password":"wrong-password-value"}`, "", "192.0.2.9:1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("additional failed login status = %d", response.Code)
	}

	cookieHeader, cookie := loginFixture(t, fixture.server)
	if !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" {
		t.Fatalf("session cookie = %+v", cookie)
	}
	if response := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.start","targets":["missing"]}`, cookieHeader, "192.0.2.1:1"); response.Code != http.StatusOK {
		t.Fatalf("session-authenticated action status = %d", response.Code)
	}
	if response := performRequest(fixture.server, http.MethodPost, "/api/v1/auth/logout", "", cookieHeader, "192.0.2.1:1"); response.Code != http.StatusOK {
		t.Fatalf("logout status = %d body=%s", response.Code, response.Body.String())
	}
	if response := performRequest(fixture.server, http.MethodGet, "/api/v1/auth/session", "", cookieHeader, "192.0.2.1:1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("logged out session status = %d", response.Code)
	}
}

func TestSessionExpiration(t *testing.T) {
	now := time.Now()
	fixture := newWebFixture(t, &testRunner{}, func(options *Options) { options.Now = func() time.Time { return now } })
	cookie, _ := loginFixture(t, fixture.server)
	now = now.Add(13 * time.Hour)
	response := performRequest(fixture.server, http.MethodGet, "/api/v1/auth/session", "", cookie, "192.0.2.1:1")
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status = %d", response.Code)
	}
}

func TestDashboardAndAllActionsDoNotLeakSecrets(t *testing.T) {
	runner := &testRunner{block: true, start: make(chan struct{})}
	fixture := newWebFixture(t, runner, nil)
	cookie, _ := loginFixture(t, fixture.server)

	start := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.start","targets":["main","missing"]}`, cookie, "192.0.2.1:1")
	if start.Code != http.StatusOK || !strings.Contains(start.Body.String(), `"changed":["main"]`) || !strings.Contains(start.Body.String(), `"unknown":["missing"]`) {
		t.Fatalf("queue start = %d %s", start.Code, start.Body.String())
	}
	stop := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.stop","targets":["main"]}`, cookie, "192.0.2.1:1")
	if stop.Code != http.StatusOK || !strings.Contains(stop.Body.String(), `"changed":["main"]`) {
		t.Fatalf("queue stop = %d %s", stop.Code, stop.Body.String())
	}
	keepaliveStart := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"keepalive.start","targets":["main"]}`, cookie, "192.0.2.1:1")
	if keepaliveStart.Code != http.StatusOK || !strings.Contains(keepaliveStart.Body.String(), `"changed":["main"]`) {
		t.Fatalf("keepalive start = %d %s", keepaliveStart.Code, keepaliveStart.Body.String())
	}
	keepaliveStop := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"keepalive.stop","targets":[]}`, cookie, "192.0.2.1:1")
	if keepaliveStop.Code != http.StatusOK || !strings.Contains(keepaliveStop.Body.String(), `"changed":["main"]`) {
		t.Fatalf("keepalive stop = %d %s", keepaliveStop.Code, keepaliveStop.Body.String())
	}
	if invalid := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"config.write","targets":[]}`, cookie, "192.0.2.1:1"); invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid action status = %d", invalid.Code)
	}

	dashboard := performRequest(fixture.server, http.MethodGet, "/api/v1/dashboard", "", cookie, "192.0.2.1:1")
	if dashboard.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d", dashboard.Code)
	}
	body := dashboard.Body.String()
	for _, secret := range []string{"super-secret-api-key", "backup-secret", "SECRET_ENV_NAME", "private-path", "https://api.example.test"} {
		if strings.Contains(body, secret) {
			t.Fatalf("dashboard leaked %q: %s", secret, body)
		}
	}
	if !strings.Contains(body, `"api_host":"api.example.test"`) || strings.Contains(body, `"source":"web"`) || strings.Contains(body, `"activities"`) {
		t.Fatalf("dashboard public state/history shape: %s", body)
	}
}

func TestAdhocRunReturnsOutputExitCodeAndRejectsBusyTarget(t *testing.T) {
	runner := &testRunner{adhocResult: codex.Result{
		Success: true, Response: "manual answer", ProcessOutput: "codex trace", ExitCode: 0, Duration: 1250 * time.Millisecond,
	}}
	fixture := newWebFixture(t, runner, nil)
	cookie, _ := loginFixture(t, fixture.server)

	response := performRequest(fixture.server, http.MethodPost, "/api/v1/targets/1/adhoc", `{"prompt":"  explain this\ncarefully  "}`, cookie, "192.0.2.1:1")
	if response.Code != http.StatusOK {
		t.Fatalf("adhoc status=%d body=%s", response.Code, response.Body.String())
	}
	var result adhocRunResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.TargetID != 1 || result.Target != "main" || !result.Success || result.Output != "manual answer" || result.ProcessOutput != "codex trace" || result.ExitCode != 0 || result.DurationMS != 1250 {
		t.Fatalf("adhoc response = %+v", result)
	}
	if got := runner.capturedAdhocPrompt(); got != "explain this\ncarefully" {
		t.Fatalf("adhoc prompt = %q", got)
	}
	if invalid := performRequest(fixture.server, http.MethodPost, "/api/v1/targets/1/adhoc", `{"prompt":"  "}`, cookie, "192.0.2.1:1"); invalid.Code != http.StatusBadRequest {
		t.Fatalf("empty prompt status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	if missing := performRequest(fixture.server, http.MethodPost, "/api/v1/targets/999/adhoc", `{"prompt":"hello"}`, cookie, "192.0.2.1:1"); missing.Code != http.StatusNotFound {
		t.Fatalf("missing target status=%d body=%s", missing.Code, missing.Body.String())
	}

	blocking := &testRunner{block: true, start: make(chan struct{})}
	busyFixture := newWebFixture(t, blocking, nil)
	busyCookie, _ := loginFixture(t, busyFixture.server)
	busyFixture.manager.Start([]string{"main"}, jobs.Subscriber{})
	select {
	case <-blocking.start:
	case <-time.After(time.Second):
		t.Fatal("queue runner did not start")
	}
	busy := performRequest(busyFixture.server, http.MethodPost, "/api/v1/targets/1/adhoc", `{"prompt":"hello"}`, busyCookie, "192.0.2.1:1")
	if busy.Code != http.StatusConflict {
		t.Fatalf("busy target status=%d body=%s", busy.Code, busy.Body.String())
	}
	busyFixture.manager.StopTask([]string{"main"})
}

func TestActionsRejectOversizedAndUnknownJSON(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{}, nil)
	cookie, _ := loginFixture(t, fixture.server)
	unknown := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", `{"action":"queue.start","targets":[],"extra":true}`, cookie, "192.0.2.1:1")
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d", unknown.Code)
	}
	oversized := `{"action":"queue.start","targets":["` + strings.Repeat("x", maxJSONBody) + `"]}`
	response := performRequest(fixture.server, http.MethodPost, "/api/v1/actions", oversized, cookie, "192.0.2.1:1")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("oversized body status = %d", response.Code)
	}
}

func TestHealthAndStaticUIArePublic(t *testing.T) {
	fixture := newWebFixture(t, &testRunner{}, nil)
	if response := performRequest(fixture.server, http.MethodGet, "/healthz", "", "", "192.0.2.1:1"); response.Code != http.StatusOK {
		t.Fatalf("health status = %d", response.Code)
	}
	response := performRequest(fixture.server, http.MethodGet, "/some/client/route", "", "", "192.0.2.1:1")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "<div id=\"root\">") {
		t.Fatalf("SPA status=%d body=%s", response.Code, response.Body.String())
	}
	if response := performRequest(fixture.server, http.MethodGet, "/assets/missing.js", "", "", "192.0.2.1:1"); response.Code != http.StatusNotFound {
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
	if event != "snapshot" || !strings.Contains(data, `"version":"test-version"`) || strings.Contains(data, `"activities"`) {
		t.Fatalf("initial event=%q data=%s", event, data)
	}
	event, _ = readSSEEvent(t, reader)
	if event != "heartbeat" {
		t.Fatalf("second event = %q, want heartbeat", event)
	}
	fixture.manager.Start([]string{"main"}, jobs.Subscriber{})
	foundState := false
	for attempt := 0; attempt < 5; attempt++ {
		event, data = readSSEEvent(t, reader)
		if event == "state" && strings.Contains(data, `"state":"running"`) {
			foundState = true
			break
		}
	}
	if !foundState {
		t.Fatal("did not receive realtime state event")
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

func TestDatabaseSetupCreatesSessionAndRejectsLaterSetup(t *testing.T) {
	fixture := newDatabaseWebFixture(t, &testRunner{})
	status := performRequest(fixture.server, http.MethodGet, "/api/v1/setup/status", "", "", "192.0.2.20:1")
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"required":true`) || !strings.Contains(status.Body.String(), `"suggested_username":"admin"`) {
		t.Fatalf("setup status = %d %s", status.Code, status.Body.String())
	}
	cookie := setupDatabaseFixture(t, fixture.server, "owner", "abcde")
	if response := performRequest(fixture.server, http.MethodGet, "/api/v1/config", "", cookie, "192.0.2.20:1"); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"username":"owner"`) {
		t.Fatalf("config after setup = %d %s", response.Code, response.Body.String())
	}
	second := performRequest(fixture.server, http.MethodPost, "/api/v1/setup", `{"username":"other","password":"correct-horse-battery"}`, "", "192.0.2.21:1")
	if second.Code != http.StatusConflict {
		t.Fatalf("second setup = %d %s", second.Code, second.Body.String())
	}
}

func TestDatabaseSetupRaceOnlyCreatesOneAdministrator(t *testing.T) {
	fixture := newDatabaseWebFixture(t, &testRunner{})
	httpServer := httptest.NewServer(fixture.server.Handler())
	defer httpServer.Close()

	const attempts = 4
	start := make(chan struct{})
	statuses := make(chan int, attempts)
	var wg sync.WaitGroup
	for index := 0; index < attempts; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			body := fmt.Sprintf(`{"username":"owner-%d","password":"correct-horse-battery"}`, index)
			response, err := http.Post(httpServer.URL+"/api/v1/setup", "application/json", strings.NewReader(body))
			if err != nil {
				t.Errorf("setup request: %v", err)
				return
			}
			_ = response.Body.Close()
			statuses <- response.StatusCode
		}(index)
	}
	close(start)
	wg.Wait()
	close(statuses)

	successes, conflicts := 0, 0
	for status := range statuses {
		switch status {
		case http.StatusOK:
			successes++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("unexpected setup status %d", status)
		}
	}
	if successes != 1 || conflicts != attempts-1 {
		t.Fatalf("setup race successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestConfigurationTargetLifecycleRestartStatusAndAccountRevocation(t *testing.T) {
	runner := &testRunner{block: true, start: make(chan struct{})}
	fixture := newDatabaseWebFixture(t, runner)
	cookie := setupDatabaseFixture(t, fixture.server, "owner", "correct-horse-battery")

	createBody := `{"name":"main","api_base_url":"https://api.example.test/v1/private","api_key":"target-secret-value","model":"gpt-test","wire_api":"responses","config_overrides":[]}`
	created := performRequest(fixture.server, http.MethodPost, "/api/v1/targets", createBody, cookie, "192.0.2.20:1")
	if created.Code != http.StatusCreated {
		t.Fatalf("create target = %d %s", created.Code, created.Body.String())
	}
	if strings.Contains(created.Body.String(), "target-secret-value") || !strings.Contains(created.Body.String(), `"api_key_set":true`) {
		t.Fatalf("target response leaked or omitted key state: %s", created.Body.String())
	}
	var configuration configurationResponse
	if err := json.Unmarshal(created.Body.Bytes(), &configuration); err != nil {
		t.Fatal(err)
	}
	if len(configuration.Targets) != 1 {
		t.Fatalf("target config = %+v", configuration.Targets)
	}
	id := configuration.Targets[0].ID

	fixture.manager.Start([]string{"main"}, jobs.Subscriber{})
	select {
	case <-runner.start:
	case <-time.After(time.Second):
		t.Fatal("target request did not start")
	}
	updateBody := `{"sort_order":0,"name":"renamed","api_base_url":"https://api.example.test/v1","api_key":"","model":"gpt-next","wire_api":"responses","config_overrides":[]}`
	busyUpdate := performRequest(fixture.server, http.MethodPut, fmt.Sprintf("/api/v1/targets/%d", id), updateBody, cookie, "192.0.2.20:1")
	if busyUpdate.Code != http.StatusOK || !strings.Contains(busyUpdate.Body.String(), `"name":"renamed"`) {
		t.Fatalf("running target update = %d %s", busyUpdate.Code, busyUpdate.Body.String())
	}
	deadline := time.Now().Add(time.Second)
	for fixture.manager.ComprehensiveSnapshot().Targets[0].Busy && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	persisted, err := fixture.store.Load(context.Background())
	if err != nil || persisted.Config.Codex.Targets[0].APIKey != "target-secret-value" {
		t.Fatalf("blank API key did not keep value: snapshot=%+v err=%v", persisted, err)
	}

	codexBody := `{"binary":"codex-next","prompts_file":"prompts.txt","prompts":["health check"],"request_timeout_seconds":90,"retry_min_seconds":2,"retry_max_seconds":4,"keepalive_min_seconds":10,"keepalive_max_seconds":20,"max_parallel":3,"success_message":"ok","reasoning_effort":"medium","config_overrides":[]}`
	codexUpdate := performRequest(fixture.server, http.MethodPut, "/api/v1/config/codex", codexBody, cookie, "192.0.2.20:1")
	if codexUpdate.Code != http.StatusOK || strings.Contains(codexUpdate.Body.String(), `"codex.binary"`) {
		t.Fatalf("codex update = %d %s", codexUpdate.Code, codexUpdate.Body.String())
	}
	if snapshot := fixture.manager.ComprehensiveSnapshot(); snapshot.MaxParallel != 3 {
		t.Fatalf("hot max_parallel = %d", snapshot.MaxParallel)
	}
	invalidWeb := performRequest(fixture.server, http.MethodPut, "/api/v1/config/web", `{"listen_address":"not-an-address","cookie_secure":false,"trusted_proxies":[]}`, cookie, "192.0.2.20:1")
	if invalidWeb.Code != http.StatusBadRequest {
		t.Fatalf("invalid web config = %d %s", invalidWeb.Code, invalidWeb.Body.String())
	}
	invalidAccount := performRequest(fixture.server, http.MethodPut, "/api/v1/account", `{"username":"owner","new_password":"1234"}`, cookie, "192.0.2.20:1")
	if invalidAccount.Code != http.StatusBadRequest {
		t.Fatalf("invalid account password = %d %s", invalidAccount.Code, invalidAccount.Body.String())
	}
	dashboard := performRequest(fixture.server, http.MethodGet, "/api/v1/dashboard", "", cookie, "192.0.2.20:1")
	if dashboard.Code != http.StatusOK || strings.Contains(dashboard.Body.String(), `"restart_required":true`) || strings.Contains(dashboard.Body.String(), "target-secret-value") {
		t.Fatalf("dashboard restart/security = %d %s", dashboard.Code, dashboard.Body.String())
	}

	account := performRequest(fixture.server, http.MethodPut, "/api/v1/account", `{"username":"new-owner","new_password":"new-correct-horse-battery","current_password":"correct-horse-battery"}`, cookie, "192.0.2.20:1")
	if account.Code != http.StatusOK || !strings.Contains(account.Body.String(), `"reauthentication_required":true`) {
		t.Fatalf("account update = %d %s", account.Code, account.Body.String())
	}
	if response := performRequest(fixture.server, http.MethodGet, "/api/v1/config", "", cookie, "192.0.2.20:1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("old session after account update = %d", response.Code)
	}
	login := performRequest(fixture.server, http.MethodPost, "/api/v1/auth/login", `{"username":"new-owner","password":"new-correct-horse-battery"}`, "", "192.0.2.30:1")
	if login.Code != http.StatusOK {
		t.Fatalf("new credentials login = %d %s", login.Code, login.Body.String())
	}
}

func TestConfigurationSecretsRequireSessionAndReturnPlaintextOnlyOnExplicitRequest(t *testing.T) {
	fixture := newDatabaseWebFixture(t, &testRunner{})
	if response := performRequest(fixture.server, http.MethodGet, "/api/v1/config/secrets", "", "", "192.0.2.20:1"); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated secrets response = %d %s", response.Code, response.Body.String())
	}
	cookie := setupDatabaseFixture(t, fixture.server, "owner", "correct-horse-battery")
	create := performRequest(fixture.server, http.MethodPost, "/api/v1/targets", `{"name":"main","api_base_url":"https://api.example.test/v1","api_key":"target-secret","model":"gpt-test","wire_api":"responses","config_overrides":[]}`, cookie, "192.0.2.20:1")
	if create.Code != http.StatusCreated {
		t.Fatalf("create target = %d %s", create.Code, create.Body.String())
	}
	openILink := performRequest(fixture.server, http.MethodPut, "/api/v1/config/openilink", `{"enabled":false,"base_url":"https://hub.example","allowed_user_ids":[],"http_timeout_seconds":15,"token":"openilink-secret","clear_token":false}`, cookie, "192.0.2.20:1")
	if openILink.Code != http.StatusOK {
		t.Fatalf("save OpenILink = %d %s", openILink.Code, openILink.Body.String())
	}
	telegram := performRequest(fixture.server, http.MethodPut, "/api/v1/config/telegram", `{"enabled":false,"base_url":"https://api.telegram.org","allowed_user_ids":[],"http_timeout_seconds":45,"poll_timeout_seconds":30,"token":"telegram-secret","clear_token":false}`, cookie, "192.0.2.20:1")
	if telegram.Code != http.StatusOK {
		t.Fatalf("save Telegram = %d %s", telegram.Code, telegram.Body.String())
	}
	masked := performRequest(fixture.server, http.MethodGet, "/api/v1/config", "", cookie, "192.0.2.20:1")
	if strings.Contains(masked.Body.String(), "target-secret") || strings.Contains(masked.Body.String(), "openilink-secret") || strings.Contains(masked.Body.String(), "telegram-secret") {
		t.Fatalf("normal configuration response leaked a secret: %s", masked.Body.String())
	}
	revealed := performRequest(fixture.server, http.MethodGet, "/api/v1/config/secrets", "", cookie, "192.0.2.20:1")
	if revealed.Code != http.StatusOK {
		t.Fatalf("secrets response = %d %s", revealed.Code, revealed.Body.String())
	}
	var secrets configurationSecretsResponse
	if err := json.Unmarshal(revealed.Body.Bytes(), &secrets); err != nil {
		t.Fatal(err)
	}
	if secrets.OpenILinkToken != "openilink-secret" || secrets.TelegramToken != "telegram-secret" || len(secrets.Targets) != 1 || secrets.Targets[0].APIKey != "target-secret" {
		t.Fatalf("revealed secrets = %+v", secrets)
	}
}

func TestOpenILinkTokenMaskKeepAndClearRules(t *testing.T) {
	fixture := newDatabaseWebFixture(t, &testRunner{})
	cookie := setupDatabaseFixture(t, fixture.server, "owner", "correct-horse-battery")
	base := `{"enabled":false,"base_url":"https://hub.example","allowed_user_ids":[],"http_timeout_seconds":15,`

	setToken := performRequest(fixture.server, http.MethodPut, "/api/v1/config/openilink", base+`"token":"openilink-secret","clear_token":false}`, cookie, "192.0.2.20:1")
	if setToken.Code != http.StatusOK || strings.Contains(setToken.Body.String(), "openilink-secret") || !strings.Contains(setToken.Body.String(), `"token_set":true`) {
		t.Fatalf("set token = %d %s", setToken.Code, setToken.Body.String())
	}
	keepToken := performRequest(fixture.server, http.MethodPut, "/api/v1/config/openilink", base+`"token":"","clear_token":false}`, cookie, "192.0.2.20:1")
	if keepToken.Code != http.StatusOK || !strings.Contains(keepToken.Body.String(), `"token_set":true`) {
		t.Fatalf("keep token = %d %s", keepToken.Code, keepToken.Body.String())
	}
	persisted, err := fixture.store.Load(context.Background())
	if err != nil || persisted.Config.OpenILink.Token != "openilink-secret" {
		t.Fatalf("persisted token after blank update = %q, err=%v", persisted.Config.OpenILink.Token, err)
	}
	invalidClear := performRequest(fixture.server, http.MethodPut, "/api/v1/config/openilink", `{"enabled":true,"base_url":"https://hub.example","allowed_user_ids":[],"http_timeout_seconds":15,"token":"","clear_token":true}`, cookie, "192.0.2.20:1")
	if invalidClear.Code != http.StatusBadRequest {
		t.Fatalf("enabled token clear = %d %s", invalidClear.Code, invalidClear.Body.String())
	}
	clearToken := performRequest(fixture.server, http.MethodPut, "/api/v1/config/openilink", base+`"token":"","clear_token":true}`, cookie, "192.0.2.20:1")
	if clearToken.Code != http.StatusOK || !strings.Contains(clearToken.Body.String(), `"token_set":false`) {
		t.Fatalf("clear token = %d %s", clearToken.Code, clearToken.Body.String())
	}
}

func TestTelegramTokenMaskKeepAndClearRules(t *testing.T) {
	fixture := newDatabaseWebFixture(t, &testRunner{})
	cookie := setupDatabaseFixture(t, fixture.server, "owner", "correct-horse-battery")
	base := `{"enabled":false,"base_url":"https://api.telegram.org","allowed_user_ids":["123"],"http_timeout_seconds":45,"poll_timeout_seconds":30,`

	setToken := performRequest(fixture.server, http.MethodPut, "/api/v1/config/telegram", base+`"token":"telegram-secret","clear_token":false}`, cookie, "192.0.2.20:1")
	if setToken.Code != http.StatusOK || strings.Contains(setToken.Body.String(), "telegram-secret") || !strings.Contains(setToken.Body.String(), `"token_set":true`) {
		t.Fatalf("set Telegram token = %d %s", setToken.Code, setToken.Body.String())
	}
	keepToken := performRequest(fixture.server, http.MethodPut, "/api/v1/config/telegram", base+`"token":"","clear_token":false}`, cookie, "192.0.2.20:1")
	if keepToken.Code != http.StatusOK || !strings.Contains(keepToken.Body.String(), `"token_set":true`) {
		t.Fatalf("keep Telegram token = %d %s", keepToken.Code, keepToken.Body.String())
	}
	persisted, err := fixture.store.Load(context.Background())
	if err != nil || persisted.Config.Telegram.Token != "telegram-secret" {
		t.Fatalf("persisted Telegram token = %q, err=%v", persisted.Config.Telegram.Token, err)
	}
	invalidClear := performRequest(fixture.server, http.MethodPut, "/api/v1/config/telegram", `{"enabled":true,"base_url":"https://api.telegram.org","allowed_user_ids":[],"http_timeout_seconds":45,"poll_timeout_seconds":30,"token":"","clear_token":true}`, cookie, "192.0.2.20:1")
	if invalidClear.Code != http.StatusBadRequest {
		t.Fatalf("enabled Telegram token clear = %d %s", invalidClear.Code, invalidClear.Body.String())
	}
	clearToken := performRequest(fixture.server, http.MethodPut, "/api/v1/config/telegram", base+`"token":"","clear_token":true}`, cookie, "192.0.2.20:1")
	if clearToken.Code != http.StatusOK || !strings.Contains(clearToken.Body.String(), `"token_set":false`) {
		t.Fatalf("clear Telegram token = %d %s", clearToken.Code, clearToken.Body.String())
	}
}

func TestHotCodexConfigDoesNotRequireRestartAndStartupRevisionRemainsCompatible(t *testing.T) {
	fixture := newDatabaseWebFixture(t, &testRunner{})
	cookie := setupDatabaseFixture(t, fixture.server, "owner", "correct-horse-battery")
	codexBody := `{"binary":"codex-next","prompts_file":"prompts.txt","prompts":["health check"],"request_timeout_seconds":180,"retry_min_seconds":3,"retry_max_seconds":8,"keepalive_min_seconds":2700,"keepalive_max_seconds":3300,"max_parallel":2,"success_message":"ok","reasoning_effort":"low","config_overrides":[]}`
	updated := performRequest(fixture.server, http.MethodPut, "/api/v1/config/codex", codexBody, cookie, "192.0.2.20:1")
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), `"restart_required":false`) {
		t.Fatalf("pre-restart config = %d %s", updated.Code, updated.Body.String())
	}

	snapshot, err := fixture.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.MarkStartupLoaded(context.Background(), snapshot.Revision); err != nil {
		t.Fatal(err)
	}
	snapshot, err = fixture.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := jobs.New(ctx, snapshot.Config.Codex.Targets, &testRunner{}, nil, nil, snapshot.Config.RetryMin(), snapshot.Config.RetryMax(), snapshot.Config.KeepaliveMin(), snapshot.Config.KeepaliveMax(), snapshot.Config.Codex.MaxParallel, snapshot.Config.Codex.SuccessMessage, snapshot.Config.Web.ActivityLimit)
	restarted, err := New(Options{
		Manager: manager, OpenILinkStatus: hub.NewStatusStore(hub.StatusDisabled),
		CookieSecure: snapshot.Config.Web.CookieSecure, Version: "test-version", Shutdown: make(chan struct{}),
		ConfigStore: fixture.store, InitialConfig: snapshot,
	})
	if err != nil {
		t.Fatal(err)
	}
	login := performRequest(restarted, http.MethodPost, "/api/v1/auth/login", `{"username":"owner","password":"correct-horse-battery"}`, "", "192.0.2.30:1")
	if login.Code != http.StatusOK {
		t.Fatalf("restart login = %d %s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	configResponse := performRequest(restarted, http.MethodGet, "/api/v1/config", "", cookies[0].Name+"="+cookies[0].Value, "192.0.2.30:1")
	if configResponse.Code != http.StatusOK || !strings.Contains(configResponse.Body.String(), `"restart_required":false`) || !strings.Contains(configResponse.Body.String(), `"loaded_startup_revision":`+fmt.Sprint(snapshot.Revision)) {
		t.Fatalf("post-restart config = %d %s", configResponse.Code, configResponse.Body.String())
	}
}

func TestCodexConfigurationRejectsEmptyPromptList(t *testing.T) {
	fixture := newDatabaseWebFixture(t, &testRunner{})
	cookie := setupDatabaseFixture(t, fixture.server, "owner", "correct-horse-battery")
	body := `{"binary":"codex","prompts_file":"prompts.txt","prompts":[],"request_timeout_seconds":180,"retry_min_seconds":3,"retry_max_seconds":8,"keepalive_min_seconds":2700,"keepalive_max_seconds":3300,"max_parallel":2,"success_message":"ok","reasoning_effort":"low","config_overrides":[]}`
	response := performRequest(fixture.server, http.MethodPut, "/api/v1/config/codex", body, cookie, "192.0.2.20:1")
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "at least one prompt") {
		t.Fatalf("empty prompt response = %d %s", response.Code, response.Body.String())
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
