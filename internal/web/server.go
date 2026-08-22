package web

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"codex-queue-bot/internal/codex"
	appconfig "codex-queue-bot/internal/config"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/storage"

	"github.com/gin-gonic/gin"
)

const (
	sessionCookieName = "codex_queue_session"
	defaultSessionTTL = 12 * time.Hour
	maxJSONBody       = 64 << 10
)

type Options struct {
	Manager           *jobs.Manager
	OpenILinkStatus   *hub.StatusStore
	TelegramStatus    *hub.StatusStore
	Username          string
	Password          string
	CookieSecure      bool
	TrustedProxies    []string
	Version           string
	Logger            *slog.Logger
	Shutdown          <-chan struct{}
	SessionTTL        time.Duration
	HeartbeatInterval time.Duration
	ObserverBuffer    int
	Now               func() time.Time
	Assets            fs.FS
	ConfigStore       *storage.Store
	InitialConfig     storage.Snapshot
	Runner            *codex.Runner
	// ReloadMessages is called after a persisted OpenILink/Telegram update.
	// Implementations should synchronously swap clients (or return an error)
	// so the new settings take effect without a process restart.
	ReloadMessages func(context.Context, storage.Snapshot) error
}

type Server struct {
	manager        *jobs.Manager
	status         *hub.StatusStore
	telegramStatus *hub.StatusStore
	username       string
	passwordHash   [32]byte
	cookieSecure   bool
	version        string
	logger         *slog.Logger
	shutdown       <-chan struct{}
	sessionTTL     time.Duration
	heartbeat      time.Duration
	observerBuffer int
	now            func() time.Time
	assets         fs.FS
	engine         *gin.Engine
	configStore    *storage.Store
	runner         *codex.Runner
	reloadMessages func(context.Context, storage.Snapshot) error
	startupConfig  appconfig.Config

	mu              sync.Mutex
	sessions        map[string]session
	sessionsChanged chan struct{}

	configMu       sync.RWMutex
	configSnapshot storage.Snapshot
	configChanged  chan struct{}
	configWriteMu  sync.Mutex
}

type session struct {
	Username  string
	ExpiresAt time.Time
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type actionRequest struct {
	Action  string   `json:"action"`
	Targets []string `json:"targets"`
}

type sessionResponse struct {
	Authenticated bool      `json:"authenticated"`
	Username      string    `json:"username"`
	ExpiresAt     time.Time `json:"expires_at"`
}

type actionResponse struct {
	Changed   []string `json:"changed"`
	Unchanged []string `json:"unchanged"`
	Unknown   []string `json:"unknown"`
}

func New(options Options) (*Server, error) {
	if options.Manager == nil {
		return nil, errors.New("web manager is required")
	}
	if options.OpenILinkStatus == nil {
		options.OpenILinkStatus = hub.NewStatusStore(hub.StatusDisabled)
	}
	if options.TelegramStatus == nil {
		options.TelegramStatus = hub.NewStatusStore(hub.StatusDisabled)
	}
	options.Username = strings.TrimSpace(options.Username)
	if options.ConfigStore == nil {
		if options.Username == "" {
			return nil, errors.New("web administrator username is required")
		}
		if len([]rune(options.Password)) < appconfig.MinimumAdminPasswordLength {
			return nil, fmt.Errorf("web administrator password must be at least %d characters", appconfig.MinimumAdminPasswordLength)
		}
	} else if options.InitialConfig.Revision == 0 {
		loaded, err := options.ConfigStore.Load(context.Background())
		if err != nil {
			return nil, fmt.Errorf("load Web configuration: %w", err)
		}
		options.InitialConfig = loaded
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.SessionTTL <= 0 {
		options.SessionTTL = defaultSessionTTL
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = 15 * time.Second
	}
	if options.ObserverBuffer <= 0 {
		options.ObserverBuffer = 32
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Assets == nil {
		options.Assets = uiFileSystem()
	}
	if options.Shutdown == nil {
		never := make(chan struct{})
		options.Shutdown = never
	}

	server := &Server{
		manager:         options.Manager,
		status:          options.OpenILinkStatus,
		telegramStatus:  options.TelegramStatus,
		username:        options.Username,
		passwordHash:    sha256.Sum256([]byte(options.Password)),
		cookieSecure:    options.CookieSecure,
		version:         options.Version,
		logger:          options.Logger,
		shutdown:        options.Shutdown,
		sessionTTL:      options.SessionTTL,
		heartbeat:       options.HeartbeatInterval,
		observerBuffer:  options.ObserverBuffer,
		now:             options.Now,
		assets:          options.Assets,
		configStore:     options.ConfigStore,
		runner:          options.Runner,
		reloadMessages:  options.ReloadMessages,
		sessions:        make(map[string]session),
		sessionsChanged: make(chan struct{}),
		configChanged:   make(chan struct{}),
	}
	if options.ConfigStore != nil {
		server.configSnapshot = options.InitialConfig
		server.startupConfig = options.InitialConfig.Config
	}
	if server.version == "" {
		server.version = "dev"
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery(), server.securityHeaders())
	if err := engine.SetTrustedProxies(cleanStrings(options.TrustedProxies)); err != nil {
		return nil, fmt.Errorf("configure trusted proxies: %w", err)
	}
	server.engine = engine
	server.routes()
	return server, nil
}

func (s *Server) Handler() http.Handler { return s.engine }

func (s *Server) routes() {
	s.engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	api := s.engine.Group("/api/v1")
	api.Use(func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Next()
	})
	api.GET("/setup/status", s.setupStatus)
	api.POST("/setup", s.setup)
	api.POST("/auth/login", s.login)

	authorized := api.Group("")
	authorized.Use(s.requireSession())
	authorized.GET("/auth/session", s.getSession)
	authorized.POST("/auth/logout", s.logout)
	authorized.GET("/dashboard", s.dashboard)
	authorized.POST("/actions", s.actions)
	authorized.GET("/events", s.events)
	authorized.GET("/config", s.getConfig)
	authorized.PUT("/config/codex", s.updateCodexConfig)
	authorized.PUT("/config/openilink", s.updateOpenILinkConfig)
	authorized.PUT("/config/telegram", s.updateTelegramConfig)
	authorized.PUT("/config/web", s.updateWebConfig)
	authorized.PUT("/account", s.updateAccount)
	authorized.POST("/targets", s.createTarget)
	authorized.PUT("/targets/:id", s.updateTarget)
	authorized.DELETE("/targets/:id", s.deleteTarget)

	s.engine.NoRoute(s.serveUI)
}

func (s *Server) securityHeaders() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("X-Frame-Options", "DENY")
		c.Header("Referrer-Policy", "no-referrer")
		c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; frame-ancestors 'none'")
		c.Next()
	}
}

func (s *Server) login(c *gin.Context) {
	var request loginRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "用户名或密码错误"})
		return
	}
	authenticatedUsername := s.username
	authenticated := false
	if s.configStore != nil {
		var err error
		authenticatedUsername, authenticated, err = s.configStore.VerifyAdmin(c.Request.Context(), request.Username, request.Password)
		if err != nil {
			s.logger.Error("failed to verify administrator credentials", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "暂时无法登录"})
			return
		}
	} else {
		usernameHash := sha256.Sum256([]byte(request.Username))
		expectedUsernameHash := sha256.Sum256([]byte(s.username))
		passwordHash := sha256.Sum256([]byte(request.Password))
		usernameOK := subtle.ConstantTimeCompare(usernameHash[:], expectedUsernameHash[:]) == 1
		passwordOK := subtle.ConstantTimeCompare(passwordHash[:], s.passwordHash[:]) == 1
		authenticated = usernameOK && passwordOK
	}
	if !authenticated {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "用户名或密码错误"})
		return
	}

	// Rotate any existing session cookie on every successful login to avoid
	// session fixation when a browser reuses a stale cookie.
	if previous, err := c.Cookie(sessionCookieName); err == nil && previous != "" {
		s.mu.Lock()
		delete(s.sessions, previous)
		s.signalSessionsChangedLocked()
		s.mu.Unlock()
	}
	current, err := s.establishSession(c, authenticatedUsername)
	if err != nil {
		s.logger.Error("failed to create web session", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "暂时无法登录"})
		return
	}
	c.JSON(http.StatusOK, makeSessionResponse(current))
}

func (s *Server) getSession(c *gin.Context) {
	current, _ := c.Get("session")
	c.JSON(http.StatusOK, makeSessionResponse(current.(session)))
}

func (s *Server) logout(c *gin.Context) {
	sessionID, _ := c.Get("session_id")
	s.mu.Lock()
	delete(s.sessions, sessionID.(string))
	s.signalSessionsChangedLocked()
	s.mu.Unlock()
	s.setSessionCookie(c, "", -1)
	c.JSON(http.StatusOK, gin.H{"logged_out": true})
}

func (s *Server) dashboard(c *gin.Context) {
	snapshot := s.manager.ComprehensiveSnapshot()
	c.JSON(http.StatusOK, s.dashboardPayload(snapshot, s.status.Snapshot(), s.telegramStatus.Snapshot()))
}

func (s *Server) actions(c *gin.Context) {
	var request actionRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	request.Action = strings.TrimSpace(request.Action)
	if len(request.Targets) > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "目标数量过多"})
		return
	}
	for i := range request.Targets {
		request.Targets[i] = strings.TrimSpace(request.Targets[i])
		if request.Targets[i] == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "目标名不能为空"})
			return
		}
	}
	response := actionResponse{}
	switch request.Action {
	case "queue.start":
		result := s.manager.Start(request.Targets, jobs.Subscriber{})
		response = actionResponse{Changed: result.Started, Unchanged: result.Already, Unknown: result.Unknown}
	case "keepalive.start":
		result := s.manager.StartKeepalive(request.Targets)
		response = actionResponse{Changed: result.Started, Unchanged: result.Already, Unknown: result.Unknown}
	case "task.stop", "queue.stop", "keepalive.stop":
		result := s.manager.StopTask(request.Targets)
		response = actionResponse{Changed: result.Stopped, Unchanged: result.Inactive, Unknown: result.Unknown}
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "不支持的操作"})
		return
	}
	response.Changed = nonNilStrings(response.Changed)
	response.Unchanged = nonNilStrings(response.Unchanged)
	response.Unknown = nonNilStrings(response.Unknown)
	c.JSON(http.StatusOK, response)
}

func (s *Server) requireSession() gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionID, err := c.Cookie(sessionCookieName)
		if err != nil || sessionID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录或会话已过期"})
			return
		}
		now := s.now()
		s.mu.Lock()
		current, ok := s.sessions[sessionID]
		if ok && !current.ExpiresAt.After(now) {
			delete(s.sessions, sessionID)
			ok = false
		}
		s.mu.Unlock()
		if !ok {
			s.setSessionCookie(c, "", -1)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录或会话已过期"})
			return
		}
		c.Set("session_id", sessionID)
		c.Set("session", current)
		c.Next()
	}
}

func (s *Server) purgeExpiredSessionsLocked(now time.Time) {
	for id, current := range s.sessions {
		if !current.ExpiresAt.After(now) {
			delete(s.sessions, id)
		}
	}
}

func (s *Server) setSessionCookie(c *gin.Context, value string, maxAge int) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   s.cookieSecure,
		SameSite: http.SameSiteStrictMode,
	})
}

func (s *Server) establishSession(c *gin.Context, username string) (session, error) {
	sessionID, err := randomToken()
	if err != nil {
		return session{}, err
	}
	now := s.now()
	current := session{Username: username, ExpiresAt: now.Add(s.sessionTTL)}
	s.mu.Lock()
	s.sessions[sessionID] = current
	s.purgeExpiredSessionsLocked(now)
	s.signalSessionsChangedLocked()
	s.mu.Unlock()
	s.setSessionCookie(c, sessionID, int(s.sessionTTL.Seconds()))
	return current, nil
}

func (s *Server) revokeSessions() {
	s.mu.Lock()
	s.sessions = make(map[string]session)
	s.signalSessionsChangedLocked()
	s.mu.Unlock()
}

func (s *Server) signalSessionsChangedLocked() {
	close(s.sessionsChanged)
	s.sessionsChanged = make(chan struct{})
}

func (s *Server) sessionChanges() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionsChanged
}

func (s *Server) sessionExists(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.sessions[id]
	return ok && current.ExpiresAt.After(s.now())
}

func makeSessionResponse(current session) sessionResponse {
	return sessionResponse{
		Authenticated: true,
		Username:      current.Username,
		ExpiresAt:     current.ExpiresAt,
	}
}

func randomToken() (string, error) {
	buffer := make([]byte, 32)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func decodeJSON(c *gin.Context, destination any) error {
	if c.Request.Body == nil {
		return errors.New("request body is required")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxJSONBody)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func cleanStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func (s *Server) serveUI(c *gin.Context) {
	if strings.HasPrefix(c.Request.URL.Path, "/api/") {
		c.JSON(http.StatusNotFound, gin.H{"error": "接口不存在"})
		return
	}
	requested := strings.TrimPrefix(path.Clean(c.Request.URL.Path), "/")
	if requested == "." || requested == "" {
		requested = "index.html"
	}
	data, err := fs.ReadFile(s.assets, requested)
	if err != nil {
		// Client-side routes (for example a future /targets/:name view) should
		// still load the application shell, while missing asset paths remain a
		// normal 404 below.
		if path.Ext(requested) != "" {
			c.String(http.StatusNotFound, "UI asset not found")
			return
		}
		requested = "index.html"
		data, err = fs.ReadFile(s.assets, requested)
	}
	if err != nil {
		c.String(http.StatusNotFound, "UI not found")
		return
	}
	contentType := mime.TypeByExtension(path.Ext(requested))
	if contentType == "" {
		contentType = http.DetectContentType(data)
	}
	c.Header("Content-Type", contentType)
	if requested == "index.html" {
		c.Header("Cache-Control", "no-cache")
	} else {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
	}
	c.Data(http.StatusOK, contentType, data)
}
