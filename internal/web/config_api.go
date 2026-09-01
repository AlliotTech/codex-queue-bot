package web

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/storage"

	"github.com/gin-gonic/gin"
)

type setupStatusResponse struct {
	Required          bool   `json:"required"`
	SuggestedUsername string `json:"suggested_username"`
}

type setupRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type codexConfigRequest struct {
	Binary               string   `json:"binary"`
	PromptsFile          string   `json:"prompts_file"`
	Prompts              []string `json:"prompts"`
	RequestTimeoutSecond int      `json:"request_timeout_seconds"`
	RetryMinSecond       int      `json:"retry_min_seconds"`
	RetryMaxSecond       int      `json:"retry_max_seconds"`
	KeepaliveMinSecond   int      `json:"keepalive_min_seconds"`
	KeepaliveMaxSecond   int      `json:"keepalive_max_seconds"`
	MaxParallel          int      `json:"max_parallel"`
	SuccessMessage       string   `json:"success_message"`
	ReasoningEffort      string   `json:"reasoning_effort"`
	ConfigOverrides      []string `json:"config_overrides"`
}

type openILinkConfigRequest struct {
	Enabled           bool     `json:"enabled"`
	BaseURL           string   `json:"base_url"`
	Token             string   `json:"token"`
	ClearToken        bool     `json:"clear_token"`
	AllowedUserIDs    []string `json:"allowed_user_ids"`
	HTTPTimeoutSecond int      `json:"http_timeout_seconds"`
}

type telegramConfigRequest struct {
	Enabled           bool     `json:"enabled"`
	BaseURL           string   `json:"base_url"`
	Token             string   `json:"token"`
	ClearToken        bool     `json:"clear_token"`
	AllowedUserIDs    []string `json:"allowed_user_ids"`
	HTTPTimeoutSecond int      `json:"http_timeout_seconds"`
	PollTimeoutSecond int      `json:"poll_timeout_seconds"`
}

type webConfigRequest struct {
	ListenAddress  string   `json:"listen_address"`
	CookieSecure   bool     `json:"cookie_secure"`
	TrustedProxies []string `json:"trusted_proxies"`
	ActivityLimit  int      `json:"activity_limit"`
}

type accountUpdateRequest struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	NewPassword     string `json:"new_password"`
	CurrentPassword string `json:"current_password"`
}

type targetConfigRequest struct {
	SortOrder       *int     `json:"sort_order"`
	Name            string   `json:"name"`
	APIBaseURL      string   `json:"api_base_url"`
	APIKey          string   `json:"api_key"`
	Model           string   `json:"model"`
	WireAPI         string   `json:"wire_api"`
	ConfigOverrides []string `json:"config_overrides"`
}

type configurationResponse struct {
	Revision              int64                      `json:"revision"`
	LoadedStartupRevision int64                      `json:"loaded_startup_revision"`
	RestartRequired       bool                       `json:"restart_required"`
	RestartFields         []string                   `json:"restart_fields"`
	Codex                 codexConfigurationResponse `json:"codex"`
	OpenILink             openILinkConfiguration     `json:"openilink"`
	Telegram              telegramConfiguration      `json:"telegram"`
	Web                   webConfigurationResponse   `json:"web"`
	Account               accountConfiguration       `json:"account"`
	Targets               []targetConfiguration      `json:"targets"`
}

type codexConfigurationResponse struct {
	Binary               string   `json:"binary"`
	PromptsFile          string   `json:"prompts_file"`
	Prompts              []string `json:"prompts"`
	RequestTimeoutSecond int      `json:"request_timeout_seconds"`
	RetryMinSecond       int      `json:"retry_min_seconds"`
	RetryMaxSecond       int      `json:"retry_max_seconds"`
	KeepaliveMinSecond   int      `json:"keepalive_min_seconds"`
	KeepaliveMaxSecond   int      `json:"keepalive_max_seconds"`
	MaxParallel          int      `json:"max_parallel"`
	SuccessMessage       string   `json:"success_message"`
	ReasoningEffort      string   `json:"reasoning_effort"`
	ConfigOverrides      []string `json:"config_overrides"`
}

type openILinkConfiguration struct {
	Enabled           bool     `json:"enabled"`
	BaseURL           string   `json:"base_url"`
	TokenSet          bool     `json:"token_set"`
	AllowedUserIDs    []string `json:"allowed_user_ids"`
	HTTPTimeoutSecond int      `json:"http_timeout_seconds"`
}

type telegramConfiguration struct {
	Enabled           bool     `json:"enabled"`
	BaseURL           string   `json:"base_url"`
	TokenSet          bool     `json:"token_set"`
	AllowedUserIDs    []string `json:"allowed_user_ids"`
	HTTPTimeoutSecond int      `json:"http_timeout_seconds"`
	PollTimeoutSecond int      `json:"poll_timeout_seconds"`
}

type webConfigurationResponse struct {
	ListenAddress  string   `json:"listen_address"`
	CookieSecure   bool     `json:"cookie_secure"`
	TrustedProxies []string `json:"trusted_proxies"`
	ActivityLimit  int      `json:"activity_limit"`
}

type accountConfiguration struct {
	Username string `json:"username"`
}

type targetConfiguration struct {
	ID              int64    `json:"id"`
	SortOrder       int      `json:"sort_order"`
	Name            string   `json:"name"`
	APIBaseURL      string   `json:"api_base_url"`
	APIKeySet       bool     `json:"api_key_set"`
	Model           string   `json:"model"`
	WireAPI         string   `json:"wire_api"`
	ConfigOverrides []string `json:"config_overrides"`
	Busy            bool     `json:"busy"`
}

// configurationSecretsResponse is intentionally served by a separate,
// authenticated endpoint. The normal configuration payload only exposes
// whether a secret is set, so routine polling and React state updates cannot
// accidentally copy credentials into the browser.
type configurationSecretsResponse struct {
	OpenILinkToken string                 `json:"openilink_token"`
	TelegramToken  string                 `json:"telegram_token"`
	Targets        []targetSecretResponse `json:"targets"`
}

type targetSecretResponse struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	APIKey string `json:"api_key"`
}

func (s *Server) setupStatus(c *gin.Context) {
	if s.configStore == nil {
		c.JSON(http.StatusOK, setupStatusResponse{Required: false, SuggestedUsername: s.username})
		return
	}
	snapshot := s.currentConfiguration()
	c.JSON(http.StatusOK, setupStatusResponse{Required: snapshot.SetupRequired, SuggestedUsername: snapshot.SuggestedUsername})
}

func (s *Server) setup(c *gin.Context) {
	if s.configStore == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "管理员已经初始化"})
		return
	}
	var request setupRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	request.Username = strings.TrimSpace(request.Username)
	if request.Username == "" || len([]rune(request.Password)) < config.MinimumAdminPasswordLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("用户名不能为空，密码至少需要 %d 个字符", config.MinimumAdminPasswordLength)})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	snapshot, err := s.configStore.SetupAdmin(c.Request.Context(), request.Username, request.Password)
	if errors.Is(err, storage.ErrAlreadySetup) {
		c.JSON(http.StatusConflict, gin.H{"error": "管理员已经初始化"})
		return
	}
	if err != nil {
		s.logger.Error("failed to initialize administrator", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "初始化失败"})
		return
	}
	s.setConfiguration(snapshot)
	current, err := s.establishSession(c, request.Username)
	if err != nil {
		s.logger.Error("failed to establish setup session", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "管理员已创建，请重新登录"})
		return
	}
	c.JSON(http.StatusOK, makeSessionResponse(current))
}

func (s *Server) getConfig(c *gin.Context) {
	if s.configStore == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "配置接口不可用"})
		return
	}
	c.JSON(http.StatusOK, s.configurationPayload(s.currentConfiguration()))
}

// getConfigSecrets reveals decrypted credentials only after the caller has an
// authenticated administrator session and explicitly requests them. The
// endpoint is separate from /config so normal configuration loads remain
// secret-free and are safe to cache in client state.
func (s *Server) getConfigSecrets(c *gin.Context) {
	if s.configStore == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "配置接口不可用"})
		return
	}
	c.JSON(http.StatusOK, s.configurationSecretsPayload(s.currentConfiguration()))
}

func (s *Server) updateCodexConfig(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	var request codexConfigRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	value := config.CodexConfig{
		Binary: request.Binary, PromptsFile: request.PromptsFile, Prompts: cleanStrings(request.Prompts), PromptsPersisted: true,
		RequestTimeoutSecond: request.RequestTimeoutSecond,
		RetryMinSecond:       request.RetryMinSecond, RetryMaxSecond: request.RetryMaxSecond,
		KeepaliveMinSecond: request.KeepaliveMinSecond, KeepaliveMaxSecond: request.KeepaliveMaxSecond,
		MaxParallel: request.MaxParallel, SuccessMessage: request.SuccessMessage,
		ReasoningEffort: request.ReasoningEffort, ConfigOverrides: cleanStrings(request.ConfigOverrides),
	}
	if err := config.ValidateCodex(value); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	snapshot, err := s.configStore.UpdateCodex(c.Request.Context(), value)
	if err != nil {
		s.writeConfigurationError(c, err)
		return
	}
	s.applyRuntimeConfiguration(snapshot)
	s.setConfiguration(snapshot)
	c.JSON(http.StatusOK, s.configurationPayload(snapshot))
}

func (s *Server) updateOpenILinkConfig(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	var request openILinkConfigRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	if request.ClearToken && request.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"error": "只能在 OpenILink 关闭时清除 Token"})
		return
	}
	if request.ClearToken && strings.TrimSpace(request.Token) != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "不能同时设置并清除 Token"})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	value := config.OpenILinkConfig{
		Enabled: request.Enabled, BaseURL: request.BaseURL,
		AllowedUserIDs: cleanStrings(request.AllowedUserIDs), HTTPTimeoutSecond: request.HTTPTimeoutSecond,
	}
	current := s.currentConfiguration()
	effectiveToken := current.Config.OpenILink.Token
	var token *string
	if trimmed := strings.TrimSpace(request.Token); trimmed != "" {
		token = &trimmed
		effectiveToken = trimmed
	} else if request.ClearToken {
		empty := ""
		token = &empty
		effectiveToken = ""
	}
	value.Token = effectiveToken
	value.SetEnabledExplicit(request.Enabled)
	if err := config.ValidateOpenILink(value); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	snapshot, err := s.configStore.UpdateOpenILink(c.Request.Context(), value, token)
	if err != nil {
		s.writeConfigurationError(c, err)
		return
	}
	if s.reloadMessages != nil {
		if reloadErr := s.reloadMessages(c.Request.Context(), snapshot); reloadErr != nil {
			s.logger.Error("failed to reload OpenILink client", "error", reloadErr)
		}
	}
	s.setConfiguration(snapshot)
	c.JSON(http.StatusOK, s.configurationPayload(snapshot))
}

func (s *Server) updateTelegramConfig(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	var request telegramConfigRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	if request.ClearToken && request.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"error": "只能在 Telegram 关闭时清除 Token"})
		return
	}
	if request.ClearToken && strings.TrimSpace(request.Token) != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "不能同时设置并清除 Token"})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	value := config.TelegramConfig{
		Enabled: request.Enabled, BaseURL: request.BaseURL,
		AllowedUserIDs: cleanStrings(request.AllowedUserIDs), HTTPTimeoutSecond: request.HTTPTimeoutSecond,
		PollTimeoutSecond: request.PollTimeoutSecond,
	}
	current := s.currentConfiguration()
	effectiveToken := current.Config.Telegram.Token
	var token *string
	if trimmed := strings.TrimSpace(request.Token); trimmed != "" {
		token = &trimmed
		effectiveToken = trimmed
	} else if request.ClearToken {
		empty := ""
		token = &empty
		effectiveToken = ""
	}
	value.Token = effectiveToken
	value.SetEnabledExplicit(request.Enabled)
	if err := config.ValidateTelegram(value); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	snapshot, err := s.configStore.UpdateTelegram(c.Request.Context(), value, token)
	if err != nil {
		s.writeConfigurationError(c, err)
		return
	}
	if s.reloadMessages != nil {
		if reloadErr := s.reloadMessages(c.Request.Context(), snapshot); reloadErr != nil {
			s.logger.Error("failed to reload Telegram client", "error", reloadErr)
		}
	}
	s.setConfiguration(snapshot)
	c.JSON(http.StatusOK, s.configurationPayload(snapshot))
}

func (s *Server) updateWebConfig(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	var request webConfigRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	value := config.WebConfig{
		ListenAddress: request.ListenAddress, CookieSecure: request.CookieSecure,
		TrustedProxies: cleanStrings(request.TrustedProxies), ActivityLimit: request.ActivityLimit,
	}
	if err := config.ValidateWeb(value); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	snapshot, err := s.configStore.UpdateWeb(c.Request.Context(), value)
	if err != nil {
		s.writeConfigurationError(c, err)
		return
	}
	s.applyRuntimeConfiguration(snapshot)
	s.setConfiguration(snapshot)
	c.JSON(http.StatusOK, s.configurationPayload(snapshot))
}

func (s *Server) updateAccount(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	var request accountUpdateRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	current := s.currentConfiguration()
	username := strings.TrimSpace(request.Username)
	if username == "" {
		username = current.Config.Web.AdminUsername
	}
	newPassword := request.NewPassword
	if newPassword == "" {
		newPassword = request.Password
	}
	if request.NewPassword != "" && request.Password != "" && request.NewPassword != request.Password {
		c.JSON(http.StatusBadRequest, gin.H{"error": "password 与 new_password 不一致"})
		return
	}
	if newPassword != "" && len([]rune(newPassword)) < config.MinimumAdminPasswordLength {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("管理员密码至少需要 %d 个字符", config.MinimumAdminPasswordLength)})
		return
	}
	if request.CurrentPassword != "" {
		sessionValue, _ := c.Get("session")
		_, ok, err := s.configStore.VerifyAdmin(c.Request.Context(), sessionValue.(session).Username, request.CurrentPassword)
		if err != nil {
			s.writeConfigurationError(c, err)
			return
		}
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "当前密码不正确"})
			return
		}
	}
	if username == current.Config.Web.AdminUsername && newPassword == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "没有需要保存的账号变更"})
		return
	}
	snapshot, err := s.configStore.UpdateAccount(c.Request.Context(), username, newPassword)
	if err != nil {
		s.writeConfigurationError(c, err)
		return
	}
	s.setConfiguration(snapshot)
	s.revokeSessions()
	s.setSessionCookie(c, "", -1)
	c.JSON(http.StatusOK, gin.H{"reauthentication_required": true, "revision": snapshot.Revision})
}

func (s *Server) createTarget(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	var request targetConfigRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	target := targetFromRequest(request)
	if strings.TrimSpace(target.APIKey) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "创建 target 时必须提供 API Key"})
		return
	}
	validationTarget := config.NormalizeTarget(target)
	if request.SortOrder == nil {
		validationTarget.SortOrder = 0
	}
	if err := config.ValidateTarget(validationTarget); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	var snapshot storage.Snapshot
	err := s.manager.CreateTarget(&target, func() error {
		var created config.Target
		var err error
		snapshot, created, err = s.configStore.CreateTarget(c.Request.Context(), target)
		if err == nil {
			target = created
		}
		return err
	})
	if err != nil {
		s.writeTargetError(c, err)
		return
	}
	s.setConfiguration(snapshot)
	c.JSON(http.StatusCreated, s.configurationPayload(snapshot))
}

func (s *Server) updateTarget(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	id, ok := parseTargetID(c)
	if !ok {
		return
	}
	var request targetConfigRequest
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式不正确"})
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	current, exists := s.manager.TargetByID(id)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "target 不存在"})
		return
	}
	target := targetFromRequest(request)
	target.ID = id
	if request.SortOrder == nil {
		target.SortOrder = current.SortOrder
	}
	target.APIKey = current.APIKey
	var apiKey *string
	if trimmed := strings.TrimSpace(request.APIKey); trimmed != "" {
		apiKey = &trimmed
		target.APIKey = trimmed
	}
	if err := config.ValidateTarget(config.NormalizeTarget(target)); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var snapshot storage.Snapshot
	err := s.manager.UpdateTarget(id, &target, func() error {
		var updated config.Target
		var err error
		snapshot, updated, err = s.configStore.UpdateTarget(c.Request.Context(), id, target, apiKey)
		if err == nil {
			target = updated
		}
		return err
	})
	if err != nil {
		s.writeTargetError(c, err)
		return
	}
	s.setConfiguration(snapshot)
	c.JSON(http.StatusOK, s.configurationPayload(snapshot))
}

func (s *Server) deleteTarget(c *gin.Context) {
	if !s.requireConfigStore(c) {
		return
	}
	id, ok := parseTargetID(c)
	if !ok {
		return
	}
	s.configWriteMu.Lock()
	defer s.configWriteMu.Unlock()
	var snapshot storage.Snapshot
	err := s.manager.DeleteTarget(id, func() error {
		var err error
		snapshot, err = s.configStore.DeleteTarget(c.Request.Context(), id)
		return err
	})
	if err != nil {
		s.writeTargetError(c, err)
		return
	}
	s.setConfiguration(snapshot)
	c.JSON(http.StatusOK, s.configurationPayload(snapshot))
}

func targetFromRequest(request targetConfigRequest) config.Target {
	sortOrder := -1
	if request.SortOrder != nil {
		sortOrder = *request.SortOrder
	}
	return config.Target{
		SortOrder: sortOrder, Name: request.Name, APIBaseURL: request.APIBaseURL,
		APIKey: request.APIKey, Model: request.Model, WireAPI: request.WireAPI,
		ConfigOverrides: cleanStrings(request.ConfigOverrides),
	}
}

func parseTargetID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "target ID 不正确"})
		return 0, false
	}
	return id, true
}

func (s *Server) requireConfigStore(c *gin.Context) bool {
	if s.configStore != nil {
		return true
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "配置接口不可用"})
	return false
}

func (s *Server) writeTargetError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, jobs.ErrTargetBusy):
		c.JSON(http.StatusConflict, gin.H{"error": "target 任务未能在 10 秒内停止，未保存修改"})
	case errors.Is(err, jobs.ErrTargetNotFound), errors.Is(err, storage.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "target 不存在"})
	case errors.Is(err, jobs.ErrTargetConflict), errors.Is(err, storage.ErrConflict):
		c.JSON(http.StatusBadRequest, gin.H{"error": "target 名称必须保持大小写不敏感唯一"})
	default:
		s.writeConfigurationError(c, err)
	}
}

func (s *Server) writeConfigurationError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "配置不存在"})
	case errors.Is(err, storage.ErrConflict):
		c.JSON(http.StatusBadRequest, gin.H{"error": "配置与现有数据冲突"})
	case strings.Contains(err.Error(), "required"),
		strings.Contains(err.Error(), "must be"),
		strings.Contains(err.Error(), "at least"),
		strings.Contains(err.Error(), "positive"):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		s.logger.Error("configuration update failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存配置失败"})
	}
}

func (s *Server) applyRuntimeConfiguration(snapshot storage.Snapshot) {
	cfg := snapshot.Config
	if s.runner != nil {
		s.runner.UpdatePaths(cfg.Codex.Binary, cfg.Codex.PromptsFile, cfg.Codex.Prompts, cfg.Codex.PromptsPersisted)
		s.runner.UpdateRuntime(cfg.RequestTimeout(), cfg.Codex.ReasoningEffort, cfg.Codex.ConfigOverrides)
	}
	s.manager.UpdateSettings(
		cfg.RetryMin(), cfg.RetryMax(), cfg.KeepaliveMin(), cfg.KeepaliveMax(),
		cfg.Codex.MaxParallel, cfg.Codex.SuccessMessage,
	)
}

func (s *Server) currentConfiguration() storage.Snapshot {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.configSnapshot
}

func (s *Server) setConfiguration(snapshot storage.Snapshot) {
	s.configMu.Lock()
	s.configSnapshot = snapshot
	close(s.configChanged)
	s.configChanged = make(chan struct{})
	s.configMu.Unlock()
}

func (s *Server) configurationChanges() <-chan struct{} {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.configChanged
}

func (s *Server) configurationPayload(snapshot storage.Snapshot) configurationResponse {
	fields := s.restartFields(snapshot.Config)
	result := configurationResponse{
		Revision: snapshot.Revision, LoadedStartupRevision: snapshot.LoadedStartupRevision,
		RestartRequired: len(fields) > 0, RestartFields: fields,
		Codex: codexConfigurationResponse{
			Binary: snapshot.Config.Codex.Binary, PromptsFile: snapshot.Config.Codex.PromptsFile, Prompts: nonNilStrings(snapshot.Config.Codex.Prompts),
			RequestTimeoutSecond: snapshot.Config.Codex.RequestTimeoutSecond,
			RetryMinSecond:       snapshot.Config.Codex.RetryMinSecond, RetryMaxSecond: snapshot.Config.Codex.RetryMaxSecond,
			KeepaliveMinSecond: snapshot.Config.Codex.KeepaliveMinSecond, KeepaliveMaxSecond: snapshot.Config.Codex.KeepaliveMaxSecond,
			MaxParallel: snapshot.Config.Codex.MaxParallel, SuccessMessage: snapshot.Config.Codex.SuccessMessage,
			ReasoningEffort: snapshot.Config.Codex.ReasoningEffort, ConfigOverrides: nonNilStrings(snapshot.Config.Codex.ConfigOverrides),
		},
		OpenILink: openILinkConfiguration{
			Enabled: snapshot.Config.OpenILink.Enabled, BaseURL: snapshot.Config.OpenILink.BaseURL,
			TokenSet: snapshot.Config.OpenILink.Token != "", AllowedUserIDs: nonNilStrings(snapshot.Config.OpenILink.AllowedUserIDs),
			HTTPTimeoutSecond: snapshot.Config.OpenILink.HTTPTimeoutSecond,
		},
		Telegram: telegramConfiguration{
			Enabled: snapshot.Config.Telegram.Enabled, BaseURL: snapshot.Config.Telegram.BaseURL,
			TokenSet: snapshot.Config.Telegram.Token != "", AllowedUserIDs: nonNilStrings(snapshot.Config.Telegram.AllowedUserIDs),
			HTTPTimeoutSecond: snapshot.Config.Telegram.HTTPTimeoutSecond, PollTimeoutSecond: snapshot.Config.Telegram.PollTimeoutSecond,
		},
		Web: webConfigurationResponse{
			ListenAddress: snapshot.Config.Web.ListenAddress, CookieSecure: snapshot.Config.Web.CookieSecure,
			TrustedProxies: nonNilStrings(snapshot.Config.Web.TrustedProxies), ActivityLimit: snapshot.Config.Web.ActivityLimit,
		},
		Account: accountConfiguration{Username: snapshot.Config.Web.AdminUsername},
		Targets: make([]targetConfiguration, 0, len(snapshot.Config.Codex.Targets)),
	}
	busy := map[int64]bool{}
	for _, target := range s.manager.ComprehensiveSnapshot().Targets {
		busy[target.ID] = target.Busy
	}
	for _, target := range snapshot.Config.Codex.Targets {
		result.Targets = append(result.Targets, targetConfiguration{
			ID: target.ID, SortOrder: target.SortOrder, Name: target.Name, APIBaseURL: target.APIBaseURL,
			APIKeySet: target.APIKey != "", Model: target.Model, WireAPI: target.WireAPI,
			ConfigOverrides: nonNilStrings(target.ConfigOverrides), Busy: busy[target.ID],
		})
	}
	return result
}

func (s *Server) configurationSecretsPayload(snapshot storage.Snapshot) configurationSecretsResponse {
	result := configurationSecretsResponse{
		OpenILinkToken: snapshot.Config.OpenILink.Token,
		TelegramToken:  snapshot.Config.Telegram.Token,
		Targets:        make([]targetSecretResponse, 0, len(snapshot.Config.Codex.Targets)),
	}
	for _, target := range snapshot.Config.Codex.Targets {
		result.Targets = append(result.Targets, targetSecretResponse{
			ID: target.ID, Name: target.Name, APIKey: target.APIKey,
		})
	}
	return result
}

func (s *Server) restartFields(current config.Config) []string {
	if s.configStore == nil {
		return []string{}
	}
	startup := s.startupConfig
	fields := make([]string, 0, 3)
	if current.Web.ListenAddress != startup.Web.ListenAddress {
		fields = append(fields, "web.listen_address")
	}
	if current.Web.CookieSecure != startup.Web.CookieSecure {
		fields = append(fields, "web.cookie_secure")
	}
	if !slices.Equal(current.Web.TrustedProxies, startup.Web.TrustedProxies) {
		fields = append(fields, "web.trusted_proxies")
	}
	return fields
}
