package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/proxyenv"
	"codex-queue-bot/internal/storage"
	"codex-queue-bot/internal/web"
)

var version = "dev"

func main() {
	configDefault := os.Getenv("CONFIG_FILE")
	if configDefault == "" {
		configDefault = "config.json"
	}
	configPath := flag.String("config", configDefault, "deprecated (configuration is stored in SQLite)")
	dbDefault := os.Getenv("CODEX_QUEUE_DB_PATH")
	if dbDefault == "" {
		dbDefault = "data/codex-queue-bot.db"
	}
	dbPath := flag.String("db", dbDefault, "path to SQLite configuration database")
	checkOnly := flag.Bool("check", false, "validate configuration, Codex executable, and prompts, then exit")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	_ = configPath // retained only so old launch scripts continue to parse.

	if *showVersion {
		fmt.Println(version)
		return
	}

	logger := newLogger(os.Getenv("LOG_LEVEL"))
	slog.SetDefault(logger)
	proxyConfig := proxyenv.Apply()
	proxyResolver, proxyErr := proxyenv.Resolve(os.Environ())
	if proxyErr != nil {
		logger.Error("invalid outbound proxy configuration", "error", proxyErr)
		os.Exit(2)
	}
	if proxyConfig.Enabled() {
		logger.Info(
			"outbound proxy enabled",
			"http", proxyConfig.HTTPProxy != "",
			"https", proxyConfig.HTTPSProxy != "",
			"all", proxyConfig.AllProxy != "",
			"no_proxy", proxyConfig.NoProxy != "",
		)
	}
	configStore, err := storage.Open(context.Background(), storage.Options{
		Path:            *dbPath,
		MasterKeyBase64: os.Getenv("CODEX_QUEUE_MASTER_KEY"),
	})
	if err != nil {
		logger.Error("configuration database error", "error", err)
		os.Exit(2)
	}
	defer configStore.Close()
	snapshot, err := configStore.Load(context.Background())
	if err != nil {
		logger.Error("load configuration database", "error", err)
		os.Exit(2)
	}
	cfg := &snapshot.Config

	runner := &codex.Runner{
		Binary:           cfg.Codex.Binary,
		PromptsFile:      cfg.Codex.PromptsFile,
		Prompts:          append([]string(nil), cfg.Codex.Prompts...),
		PromptsPersisted: cfg.Codex.PromptsPersisted,
		Timeout:          cfg.RequestTimeout(),
		ReasoningEffort:  cfg.Codex.ReasoningEffort,
		Overrides:        cfg.Codex.ConfigOverrides,
		Logger:           logger,
		Proxy:            proxyResolver,
	}
	if len(cfg.Codex.Targets) > 0 || *checkOnly {
		if err := runner.Check(); err != nil {
			logger.Error("Codex preflight failed", "error", err)
			os.Exit(2)
		}
	} else {
		logger.Info("skipping Codex preflight because no targets are configured")
	}
	if *checkOnly {
		fmt.Printf("configuration OK: revision=%d, setup_required=%t, %d target(s), Codex=%s, prompts=%s\n", snapshot.Revision, snapshot.SetupRequired, len(cfg.Codex.Targets), cfg.Codex.Binary, cfg.Codex.PromptsFile)
		return
	}
	if err := configStore.MarkStartupLoaded(context.Background(), snapshot.Revision); err != nil {
		logger.Error("record startup configuration revision", "error", err)
		os.Exit(2)
	}
	snapshot.LoadedStartupRevision = snapshot.Revision

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	statusStore := hub.NewStatusStore(hub.StatusDisabled)
	telegramStatusStore := hub.NewStatusStore(hub.StatusDisabled)
	manager := jobs.New(
		ctx,
		cfg.Codex.Targets,
		runner,
		nil,
		logger,
		cfg.RetryMin(),
		cfg.RetryMax(),
		cfg.KeepaliveMin(),
		cfg.KeepaliveMax(),
		cfg.Codex.MaxParallel,
		cfg.Codex.SuccessMessage,
	)
	messageRuntime := newMessageRuntime(ctx, manager, proxyResolver, logger, statusStore, telegramStatusStore)
	manager.SetMessenger(messageRuntime.DefaultMessenger())
	if err := messageRuntime.Reload(snapshot); err != nil {
		logger.Error("message adapter configuration error", "error", err)
		os.Exit(2)
	}

	webServer, err := web.New(web.Options{
		Manager:         manager,
		OpenILinkStatus: statusStore,
		TelegramStatus:  telegramStatusStore,
		CookieSecure:    cfg.Web.CookieSecure,
		TrustedProxies:  cfg.Web.TrustedProxies,
		Version:         version,
		Logger:          logger,
		Shutdown:        ctx.Done(),
		ConfigStore:     configStore,
		InitialConfig:   snapshot,
		Runner:          runner,
		ReloadMessages:  func(_ context.Context, next storage.Snapshot) error { return messageRuntime.Reload(next) },
	})
	if err != nil {
		logger.Error("web server configuration error", "error", err)
		os.Exit(2)
	}
	listener, err := net.Listen("tcp", cfg.Web.ListenAddress)
	if err != nil {
		logger.Error("Gin listen failed", "address", cfg.Web.ListenAddress, "error", err)
		os.Exit(1)
	}
	httpServer := &http.Server{
		Handler:           webServer.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- httpServer.Serve(listener)
	}()

	logger.Info(
		"Codex Web console started",
		"version", version,
		"listen_address", cfg.Web.ListenAddress,
		"openilink_enabled", cfg.OpenILinkEnabled(),
		"telegram_enabled", cfg.TelegramEnabled(),
		"targets", strings.Join(manager.TargetNames(), ","),
		"max_parallel", cfg.Codex.MaxParallel,
		"keepalive_min", cfg.KeepaliveMin(),
		"keepalive_max", cfg.KeepaliveMax(),
	)

	exitCode := 0
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Gin server stopped", "error", err)
			exitCode = 1
		}
		stop()
	}
	manager.BeginShutdown()
	messageRuntime.Close()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("web server graceful shutdown failed", "error", err)
		_ = httpServer.Close()
		if exitCode == 0 {
			exitCode = 1
		}
	}
	if err := manager.Wait(shutdownCtx); err != nil {
		logger.Error("Codex process shutdown timed out", "error", err)
		if exitCode == 0 {
			exitCode = 1
		}
	}
	logger.Info("Codex queue bot stopped")
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func newLogger(rawLevel string) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(rawLevel)) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
