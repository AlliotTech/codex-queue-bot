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
	"codex-queue-bot/internal/commands"
	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/proxyenv"
	"codex-queue-bot/internal/web"
)

var version = "dev"

func main() {
	configDefault := os.Getenv("CONFIG_FILE")
	if configDefault == "" {
		configDefault = "config.json"
	}
	configPath := flag.String("config", configDefault, "path to JSON configuration")
	checkOnly := flag.Bool("check", false, "validate configuration, Codex executable, and prompts, then exit")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	logger := newLogger(os.Getenv("LOG_LEVEL"))
	slog.SetDefault(logger)
	proxyConfig := proxyenv.Apply()
	if proxyConfig.Enabled() {
		logger.Info(
			"outbound proxy enabled",
			"http", proxyConfig.HTTPProxy != "",
			"https", proxyConfig.HTTPSProxy != "",
			"all", proxyConfig.AllProxy != "",
			"no_proxy", proxyConfig.NoProxy != "",
		)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("configuration error", "error", err)
		os.Exit(2)
	}
	adminPassword, err := cfg.AdminPassword()
	if err != nil {
		logger.Error("web administrator configuration error", "error", err)
		os.Exit(2)
	}

	runner := &codex.Runner{
		Binary:          cfg.Codex.Binary,
		PromptsFile:     cfg.Codex.PromptsFile,
		Timeout:         cfg.RequestTimeout(),
		ReasoningEffort: cfg.Codex.ReasoningEffort,
		Overrides:       cfg.Codex.ConfigOverrides,
		Logger:          logger,
	}
	if err := runner.Check(); err != nil {
		logger.Error("Codex preflight failed", "error", err)
		os.Exit(2)
	}
	if *checkOnly {
		fmt.Printf("configuration OK: %d target(s), Codex=%s, prompts=%s\n", len(cfg.Codex.Targets), cfg.Codex.Binary, cfg.Codex.PromptsFile)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	statusStore := hub.NewStatusStore(hub.StatusDisabled)
	var hubClient *hub.Client
	var messenger jobs.Messenger
	if cfg.OpenILinkEnabled() {
		hubClient = hub.New(cfg.OpenILink.BaseURL, cfg.OpenILink.Token, cfg.HTTPTimeout(), logger)
		statusStore = hubClient.StatusStore()
		messenger = hubClient
	}
	manager := jobs.New(
		ctx,
		cfg.Codex.Targets,
		runner,
		messenger,
		logger,
		cfg.RetryMin(),
		cfg.RetryMax(),
		cfg.KeepaliveMin(),
		cfg.KeepaliveMax(),
		cfg.Codex.MaxParallel,
		cfg.Codex.SuccessMessage,
		cfg.Web.ActivityLimit,
	)

	webServer, err := web.New(web.Options{
		Manager:         manager,
		OpenILinkStatus: statusStore,
		Username:        cfg.Web.AdminUsername,
		Password:        adminPassword,
		CookieSecure:    cfg.Web.CookieSecure,
		TrustedProxies:  cfg.Web.TrustedProxies,
		Version:         version,
		Logger:          logger,
		Shutdown:        ctx.Done(),
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

	if hubClient != nil {
		handler := commands.New(manager, hubClient, logger, cfg.OpenILink.AllowedUserIDs)
		go func() {
			if err := hubClient.Run(ctx, handler.Handle); err != nil {
				if errors.Is(err, hub.ErrUnauthorized) {
					logger.Error("OpenILink authentication failed; Web console remains available", "error", err)
				} else if ctx.Err() == nil {
					logger.Error("OpenILink listener stopped; Web console remains available", "error", err)
				}
			}
		}()
	}

	logger.Info(
		"Codex Web console started",
		"version", version,
		"listen_address", cfg.Web.ListenAddress,
		"openilink_enabled", cfg.OpenILinkEnabled(),
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
