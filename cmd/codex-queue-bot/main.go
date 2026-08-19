package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"codex-queue-bot/internal/codex"
	"codex-queue-bot/internal/commands"
	"codex-queue-bot/internal/config"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/proxyenv"
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
	hubClient := hub.New(cfg.OpenILink.BaseURL, cfg.OpenILink.Token, cfg.HTTPTimeout(), logger)
	manager := jobs.New(
		ctx,
		cfg.Codex.Targets,
		runner,
		hubClient,
		logger,
		cfg.RetryMin(),
		cfg.RetryMax(),
		cfg.Codex.MaxParallel,
		cfg.Codex.SuccessMessage,
	)
	handler := commands.New(manager, hubClient, logger, cfg.OpenILink.AllowedUserIDs)

	logger.Info(
		"Codex queue bot started",
		"version", version,
		"openilink", cfg.OpenILink.BaseURL,
		"targets", strings.Join(manager.TargetNames(), ","),
		"max_parallel", cfg.Codex.MaxParallel,
	)
	if err := hubClient.Run(ctx, handler.Handle); err != nil {
		logger.Error("OpenILink listener stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("Codex queue bot stopped")
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
