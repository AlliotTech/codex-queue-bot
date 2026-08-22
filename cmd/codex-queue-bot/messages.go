package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"codex-queue-bot/internal/commands"
	"codex-queue-bot/internal/hub"
	"codex-queue-bot/internal/jobs"
	"codex-queue-bot/internal/messaging"
	"codex-queue-bot/internal/proxyenv"
	"codex-queue-bot/internal/storage"
	"codex-queue-bot/internal/telegram"
)

// messageRuntime owns both adapters and swaps them as a unit of configuration
// state.  The per-adapter messaging.Proxy values never change, so job
// subscribers created before a reload still send through the current client.
type messageRuntime struct {
	ctx      context.Context
	manager  *jobs.Manager
	logger   *slog.Logger
	resolver *proxyenv.Resolver

	openStatus     *hub.StatusStore
	telegramStatus *hub.StatusStore
	openProxy      *messaging.Proxy
	telegramProxy  *messaging.Proxy
	defaultProxy   *messaging.Proxy

	reloadMu       sync.Mutex
	mu             sync.Mutex
	generation     uint64
	openClient     *hub.Client
	openCancel     context.CancelFunc
	telegramClient *telegram.Client
	telegramCancel context.CancelFunc
}

func newMessageRuntime(ctx context.Context, manager *jobs.Manager, resolver *proxyenv.Resolver, logger *slog.Logger, openStatus, telegramStatus *hub.StatusStore) *messageRuntime {
	return &messageRuntime{
		ctx: ctx, manager: manager, resolver: resolver, logger: logger,
		openStatus: openStatus, telegramStatus: telegramStatus,
		openProxy: messaging.NewProxy(nil), telegramProxy: messaging.NewProxy(nil), defaultProxy: messaging.NewProxy(nil),
	}
}

func (r *messageRuntime) OpenMessenger() jobs.Messenger     { return r.openProxy }
func (r *messageRuntime) TelegramMessenger() jobs.Messenger { return r.telegramProxy }
func (r *messageRuntime) DefaultMessenger() jobs.Messenger  { return r.defaultProxy }

func (r *messageRuntime) Reload(snapshot storage.Snapshot) error {
	if r == nil {
		return nil
	}
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	r.mu.Lock()
	r.generation++
	generation := r.generation
	oldOpen, oldOpenCancel := r.openClient, r.openCancel
	oldTelegram, oldTelegramCancel := r.telegramClient, r.telegramCancel
	r.openClient, r.openCancel = nil, nil
	r.telegramClient, r.telegramCancel = nil, nil
	r.mu.Unlock()
	r.openProxy.Set(nil)
	r.telegramProxy.Set(nil)
	r.defaultProxy.Set(nil)
	if oldOpenCancel != nil {
		oldOpenCancel()
	}
	if oldOpen != nil {
		oldOpen.Close()
	}
	if oldTelegramCancel != nil {
		oldTelegramCancel()
	}
	if oldTelegram != nil {
		oldTelegram.Close()
	}
	r.openStatus.Set(hub.StatusDisabled, "")
	r.telegramStatus.Set(hub.StatusDisabled, "")

	cfg := snapshot.Config
	if cfg.OpenILinkEnabled() && cfg.OpenILink.Token != "" {
		client := hub.New(cfg.OpenILink.BaseURL, cfg.OpenILink.Token, cfg.HTTPTimeout(), r.logger, r.resolver)
		r.openProxy.Set(client)
		r.defaultProxy.Set(r.openProxy)
		r.openStatus.Set(hub.StatusConnecting, "")
		clientCtx, cancel := context.WithCancel(r.ctx)
		r.mu.Lock()
		r.openClient, r.openCancel = client, cancel
		r.mu.Unlock()
		handler := commands.NewAdapter(r.manager, r.openProxy, r.logger, cfg.OpenILink.AllowedUserIDs, "OpenILink")
		go r.forwardStatus(clientCtx, generation, client, client.StatusStore(), r.openStatus)
		go r.runOpenILink(clientCtx, client, handler)
	}
	if cfg.TelegramEnabled() && cfg.Telegram.Token != "" {
		client := telegram.New(cfg.Telegram.BaseURL, cfg.Telegram.Token, cfg.TelegramHTTPTimeout(), cfg.TelegramPollTimeout(), r.logger, r.resolver)
		r.telegramProxy.Set(client)
		if r.defaultProxy.Client() == nil {
			r.defaultProxy.Set(r.telegramProxy)
		}
		r.telegramStatus.Set(hub.StatusConnecting, "")
		clientCtx, cancel := context.WithCancel(r.ctx)
		r.mu.Lock()
		r.telegramClient, r.telegramCancel = client, cancel
		r.mu.Unlock()
		handler := commands.NewAdapter(r.manager, r.telegramProxy, r.logger, cfg.Telegram.AllowedUserIDs, "Telegram")
		go r.forwardStatus(clientCtx, generation, client, client.StatusStore(), r.telegramStatus)
		go r.runTelegram(clientCtx, client, handler)
	}
	return nil
}

func (r *messageRuntime) runOpenILink(ctx context.Context, client *hub.Client, handler *commands.Handler) {
	if err := client.Run(ctx, handler.Handle); err != nil && ctx.Err() == nil && !errors.Is(err, hub.ErrUnauthorized) {
		r.logger.Error("OpenILink listener stopped; Web console remains available", "error", err)
	}
}

func (r *messageRuntime) runTelegram(ctx context.Context, client *telegram.Client, handler *commands.Handler) {
	if err := client.Run(ctx, handler.Handle); err != nil && ctx.Err() == nil && !errors.Is(err, telegram.ErrUnauthorized) {
		r.logger.Error("Telegram listener stopped; Web console remains available", "error", err)
	}
}

func (r *messageRuntime) forwardStatus(ctx context.Context, generation uint64, client any, source *hub.StatusStore, target *hub.StatusStore) {
	initial, subscription := source.Observe(16)
	defer subscription.Close()
	if !r.setStatusIfCurrent(generation, client, target, initial) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-subscription.Updates:
			if !ok {
				return
			}
			if !r.setStatusIfCurrent(generation, client, target, update) {
				return
			}
		}
	}
}

func (r *messageRuntime) setStatusIfCurrent(generation uint64, client any, target *hub.StatusStore, update hub.StatusSnapshot) bool {
	r.mu.Lock()
	current := generation == r.generation
	if current {
		switch target {
		case r.openStatus:
			current = client == r.openClient
		case r.telegramStatus:
			current = client == r.telegramClient
		default:
			current = false
		}
	}
	if current {
		target.Set(update.State, update.Error)
	}
	r.mu.Unlock()
	return current
}

func (r *messageRuntime) Close() {
	if r == nil {
		return
	}
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	r.mu.Lock()
	r.generation++
	open, openCancel := r.openClient, r.openCancel
	tg, tgCancel := r.telegramClient, r.telegramCancel
	r.openClient, r.openCancel, r.telegramClient, r.telegramCancel = nil, nil, nil, nil
	r.mu.Unlock()
	if openCancel != nil {
		openCancel()
	}
	if open != nil {
		open.Close()
	}
	if tgCancel != nil {
		tgCancel()
	}
	if tg != nil {
		tg.Close()
	}
	r.openProxy.Set(nil)
	r.telegramProxy.Set(nil)
	r.defaultProxy.Set(nil)
	r.openStatus.Set(hub.StatusDisabled, "")
	r.telegramStatus.Set(hub.StatusDisabled, "")
}
