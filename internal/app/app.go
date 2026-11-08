package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/theillarion/tg-net-bot/internal/cheburcheck"
	"github.com/theillarion/tg-net-bot/internal/checker"
	"github.com/theillarion/tg-net-bot/internal/config"
	"github.com/theillarion/tg-net-bot/internal/monitor"
	"github.com/theillarion/tg-net-bot/internal/storage"
	"github.com/theillarion/tg-net-bot/internal/telegram"
)

func Serve(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	return serve(ctx, cfg, logger, func(ctx context.Context, path string) (appState, error) {
		return storage.Open(ctx, path)
	}, telegram.NewAPI, runTelegramBot)
}

type appState interface {
	monitor.Repository
	telegram.InteractionStore
	Close() error
}

type botRunner func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error

func runTelegramBot(ctx context.Context, api *telegram.API, service *monitor.Service, allowed map[int64]struct{}, logger *slog.Logger, store telegram.InteractionStore) error {
	return telegram.NewBot(api, service, allowed, logger, store).Run(ctx)
}

func serve(ctx context.Context, cfg config.Config, logger *slog.Logger, open func(context.Context, string) (appState, error), newAPI func(string, string) (*telegram.API, error), runBot botRunner) error {
	state, err := open(ctx, cfg.DatabasePath)
	if err != nil {
		return fmt.Errorf("open application database: %w", err)
	}
	defer func() {
		if err := state.Close(); err != nil && logger != nil {
			logger.Error("close application database", "error", err)
		}
	}()
	network := checker.New()
	blocks := cheburcheck.New()
	api, err := newAPI(cfg.Token, cfg.Proxy)
	if err != nil {
		return fmt.Errorf("initialize Telegram API: %w", err)
	}
	service, err := monitor.New(ctx, state, network, blocks, api, logger)
	if err != nil {
		return fmt.Errorf("initialize monitor: %w", err)
	}
	if logger != nil {
		logger.Info("application started", "database_path", cfg.DatabasePath)
	}
	go service.Run(ctx)
	if err = runBot(ctx, api, service, cfg.AllowedUsers, logger, state); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run Telegram bot: %w", err)
	}
	return nil
}
