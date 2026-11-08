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
	state, err := storage.Open(ctx, cfg.DatabasePath)
	if err != nil {
		return fmt.Errorf("open application database: %w", err)
	}
	defer func() {
		if err := state.Close(); err != nil {
			logger.Error("close application database", "error", err)
		}
	}()
	network := checker.New()
	blocks := cheburcheck.New()
	api, err := telegram.NewAPI(cfg.Token, cfg.Proxy)
	if err != nil {
		return fmt.Errorf("initialize Telegram API: %w", err)
	}
	service, err := monitor.New(ctx, state, network, blocks, api, logger)
	if err != nil {
		return fmt.Errorf("initialize monitor: %w", err)
	}
	bot := telegram.NewBot(api, service, cfg.AllowedUsers, logger)
	go service.Run(ctx)
	if err = bot.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run Telegram bot: %w", err)
	}
	return nil
}
