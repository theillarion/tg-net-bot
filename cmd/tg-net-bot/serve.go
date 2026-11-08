package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/theillarion/tg-net-bot/internal/app"
	"github.com/theillarion/tg-net-bot/internal/config"
)

func newServeCommand() *cobra.Command {
	var databasePath string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the Telegram bot and alert scheduler",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load()
			if err != nil {
				return err
			}
			if cmd.Flags().Changed("database") {
				cfg.DatabasePath = databasePath
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			logger := slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), nil))
			if err := app.Serve(ctx, cfg, logger); err != nil {
				return fmt.Errorf("serve bot: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&databasePath, "database", "", "SQLite database path (overrides DATABASE_PATH)")
	return cmd
}
