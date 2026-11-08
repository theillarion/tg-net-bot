package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/theillarion/tg-net-bot/internal/cheburcheck"
	"github.com/theillarion/tg-net-bot/internal/checker"
)

func newBlockCommand() *cobra.Command {
	return newBlockCommandWith(func(ctx context.Context, host string) (string, error) { return cheburcheck.New().Check(ctx, host) })
}

func newBlockCommandWith(check func(context.Context, string) (string, error)) *cobra.Command {
	return &cobra.Command{
		Use:   "block <server>",
		Short: "Query Cheburcheck for a server",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !checker.ValidHost(args[0]) {
				return fmt.Errorf("invalid server %q: expected an IP or domain", args[0])
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 90*time.Second)
			defer cancel()
			result, err := check(ctx, args[0])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", args[0], strings.TrimSpace(result))
			return err
		},
	}
}
