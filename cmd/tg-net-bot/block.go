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
	return &cobra.Command{
		Use:   "block <IP-or-domain>",
		Short: "Query Cheburcheck for an address",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !checker.ValidHost(args[0]) {
				return fmt.Errorf("invalid IP address or domain: %q", args[0])
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 90*time.Second)
			defer cancel()
			result, err := cheburcheck.New().Check(ctx, args[0])
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s: %s\n", args[0], strings.TrimSpace(result))
			return err
		},
	}
}
