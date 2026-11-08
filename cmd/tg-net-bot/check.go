package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/theillarion/tg-net-bot/internal/checker"
)

func newCheckCommand() *cobra.Command {
	var method, argument string
	cmd := &cobra.Command{
		Use:   "check [flags] <IP-or-domain>",
		Short: "Run a one-time availability check",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !checker.ValidHost(args[0]) {
				return fmt.Errorf("invalid IP address or domain: %q", args[0])
			}
			m, arg, err := checker.Parse(method, argument)
			if err != nil {
				return fmt.Errorf("invalid check options: %w", err)
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 12*time.Second)
			defer cancel()
			up, detail := checker.New().Check(ctx, args[0], m, arg)
			status := "down"
			if up {
				status = "up"
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]string{"target": args[0], "status": status, "method": string(m), "detail": detail})
		},
	}
	cmd.Flags().StringVar(&method, "method", "icmp", "Check method: icmp, tcp, or http")
	cmd.Flags().StringVar(&argument, "arg", "", "TCP port, HTTP scheme, or URL")
	return cmd
}
