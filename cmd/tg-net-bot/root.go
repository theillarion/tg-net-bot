package main

import (
	"io"

	"github.com/spf13/cobra"
)

func newRootCommand(stdout, stderr io.Writer) *cobra.Command {
	root := &cobra.Command{
		Use:           "tg-net-bot",
		Short:         "Network checks and Telegram alerts",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.AddCommand(newServeCommand(), newCheckCommand(), newBlockCommand(), newVersionCommand())
	return root
}
