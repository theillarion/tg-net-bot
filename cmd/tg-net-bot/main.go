package main

import (
	"context"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(context.Background(), os.Stdout, os.Stderr); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, stdout, stderr io.Writer, args ...string) error {
	command := newRootCommand(stdout, stderr)
	command.SetArgs(args)
	if err := command.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(stderr, "error:", err)
		return err
	}
	return nil
}
