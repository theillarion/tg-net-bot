package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if err := newRootCommand(os.Stdout, os.Stderr).ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
