package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunReportsCommandFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), &stdout, &stderr, "unknown")
	if err == nil {
		t.Fatal("run without a command should fail")
	}
	if !strings.Contains(stderr.String(), "error:") {
		t.Fatalf("stderr does not report failure: %q", stderr.String())
	}
}

func TestRunHelpSucceeds(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), &stdout, &stderr, "--help"); err != nil {
		t.Fatalf("run help: %v", err)
	}
	if stdout.Len() == 0 || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}
