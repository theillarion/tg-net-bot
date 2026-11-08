package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestBlockCommandResults(t *testing.T) {
	cmd := newBlockCommandWith(func(_ context.Context, host string) (string, error) { return " verdict ", nil })
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"example.com"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if out.String() != "example.com: verdict\n" {
		t.Fatalf("output %q", out.String())
	}
	cmd = newBlockCommandWith(func(_ context.Context, _ string) (string, error) { return "", errors.New("service failed") })
	cmd.SetArgs([]string{"example.com"})
	if err := cmd.Execute(); err == nil || err.Error() != "service failed" {
		t.Fatalf("error %v", err)
	}
}

type failedOutput struct{}

func (failedOutput) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func TestBlockCommandWriteFailure(t *testing.T) {
	cmd := newBlockCommandWith(func(context.Context, string) (string, error) { return "ok", nil })
	cmd.SetOut(failedOutput{})
	cmd.SetArgs([]string{"example.com"})
	if err := cmd.Execute(); err == nil || err.Error() != "write failed" {
		t.Fatalf("write error=%v", err)
	}
}
