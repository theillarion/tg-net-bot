package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/theillarion/tg-net-bot/internal/config"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func execute(args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	c := newRootCommand(&out, &errOut)
	c.SetArgs(args)
	err := c.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}
func TestRootVersionAndErrors(t *testing.T) {
	out, _, err := execute("version")
	if err != nil || !strings.Contains(out, "dev") {
		t.Fatalf("version %q %v", out, err)
	}
	if _, _, err = execute("check"); err == nil {
		t.Fatal("missing check target accepted")
	}
	if _, _, err = execute("check", "bad/path"); err == nil {
		t.Fatal("invalid check host accepted")
	}
	if _, _, err = execute("check", "example.com", "--method", "bogus"); err == nil {
		t.Fatal("invalid method accepted")
	}
	if _, _, err = execute("block", "bad/path"); err == nil {
		t.Fatal("invalid block target accepted")
	}
	if _, _, err = execute("serve", "extra"); err == nil {
		t.Fatal("serve args accepted")
	}
	if _, _, err = execute("version", "extra"); err == nil {
		t.Fatal("version args accepted")
	}
	if _, _, err = execute("missing"); err == nil {
		t.Fatal("unknown command accepted")
	}
}
func TestCheckCommandHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	out, _, err := execute("check", "127.0.0.1", "--method", "http", "--arg", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"status":"up"`) || !strings.Contains(out, `"target":"127.0.0.1"`) {
		t.Fatalf("check output %s", out)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	if _, _, err = execute("check", "127.0.0.1", "--method", "tcp", "--arg", port); err != nil {
		t.Fatal(err)
	}
}
func TestServeConfigError(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "")
	t.Setenv("TALAGRAM_ALLOW_USERS", "")
	_, _, err := execute("serve")
	if err == nil || !strings.Contains(err.Error(), "TELEGRAM_TOKEN") {
		t.Fatalf("serve error %v", err)
	}
	_ = os.Getenv("TELEGRAM_TOKEN")
}

func TestServeCommandWiring(t *testing.T) {
	want := config.Config{DatabasePath: "default"}
	called := false
	cmd := newServeCommandWith(func() (config.Config, error) { return want, nil }, func(_ context.Context, got config.Config, _ *slog.Logger) error {
		called = true
		if got.DatabasePath != "override" {
			t.Fatalf("database path=%q", got.DatabasePath)
		}
		return nil
	})
	cmd.SetArgs([]string{"--database", "override"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("serve callback not called")
	}
	cmd = newServeCommandWith(func() (config.Config, error) { return config.Config{}, errors.New("config broken") }, func(context.Context, config.Config, *slog.Logger) error {
		t.Fatal("serve called after config failure")
		return nil
	})
	if err := cmd.Execute(); err == nil || err.Error() != "config broken" {
		t.Fatalf("config error=%v", err)
	}
	cmd = newServeCommandWith(func() (config.Config, error) { return want, nil }, func(context.Context, config.Config, *slog.Logger) error { return errors.New("runtime broken") })
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "serve bot: runtime broken") {
		t.Fatalf("serve error=%v", err)
	}
}

func TestMainVersionPath(t *testing.T) {
	old := os.Args
	os.Args = []string{"tg-net-bot", "version"}
	defer func() { os.Args = old }()
	main()
}
