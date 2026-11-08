package app

import (
	"bytes"
	"context"
	"errors"
	"github.com/theillarion/tg-net-bot/internal/config"
	"github.com/theillarion/tg-net-bot/internal/domain"
	"github.com/theillarion/tg-net-bot/internal/monitor"
	"github.com/theillarion/tg-net-bot/internal/storage"
	"github.com/theillarion/tg-net-bot/internal/telegram"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServeCanceled(t *testing.T) {
	cfg := config.Config{DatabasePath: filepath.Join(t.TempDir(), "state.db"), Token: "test", AllowedUsers: map[int64]struct{}{1: {}}}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	err := serve(context.Background(), cfg, logger, func(ctx context.Context, path string) (appState, error) { return storage.Open(ctx, path) }, telegram.NewAPI, func(ctx context.Context, _ *telegram.API, _ *monitor.Service, _ map[int64]struct{}, _ *slog.Logger, store telegram.InteractionStore) error {
		want := domain.Interaction{Step: "add-host", Name: "edge"}
		if err := store.SaveInteraction(ctx, 1, want); err != nil {
			return err
		}
		got, err := store.LoadInteractions(ctx)
		if err != nil {
			return err
		}
		if got[1] != want {
			return errors.New("app did not pass the SQLite interaction store")
		}
		return context.Canceled
	})
	if err != nil {
		t.Fatalf("Serve with graceful cancellation: %v", err)
	}
	if !strings.Contains(logs.String(), "application started") {
		t.Fatalf("startup was not logged: %s", logs.String())
	}
}
func TestServeInvalidProxy(t *testing.T) {
	err := Serve(context.Background(), config.Config{DatabasePath: filepath.Join(t.TempDir(), "state.db"), Token: "test", Proxy: "://bad", AllowedUsers: map[int64]struct{}{1: {}}}, nil)
	if err == nil {
		t.Fatal("expected proxy initialization error")
	}
}

type failingLoadState struct{ closeErr error }

func (f failingLoadState) Load(context.Context) (domain.State, error) {
	return domain.State{}, errors.New("load failed")
}
func (f failingLoadState) InsertTarget(context.Context, int64, domain.Target) error   { return nil }
func (f failingLoadState) DeleteTarget(context.Context, int64, string) error          { return nil }
func (f failingLoadState) InsertAlert(context.Context, domain.Alert) error            { return nil }
func (f failingLoadState) DeleteAlert(context.Context, int64, string) error           { return nil }
func (f failingLoadState) SetAlertLastCheck(context.Context, string, time.Time) error { return nil }
func (f failingLoadState) SetAlertLastUp(context.Context, string, bool) error         { return nil }
func (f failingLoadState) LoadInteractions(context.Context) (map[int64]domain.Interaction, error) {
	return nil, nil
}
func (f failingLoadState) SaveInteraction(context.Context, int64, domain.Interaction) error {
	return nil
}
func (f failingLoadState) DeleteInteraction(context.Context, int64) error { return nil }
func (f failingLoadState) Close() error                                   { return f.closeErr }
func TestServeDependencyFailures(t *testing.T) {
	cfg := config.Config{DatabasePath: "x", Token: "x", AllowedUsers: map[int64]struct{}{1: {}}}
	if err := serve(context.Background(), cfg, nil, func(context.Context, string) (appState, error) { return nil, errors.New("open failed") }, telegram.NewAPI, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "open application database") {
		t.Fatalf("open error=%v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := serve(context.Background(), cfg, logger, func(context.Context, string) (appState, error) {
		return failingLoadState{closeErr: errors.New("close failed")}, nil
	}, func(string, string) (*telegram.API, error) { return &telegram.API{}, nil }, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "initialize monitor") {
		t.Fatalf("monitor error=%v", err)
	}
}

func TestRunTelegramBotHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runTelegramBot(ctx, &telegram.API{}, nil, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("runTelegramBot error=%v", err)
	}
}
