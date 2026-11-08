package app

import (
	"bytes"
	"context"
	"errors"
	"github.com/theillarion/tg-net-bot/internal/blockcheck"
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
	err := serve(context.Background(), cfg, logger, func(ctx context.Context, path string) (appState, error) { return storage.Open(ctx, path) }, testCacheOpener, telegram.NewAPI, func(ctx context.Context, _ *telegram.API, _ *monitor.Service, _ map[int64]struct{}, _ *slog.Logger, store telegram.InteractionStore) error {
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
	if err := serve(context.Background(), cfg, nil, func(context.Context, string) (appState, error) { return nil, errors.New("open failed") }, testCacheOpener, telegram.NewAPI, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "open application database") {
		t.Fatalf("open error=%v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := serve(context.Background(), cfg, logger, func(context.Context, string) (appState, error) {
		return failingLoadState{closeErr: errors.New("close failed")}, nil
	}, testCacheOpener, func(string, string) (*telegram.API, error) { return &telegram.API{}, nil }, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "initialize monitor") {
		t.Fatalf("monitor error=%v", err)
	}
}

func TestServeReportsRedisStartupFailure(t *testing.T) {
	cfg := config.Config{DatabasePath: "state.sqlite", Token: "x", RedisAddr: "127.0.0.1:6379", AllowedUsers: map[int64]struct{}{1: {}}}
	err := serve(context.Background(), cfg, nil, func(context.Context, string) (appState, error) {
		return failingLoadState{}, nil
	}, func(context.Context, string, string) (blockcheck.Cache, io.Closer, error) {
		return nil, nil, errors.New("redis unavailable")
	}, telegram.NewAPI, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "open block check Redis cache") || !strings.Contains(err.Error(), "redis unavailable") {
		t.Fatalf("Redis startup error=%v", err)
	}
}

func TestServeWrapsBotRunFailure(t *testing.T) {
	cfg := config.Config{DatabasePath: "state.sqlite", Token: "x", RedisAddr: "shared-cache:6379", AllowedUsers: map[int64]struct{}{1: {}}}
	err := serve(context.Background(), cfg, nil, func(context.Context, string) (appState, error) {
		return storage.Open(context.Background(), filepath.Join(t.TempDir(), "state.sqlite"))
	}, testCacheOpener, func(string, string) (*telegram.API, error) { return &telegram.API{}, nil }, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return errors.New("bot unavailable")
	})
	if err == nil || !strings.Contains(err.Error(), "run Telegram bot") || !strings.Contains(err.Error(), "bot unavailable") {
		t.Fatalf("bot run error=%v", err)
	}
}

type appTestCloser struct{ err error }

func (c appTestCloser) Close() error { return c.err }

func TestServeLogsRedisCloseFailure(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	cfg := config.Config{DatabasePath: "state.sqlite", Token: "x", RedisAddr: "shared-cache:6379", AllowedUsers: map[int64]struct{}{1: {}}}
	err := serve(context.Background(), cfg, logger, func(context.Context, string) (appState, error) {
		return storage.Open(context.Background(), filepath.Join(t.TempDir(), "state.sqlite"))
	}, func(context.Context, string, string) (blockcheck.Cache, io.Closer, error) {
		return appTestCache{}, appTestCloser{err: errors.New("redis close failed")}, nil
	}, func(string, string) (*telegram.API, error) { return &telegram.API{}, nil }, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return nil
	})
	if err != nil {
		t.Fatalf("serve error=%v", err)
	}
	if !strings.Contains(logs.String(), "close block check Redis cache") {
		t.Fatalf("Redis close failure was not logged: %s", logs.String())
	}
}

func TestServeReportsTelegramAPIInitializationFailure(t *testing.T) {
	cfg := config.Config{DatabasePath: "state.sqlite", Token: "x", RedisAddr: "shared-cache:6379", AllowedUsers: map[int64]struct{}{1: {}}}
	err := serve(context.Background(), cfg, nil, func(context.Context, string) (appState, error) {
		return storage.Open(context.Background(), filepath.Join(t.TempDir(), "state.sqlite"))
	}, testCacheOpener, func(string, string) (*telegram.API, error) {
		return nil, errors.New("Telegram API unavailable")
	}, func(context.Context, *telegram.API, *monitor.Service, map[int64]struct{}, *slog.Logger, telegram.InteractionStore) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "initialize Telegram API") || !strings.Contains(err.Error(), "Telegram API unavailable") {
		t.Fatalf("Telegram API error=%v", err)
	}
}

type appTestCache struct{}

func (appTestCache) Save(context.Context, string, blockcheck.Result, time.Time) error { return nil }
func (appTestCache) Get(context.Context, string) (blockcheck.Result, error) {
	return blockcheck.Result{}, blockcheck.ErrNotFound
}

func testCacheOpener(context.Context, string, string) (blockcheck.Cache, io.Closer, error) {
	return appTestCache{}, io.NopCloser(strings.NewReader("")), nil
}

func TestRunTelegramBotHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runTelegramBot(ctx, &telegram.API{}, nil, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("runTelegramBot error=%v", err)
	}
}
