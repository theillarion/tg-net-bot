//go:build integration

package app

import (
	"context"
	"github.com/theillarion/tg-net-bot/internal/blockcheck"
	"github.com/theillarion/tg-net-bot/internal/checker"
	"github.com/theillarion/tg-net-bot/internal/domain"
	"github.com/theillarion/tg-net-bot/internal/monitor"
	"github.com/theillarion/tg-net-bot/internal/storage"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

type integrationNetwork struct{}

func (integrationNetwork) Check(context.Context, string, checker.Method, string) (bool, string) {
	return true, "reachable"
}

type integrationBlock struct{}

func (integrationBlock) Check(_ context.Context, target string) (blockcheck.Result, error) {
	return blockcheck.Result{Target: target}, nil
}
func (integrationBlock) Probe(context.Context, string) (blockcheck.ProbeResult, error) {
	return blockcheck.ProbeResult{}, nil
}
func (b integrationBlock) CheckBatch(ctx context.Context, targets []string, emit func(blockcheck.BatchItem)) error {
	for _, target := range targets {
		result, err := b.Check(ctx, target)
		emit(blockcheck.BatchItem{Target: target, Result: result, Status: "complete"})
		if err != nil {
			return err
		}
	}
	return nil
}

type integrationNotifier struct{}

func (integrationNotifier) Send(context.Context, int64, string) error { return nil }
func TestSQLiteMonitorIntegration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "data", "bot.sqlite")
	repo, err := storage.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := monitor.New(ctx, repo, integrationNetwork{}, integrationBlock{}, integrationNotifier{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err = svc.AddTarget(ctx, 22, "edge", "2001:db8::5"); err != nil {
		t.Fatal(err)
	}
	alert, err := svc.AddAlertWithID(ctx, "form-request-22", 22, 22, "edge", "tcp", "443", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if alert.Interval != time.Minute || alert.Method != "tcp" || alert.Argument != "443" {
		t.Fatalf("created alert: %+v", alert)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { svc.Run(runCtx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		state, loadErr := repo.Load(ctx)
		if loadErr != nil {
			cancel()
			<-done
			t.Fatal(loadErr)
		}
		if len(state.Alerts) == 1 && state.Alerts[0].LastUp != nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("scheduled alert did not persist a result")
		}
		time.Sleep(10 * time.Millisecond)
	}
	interaction := domain.Interaction{Step: "add-alert-interval", Name: "edge", Host: "2001:db8::5", Method: "tcp", Argument: "443", RequestID: "form-request-22"}
	if err = repo.SaveInteraction(ctx, 22, interaction); err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	cancel()
	<-done
	if err = repo.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Targets["22"]) != 1 || len(state.Alerts) != 1 || state.Alerts[0].Target != (domain.Target{Name: "edge", Host: "2001:db8::5"}) || state.Alerts[0].LastUp == nil || !*state.Alerts[0].LastUp || state.Alerts[0].LastCheck.IsZero() {
		t.Fatalf("persisted state %#v", state)
	}
	interactions, err := reopened.LoadInteractions(ctx)
	if err != nil || interactions[22] != interaction {
		t.Fatalf("restored dialog state = %+v, err=%v", interactions[22], err)
	}
	restoredService, err := monitor.New(ctx, reopened, integrationNetwork{}, integrationBlock{}, integrationNotifier{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	retried, err := restoredService.AddAlertWithID(ctx, "form-request-22", 22, 22, "edge", "tcp", "443", time.Minute)
	if err != nil || retried.ID != alert.ID || len(restoredService.Alerts(22)) != 1 {
		t.Fatalf("restored alert retry = %+v; alerts=%+v; err=%v", retried, restoredService.Alerts(22), err)
	}
}
