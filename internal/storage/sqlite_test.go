package storage

import (
	"context"
	"database/sql"
	"github.com/theillarion/tg-net-bot/internal/domain"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteLifecycle(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "nested", "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := db.Load(ctx)
	if err != nil || len(s.Targets) != 0 || len(s.Alerts) != 0 {
		t.Fatalf("initial Load = %#v, %v", s, err)
	}
	target := domain.Target{Name: "edge", Host: "2001:db8::1"}
	if err = db.InsertTarget(ctx, 42, target); err != nil {
		t.Fatal(err)
	}
	last := time.Now().UTC().Truncate(time.Second)
	alert := domain.Alert{ID: "a", ChatID: 9, UserID: 42, Target: target, Method: "tcp", Argument: "443", Interval: time.Minute}
	if err = db.InsertAlert(ctx, alert); err != nil {
		t.Fatal(err)
	}
	if err = db.SetAlertLastCheck(ctx, "a", last); err != nil {
		t.Fatal(err)
	}
	if err = db.SetAlertLastUp(ctx, "a", true); err != nil {
		t.Fatal(err)
	}
	s, err = db.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Targets["42"]) != 1 || len(s.Alerts) != 1 || s.Alerts[0].LastUp == nil || !*s.Alerts[0].LastUp || s.Alerts[0].LastCheck.Unix() != last.Unix() {
		t.Fatalf("loaded state: %#v", s)
	}
	if err = db.DeleteAlert(ctx, 99, "a"); err == nil {
		t.Fatalf("expected missing alert error: %v", err)
	}
	if err = db.DeleteTarget(ctx, 42, "edge"); err != nil {
		t.Fatal(err)
	}
	s, err = db.Load(ctx)
	if err != nil || len(s.Alerts) != 0 || len(s.Targets["42"]) != 0 {
		t.Fatalf("cascade state=%#v err=%v", s, err)
	}
	if err = db.DeleteTarget(ctx, 42, "missing"); err == nil {
		t.Fatal("expected missing target error")
	}
}

func TestInteractionSurvivesSQLiteRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bot.sqlite")
	state := domain.Interaction{Step: "manual-tcp-port", Name: "prod", Host: "illarion.tech", Method: "tcp", RequestID: "form-id"}
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.SaveInteraction(ctx, 42, state); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	interactions, err := db.LoadInteractions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := interactions[42]; got != state {
		t.Fatalf("restored interaction = %+v, want %+v", got, state)
	}
	state.Step, state.Argument = "add-alert-interval", "443"
	if err = db.SaveInteraction(ctx, 42, state); err != nil {
		t.Fatal(err)
	}
	interactions, err = db.LoadInteractions(ctx)
	if err != nil || interactions[42] != state || len(interactions) != 1 {
		t.Fatalf("upserted interactions = %+v, err=%v", interactions, err)
	}
	if err = db.DeleteInteraction(ctx, 42); err != nil {
		t.Fatal(err)
	}
	interactions, err = db.LoadInteractions(ctx)
	if err != nil || len(interactions) != 0 {
		t.Fatalf("deleted interactions = %+v, err=%v", interactions, err)
	}
}

func TestOpenMigratesLegacyInteractionTable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(`CREATE TABLE interactions(user_id INTEGER PRIMARY KEY, step TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', host TEXT NOT NULL DEFAULT '', method TEXT NOT NULL DEFAULT '', argument TEXT NOT NULL DEFAULT '')`); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(`INSERT INTO interactions(user_id, step, name, host, method, argument) VALUES(7, 'add-alert-interval', 'edge', 'example.com', 'tcp', '443')`); err != nil {
		t.Fatal(err)
	}
	if err = raw.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	interactions, err := db.LoadInteractions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	legacy := domain.Interaction{Step: "add-alert-interval", Name: "edge", Host: "example.com", Method: "tcp", Argument: "443"}
	if interactions[7] != legacy {
		t.Fatalf("migrated interaction = %+v, want %+v", interactions[7], legacy)
	}
	legacy.RequestID = "form-id"
	if err = db.SaveInteraction(ctx, 7, legacy); err != nil {
		t.Fatal(err)
	}
	interactions, err = db.LoadInteractions(ctx)
	if err != nil || interactions[7] != legacy {
		t.Fatalf("updated migrated interaction = %+v, err=%v", interactions[7], err)
	}
}

func TestOpenCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(ctx, filepath.Join(t.TempDir(), "x.db")); err == nil {
		t.Fatal("expected canceled open error")
	}
}

func TestSQLiteFailures(t *testing.T) {
	ctx := context.Background()
	blockedDir := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedDir, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, filepath.Join(blockedDir, "db.sqlite")); err == nil {
		t.Fatal("expected database directory creation failure")
	}
	broken := filepath.Join(t.TempDir(), "broken.sqlite")
	if err := os.WriteFile(broken, []byte("not sqlite"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, broken); err == nil {
		t.Fatal("expected malformed database initialization failure")
	}
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = db.Load(cancelled); err == nil {
		t.Fatal("expected cancelled load")
	}
	alert := domain.Alert{ID: "x", Target: domain.Target{Name: "x"}, Interval: time.Second}
	operations := []struct {
		name string
		run  func() error
	}{
		{"insert target", func() error { return db.InsertTarget(cancelled, 1, domain.Target{Name: "x", Host: "x"}) }},
		{"delete target", func() error { return db.DeleteTarget(cancelled, 1, "x") }},
		{"insert alert", func() error { return db.InsertAlert(cancelled, alert) }},
		{"delete alert", func() error { return db.DeleteAlert(cancelled, 1, "x") }},
		{"set last check", func() error { return db.SetAlertLastCheck(cancelled, "x", time.Now()) }},
		{"set last up", func() error { return db.SetAlertLastUp(cancelled, "x", true) }},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			if err := op.run(); err == nil {
				t.Fatal("expected canceled database operation error")
			}
		})
	}
}
