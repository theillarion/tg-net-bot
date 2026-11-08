package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/theillarion/tg-net-bot/internal/blockcheck"
	"github.com/theillarion/tg-net-bot/internal/domain"
)

func TestSQLiteUnavailableDatabase(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		run  func() error
	}{
		{"load", func() error { _, err := db.Load(ctx); return err }},
		{"insert target", func() error { return db.InsertTarget(ctx, 1, domain.Target{Name: "edge", Host: "example.com"}) }},
		{"delete target", func() error { return db.DeleteTarget(ctx, 1, "edge") }},
		{"insert alert", func() error { return db.InsertAlert(ctx, domain.Alert{ID: "a", Interval: time.Minute}) }},
		{"delete alert", func() error { return db.DeleteAlert(ctx, 1, "a") }},
		{"update check time", func() error { return db.SetAlertLastCheck(ctx, "a", time.Now()) }},
		{"update state", func() error { return db.SetAlertLastUp(ctx, "a", false) }},
		{"load interactions", func() error { _, err := db.LoadInteractions(ctx); return err }},
		{"save interaction", func() error { return db.SaveInteraction(ctx, 1, domain.Interaction{Step: "add-name"}) }},
		{"delete interaction", func() error { return db.DeleteInteraction(ctx, 1) }},
		{"save block cache", func() error {
			return db.Save(ctx, "key", blockcheck.Result{Geo: json.RawMessage(`{"country":"RU"}`)}, time.Now().Add(time.Hour))
		}},
		{"get block cache", func() error { _, err := db.Get(ctx, "key"); return err }},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			if err := op.run(); err == nil {
				t.Fatal("operation on closed SQLite database succeeded")
			}
		})
	}
}

func TestOpenRejectsInvalidDatabasePath(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), filepath.Join(file, "state.sqlite")); err == nil {
		t.Fatal("expected database directory error")
	}
	broken := filepath.Join(t.TempDir(), "broken.sqlite")
	if err := os.WriteFile(broken, []byte("not a SQLite database"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), broken); err == nil {
		t.Fatal("expected invalid SQLite database error")
	}
}

func TestSQLiteDetectsDamagedStoredRows(t *testing.T) {
	ctx := context.Background()
	// SQLite permits dynamically typed values even in INTEGER-affinity columns.
	targetPath := filepath.Join(t.TempDir(), "bad-target.sqlite")
	raw, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(`CREATE TABLE targets(user_id INTEGER, name TEXT, host TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(`INSERT INTO targets VALUES('not-a-user-id','edge','example.com')`); err != nil {
		t.Fatal(err)
	}
	if err = raw.Close(); err != nil {
		t.Fatal(err)
	}
	targetDB, err := Open(ctx, targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = targetDB.Load(ctx); err == nil {
		t.Fatal("Load accepted malformed target row")
	}
	_ = targetDB.Close()

	alertPath := filepath.Join(t.TempDir(), "bad-alert.sqlite")
	alertDB, err := Open(ctx, alertPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = alertDB.InsertTarget(ctx, 1, domain.Target{Name: "edge", Host: "example.com"}); err != nil {
		t.Fatal(err)
	}
	_, err = alertDB.db.ExecContext(ctx, `INSERT INTO alerts(id,chat_id,user_id,target_name,target_host,method,argument,interval_ns) VALUES('bad','not-a-chat-id',1,'edge','example.com','icmp','',1000)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = alertDB.Load(ctx); err == nil {
		t.Fatal("Load accepted malformed alert row")
	}
	_ = alertDB.Close()

	interactionPath := filepath.Join(t.TempDir(), "bad-interaction.sqlite")
	raw, err = sql.Open("sqlite", interactionPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(`CREATE TABLE interactions(user_id INTEGER, step TEXT, name TEXT, host TEXT, method TEXT, argument TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(`INSERT INTO interactions VALUES('not-a-user-id','add-name','','','','')`); err != nil {
		t.Fatal(err)
	}
	if err = raw.Close(); err != nil {
		t.Fatal(err)
	}
	interactionDB, err := Open(ctx, interactionPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = interactionDB.LoadInteractions(ctx); err == nil {
		t.Fatal("LoadInteractions accepted malformed user id")
	}
	_ = interactionDB.Close()
}

func TestOpenRejectsIncompatibleExistingSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incompatible.sqlite")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = raw.Exec(`CREATE TABLE alerts(id TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err = raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(context.Background(), path); err == nil {
		t.Fatal("expected incompatible schema initialization error")
	}
}
