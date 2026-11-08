package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/theillarion/tg-net-bot/internal/domain"
	_ "modernc.org/sqlite"
)

type SQLite struct{ db *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS targets (
    user_id INTEGER NOT NULL,
    name TEXT NOT NULL COLLATE NOCASE,
    host TEXT NOT NULL,
    PRIMARY KEY (user_id, name)
);
CREATE TABLE IF NOT EXISTS alerts (
    id TEXT PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    target_name TEXT NOT NULL COLLATE NOCASE,
    target_host TEXT NOT NULL,
    method TEXT NOT NULL,
    argument TEXT NOT NULL DEFAULT '',
    interval_ns INTEGER NOT NULL,
    last_check_ns INTEGER NOT NULL DEFAULT 0,
    last_up INTEGER NULL,
    FOREIGN KEY (user_id, target_name) REFERENCES targets(user_id, name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS alerts_user_id_idx ON alerts(user_id);
`

func Open(ctx context.Context, path string) (*SQLite, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	dsnURL := url.URL{Scheme: "file", Path: absPath}
	query := dsnURL.Query()
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	dsnURL.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", dsnURL.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err = db.PingContext(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("connect to SQLite database: %w", err), db.Close())
	}
	if err = initializeSchema(ctx, db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &SQLite{db: db}, nil
}

func initializeSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite schema initialization: %w", err)
	}
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return errors.Join(fmt.Errorf("initialize SQLite schema: %w", err), tx.Rollback())
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite schema initialization: %w", err)
	}
	return nil
}

func (s *SQLite) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close SQLite database: %w", err)
	}
	return nil
}

func (s *SQLite) Load(ctx context.Context) (domain.State, error) {
	state := domain.EmptyState()
	rows, err := s.db.QueryContext(ctx, "SELECT user_id, name, host FROM targets ORDER BY user_id, name")
	if err != nil {
		return domain.State{}, fmt.Errorf("query targets: %w", err)
	}
	for rows.Next() {
		var userID int64
		var target domain.Target
		if err := rows.Scan(&userID, &target.Name, &target.Host); err != nil {
			return domain.State{}, errors.Join(fmt.Errorf("scan target: %w", err), rows.Close())
		}
		key := fmt.Sprint(userID)
		state.Targets[key] = append(state.Targets[key], target)
	}
	if err = rows.Err(); err != nil {
		return domain.State{}, errors.Join(fmt.Errorf("iterate targets: %w", err), rows.Close())
	}
	if err = rows.Close(); err != nil {
		return domain.State{}, fmt.Errorf("close target rows: %w", err)
	}

	rows, err = s.db.QueryContext(ctx, `SELECT id, chat_id, user_id, target_name, target_host, method, argument, interval_ns, last_check_ns, last_up FROM alerts ORDER BY id`)
	if err != nil {
		return domain.State{}, fmt.Errorf("query alerts: %w", err)
	}
	for rows.Next() {
		var alert domain.Alert
		var intervalNS, lastCheckNS int64
		var lastUp sql.NullInt64
		if err := rows.Scan(&alert.ID, &alert.ChatID, &alert.UserID, &alert.Target.Name, &alert.Target.Host, &alert.Method, &alert.Argument, &intervalNS, &lastCheckNS, &lastUp); err != nil {
			return domain.State{}, errors.Join(fmt.Errorf("scan alert: %w", err), rows.Close())
		}
		alert.Interval = time.Duration(intervalNS)
		if lastCheckNS != 0 {
			alert.LastCheck = time.Unix(0, lastCheckNS)
		}
		if lastUp.Valid {
			up := lastUp.Int64 != 0
			alert.LastUp = &up
		}
		state.Alerts = append(state.Alerts, alert)
	}
	if err = rows.Err(); err != nil {
		return domain.State{}, errors.Join(fmt.Errorf("iterate alerts: %w", err), rows.Close())
	}
	if err = rows.Close(); err != nil {
		return domain.State{}, fmt.Errorf("close alert rows: %w", err)
	}
	return state, nil
}

func (s *SQLite) InsertTarget(ctx context.Context, userID int64, target domain.Target) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO targets (user_id, name, host) VALUES (?, ?, ?)", userID, target.Name, target.Host)
	if err != nil {
		return fmt.Errorf("insert target: %w", err)
	}
	return nil
}

func (s *SQLite) DeleteTarget(ctx context.Context, userID int64, name string) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM targets WHERE user_id = ? AND name = ?", userID, name)
	if err != nil {
		return fmt.Errorf("delete target: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted target count: %w", err)
	}
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *SQLite) InsertAlert(ctx context.Context, alert domain.Alert) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO alerts (id, chat_id, user_id, target_name, target_host, method, argument, interval_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, alert.ID, alert.ChatID, alert.UserID, alert.Target.Name, alert.Target.Host, alert.Method, alert.Argument, int64(alert.Interval))
	if err != nil {
		return fmt.Errorf("insert alert: %w", err)
	}
	return nil
}

func (s *SQLite) DeleteAlert(ctx context.Context, userID int64, id string) error {
	result, err := s.db.ExecContext(ctx, "DELETE FROM alerts WHERE user_id = ? AND id = ?", userID, id)
	if err != nil {
		return fmt.Errorf("delete alert: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read deleted alert count: %w", err)
	}
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *SQLite) SetAlertLastCheck(ctx context.Context, id string, checkedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, "UPDATE alerts SET last_check_ns = ? WHERE id = ?", checkedAt.UnixNano(), id)
	if err != nil {
		return fmt.Errorf("update alert check time: %w", err)
	}
	return nil
}

func (s *SQLite) SetAlertLastUp(ctx context.Context, id string, up bool) error {
	value := 0
	if up {
		value = 1
	}
	_, err := s.db.ExecContext(ctx, "UPDATE alerts SET last_up = ? WHERE id = ?", value, id)
	if err != nil {
		return fmt.Errorf("update alert state: %w", err)
	}
	return nil
}
