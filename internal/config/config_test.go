package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name, token, users, proxy, db string
		wantErr                       string
		wantUsers                     int
		wantDB                        string
	}{
		{name: "valid defaults", token: " token ", users: "1, 2", wantUsers: 2, wantDB: "./data/tg-net-bot.sqlite"},
		{name: "database and proxy", token: "x", users: "-9", proxy: "socks5://localhost:1080", db: "/tmp/db.sqlite", wantUsers: 1, wantDB: "/tmp/db.sqlite"},
		{name: "missing token", users: "1", wantErr: "TELEGRAM_TOKEN"},
		{name: "missing allowed users", token: "x", users: "abc,0", wantErr: "TALAGRAM_ALLOW_USERS"},
		{name: "invalid proxy", token: "x", users: "1", proxy: "localhost:123", wantErr: "TELEGRAM_PROXY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TELEGRAM_TOKEN", tt.token)
			t.Setenv("TALAGRAM_ALLOW_USERS", tt.users)
			t.Setenv("TELEGRAM_PROXY", tt.proxy)
			t.Setenv("DATABASE_PATH", tt.db)
			t.Setenv("BLOCKCHECK_CACHE_TTL", "")
			t.Setenv("REDIS_ADDR", "")
			t.Setenv("REDIS_PASSWORD", "")
			got, err := Load()
			if tt.wantErr != "" {
				if err == nil || !contains(err.Error(), tt.wantErr) {
					t.Fatalf("Load error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got.AllowedUsers) != tt.wantUsers || got.DatabasePath != tt.wantDB {
				t.Fatalf("Load = %+v", got)
			}
		})
	}
}

func TestLoadBlockCheckCacheTTL(t *testing.T) {
	t.Setenv("TELEGRAM_TOKEN", "x")
	t.Setenv("TALAGRAM_ALLOW_USERS", "1")
	t.Setenv("TELEGRAM_PROXY", "")
	t.Setenv("DATABASE_PATH", "")
	t.Setenv("BLOCKCHECK_CACHE_TTL", "45m")
	got, err := Load()
	if err != nil || got.BlockCheckCacheTTL != 45*time.Minute {
		t.Fatalf("ttl=%s err=%v", got.BlockCheckCacheTTL, err)
	}
	t.Setenv("BLOCKCHECK_CACHE_TTL", "0")
	if _, err := Load(); err == nil {
		t.Fatal("non-positive cache ttl accepted")
	}
}

func TestLoadBlockCheckSettings(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis.internal:6380")
	t.Setenv("REDIS_PASSWORD", "secret-for-test")
	t.Setenv("BLOCKCHECK_CACHE_TTL", "45m")
	address, password, ttl, err := LoadBlockCheckSettings()
	if err != nil || address != "redis.internal:6380" || password != "secret-for-test" || ttl != 45*time.Minute {
		t.Fatalf("settings=(%q,%q,%s), err=%v", address, password, ttl, err)
	}
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("BLOCKCHECK_CACHE_TTL", "invalid")
	if _, _, _, err := LoadBlockCheckSettings(); err == nil || !contains(err.Error(), "BLOCKCHECK_CACHE_TTL") {
		t.Fatalf("invalid ttl error=%v", err)
	}
	t.Setenv("BLOCKCHECK_CACHE_TTL", "3h")
	t.Setenv("REDIS_ADDR", "not-an-address")
	if _, _, _, err := LoadBlockCheckSettings(); err == nil || !contains(err.Error(), "REDIS_ADDR") {
		t.Fatalf("invalid Redis address error=%v", err)
	}
	for _, address := range []string{"redis://cache.internal:6379", "[::1]:6379"} {
		if err := validateRedisAddress(address); err != nil {
			t.Errorf("valid Redis address %q rejected: %v", address, err)
		}
	}
	for _, address := range []string{"redis://cache.internal:nope", "cache.internal:70000", ":6379"} {
		if err := validateRedisAddress(address); err == nil {
			t.Errorf("invalid Redis address %q accepted", address)
		}
	}
}
func contains(s, sub string) bool {
	return len(s) >= len(sub) && func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}
