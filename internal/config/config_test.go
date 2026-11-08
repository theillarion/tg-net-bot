package config

import "testing"

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
