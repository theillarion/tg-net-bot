package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Token        string
	AllowedUsers map[int64]struct{}
	Proxy        string
	DatabasePath string
}

func Load() (Config, error) {
	c := Config{Token: strings.TrimSpace(os.Getenv("TELEGRAM_TOKEN")), AllowedUsers: make(map[int64]struct{}), Proxy: strings.TrimSpace(os.Getenv("TELEGRAM_PROXY")), DatabasePath: strings.TrimSpace(os.Getenv("DATABASE_PATH"))}
	if c.DatabasePath == "" {
		c.DatabasePath = "./data/tg-net-bot.sqlite"
	}
	if c.Token == "" {
		return Config{}, fmt.Errorf("TELEGRAM_TOKEN is required")
	}
	raw := os.Getenv("TALAGRAM_ALLOW_USERS")
	for _, part := range strings.Split(raw, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || id == 0 {
			continue
		}
		c.AllowedUsers[id] = struct{}{}
	}
	if len(c.AllowedUsers) == 0 {
		return Config{}, fmt.Errorf("TALAGRAM_ALLOW_USERS must contain numeric Telegram user IDs")
	}
	if c.Proxy != "" {
		u, err := url.Parse(c.Proxy)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return Config{}, fmt.Errorf("TELEGRAM_PROXY must be an HTTP(S) or SOCKS5 URL")
		}
	}
	return c, nil
}
