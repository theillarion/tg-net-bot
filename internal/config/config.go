package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Token              string
	AllowedUsers       map[int64]struct{}
	Proxy              string
	DatabasePath       string
	RedisAddr          string
	RedisPassword      string
	BlockCheckCacheTTL time.Duration
}

func Load() (Config, error) {
	redisAddr, redisPassword, cacheTTL, err := LoadBlockCheckSettings()
	if err != nil {
		return Config{}, err
	}
	c := Config{Token: strings.TrimSpace(os.Getenv("TELEGRAM_TOKEN")), AllowedUsers: make(map[int64]struct{}), Proxy: strings.TrimSpace(os.Getenv("TELEGRAM_PROXY")), DatabasePath: strings.TrimSpace(os.Getenv("DATABASE_PATH")), RedisAddr: redisAddr, RedisPassword: redisPassword, BlockCheckCacheTTL: cacheTTL}
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

// LoadBlockCheckSettings reads shared Redis and cache TTL configuration without
// requiring Telegram credentials, so the CLI uses the same settings as serve.
func LoadBlockCheckSettings() (string, string, time.Duration, error) {
	address := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if address == "" {
		address = "127.0.0.1:6379"
	}
	if err := validateRedisAddress(address); err != nil {
		return "", "", 0, fmt.Errorf("REDIS_ADDR must be host:port or a redis:// URL: %w", err)
	}
	ttl := 3 * time.Hour
	if raw := strings.TrimSpace(os.Getenv("BLOCKCHECK_CACHE_TTL")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return "", "", 0, fmt.Errorf("BLOCKCHECK_CACHE_TTL must be a positive Go duration")
		}
		ttl = parsed
	}
	return address, os.Getenv("REDIS_PASSWORD"), ttl, nil
}

func validateRedisAddress(address string) error {
	if strings.HasPrefix(address, "redis://") {
		parsed, err := url.Parse(address)
		if err != nil || parsed.Scheme != "redis" || parsed.Hostname() == "" || parsed.Port() == "" {
			return fmt.Errorf("invalid Redis URL")
		}
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("invalid Redis port")
		}
		return nil
	}
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("invalid Redis address")
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid Redis port")
	}
	return nil
}
