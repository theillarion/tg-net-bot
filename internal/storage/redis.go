package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/theillarion/tg-net-bot/internal/blockcheck"
)

type redisCacheClient interface {
	Set(context.Context, string, any, time.Duration) error
	Get(context.Context, string) (string, error)
	Del(context.Context, string) error
	Ping(context.Context) error
	Close() error
}

type redisClient struct{ client *redis.Client }

func (c redisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	return c.client.Set(ctx, key, value, expiration).Err()
}
func (c redisClient) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}
func (c redisClient) Del(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}
func (c redisClient) Ping(ctx context.Context) error { return c.client.Ping(ctx).Err() }
func (c redisClient) Close() error                   { return c.client.Close() }

// RedisCache stores temporary Cheburcheck probe context in a shared Redis keyspace.
type RedisCache struct{ client redisCacheClient }

func NewRedisCache(client redisCacheClient) *RedisCache { return &RedisCache{client: client} }

func OpenRedisCache(ctx context.Context, address, password string) (*RedisCache, error) {
	options := &redis.Options{Addr: address, Password: password, Protocol: 2, DisableIdentity: true}
	if strings.HasPrefix(address, "redis://") {
		parsed, err := redis.ParseURL(address)
		if err != nil {
			return nil, fmt.Errorf("parse Redis URL: %w", err)
		}
		parsed.Protocol = 2
		parsed.DisableIdentity = true
		options = parsed
		if password != "" {
			options.Password = password
		}
	}
	if strings.TrimSpace(options.Addr) == "" {
		return nil, fmt.Errorf("Redis address is required")
	}
	client := redis.NewClient(options)
	return openRedisCacheWithClient(ctx, redisClient{client: client})
}

func openRedisCacheWithClient(ctx context.Context, client redisCacheClient) (*RedisCache, error) {
	cache := NewRedisCache(client)
	if err := cache.client.Ping(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("connect to Redis: %w", err), cache.Close())
	}
	return cache, nil
}

func (c *RedisCache) Close() error {
	if err := c.client.Close(); err != nil {
		return fmt.Errorf("close Redis cache: %w", err)
	}
	return nil
}

func (c *RedisCache) Save(ctx context.Context, key string, result blockcheck.Result, expiresAt time.Time) error {
	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		if err := c.client.Del(ctx, key); err != nil {
			return fmt.Errorf("delete expired block check cache entry: %w", err)
		}
		return nil
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode block check cache entry: %w", err)
	}
	if err := c.client.Set(ctx, key, payload, ttl); err != nil {
		return fmt.Errorf("save block check cache entry to Redis: %w", err)
	}
	return nil
}

func (c *RedisCache) Get(ctx context.Context, key string) (blockcheck.Result, error) {
	payload, err := c.client.Get(ctx, key)
	if errors.Is(err, redis.Nil) {
		return blockcheck.Result{}, blockcheck.ErrNotFound
	}
	if err != nil {
		return blockcheck.Result{}, fmt.Errorf("load block check cache entry from Redis: %w", err)
	}
	var result blockcheck.Result
	if err := json.Unmarshal([]byte(payload), &result); err != nil {
		return blockcheck.Result{}, fmt.Errorf("decode block check cache entry: %w", err)
	}
	return result, nil
}
