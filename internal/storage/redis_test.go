package storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/theillarion/tg-net-bot/internal/blockcheck"
)

type fakeRedisEntry struct {
	value   string
	expires time.Time
}

type fakeRedisClient struct {
	entries  map[string]fakeRedisEntry
	setErr   error
	getErr   error
	delErr   error
	pingErr  error
	closeErr error
	lastTTL  time.Duration
	closed   bool
}

func newFakeRedisClient() *fakeRedisClient {
	return &fakeRedisClient{entries: make(map[string]fakeRedisEntry)}
}
func (c *fakeRedisClient) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.setErr != nil {
		return c.setErr
	}
	c.lastTTL = ttl
	c.entries[key] = fakeRedisEntry{value: string(value.([]byte)), expires: time.Now().Add(ttl)}
	return nil
}
func (c *fakeRedisClient) Get(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if c.getErr != nil {
		return "", c.getErr
	}
	entry, ok := c.entries[key]
	if !ok || !time.Now().Before(entry.expires) {
		delete(c.entries, key)
		return "", redis.Nil
	}
	return entry.value, nil
}
func (c *fakeRedisClient) Del(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.delErr != nil {
		return c.delErr
	}
	delete(c.entries, key)
	return nil
}
func (c *fakeRedisClient) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.pingErr
}
func (c *fakeRedisClient) Close() error { c.closed = true; return c.closeErr }

func TestRedisCacheRoundTripTTLAndExpiredWrite(t *testing.T) {
	client := newFakeRedisClient()
	cache := NewRedisCache(client)
	result := blockcheck.Result{ID: "upstream-id", CacheKey: "cache-key", Target: "example.net", CheckedAt: time.Now(), Blocked: true}
	expiresAt := time.Now().Add(time.Minute)
	if err := cache.Save(context.Background(), result.CacheKey, result, expiresAt); err != nil {
		t.Fatal(err)
	}
	if client.lastTTL <= 0 || client.lastTTL > time.Minute {
		t.Fatalf("TTL=%s", client.lastTTL)
	}
	got, err := NewRedisCache(newRedisClientView(client)).Get(context.Background(), result.CacheKey)
	if err != nil || got.ID != result.ID || got.CacheKey != result.CacheKey || got.Target != result.Target || !got.CheckedAt.Equal(result.CheckedAt) || !got.Blocked {
		t.Fatalf("shared read result=%+v err=%v", got, err)
	}
	if err := cache.Save(context.Background(), "expired", result, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, exists := client.entries["expired"]; exists {
		t.Fatal("expired entry was retained")
	}
	if _, err := cache.Get(context.Background(), "missing"); !errors.Is(err, blockcheck.ErrNotFound) {
		t.Fatalf("miss error=%v", err)
	}
	client.entries["expired-key"] = fakeRedisEntry{value: "{}", expires: time.Now().Add(-time.Second)}
	if _, err := cache.Get(context.Background(), "expired-key"); !errors.Is(err, blockcheck.ErrNotFound) {
		t.Fatalf("expired error=%v", err)
	}
}

// A second adapter over the same backing client models a separate process sharing Redis.
func newRedisClientView(client *fakeRedisClient) redisCacheClient { return client }

func TestRedisCacheReportsMalformedPayloadAndBackendErrors(t *testing.T) {
	client := newFakeRedisClient()
	cache := NewRedisCache(client)
	client.entries["bad"] = fakeRedisEntry{value: "{", expires: time.Now().Add(time.Minute)}
	if _, err := cache.Get(context.Background(), "bad"); err == nil || !strings.Contains(err.Error(), "decode block check cache entry") {
		t.Fatalf("decode error=%v", err)
	}
	client.getErr = errors.New("redis read failed")
	if _, err := cache.Get(context.Background(), "key"); err == nil || !strings.Contains(err.Error(), "load block check cache entry") {
		t.Fatalf("get error=%v", err)
	}
	client.setErr = errors.New("redis write failed")
	if err := cache.Save(context.Background(), "key", blockcheck.Result{}, time.Now().Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "save block check cache entry") {
		t.Fatalf("set error=%v", err)
	}
	client.delErr = errors.New("redis delete failed")
	if err := cache.Save(context.Background(), "key", blockcheck.Result{}, time.Now().Add(-time.Second)); err == nil || !strings.Contains(err.Error(), "delete expired") {
		t.Fatalf("delete error=%v", err)
	}
}

func TestRedisCacheRejectsMalformedResultBeforeWriteAndInvalidURL(t *testing.T) {
	cache := NewRedisCache(newFakeRedisClient())
	result := blockcheck.Result{Geo: []byte("{")}
	if err := cache.Save(context.Background(), "key", result, time.Now().Add(time.Minute)); err == nil || !strings.Contains(err.Error(), "encode block check cache entry") {
		t.Fatalf("malformed result save error=%v", err)
	}
	if _, err := OpenRedisCache(context.Background(), "redis://%", ""); err == nil || !strings.Contains(err.Error(), "parse Redis URL") {
		t.Fatalf("invalid Redis URL error=%v", err)
	}
}

func TestRedisCacheContextCancellationAndStartupFailure(t *testing.T) {
	client := newFakeRedisClient()
	cache := NewRedisCache(client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cache.Save(ctx, "key", blockcheck.Result{}, time.Now().Add(time.Minute)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled save error=%v", err)
	}
	if _, err := cache.Get(ctx, "key"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled get error=%v", err)
	}
	client.pingErr = errors.New("redis unavailable")
	client.closeErr = errors.New("close failed")
	if _, err := openRedisCacheWithClient(context.Background(), client); err == nil || !strings.Contains(err.Error(), "connect to Redis") || !strings.Contains(err.Error(), "close failed") || !client.closed {
		t.Fatalf("startup failure=%v closed=%v", err, client.closed)
	}
	if err := cache.Close(); err == nil || !strings.Contains(err.Error(), "close Redis cache") {
		t.Fatalf("close error=%v", err)
	}
}

func TestOpenRedisCacheUsesRedisProtocolAndSharedKeyspace(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan struct{})
	entries := make(map[string]fakeRedisEntry)
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			args, readErr := readRESPCommand(reader)
			if readErr != nil {
				return
			}
			switch strings.ToUpper(args[0]) {
			case "PING":
				_, _ = io.WriteString(conn, "+PONG\r\n")
			case "AUTH":
				_, _ = io.WriteString(conn, "+OK\r\n")
			case "SET":
				ttl := time.Hour
				for i := 3; i+1 < len(args); i++ {
					if strings.EqualFold(args[i], "PX") {
						millis, _ := strconv.Atoi(args[i+1])
						ttl = time.Duration(millis) * time.Millisecond
					}
				}
				entries[args[1]] = fakeRedisEntry{value: args[2], expires: time.Now().Add(ttl)}
				_, _ = io.WriteString(conn, "+OK\r\n")
			case "GET":
				entry, ok := entries[args[1]]
				if !ok || !time.Now().Before(entry.expires) {
					_, _ = io.WriteString(conn, "$-1\r\n")
					continue
				}
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(entry.value), entry.value)
			case "DEL":
				delete(entries, args[1])
				_, _ = io.WriteString(conn, ":1\r\n")
			default:
				_, _ = io.WriteString(conn, "-ERR unsupported command\r\n")
			}
		}
	}()

	cache, err := OpenRedisCache(context.Background(), "redis://:url-password@"+listener.Addr().String(), "env-password")
	if err != nil {
		t.Fatal(err)
	}
	result := blockcheck.Result{ID: "shared-upstream-id", Target: "shared.example", CheckedAt: time.Now()}
	if err := cache.Save(context.Background(), "shared-key", result, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := NewRedisCache(redisClient{client: cache.client.(redisClient).client}).Get(context.Background(), "shared-key")
	if err != nil || got.ID != result.ID || got.Target != result.Target {
		t.Fatalf("shared Redis result=%+v err=%v", got, err)
	}
	if err := cache.Save(context.Background(), "expired-key", result, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	<-serverDone
}

func TestOpenRedisCacheReportsConnectionFailure(t *testing.T) {
	if _, err := OpenRedisCache(context.Background(), "", ""); err == nil || !strings.Contains(err.Error(), "Redis address is required") {
		t.Fatalf("empty Redis address error=%v", err)
	}
	if _, err := OpenRedisCache(context.Background(), "127.0.0.1:1", ""); err == nil || !strings.Contains(err.Error(), "connect to Redis") {
		t.Fatalf("connection error=%v", err)
	}
}

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' {
		return nil, fmt.Errorf("invalid RESP array")
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, count)
	for i := range args {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 4 || line[0] != '$' {
			return nil, fmt.Errorf("invalid RESP bulk string")
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}
