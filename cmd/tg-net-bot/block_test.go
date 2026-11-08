package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/theillarion/tg-net-bot/internal/blockcheck"
)

type fakeBlockCLI struct {
	items []blockcheck.BatchItem
	probe blockcheck.ProbeResult
	err   error
}

type testCloser struct{ err error }

func (c testCloser) Close() error { return c.err }

type failedOutput struct{}

func (failedOutput) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

type failAfterWrites struct {
	remaining int
}

func (w *failAfterWrites) Write(p []byte) (int, error) {
	if w.remaining == 0 {
		return 0, errors.New("output unavailable")
	}
	w.remaining--
	return len(p), nil
}

func (f fakeBlockCLI) CheckBatch(_ context.Context, targets []string, emit func(blockcheck.BatchItem)) error {
	for _, item := range f.items {
		emit(item)
	}
	return f.err
}
func (f fakeBlockCLI) Probe(context.Context, string) (blockcheck.ProbeResult, error) {
	return f.probe, f.err
}

func factoryFor(service blockCLIService, err error) blockCLIFactory {
	return func(string, string, time.Duration) (blockCLIService, io.Closer, error) {
		return service, io.NopCloser(strings.NewReader("")), err
	}
}

func TestBlockCommandShowsStructuredPrimaryAndBatchResults(t *testing.T) {
	service := fakeBlockCLI{items: []blockcheck.BatchItem{
		{Target: "a.example", Status: "complete", Result: blockcheck.Result{Target: "a.example", Blocked: false, CacheKey: "abc"}},
		{Target: "b.example", Status: "limited", Error: "Cheburcheck rate limit reached for /api/v1/check"},
		{Target: "c.example", Status: "skipped", Error: "upstream rate limit reached; request was not sent"},
	}}
	cmd := newBlockCommandWith(factoryFor(service, nil))
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"a.example", "b.example", "c.example", "--output", "json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"target":"a.example"`, `"status":"limited"`, `"status":"skipped"`, `"cache_key":"abc"`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q: %s", want, out.String())
		}
	}
}

func TestBlockCommandRejectsBadTargetAndFormats(t *testing.T) {
	for _, args := range [][]string{{"example.com/path"}, {"example.com", "--output", "xml"}} {
		cmd := newBlockCommandWith(factoryFor(fakeBlockCLI{}, nil))
		cmd.SetArgs(args)
		if err := cmd.Execute(); err == nil {
			t.Fatalf("args %v accepted", args)
		}
	}
}

func TestBlockCommandPropagatesServiceAndProbe(t *testing.T) {
	cmd := newBlockCommandWith(factoryFor(fakeBlockCLI{err: errors.New("offline")}, nil))
	cmd.SetArgs([]string{"example.com", "--output", "ndjson"})
	if err := cmd.Execute(); err == nil || err.Error() != "offline" {
		t.Fatalf("batch error=%v", err)
	}
	cmd = newBlockCommandWith(factoryFor(fakeBlockCLI{probe: blockcheck.ProbeResult{Stats: blockcheck.ProbeStats{Online: 4, Responded: 3, Unanswered: 1}}}, nil))
	var out strings.Builder
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"probe", "cache-key"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"responded":3`) {
		t.Fatalf("probe output=%s", out.String())
	}
}

func TestBlockCommandCacheConfigurationAndFactoryFailures(t *testing.T) {
	cmd := newBlockCommandWith(factoryFor(fakeBlockCLI{}, nil))
	cmd.SetArgs([]string{"edge.example", "--cache-ttl", "0s"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "cache ttl") {
		t.Fatalf("invalid cache ttl error=%v", err)
	}
	cmd = newBlockCommandWith(factoryFor(fakeBlockCLI{}, errors.New("storage unavailable")))
	cmd.SetArgs([]string{"edge.example"})
	if err := cmd.Execute(); err == nil || err.Error() != "storage unavailable" {
		t.Fatalf("factory error=%v", err)
	}
	probeErr := errors.New("probe failed")
	cmd = newBlockCommandWith(factoryFor(fakeBlockCLI{err: probeErr}, nil))
	cmd.SetArgs([]string{"probe", "cache-key"})
	if err := cmd.Execute(); err == nil || !errors.Is(err, probeErr) {
		t.Fatalf("probe error=%v", err)
	}
}

func TestBlockCommandsUseSharedRedisConfigAndOverrides(t *testing.T) {
	t.Setenv("REDIS_ADDR", "shared-cache:6379")
	t.Setenv("REDIS_PASSWORD", "redis-test-password")
	t.Setenv("BLOCKCHECK_CACHE_TTL", "45m")
	var gotAddress, gotPassword string
	var gotTTL time.Duration
	factory := func(address, password string, ttl time.Duration) (blockCLIService, io.Closer, error) {
		gotAddress, gotPassword, gotTTL = address, password, ttl
		return fakeBlockCLI{}, io.NopCloser(strings.NewReader("")), nil
	}
	cmd := newBlockCommandWith(factory)
	cmd.SetArgs([]string{"edge.example"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotAddress != "shared-cache:6379" || gotPassword != "redis-test-password" || gotTTL != 45*time.Minute {
		t.Fatalf("env Redis config=(%q,%q,%s)", gotAddress, gotPassword, gotTTL)
	}
	cmd = newBlockCommandWith(factory)
	cmd.SetArgs([]string{"edge.example", "--redis-url", "redis://override:6380", "--cache-ttl", "12m"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotAddress != "redis://override:6380" || gotPassword != "redis-test-password" || gotTTL != 12*time.Minute {
		t.Fatalf("override Redis config=(%q,%q,%s)", gotAddress, gotPassword, gotTTL)
	}
	cmd = newBlockCommandWith(factory)
	cmd.SetArgs([]string{"probe", "cache-key"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotAddress != "shared-cache:6379" || gotPassword != "redis-test-password" || gotTTL != 45*time.Minute {
		t.Fatalf("probe Redis config=(%q,%q,%s)", gotAddress, gotPassword, gotTTL)
	}
	cmd = newBlockCommandWith(factory)
	cmd.SetArgs([]string{"--redis-url", "redis://probe-override:6381", "--cache-ttl", "8m", "probe", "cache-key"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if gotAddress != "redis://probe-override:6381" || gotPassword != "redis-test-password" || gotTTL != 8*time.Minute {
		t.Fatalf("probe override Redis config=(%q,%q,%s)", gotAddress, gotPassword, gotTTL)
	}
}

func TestOpenBlockServiceRejectsInvalidTTLAndBaseVerdict(t *testing.T) {
	if _, _, err := openBlockService("127.0.0.1:6379", "", 0); err == nil {
		t.Fatal("invalid cache ttl accepted")
	}
	if _, _, err := openBlockService("redis://%", "", time.Hour); err == nil || !strings.Contains(err.Error(), "parse Redis URL") {
		t.Fatalf("invalid Redis URL error=%v", err)
	}
	if baseVerdict(true) != "listed as blocked" || baseVerdict(false) != "not found in block-lists" {
		t.Fatal("base verdict text changed unexpectedly")
	}
}

type cliTestBlockCache struct{}

func (cliTestBlockCache) Save(context.Context, string, blockcheck.Result, time.Time) error {
	return nil
}
func (cliTestBlockCache) Get(context.Context, string) (blockcheck.Result, error) {
	return blockcheck.Result{}, blockcheck.ErrNotFound
}

func TestOpenBlockServiceUsesSharedCachePort(t *testing.T) {
	var gotAddress, gotPassword string
	service, closer, err := openBlockServiceWith(context.Background(), "redis.internal:6379", "pw", time.Minute, func(_ context.Context, address, password string) (blockcheck.Cache, io.Closer, error) {
		gotAddress, gotPassword = address, password
		return cliTestBlockCache{}, io.NopCloser(strings.NewReader("")), nil
	})
	if err != nil || service == nil || closer == nil {
		t.Fatalf("service=%v closer=%v err=%v", service, closer, err)
	}
	if gotAddress != "redis.internal:6379" || gotPassword != "pw" {
		t.Fatalf("cache config=(%q,%q)", gotAddress, gotPassword)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
}

type trackingCLICacheCloser struct{ closed bool }

func (c *trackingCLICacheCloser) Close() error { c.closed = true; return nil }

func TestOpenBlockServicePropagatesCacheFailuresAndClosesOnInvalidCache(t *testing.T) {
	wantErr := errors.New("Redis unavailable")
	if _, _, err := openBlockServiceWith(context.Background(), "addr", "", time.Minute, func(context.Context, string, string) (blockcheck.Cache, io.Closer, error) {
		return nil, nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("cache open error=%v", err)
	}
	closer := &trackingCLICacheCloser{}
	if _, _, err := openBlockServiceWith(context.Background(), "addr", "", time.Minute, func(context.Context, string, string) (blockcheck.Cache, io.Closer, error) {
		return nil, closer, nil
	}); err == nil || !closer.closed {
		t.Fatalf("invalid cache error=%v closed=%v", err, closer.closed)
	}
}

func TestOpenBlockServiceConnectsToLocalRedisMock(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			args, err := readCLIRESPCommand(reader)
			if err != nil {
				return
			}
			switch strings.ToUpper(args[0]) {
			case "HELLO":
				_, _ = io.WriteString(conn, "%1\r\n+proto\r\n:3\r\n")
			case "CLIENT", "AUTH":
				_, _ = io.WriteString(conn, "+OK\r\n")
			case "PING":
				_, _ = io.WriteString(conn, "+PONG\r\n")
				return
			default:
				_, _ = io.WriteString(conn, "-ERR unsupported command\r\n")
			}
		}
	}()
	service, closer, err := openBlockService(listener.Addr().String(), "", time.Minute)
	if err != nil || service == nil || closer == nil {
		t.Fatalf("service=%v closer=%v err=%v", service, closer, err)
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	<-serverDone
}

func readCLIRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' {
		return nil, errors.New("invalid RESP array")
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
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
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

func TestBlockCommandTextAndOutputFailure(t *testing.T) {
	service := fakeBlockCLI{items: []blockcheck.BatchItem{{Target: "edge.example", Status: "complete", Result: blockcheck.Result{Target: "edge.example", Blocked: true}}}}
	cmd := newBlockCommandWith(factoryFor(service, nil))
	var output strings.Builder
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"edge.example"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "listed as blocked") || !strings.Contains(output.String(), "30 check/min") {
		t.Fatalf("text result=%q", output.String())
	}
	cmd = newBlockCommandWith(factoryFor(service, nil))
	cmd.SetOut(failedOutput{})
	cmd.SetArgs([]string{"edge.example", "--output", "text"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "write block check output") {
		t.Fatalf("writer failure=%v", err)
	}
}

func TestBlockCommandReportsNDJSONJSONAndProbeWriteFailures(t *testing.T) {
	for _, test := range []struct{ output, want string }{
		{output: "ndjson", want: "write block check output"},
		{output: "json", want: "output unavailable"},
	} {
		cmd := newBlockCommandWith(factoryFor(fakeBlockCLI{items: []blockcheck.BatchItem{{Target: "edge.example", Status: "complete"}}}, nil))
		cmd.SetOut(failedOutput{})
		args := []string{"edge.example", "--output", test.output}
		cmd.SetArgs(args)
		if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s output failure=%v", test.output, err)
		}
	}
	cmd := newBlockCommandWith(factoryFor(fakeBlockCLI{}, nil))
	cmd.SetOut(failedOutput{})
	cmd.SetArgs([]string{"probe", "cache-key"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "write probe output") {
		t.Fatalf("probe output failure=%v", err)
	}
}

func TestBlockCommandStopsBatchWhenNDJSONItemCannotBeWritten(t *testing.T) {
	cmd := newBlockCommandWith(factoryFor(fakeBlockCLI{items: []blockcheck.BatchItem{{Target: "edge.example", Status: "complete"}}}, nil))
	cmd.SetOut(&failAfterWrites{remaining: 1}) // Limits header succeeds; first result line fails.
	cmd.SetArgs([]string{"edge.example", "--output", "ndjson"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "write block check output") {
		t.Fatalf("NDJSON item write failure=%v", err)
	}
}

func TestBlockCommandsReturnCloserFailuresAndRejectInvalidProbeTTL(t *testing.T) {
	factory := func(string, string, time.Duration) (blockCLIService, io.Closer, error) {
		return fakeBlockCLI{}, testCloser{err: errors.New("close failed")}, nil
	}
	cmd := newBlockCommandWith(factory)
	cmd.SetArgs([]string{"edge.example"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "close block check storage") {
		t.Fatalf("close failure=%v", err)
	}
	cmd = newBlockCommandWith(factoryFor(fakeBlockCLI{}, nil))
	cmd.SetArgs([]string{"--cache-ttl", "0s", "probe", "cache-key"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "cache ttl") {
		t.Fatalf("invalid probe ttl=%v", err)
	}
}

func TestProbeCommandReturnsFactoryAndCloseErrors(t *testing.T) {
	cmd := newBlockCommandWith(factoryFor(fakeBlockCLI{}, errors.New("storage unavailable")))
	cmd.SetArgs([]string{"probe", "key"})
	if err := cmd.Execute(); err == nil || err.Error() != "storage unavailable" {
		t.Fatalf("probe factory failure=%v", err)
	}
	factory := func(string, string, time.Duration) (blockCLIService, io.Closer, error) {
		return fakeBlockCLI{}, testCloser{err: errors.New("close failed")}, nil
	}
	cmd = newBlockCommandWith(factory)
	cmd.SetArgs([]string{"probe", "key"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "close block check storage") {
		t.Fatalf("probe close failure=%v", err)
	}
}
