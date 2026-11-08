package main

import (
	"context"
	"errors"
	"io"
	"os"
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
	return func(string, time.Duration) (blockCLIService, io.Closer, error) {
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

func TestOpenBlockServiceUsesProvidedSQLitePathAndBaseVerdict(t *testing.T) {
	service, closer, err := openBlockService(t.TempDir()+"/state.sqlite", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if service == nil {
		t.Fatal("openBlockService returned nil service")
	}
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openBlockService(t.TempDir()+"/invalid.sqlite", 0); err == nil {
		t.Fatal("invalid local cache ttl accepted")
	}
	blockedPath := t.TempDir() + "/file"
	if err := os.WriteFile(blockedPath, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openBlockService(blockedPath+"/cache.sqlite", time.Hour); err == nil {
		t.Fatal("invalid database path accepted")
	}
	if baseVerdict(true) != "listed as blocked" || baseVerdict(false) != "not found in block-lists" {
		t.Fatal("base verdict text changed unexpectedly")
	}
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
	factory := func(string, time.Duration) (blockCLIService, io.Closer, error) {
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
	factory := func(string, time.Duration) (blockCLIService, io.Closer, error) {
		return fakeBlockCLI{}, testCloser{err: errors.New("close failed")}, nil
	}
	cmd = newBlockCommandWith(factory)
	cmd.SetArgs([]string{"probe", "key"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "close block check storage") {
		t.Fatalf("probe close failure=%v", err)
	}
}
