package blockcheck

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeProvider struct {
	mu            sync.Mutex
	results       []Result
	errors        []error
	calls         int
	probeID       string
	probe         ProbeResult
	probeCalls    int
	secondEntered chan struct{}
	releaseSecond chan struct{}
}

type cancelAfterCheck struct{ cancel context.CancelFunc }

func (p cancelAfterCheck) Check(context.Context, string) (Result, error) {
	p.cancel()
	return Result{ID: "remote-id"}, nil
}

func (p *fakeProvider) Check(_ context.Context, target string) (Result, error) {
	p.mu.Lock()
	i := p.calls
	p.calls++
	p.mu.Unlock()
	if i == 1 && p.secondEntered != nil {
		close(p.secondEntered)
		<-p.releaseSecond
	}
	if i < len(p.errors) && p.errors[i] != nil {
		return Result{}, p.errors[i]
	}
	result := Result{Target: target, ID: "remote-id"}
	if i < len(p.results) {
		result = p.results[i]
		result.Target = target
	}
	return result, nil
}

func (p *fakeProvider) Probe(_ context.Context, id string) (ProbeResult, error) {
	p.mu.Lock()
	p.probeCalls++
	p.probeID = id
	p.mu.Unlock()
	return p.probe, nil
}

type streamingProvider struct {
	*fakeProvider
	progress ProbeResult
	err      error
}

func (p *streamingProvider) ProbeWithEvents(_ context.Context, _ string, emit func(ProbeResult)) (ProbeResult, error) {
	if p.err != nil {
		return ProbeResult{}, p.err
	}
	emit(p.progress)
	return p.probe, nil
}

type fakeCache struct {
	mu      sync.Mutex
	entries map[string]Result
	expiry  map[string]time.Time
	err     error
}

func testTimePtr(value time.Time) *time.Time { return &value }

func newFakeCache() *fakeCache {
	return &fakeCache{entries: map[string]Result{}, expiry: map[string]time.Time{}}
}
func (c *fakeCache) Save(_ context.Context, key string, result Result, expiry time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.entries[key], c.expiry[key] = result, expiry
	return nil
}
func (c *fakeCache) Get(_ context.Context, key string) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return Result{}, c.err
	}
	result, ok := c.entries[key]
	if !ok {
		return Result{}, ErrNotFound
	}
	return result, nil
}

func serviceForTest(t *testing.T, provider PrimaryChecker, cache Cache, ttl time.Duration) *Service {
	t.Helper()
	if cache == nil {
		cache = newFakeCache()
	}
	service, err := New(provider, cache, ttl)
	if err != nil {
		t.Fatal(err)
	}
	service.wait = func(context.Context, time.Duration) error { return nil }
	return service
}

func TestCheckStoresPrimaryContextAndUsesConfiguredRatePolicy(t *testing.T) {
	provider := &fakeProvider{}
	cache := newFakeCache()
	service := serviceForTest(t, provider, cache, time.Hour)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	var intervals []time.Duration
	service.wait = func(_ context.Context, delay time.Duration) error {
		intervals = append(intervals, delay)
		return nil
	}
	result, err := service.Check(context.Background(), "host.example")
	if err != nil {
		t.Fatal(err)
	}
	if result.CacheKey == "" || !result.ProbeAvailable || result.CheckedAt != now {
		t.Fatalf("result=%+v", result)
	}
	if len(intervals) != 1 || intervals[0] != 0 {
		t.Fatalf("first request should not wait: %v", intervals)
	}
	_, err = service.Check(context.Background(), "next.example")
	if err != nil {
		t.Fatal(err)
	}
	if len(intervals) != 2 || intervals[1] != 2*time.Second {
		t.Fatalf("check limit waits=%v", intervals)
	}
	cached, err := cache.Get(context.Background(), result.CacheKey)
	if err != nil || cached.ID != result.ID || cached.Target != result.Target || !cache.expiry[result.CacheKey].Equal(now.Add(time.Hour)) {
		t.Fatalf("cached=%+v err=%v", cached, err)
	}
}

func TestCheckBoundsCacheLifetimeByUpstreamExpiryAndFallback(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		ttl       time.Duration
		upstream  *time.Time
		wantUntil time.Duration
	}{
		{name: "upstream exp before default ttl", ttl: 24 * time.Hour, upstream: testTimePtr(now.Add(4 * time.Minute)), wantUntil: 4 * time.Minute},
		{name: "configured ttl before upstream exp", ttl: time.Hour, upstream: testTimePtr(now.Add(36 * time.Hour)), wantUntil: time.Hour},
		{name: "unknown exp uses fallback", ttl: 24 * time.Hour, wantUntil: 3 * time.Hour},
		{name: "short configured ttl retained for unknown exp", ttl: time.Hour, wantUntil: time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := newFakeCache()
			provider := &fakeProvider{results: []Result{{ID: "remote-id", UpstreamExpiresAt: test.upstream}}}
			service := serviceForTest(t, provider, cache, test.ttl)
			service.now = func() time.Time { return now }
			result, err := service.Check(context.Background(), "edge.example")
			if err != nil {
				t.Fatal(err)
			}
			if result.CacheKey == "" || !result.ProbeAvailable || !cache.expiry[result.CacheKey].Equal(now.Add(test.wantUntil)) {
				t.Fatalf("result=%+v expiry=%s want=%s", result, cache.expiry[result.CacheKey], now.Add(test.wantUntil))
			}
		})
	}
}

func TestCheckSkipsCacheForAlreadyExpiredUpstreamID(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, expiry := range []time.Time{now, now.Add(-time.Second)} {
		cache := newFakeCache()
		provider := &fakeProvider{results: []Result{{ID: "expired-id", UpstreamExpiresAt: &expiry}}}
		service := serviceForTest(t, provider, cache, 24*time.Hour)
		service.now = func() time.Time { return now }
		result, err := service.Check(context.Background(), "edge.example")
		if err != nil || result.ID != "expired-id" || result.CacheKey != "" || result.ProbeAvailable {
			t.Fatalf("primary result=%+v error=%v", result, err)
		}
		if len(cache.entries) != 0 {
			t.Fatalf("expired ID was saved: %+v", cache.entries)
		}
	}
}

func TestCheckBatchPublishesFirstBeforeNextCompletes(t *testing.T) {
	provider := &fakeProvider{secondEntered: make(chan struct{}), releaseSecond: make(chan struct{})}
	service := serviceForTest(t, provider, nil, time.Hour)
	items := make(chan BatchItem, 3)
	done := make(chan error, 1)
	go func() {
		done <- service.CheckBatch(context.Background(), []string{"one.example", "two.example"}, func(item BatchItem) { items <- item })
	}()
	first := <-items
	if first.Target != "one.example" || first.Status != "complete" {
		t.Fatalf("first item=%+v", first)
	}
	<-provider.secondEntered
	select {
	case item := <-items:
		t.Fatalf("unexpected second item before release: %+v", item)
	default:
	}
	close(provider.releaseSecond)
	second := <-items
	if second.Target != "two.example" {
		t.Fatalf("second item=%+v", second)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCheckBatchStopsOnRateLimitAndMarksUnrequested(t *testing.T) {
	provider := &fakeProvider{errors: []error{&RateLimitError{Provider: "Cheburcheck", Route: "/api/v1/check"}}}
	service := serviceForTest(t, provider, nil, time.Hour)
	var items []BatchItem
	if err := service.CheckBatch(context.Background(), []string{"one.example", "two.example"}, func(item BatchItem) { items = append(items, item) }); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Status != "limited" || items[1].Status != "skipped" || provider.calls != 1 {
		t.Fatalf("items=%+v calls=%d", items, provider.calls)
	}
}

func TestProbeUsesCachedRemoteIDAndRateLimit(t *testing.T) {
	provider := &fakeProvider{probe: ProbeResult{Stats: ProbeStats{Online: 4, Responded: 3, Unanswered: 1}}}
	cache := newFakeCache()
	now := time.Now()
	cache.entries["short-key"] = Result{ID: "signed-upstream-id", CacheKey: "short-key", ProbeAvailable: true, CheckedAt: now}
	service := serviceForTest(t, provider, cache, time.Hour)
	service.now = func() time.Time { return now }
	var waited time.Duration
	service.wait = func(_ context.Context, delay time.Duration) error { waited = delay; return nil }
	result, err := service.Probe(context.Background(), "short-key")
	if err != nil {
		t.Fatal(err)
	}
	if provider.probeID != "signed-upstream-id" || waited != 0 || result.Stats.Responded != 3 {
		t.Fatalf("probe=%+v id=%q waited=%s", result, provider.probeID, waited)
	}
	_, err = service.Probe(context.Background(), "short-key")
	if err != nil {
		t.Fatal(err)
	}
	if waited != 12*time.Second {
		t.Fatalf("next probe wait=%s, want 12s", waited)
	}
}

func TestProbeExpiredAndStorageFailure(t *testing.T) {
	provider := &fakeProvider{}
	cache := newFakeCache()
	now := time.Now()
	cache.entries["expired"] = Result{ID: "id", ProbeAvailable: true, CheckedAt: now.Add(-2 * time.Hour)}
	service := serviceForTest(t, provider, cache, time.Hour)
	service.now = func() time.Time { return now }
	if _, err := service.Probe(context.Background(), "expired"); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired error=%v", err)
	}
	cache.err = errors.New("disk unavailable")
	if _, err := service.Probe(context.Background(), "expired"); err == nil || !errors.Is(err, cache.err) {
		t.Fatalf("cache error=%v", err)
	}
}

func TestProbeRechecksUpstreamExpiryAndLegacyFallback(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		result   Result
		wantCall bool
	}{
		{name: "cached upstream expiry elapsed", result: Result{ID: "jwt-id", ProbeAvailable: true, CheckedAt: now.Add(-time.Minute), UpstreamExpiresAt: testTimePtr(now)}},
		{name: "legacy cache uses maximum fallback", result: Result{ID: "legacy-id", ProbeAvailable: true, CheckedAt: now.Add(-3 * time.Hour)}},
		{name: "legacy cache remains usable inside fallback", result: Result{ID: "legacy-id", ProbeAvailable: true, CheckedAt: now.Add(-2 * time.Hour)}, wantCall: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := &fakeProvider{}
			cache := newFakeCache()
			cache.entries["cached"] = test.result
			service := serviceForTest(t, provider, cache, 24*time.Hour)
			service.now = func() time.Time { return now }
			_, err := service.Probe(context.Background(), "cached")
			if test.wantCall {
				if err != nil || provider.probeCalls != 1 {
					t.Fatalf("probe error=%v provider calls=%d", err, provider.probeCalls)
				}
			} else if !errors.Is(err, ErrExpired) || provider.probeCalls != 0 {
				t.Fatalf("probe error=%v provider calls=%d", err, provider.probeCalls)
			}
		})
	}
}

func TestProbeDoesNotCallProviderIfExpiryPassesWhileWaiting(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	upstreamExpiry := now.Add(time.Second)
	provider := &fakeProvider{}
	cache := newFakeCache()
	cache.entries["near-expiry"] = Result{ID: "jwt-id", ProbeAvailable: true, CheckedAt: now, UpstreamExpiresAt: &upstreamExpiry}
	service := serviceForTest(t, provider, cache, 24*time.Hour)
	service.now = func() time.Time { return now }
	service.wait = func(context.Context, time.Duration) error {
		now = now.Add(2 * time.Second)
		return nil
	}
	if _, err := service.Probe(context.Background(), "near-expiry"); !errors.Is(err, ErrExpired) || provider.probeCalls != 0 {
		t.Fatalf("probe error=%v provider calls=%d", err, provider.probeCalls)
	}
}

func TestCheckBatchCancellation(t *testing.T) {
	provider := &fakeProvider{secondEntered: make(chan struct{}), releaseSecond: make(chan struct{})}
	service := serviceForTest(t, provider, nil, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	items := make(chan BatchItem, 4)
	done := make(chan error, 1)
	go func() {
		done <- service.CheckBatch(ctx, []string{"one.example", "two.example", "three.example"}, func(item BatchItem) { items <- item })
	}()
	first := <-items
	if first.Target != "one.example" || first.Status != "complete" {
		t.Fatalf("completed target was not preserved: %+v", first)
	}
	<-provider.secondEntered
	cancel()
	close(provider.releaseSecond)
	current, remaining := <-items, <-items
	if current.Target != "two.example" || current.Status != "canceled" || remaining.Target != "three.example" || remaining.Status != "canceled" {
		t.Fatalf("canceled batch outcomes: current=%+v remaining=%+v", current, remaining)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
}

func TestConcurrentChecksReserveDistinctRateSlots(t *testing.T) {
	provider := &fakeProvider{}
	service := serviceForTest(t, provider, nil, time.Hour)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	delays := make(chan time.Duration, 3)
	service.wait = func(_ context.Context, delay time.Duration) error { delays <- delay; return nil }
	var wg sync.WaitGroup
	for _, target := range []string{"a.example", "b.example", "c.example"} {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			_, _ = service.Check(context.Background(), target)
		}(target)
	}
	wg.Wait()
	close(delays)
	counts := map[time.Duration]int{}
	for delay := range delays {
		counts[delay]++
	}
	if counts[0] != 1 || counts[2*time.Second] != 1 || counts[4*time.Second] != 1 {
		t.Fatalf("reserved delays=%v", counts)
	}
}

func TestServiceValidatesInputsAndPropagatesPrimaryFailures(t *testing.T) {
	if _, err := New(nil, nil, time.Hour); err == nil {
		t.Fatal("nil primary checker was accepted")
	}
	provider := &fakeProvider{errors: []error{errors.New("upstream offline")}}
	if _, err := New(provider, nil, 0); err == nil {
		t.Fatal("non-positive cache ttl was accepted")
	}
	if _, err := New(provider, nil, time.Hour); err == nil {
		t.Fatal("nil cache was accepted")
	}
	service := serviceForTest(t, provider, nil, time.Hour)
	if _, err := service.Check(context.Background(), "bad/path"); err == nil {
		t.Fatal("invalid host was accepted")
	}
	if _, err := service.Check(context.Background(), "edge.example"); err == nil || err.Error() != "upstream offline" {
		t.Fatalf("primary provider error=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	service.wait = waitContext
	if err := service.waitForSlot(canceled, time.Second, &service.nextCheck); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rate wait=%v", err)
	}
}

func TestCheckReportsCacheWriteFailureWithoutDroppingPrimaryResult(t *testing.T) {
	provider := &fakeProvider{}
	cache := newFakeCache()
	cache.err = errors.New("disk full")
	service := serviceForTest(t, provider, cache, time.Hour)
	result, err := service.Check(context.Background(), "edge.example")
	var cacheErr *CacheWriteError
	if err == nil || !errors.Is(err, cache.err) || !errors.As(err, &cacheErr) || result.Target != "edge.example" || result.ProbeAvailable || result.CacheKey != "" {
		t.Fatalf("partial result=%+v error=%v", result, err)
	}
}

func TestCheckBatchPreservesPrimaryResultOnCacheWriteFailure(t *testing.T) {
	cache := newFakeCache()
	cache.err = errors.New("Redis unavailable")
	service := serviceForTest(t, &fakeProvider{}, cache, time.Hour)
	var items []BatchItem
	if err := service.CheckBatch(context.Background(), []string{"edge.example"}, func(item BatchItem) { items = append(items, item) }); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Status != "cache_failed" || !strings.Contains(items[0].Error, "Redis unavailable") || items[0].Result.Target != "edge.example" || items[0].Result.CacheKey != "" || items[0].Result.ProbeAvailable {
		t.Fatalf("batch result=%+v", items)
	}
}

func TestCheckReturnsCanceledContextAfterProviderCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := serviceForTest(t, cancelAfterCheck{cancel: cancel}, nil, time.Hour)
	result, err := service.Check(ctx, "edge.example")
	if !errors.Is(err, context.Canceled) || result.ID != "remote-id" {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

type primaryOnly struct{}

func (primaryOnly) Check(_ context.Context, target string) (Result, error) {
	return Result{Target: target}, nil
}

func TestProbeReportsCacheAndProviderAvailabilityFailures(t *testing.T) {
	if _, err := serviceForTest(t, &fakeProvider{}, nil, time.Hour).Probe(context.Background(), "key"); err == nil {
		t.Fatal("probe without cache was accepted")
	}
	cache := newFakeCache()
	service := serviceForTest(t, &fakeProvider{}, cache, time.Hour)
	if _, err := service.Probe(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing cache key error=%v", err)
	}
	cache.entries["no-probe"] = Result{ID: "id", ProbeAvailable: true, CheckedAt: time.Now()}
	service = serviceForTest(t, primaryOnly{}, cache, time.Hour)
	if _, err := service.Probe(context.Background(), "no-probe"); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("unsupported provider probe error=%v", err)
	}
}

func TestProbeWithEventsUsesProviderStreamAndHonorsCancellation(t *testing.T) {
	ctx := context.Background()
	cache := newFakeCache()
	now := time.Now()
	cache.entries["stream"] = Result{ID: "remote", ProbeAvailable: true, CheckedAt: now}
	provider := &streamingProvider{fakeProvider: &fakeProvider{probe: ProbeResult{Target: "done"}}, progress: ProbeResult{Target: "working"}}
	service := serviceForTest(t, provider, cache, time.Hour)
	service.now = func() time.Time { return now }
	var updates []ProbeResult
	got, err := service.ProbeWithEvents(ctx, "stream", func(update ProbeResult) { updates = append(updates, update) })
	if err != nil || got.Target != "done" || len(updates) != 1 || updates[0].Target != "working" {
		t.Fatalf("probe=%+v updates=%+v err=%v", got, updates, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	service.wait = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	if _, err := service.ProbeWithEvents(canceled, "stream", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe error=%v", err)
	}
}

func TestBatchValidatesCallbackAndContinuesAfterOrdinaryFailure(t *testing.T) {
	provider := &fakeProvider{errors: []error{errors.New("temporary error"), nil}}
	service := serviceForTest(t, provider, nil, time.Hour)
	if err := service.CheckBatch(context.Background(), []string{"a.example"}, nil); err == nil {
		t.Fatal("nil event callback was accepted")
	}
	var items []BatchItem
	if err := service.CheckBatch(context.Background(), []string{"a.example", "b.example"}, func(item BatchItem) { items = append(items, item) }); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Status != "failed" || items[1].Status != "complete" {
		t.Fatalf("batch items=%+v", items)
	}
}

func TestRateLimitMessageAndWaitContext(t *testing.T) {
	if got := (&RateLimitError{Provider: "Cheburcheck", Route: "/check", RetryAfter: 3 * time.Second}).Error(); !strings.Contains(got, "3s") {
		t.Fatalf("rate limit message=%q", got)
	}
	if err := waitContext(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("short wait error=%v", err)
	}
	if err := waitContext(context.Background(), 0); err != nil {
		t.Fatalf("immediate wait error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait error=%v", err)
	}
}
