package blockcheck

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/theillarion/tg-net-bot/internal/checker"
)

type Service struct {
	provider  PrimaryChecker
	probe     ProbeProvider
	cache     Cache
	ttl       time.Duration
	mu        sync.Mutex
	nextCheck time.Time
	nextProbe time.Time
	now       func() time.Time
	wait      func(context.Context, time.Duration) error
}

func New(provider PrimaryChecker, cache Cache, ttl time.Duration) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("block check provider is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("block check cache is required")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("block check cache ttl must be positive")
	}
	service := &Service{provider: provider, cache: cache, ttl: ttl, now: time.Now, wait: waitContext}
	if probe, ok := provider.(ProbeProvider); ok {
		service.probe = probe
	}
	return service, nil
}

// Check runs only the lightweight primary request. Probe is an explicit,
// separate action because it has a much lower upstream request limit.
func (s *Service) Check(ctx context.Context, target string) (Result, error) {
	if !checker.ValidHost(target) {
		return Result{}, fmt.Errorf("invalid target: expected an IP or domain")
	}
	if err := s.waitForSlot(ctx, 2*time.Second, &s.nextCheck); err != nil {
		return Result{}, err
	}
	result, err := s.provider.Check(ctx, target)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.CheckedAt = s.now()
	result.ProbeAvailable = result.ID != ""
	if result.ID != "" && s.cache != nil {
		key := make([]byte, 9)
		if _, err := rand.Read(key); err != nil {
			return result, fmt.Errorf("create extended check key: %w", err)
		}
		result.CacheKey = base64.RawURLEncoding.EncodeToString(key)
		if err := s.cache.Save(ctx, result.CacheKey, result, result.CheckedAt.Add(s.ttl)); err != nil {
			return result, fmt.Errorf("cache extended check context: %w", err)
		}
	}
	return result, nil
}

type BatchItem struct {
	Target string `json:"target"`
	Result Result `json:"result,omitempty"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// CheckBatch emits each target's result as soon as its primary request ends.
// Checks are deliberately sequential to stay below the provider's default
// 30 requests/minute limit. A 429 stops this batch and marks remaining items.
func (s *Service) CheckBatch(ctx context.Context, targets []string, emit func(BatchItem)) error {
	if emit == nil {
		return fmt.Errorf("batch result callback is required")
	}
	for i, target := range targets {
		if err := ctx.Err(); err != nil {
			emitCanceledItems(targets[i:], err, emit)
			return err
		}
		result, err := s.Check(ctx, target)
		if err != nil {
			if ctx.Err() != nil {
				canceled := ctx.Err()
				emit(BatchItem{Target: target, Result: result, Status: "canceled", Error: canceled.Error()})
				emitCanceledItems(targets[i+1:], canceled, emit)
				return canceled
			}
			status := "failed"
			if _, ok := err.(*RateLimitError); ok {
				status = "limited"
			}
			emit(BatchItem{Target: target, Result: result, Status: status, Error: err.Error()})
			if status == "limited" {
				for _, remaining := range targets[i+1:] {
					emit(BatchItem{Target: remaining, Status: "skipped", Error: "upstream rate limit reached; request was not sent"})
				}
				return nil
			}
			continue
		}
		emit(BatchItem{Target: target, Result: result, Status: "complete"})
	}
	return nil
}

func emitCanceledItems(targets []string, err error, emit func(BatchItem)) {
	for _, target := range targets {
		emit(BatchItem{Target: target, Status: "canceled", Error: err.Error()})
	}
}

func (s *Service) Probe(ctx context.Context, key string) (ProbeResult, error) {
	return s.ProbeWithEvents(ctx, key, nil)
}

func (s *Service) ProbeWithEvents(ctx context.Context, key string, emit func(ProbeResult)) (ProbeResult, error) {
	result, err := s.cache.Get(ctx, key)
	if err != nil {
		return ProbeResult{}, err
	}
	if !result.ProbeAvailable || result.ID == "" {
		return ProbeResult{}, ErrNotFound
	}
	if s.now().Sub(result.CheckedAt) >= s.ttl {
		return ProbeResult{}, ErrExpired
	}
	if err := s.waitForSlot(ctx, 12*time.Second, &s.nextProbe); err != nil {
		return ProbeResult{}, err
	}
	if s.probe == nil {
		return ProbeResult{}, fmt.Errorf("extended probe is not supported by this provider")
	}
	if streamer, ok := s.probe.(ProbeStreamer); ok {
		return streamer.ProbeWithEvents(ctx, result.ID, emit)
	}
	probeResult, err := s.probe.Probe(ctx, result.ID)
	if err == nil && emit != nil {
		emit(probeResult)
	}
	return probeResult, err
}

func (s *Service) waitForSlot(ctx context.Context, interval time.Duration, next *time.Time) error {
	s.mu.Lock()
	slot := s.now()
	if next.After(slot) {
		slot = *next
	}
	delay := slot.Sub(s.now())
	if delay < 0 {
		delay = 0
	}
	*next = slot.Add(interval)
	s.mu.Unlock()
	return s.wait(ctx, delay)
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
