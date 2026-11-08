// Package blockcheck contains the application contract for block-list and
// extended network checks. It has no dependency on a UI or transport.
package blockcheck

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("block check not found")
	ErrExpired  = errors.New("extended check context expired")
)

// RateLimitError identifies which upstream route rejected a request.
type RateLimitError struct {
	Provider   string        `json:"provider"`
	Route      string        `json:"route"`
	RetryAfter time.Duration `json:"retry_after,omitempty"`
}

// CacheWriteError indicates that the primary result succeeded but its probe
// context could not be persisted for a later explicit probe.
type CacheWriteError struct{ Err error }

func (e *CacheWriteError) Error() string { return "cache extended check context: " + e.Err.Error() }
func (e *CacheWriteError) Unwrap() error { return e.Err }

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return e.Provider + " rate limit reached for " + e.Route + "; retry after " + e.RetryAfter.String()
	}
	return e.Provider + " rate limit reached for " + e.Route
}

// Result preserves all fields returned by the primary check endpoint.
type Result struct {
	ID                string                       `json:"id,omitempty"`
	CacheKey          string                       `json:"cache_key,omitempty"`
	Target            string                       `json:"target"`
	TargetType        string                       `json:"target_type"`
	Blocked           bool                         `json:"blocked"`
	RKNDomain         *string                      `json:"rkn_domain,omitempty"`
	IPs               []string                     `json:"ips"`
	BlockedSubnets    []string                     `json:"blocked_subnets"`
	CDNProviders      map[string][]json.RawMessage `json:"cdn_providers"`
	Geo               json.RawMessage              `json:"geo"`
	ASNInfo           json.RawMessage              `json:"asn_info,omitempty"`
	Whitelist         json.RawMessage              `json:"whitelist,omitempty"`
	SubnetSize        *string                      `json:"subnet_size,omitempty"`
	ReverseLookup     []string                     `json:"reverse_lookup"`
	Complaints        []json.RawMessage            `json:"complaints"`
	ProbeAvailable    bool                         `json:"probe_available"`
	CheckedAt         time.Time                    `json:"checked_at"`
	UpstreamExpiresAt *time.Time                   `json:"upstream_expires_at,omitempty"`
}

type ProbeStats struct {
	Online     int `json:"online"`
	Responded  int `json:"responded"`
	Unanswered int `json:"unanswered"`
	Unknown    int `json:"unknown"`
}

type ProbeResult struct {
	Target   string         `json:"target"`
	Stats    ProbeStats     `json:"stats"`
	Verdicts map[string]int `json:"verdicts"`
	Events   []ProbeEvent   `json:"events"`
}

type ProbeEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// PrimaryChecker implements one lightweight information source. Specialized
// probe support is optional so future providers do not need an unrelated method.
type PrimaryChecker interface {
	Check(context.Context, string) (Result, error)
}

type ProbeProvider interface {
	Probe(context.Context, string) (ProbeResult, error)
}

type ProbeStreamer interface {
	ProbeWithEvents(context.Context, string, func(ProbeResult)) (ProbeResult, error)
}

// Cache stores the primary result and upstream context for an explicit probe.
type Cache interface {
	Save(context.Context, string, Result, time.Time) error
	Get(context.Context, string) (Result, error)
}
