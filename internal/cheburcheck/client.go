package cheburcheck

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/theillarion/tg-net-bot/internal/blockcheck"
)

const (
	defaultBaseURL = "https://cheburcheck.ru"
	primaryRoute   = "/api/v1/check"
	probeRoute     = "/api/v1/probe/"
)

type Client struct {
	httpClient *http.Client
	baseURL    string
	now        func() time.Time
}

func New() *Client {
	return NewWithHTTPClient(defaultBaseURL, &http.Client{Timeout: 90 * time.Second})
}

func NewWithHTTPClient(baseURL string, client *http.Client) *Client {
	if client == nil {
		client = &http.Client{Timeout: 90 * time.Second}
	}
	return &Client{httpClient: client, baseURL: strings.TrimRight(baseURL, "/"), now: time.Now}
}

// Check makes exactly one lightweight request and preserves the complete
// documented primary response. The extended probe is never started here.
func (c *Client) Check(ctx context.Context, target string) (blockcheck.Result, error) {
	endpoint := c.baseURL + primaryRoute + "?target=" + url.QueryEscape(target)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return blockcheck.Result{}, fmt.Errorf("create cheburcheck request: %w", err)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return blockcheck.Result{}, fmt.Errorf("call cheburcheck: %w", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return blockcheck.Result{}, fmt.Errorf("read cheburcheck response: %w", err)
	}
	if err := responseError(res, primaryRoute); err != nil {
		return blockcheck.Result{}, err
	}
	var result blockcheck.Result
	if err := json.Unmarshal(body, &result); err != nil {
		return blockcheck.Result{}, fmt.Errorf("decode cheburcheck response: %w", err)
	}
	var required struct {
		Blocked *bool `json:"blocked"`
	}
	if err := json.Unmarshal(body, &required); err != nil || required.Blocked == nil {
		return blockcheck.Result{}, fmt.Errorf("decode cheburcheck response: required blocked verdict is missing")
	}
	if result.Target == "" {
		result.Target = target
	}
	result.CheckedAt = c.now()
	result.ProbeAvailable = result.ID != ""
	return result, nil
}

// Probe runs the expensive per-scanner check only on explicit request.
func (c *Client) Probe(ctx context.Context, id string) (blockcheck.ProbeResult, error) {
	return c.ProbeWithEvents(ctx, id, nil)
}

func (c *Client) ProbeWithEvents(ctx context.Context, id string, emit func(blockcheck.ProbeResult)) (blockcheck.ProbeResult, error) {
	if strings.TrimSpace(id) == "" {
		return blockcheck.ProbeResult{}, blockcheck.ErrNotFound
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+probeRoute+url.PathEscape(id), nil)
	if err != nil {
		return blockcheck.ProbeResult{}, fmt.Errorf("create cheburcheck probe request: %w", err)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return blockcheck.ProbeResult{}, fmt.Errorf("call cheburcheck probe: %w", err)
	}
	defer res.Body.Close()
	if err := responseError(res, probeRoute); err != nil {
		return blockcheck.ProbeResult{}, err
	}

	result := blockcheck.ProbeResult{Events: make([]blockcheck.ProbeEvent, 0), Verdicts: make(map[string]int)}
	scanner := bufio.NewScanner(io.LimitReader(res.Body, 2<<20))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	eventName := ""
	var data bytes.Buffer
	flush := func() error {
		if data.Len() == 0 {
			return nil
		}
		raw := append(json.RawMessage(nil), bytes.TrimSpace(data.Bytes())...)
		if !json.Valid(raw) {
			return fmt.Errorf("decode cheburcheck probe event: invalid JSON")
		}
		result.Events = append(result.Events, blockcheck.ProbeEvent{Type: eventName, Data: raw})
		switch eventName {
		case "started":
			var started struct {
				Target       string `json:"target"`
				OnlineProbes int    `json:"online_probes"`
			}
			if err := json.Unmarshal(raw, &started); err != nil {
				return err
			}
			result.Target = started.Target
			result.Stats.Online = started.OnlineProbes
		case "result":
			result.Stats.Responded++
			var response struct {
				Verdicts []string `json:"verdicts"`
			}
			if err := json.Unmarshal(raw, &response); err != nil {
				return err
			}
			for _, verdict := range response.Verdicts {
				result.Verdicts[verdict]++
			}
		case "done":
			var done struct {
				OnlineProbes  *int `json:"online_probes"`
				ResponseCount *int `json:"response_count"`
			}
			if err := json.Unmarshal(raw, &done); err != nil {
				return err
			}
			if done.OnlineProbes != nil {
				result.Stats.Online = *done.OnlineProbes
			}
			if done.ResponseCount != nil {
				result.Stats.Responded = *done.ResponseCount
			}
		}
		if result.Stats.Online > result.Stats.Responded {
			result.Stats.Unanswered = result.Stats.Online - result.Stats.Responded
		}
		if emit != nil {
			emit(result)
		}
		data.Reset()
		eventName = ""
		return nil
	}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			if err := flush(); err != nil {
				return result, err
			}
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("read cheburcheck probe stream: %w", err)
	}
	if err := flush(); err != nil {
		return result, err
	}
	if len(result.Events) == 0 {
		return result, fmt.Errorf("cheburcheck probe returned no events")
	}
	if result.Stats.Online > result.Stats.Responded {
		result.Stats.Unanswered = result.Stats.Online - result.Stats.Responded
	}
	return result, nil
}

func responseError(res *http.Response, route string) error {
	if route == probeRoute && res.StatusCode == http.StatusNotFound {
		return blockcheck.ErrExpired
	}
	if res.StatusCode == http.StatusTooManyRequests {
		retryAfter, _ := parseRetryAfter(res.Header.Get("Retry-After"), time.Now())
		return &blockcheck.RateLimitError{Provider: "Cheburcheck", Route: route, RetryAfter: retryAfter}
	}
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("cheburcheck returned HTTP %s", res.Status)
	}
	return nil
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := when.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

var _ blockcheck.PrimaryChecker = (*Client)(nil)
var _ blockcheck.ProbeProvider = (*Client)(nil)
