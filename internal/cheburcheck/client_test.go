package cheburcheck

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/theillarion/tg-net-bot/internal/blockcheck"
)

func TestCheckMakesOnlyPrimaryRequestAndPreservesResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != primaryRoute || r.URL.Query().Get("target") != "a b.example" {
			t.Errorf("unexpected request %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `{"id":"probe-1","target":"a b.example","target_type":"domain","blocked":false,"rkn_domain":"blocked.example","ips":["192.0.2.1"],"blocked_subnets":["192.0.2.0/24"],"cdn_providers":{"Example":[{"name":"edge"}]},"geo":{"country":"RU"},"asn_info":{"asn":64500},"whitelist":{"reason":"allow"},"subnet_size":"24","reverse_lookup":["host.example"],"complaints":[{"date":"2026-01-01"}]}`)
	}))
	defer server.Close()
	result, err := NewWithHTTPClient(server.URL, server.Client()).Check(context.Background(), "a b.example")
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("request count=%d, want just primary request", calls.Load())
	}
	if result.ID != "probe-1" || result.Blocked || result.TargetType != "domain" ||
		result.RKNDomain == nil || *result.RKNDomain != "blocked.example" ||
		len(result.IPs) != 1 || len(result.BlockedSubnets) != 1 ||
		len(result.CDNProviders["Example"]) != 1 || string(result.Geo) != `{"country":"RU"}` ||
		string(result.ASNInfo) != `{"asn":64500}` || result.Whitelist == nil ||
		result.SubnetSize == nil || len(result.ReverseLookup) != 1 || len(result.Complaints) != 1 ||
		!result.ProbeAvailable || result.CheckedAt.IsZero() {
		t.Fatalf("response fields not preserved: %+v", result)
	}
}

func TestCheckExtractsJWTExpiryAsUntrustedCacheBound(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	validID := jwtWithPayload(`{"sub":"probe","exp":` + fmt.Sprint(now.Add(24*time.Hour).Unix()) + `}`)
	for _, test := range []struct {
		name string
		id   string
		want *time.Time
	}{
		{name: "numeric date", id: validID, want: timePtr(now.Add(24 * time.Hour))},
		{name: "fractional numeric date", id: jwtWithPayload(`{"exp":` + fmt.Sprint(now.Add(time.Minute).Unix()) + ".5}"), want: timePtr(now.Add(time.Minute + 500*time.Millisecond))},
		{name: "opaque id", id: "opaque-upstream-id"},
		{name: "bad base64 payload", id: "header.@@@.signature"},
		{name: "malformed payload", id: jwtWithPayload("{")},
		{name: "missing exp", id: jwtWithPayload(`{"sub":"probe"}`)},
		{name: "string exp", id: jwtWithPayload(`{"exp":"1767225600"}`)},
		{name: "null exp", id: jwtWithPayload(`{"exp":null}`)},
		{name: "overflow exp", id: jwtWithPayload(`{"exp":1e1000}`)},
		{name: "wrong token shape", id: "header.payload"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `{"id":%q,"blocked":false}`, test.id)
			}))
			defer server.Close()
			client := NewWithHTTPClient(server.URL, server.Client())
			client.now = func() time.Time { return now }
			result, err := client.Check(context.Background(), "edge.example")
			if err != nil {
				t.Fatalf("primary check failed while parsing expiry: %v", err)
			}
			gotExpiryMissing, wantExpiryMissing := result.UpstreamExpiresAt == nil, test.want == nil
			if result.ID != test.id || !result.ProbeAvailable || gotExpiryMissing != wantExpiryMissing {
				t.Fatalf("result=%+v want exp=%v", result, test.want)
			}
			if test.want != nil && !result.UpstreamExpiresAt.Equal(*test.want) {
				t.Fatalf("expiry=%s want=%s", result.UpstreamExpiresAt, test.want)
			}
		})
	}
}

func jwtWithPayload(payload string) string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".signature"
}

func timePtr(value time.Time) *time.Time { return &value }

func TestCheckClassifiesRateLimitAndRetryAfter(t *testing.T) {
	for _, test := range []struct {
		header string
		want   time.Duration
	}{
		{header: "12", want: 12 * time.Second},
		{header: "n/a", want: 0},
		{header: "", want: 0},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", test.header)
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		_, err := NewWithHTTPClient(server.URL, server.Client()).Check(context.Background(), "x.example")
		server.Close()
		var limited *blockcheck.RateLimitError
		if !errors.As(err, &limited) || limited.Route != primaryRoute || limited.RetryAfter != test.want {
			t.Fatalf("error=%#v, rate limit retry=%s", err, test.want)
		}
	}
}

func TestCheckRejectsMissingRequiredVerdict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"target":"edge.example","id":"probe"}`)
	}))
	defer server.Close()
	if _, err := NewWithHTTPClient(server.URL, server.Client()).Check(context.Background(), "edge.example"); err == nil || !strings.Contains(err.Error(), "required blocked verdict") {
		t.Fatalf("missing verdict error=%v", err)
	}
}

func TestProbeClassifiesExpiredAndUnavailableResponses(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			_, err := NewWithHTTPClient(server.URL, server.Client()).Probe(context.Background(), "probe-id")
			if status == http.StatusNotFound {
				if !errors.Is(err, blockcheck.ErrExpired) {
					t.Fatalf("expired context error=%v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "503") {
				t.Fatalf("upstream error=%v", err)
			}
		})
	}
	if _, err := NewWithHTTPClient("http://invalid", http.DefaultClient).Probe(context.Background(), " "); !errors.Is(err, blockcheck.ErrNotFound) {
		t.Fatalf("empty remote id error=%v", err)
	}
}

func TestProbeErrorsNeverExposeJWTFromURLOrTransport(t *testing.T) {
	id := "secret-header.secret-payload.secret-signature"
	transportError := fmt.Errorf("dial failed for %s: %w", "https://cheburcheck.invalid/api/v1/probe/"+id, errors.New("connection refused"))
	cases := []struct {
		name   string
		client *http.Client
		wantIs error
	}{
		{
			name: "transport error",
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("round trip failed at %s: %w", req.URL, transportError)
			})},
		},
		{
			name: "context cancellation",
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("cancelled request %s: %w", req.URL, context.Canceled)
			})},
			wantIs: context.Canceled,
		},
		{
			name: "context deadline",
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("deadline for %s: %w", req.URL, context.DeadlineExceeded)
			})},
			wantIs: context.DeadlineExceeded,
		},
		{
			name: "stream read error",
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(failedReader{err: fmt.Errorf("stream %s failed", req.URL)}), Request: req}, nil
			})},
		},
		{
			name: "http status text",
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusInternalServerError, Status: "500 " + id, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
			})},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewWithHTTPClient("https://cheburcheck.invalid", test.client).Probe(context.Background(), id)
			if err == nil {
				t.Fatal("expected probe error")
			}
			for _, secret := range append([]string{id}, strings.Split(id, ".")...) {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("probe error leaked JWT segment %q: %v", secret, err)
				}
			}
			if test.wantIs != nil && !errors.Is(err, test.wantIs) {
				t.Fatalf("probe error=%v does not preserve %v", err, test.wantIs)
			}
		})
	}

	badURLClient := NewWithHTTPClient("https://invalid\n"+id, http.DefaultClient)
	_, err := badURLClient.Probe(context.Background(), id)
	if err == nil {
		t.Fatal("invalid probe URL was accepted")
	}
	for _, secret := range append([]string{id}, strings.Split(id, ".")...) {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("request construction error leaked JWT segment %q: %v", secret, err)
		}
	}
}

type failedReader struct{ err error }

func (r failedReader) Read([]byte) (int, error) { return 0, r.err }

func TestClientDefaultsAndHTTPDateRetryAfter(t *testing.T) {
	client := New()
	if client.baseURL != defaultBaseURL || client.httpClient.Timeout != 90*time.Second {
		t.Fatalf("default client=%+v timeout=%s", client, client.httpClient.Timeout)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	delay, ok := parseRetryAfter(now.Add(15*time.Second).Format(http.TimeFormat), now)
	if !ok || delay != 15*time.Second {
		t.Fatalf("HTTP-date Retry-After=(%s,%v)", delay, ok)
	}
	custom := NewWithHTTPClient("http://example.test/", nil)
	if custom.baseURL != "http://example.test" || custom.httpClient.Timeout != 90*time.Second {
		t.Fatalf("nil-client default=%+v", custom)
	}
}

func TestProbeParsesSSEAndDoesNotGuessVerdictClasses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != probeRoute+"probe-1" {
			t.Errorf("probe path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: started\ndata: {\"target\":\"host.example\",\"online_probes\":4}\n\nevent: result\ndata: {\"probe_id\":\"1\",\"verdicts\":[\"ok\"]}\n\nevent: result\ndata: {\"probe_id\":\"2\",\"verdicts\":[\"uncertain\",\"tspu_block\"]}\n\nevent: done\ndata: {\"online_probes\":4,\"response_count\":2}\n\n")
	}))
	defer server.Close()
	var progress []blockcheck.ProbeResult
	result, err := NewWithHTTPClient(server.URL, server.Client()).ProbeWithEvents(context.Background(), "probe-1", func(update blockcheck.ProbeResult) {
		progress = append(progress, update)
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Target != "host.example" || result.Stats.Online != 4 || result.Stats.Responded != 2 ||
		result.Stats.Unanswered != 2 || len(result.Events) != 4 || result.Verdicts["ok"] != 1 ||
		result.Verdicts["uncertain"] != 1 || result.Verdicts["tspu_block"] != 1 || result.Events[3].Type != "done" {
		t.Fatalf("probe result=%+v", result)
	}
	if len(progress) != 4 || progress[1].Stats.Responded != 1 || progress[3].Stats.Responded != 2 {
		t.Fatalf("stream progress=%+v", progress)
	}
}

func TestProbeRateLimitAndMalformedEvent(t *testing.T) {
	t.Run("429", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer server.Close()
		_, err := NewWithHTTPClient(server.URL, server.Client()).Probe(context.Background(), "id")
		var limited *blockcheck.RateLimitError
		if !errors.As(err, &limited) || limited.Route != probeRoute {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, "event: started\ndata: invalid\n\n")
		}))
		defer server.Close()
		_, err := NewWithHTTPClient(server.URL, server.Client()).Probe(context.Background(), "id")
		if err == nil || !strings.Contains(err.Error(), "invalid JSON") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestCheckTransportAndDecodeErrors(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("offline")
		})}
		_, err := NewWithHTTPClient("http://mock", client).Check(context.Background(), "x.example")
		if err == nil || !strings.Contains(err.Error(), "call cheburcheck") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("decode", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "!") }))
		defer server.Close()
		_, err := NewWithHTTPClient(server.URL, server.Client()).Check(context.Background(), "x.example")
		if err == nil || !strings.Contains(err.Error(), "decode cheburcheck response") {
			t.Fatalf("error=%v", err)
		}
	})
}

type failedReadBody struct{}

func (failedReadBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (failedReadBody) Close() error             { return nil }

func TestClientReportsHTTPBodyAndSSEStreamFailures(t *testing.T) {
	client := NewWithHTTPClient("http://mock", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/probe/") {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: failedReadBody{}, Request: req}, nil
		}
		return &http.Response{StatusCode: http.StatusBadGateway, Status: "502 Bad Gateway", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("upstream")), Request: req}, nil
	})})
	if _, err := client.Check(context.Background(), "edge.example"); err == nil || !strings.Contains(err.Error(), "HTTP status 502") {
		t.Fatalf("HTTP failure = %v", err)
	}
	if _, err := client.Probe(context.Background(), "probe-id"); err == nil || !strings.Contains(err.Error(), "read cheburcheck probe stream") {
		t.Fatalf("stream read failure = %v", err)
	}
	if _, err := NewWithHTTPClient("http://mock", &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: failedReadBody{}, Request: req}, nil
	})}).Check(context.Background(), "edge.example"); err == nil || !strings.Contains(err.Error(), "read cheburcheck response") {
		t.Fatalf("body read failure = %v", err)
	}
}

func TestProbeIgnoresUnknownSSEEventsAndRejectsEmptyStreams(t *testing.T) {
	for _, test := range []struct {
		body      string
		wantError bool
	}{
		{body: "", wantError: true},
		{body: ": heartbeat\n\n", wantError: true},
		{body: "event: custom\ndata: {\"opaque\":true}\n\n"},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, test.body) }))
		_, err := NewWithHTTPClient(server.URL, server.Client()).Probe(context.Background(), "id")
		server.Close()
		if test.wantError && (err == nil || !strings.Contains(err.Error(), "no events")) {
			t.Fatalf("body %q error=%v", test.body, err)
		}
		if !test.wantError && err != nil {
			t.Fatalf("unknown event should be preserved: %v", err)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProbeStatsUnknownWhenNoDoneCounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "event: started\ndata: {\"target\":\"x\",\"online_probes\":3}\n\nevent: result\ndata: {\"probe_id\":\"1\",\"verdicts\":[\"ok\"]}\n\n")
	}))
	defer server.Close()
	result, err := NewWithHTTPClient(server.URL, server.Client()).Probe(context.Background(), "id")
	if err != nil {
		t.Fatal(err)
	}
	if result.Stats.Online != 3 || result.Stats.Responded != 1 || result.Stats.Unanswered != 2 {
		t.Fatalf("stats=%+v", result.Stats)
	}
	var roundtrip map[string]any
	if err := json.Unmarshal(result.Events[0].Data, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if _, ok := roundtrip["target"]; !ok {
		t.Fatal(fmt.Sprintf("event lost source fields: %s", result.Events[0].Data))
	}
}
