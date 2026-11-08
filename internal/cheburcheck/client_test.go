package cheburcheck

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCheckHTTPAndProbe(t *testing.T) {
	tests := []struct {
		name, check, probe  string
		status, probeStatus int
		want                string
		wantErr             bool
	}{{name: "database verdict", check: `{"blocked":true}`, want: "database: listed as blocked"}, {name: "database false with probe error", check: `{"blocked":false,"id":"abc"}`, probeStatus: 503, want: "database: not found in blocklists; probe returned HTTP 503"}, {name: "probe result", check: `{"id":"x"}`, probe: `data: {"status":"blocked"}

data: {"status":"done"}
`, want: "status=done"}, {name: "raw body", check: `{"hello":"world"}`, want: `{"hello":"world"}`}, {name: "bad JSON", check: `!`, status: 200, wantErr: true}, {name: "check API failure", check: `oops`, status: 502, wantErr: true}, {name: "empty probe without verdict", check: `{"id":"x"}`, wantErr: true}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/check") {
					if r.URL.Query().Get("target") != "a b.example" {
						t.Errorf("target query = %q", r.URL.Query().Get("target"))
					}
					w.WriteHeader(defaultStatus(tt.status))
					fmt.Fprint(w, tt.check)
					return
				}
				if tt.probeStatus != 0 {
					w.WriteHeader(tt.probeStatus)
				} else {
					w.WriteHeader(200)
				}
				fmt.Fprint(w, tt.probe)
			}))
			defer srv.Close()
			c := &Client{httpClient: srv.Client(), baseURL: srv.URL}
			got, err := c.Check(context.Background(), "a b.example")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check error=%v", err)
			}
			if err == nil && !strings.Contains(got, tt.want) {
				t.Fatalf("Check=%q want contains %q", got, tt.want)
			}
		})
	}
}
func defaultStatus(v int) int {
	if v == 0 {
		return 200
	}
	return v
}
func TestCompactInterpretAndCanceled(t *testing.T) {
	if len(compact(strings.Repeat("x", 501))) != 503 {
		t.Fatal("compact should truncate long strings")
	}
	if got := interpret(`{"nested":[{"is_blocked":true}]}`); !strings.Contains(got, "is_blocked=true") {
		t.Fatalf("interpret=%s", got)
	}
	if got := interpret("plain"); got != "plain" {
		t.Fatalf("raw interpret=%q", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Client{httpClient: http.DefaultClient, baseURL: "http://127.0.0.1:1"}
	if _, err := c.Check(ctx, "x"); err == nil {
		t.Fatal("expected canceled request error")
	}
}

func TestCheckProbeFallbacksAndScannerLimit(t *testing.T) {
	cases := []struct {
		name, check, probe string
		want               string
		wantErr            bool
	}{
		{name: "false database only", check: `{"is_blocked":false}`, want: "database: not found in blocklists"},
		{name: "no id raw response", check: `{"status":"unknown"}`, want: `{"status":"unknown"}`},
		{name: "empty probe with verdict", check: `{"blocked":true,"id":"i"}`, want: "database: listed as blocked"},
		{name: "empty probe without verdict", check: `{"id":"i"}`, wantErr: true},
		{name: "probe plain result", check: `{"id":"i"}`, probe: "plain text", want: "plain text"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/check") {
					io.WriteString(w, tt.check)
				} else {
					io.WriteString(w, tt.probe)
				}
			}))
			defer srv.Close()
			got, err := (&Client{httpClient: srv.Client(), baseURL: srv.URL}).Check(context.Background(), "x")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check error=%v", err)
			}
			if err == nil && !strings.Contains(got, tt.want) {
				t.Fatalf("Check=%q want %q", got, tt.want)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/check") {
			io.WriteString(w, `{"blocked":true,"id":"large"}`)
			return
		}
		io.WriteString(w, "data: "+strings.Repeat("x", 1<<20))
	}))
	defer server.Close()
	got, err := (&Client{httpClient: server.Client(), baseURL: server.URL}).Check(context.Background(), "x")
	if err != nil || !strings.Contains(got, "probe read error") {
		t.Fatalf("oversized event result=%q err=%v", got, err)
	}
	_ = New()
}

type cheburRoundTrip func(*http.Request) (*http.Response, error)

func (f cheburRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failedBody struct {
	readErr, closeErr error
	data              string
	read              bool
}

func (b *failedBody) Read(p []byte) (int, error) {
	if b.read {
		return 0, io.EOF
	}
	b.read = true
	if b.readErr != nil {
		return 0, b.readErr
	}
	return copy(p, b.data), io.EOF
}
func (b *failedBody) Close() error { return b.closeErr }
func TestCheckTransportAndBodyErrors(t *testing.T) {
	t.Run("initial request failure", func(t *testing.T) {
		c := &Client{httpClient: &http.Client{Transport: cheburRoundTrip(func(*http.Request) (*http.Response, error) { return nil, fmt.Errorf("offline") })}, baseURL: "http://test"}
		if _, err := c.Check(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "call cheburcheck") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("read failure", func(t *testing.T) {
		c := &Client{httpClient: &http.Client{Transport: cheburRoundTrip(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Status: "200 OK", Body: &failedBody{readErr: io.ErrUnexpectedEOF}, Header: make(http.Header)}, nil
		})}, baseURL: "http://test"}
		if _, err := c.Check(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "read cheburcheck") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("close failure", func(t *testing.T) {
		c := &Client{httpClient: &http.Client{Transport: cheburRoundTrip(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Status: "200 OK", Body: &failedBody{data: `{"blocked":false}`, closeErr: fmt.Errorf("close")}, Header: make(http.Header)}, nil
		})}, baseURL: "http://test"}
		if _, err := c.Check(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "close cheburcheck") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("probe failure with database verdict", func(t *testing.T) {
		calls := 0
		c := &Client{httpClient: &http.Client{Transport: cheburRoundTrip(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"blocked":false,"id":"x"}`)), Header: make(http.Header)}, nil
			}
			return nil, fmt.Errorf("probe offline")
		})}, baseURL: "http://test"}
		got, err := c.Check(context.Background(), "x")
		if err != nil || !strings.Contains(got, "probe unavailable") {
			t.Fatalf("got=%q err=%v", got, err)
		}
	})
	t.Run("probe failure without verdict", func(t *testing.T) {
		calls := 0
		c := &Client{httpClient: &http.Client{Transport: cheburRoundTrip(func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"id":"x"}`)), Header: make(http.Header)}, nil
			}
			return nil, fmt.Errorf("probe offline")
		})}, baseURL: "http://test"}
		if _, err := c.Check(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "call cheburcheck probe") {
			t.Fatalf("error=%v", err)
		}
	})
}
