package telegram

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIResponsesAndWrappers(t *testing.T) {
	tests := []struct {
		name, body string
		wantErr    bool
	}{{"ok", `{"ok":true,"result":{"n":3}}`, false}, {"api failure", `{"ok":false,"description":"bad token"}`, true}, {"invalid envelope", `{`, true}, {"invalid result", `{"ok":true,"result":"wrong"}`, true}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/test") {
					t.Errorf("request %s %s", r.Method, r.URL.Path)
				}
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(tt.body))
			}))
			defer srv.Close()
			a := &API{token: "secret", client: srv.Client(), baseURL: srv.URL}
			var got struct {
				N int `json:"n"`
			}
			err := a.Call(context.Background(), "test", map[string]any{"x": 1}, &got)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Call error=%v", err)
			}
			if err == nil && got.N != 3 {
				t.Fatalf("result=%+v", got)
			}
		})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"ok":true,"result":true}`)) }))
	defer srv.Close()
	a := &API{client: srv.Client(), baseURL: srv.URL}
	ctx := context.Background()
	if err := a.Send(ctx, 1, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := a.SendWithMarkup(ctx, 1, "hello", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := a.AnswerCallback(ctx, "id", "ok"); err != nil {
		t.Fatal(err)
	}
}
func TestAPIRequestFailures(t *testing.T) {
	a := &API{client: http.DefaultClient, baseURL: "http://127.0.0.1:1"}
	if err := a.Call(context.Background(), "x", make(chan int), nil); err == nil || !strings.Contains(err.Error(), "encode") {
		t.Fatalf("marshal error=%v", err)
	}
	if err := a.Call(context.Background(), "x", nil, nil); err == nil || !strings.Contains(err.Error(), "call Telegram") {
		t.Fatalf("transport error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Call(ctx, "x", nil, nil); err == nil {
		t.Fatal("expected canceled request")
	}
}
func TestNewAPIProxyValidation(t *testing.T) {
	if _, err := NewAPI("x", "://bad"); err == nil {
		t.Fatal("expected proxy parse error")
	}
	if _, err := NewAPI("x", "socks5://127.0.0.1:1080"); err != nil {
		t.Fatal(err)
	}
}

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failingReadBody struct{}

func (failingReadBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failingReadBody) Close() error             { return nil }
func TestAPIBodyReadFailure(t *testing.T) {
	a := &API{token: "x", client: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: failingReadBody{}, Header: make(http.Header)}, nil
	})}, baseURL: "http://telegram.test"}
	if err := a.Call(context.Background(), "x", nil, nil); err == nil || !strings.Contains(err.Error(), "read Telegram response") {
		t.Fatalf("read failure=%v", err)
	}
}
