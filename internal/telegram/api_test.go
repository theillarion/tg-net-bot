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

func TestEditScreenUsesMessageIDAndHTMLKeyboard(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if !strings.HasSuffix(r.URL.Path, "/editMessageText") {
			t.Errorf("method path=%s", r.URL.Path)
		}
		var payload struct {
			ChatID    int64          `json:"chat_id"`
			MessageID int            `json:"message_id"`
			Text      string         `json:"text"`
			ParseMode string         `json:"parse_mode"`
			Markup    map[string]any `json:"reply_markup"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if payload.ChatID != 10 || payload.MessageID != 42 || payload.Text != "<b>screen</b>" || payload.ParseMode != "HTML" || payload.Markup["inline_keyboard"] == nil {
			t.Errorf("edit payload=%+v", payload)
		}
		io.WriteString(w, `{"ok":true,"result":true}`)
	}))
	defer server.Close()
	api := &API{token: "test", client: server.Client(), baseURL: server.URL}
	if err := api.EditScreen(context.Background(), 10, 42, "<b>screen</b>", map[string]any{"inline_keyboard": []any{}}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("edit request was not sent")
	}
}

func TestSendScreenRejectsMalformedOrMissingMessageID(t *testing.T) {
	for _, body := range []string{`{"ok":true,"result":"not-an-object"}`, `{"ok":true,"result":{}}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
		api := &API{token: "test", client: server.Client(), baseURL: server.URL}
		if _, err := api.SendScreen(context.Background(), 10, "screen", nil); err == nil || !strings.Contains(err.Error(), "decode Telegram sendMessage result") {
			t.Fatalf("response %s error=%v", body, err)
		}
		server.Close()
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
