//go:build functional

package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/theillarion/tg-net-bot/internal/checker"
	"github.com/theillarion/tg-net-bot/internal/domain"
	"github.com/theillarion/tg-net-bot/internal/monitor"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type botRepo struct{ state domain.State }

func (r *botRepo) Load(context.Context) (domain.State, error) { return r.state, nil }
func (r *botRepo) InsertTarget(_ context.Context, u int64, v domain.Target) error {
	r.state.Targets[fmtUser(u)] = append(r.state.Targets[fmtUser(u)], v)
	return nil
}
func (r *botRepo) DeleteTarget(context.Context, int64, string) error { return nil }
func (r *botRepo) InsertAlert(_ context.Context, a domain.Alert) error {
	r.state.Alerts = append(r.state.Alerts, a)
	return nil
}
func (r *botRepo) DeleteAlert(context.Context, int64, string) error           { return nil }
func (r *botRepo) SetAlertLastCheck(context.Context, string, time.Time) error { return nil }
func (r *botRepo) SetAlertLastUp(context.Context, string, bool) error         { return nil }
func fmtUser(v int64) string                                                  { return strconv.FormatInt(v, 10) }

type botNetwork struct{}

func (botNetwork) Check(context.Context, string, checker.Method, string) (bool, string) {
	return true, "ok"
}

type botBlock struct{}

func (botBlock) Check(context.Context, string) (string, error) { return "clear", nil }

var _ monitor.Repository = (*botRepo)(nil)

func setupBot(t *testing.T) (*Bot, *int, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	sent := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sent++
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/getUpdates") {
			io.WriteString(w, `{"ok":true,"result":[]}`)
			return
		}
		io.WriteString(w, `{"ok":true,"result":true}`)
	}))
	t.Cleanup(srv.Close)
	api := &API{token: "x", client: srv.Client(), baseURL: srv.URL}
	repo := &botRepo{state: domain.EmptyState()}
	service, err := monitor.New(context.Background(), repo, botNetwork{}, botBlock{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	return NewBot(api, service, map[int64]struct{}{1: {}}, slog.New(slog.NewTextHandler(io.Discard, nil))), &sent, &mu
}
func msg(text string) Message { return Message{Text: text, Chat: Chat{ID: 10}, From: User{ID: 1}} }
func cb(data string) CallbackQuery {
	return CallbackQuery{ID: "cb", From: User{ID: 1}, Message: &Message{Chat: Chat{ID: 10}}, Data: data}
}
func TestBotMessagesAndButtons(t *testing.T) {
	b, _, _ := setupBot(t)
	ctx := context.Background()
	b.handleMessage(ctx, msg("/start"))
	b.handleMessage(ctx, msg("/help"))
	b.handleMessage(ctx, msg("/help@netbot"))
	b.handleMessage(ctx, msg("/unknown"))
	b.handleMessage(ctx, msg(" "))
	// Add server with button guided input.
	b.handleMessage(ctx, msg(buttonAdd))
	b.handleMessage(ctx, msg(""))
	b.handleMessage(ctx, msg("prod"))
	b.handleMessage(ctx, msg("2001:db8::1"))
	b.handleMessage(ctx, msg("/add duplicate invalid/path"))
	b.handleMessage(ctx, msg("/add edge example.com"))
	b.handleMessage(ctx, msg("/hosts"))
	b.handleMessage(ctx, msg(buttonTargets))
	b.handleMessage(ctx, msg(buttonNewRule))
	b.handleMessage(ctx, msg(buttonRules))
	b.handleMessage(ctx, msg(buttonHelp))
	// Guided availability checks, including all methods and invalid targets/ports.
	b.handleMessage(ctx, msg(buttonCheck))
	b.handleMessage(ctx, msg("unknown name"))
	b.handleMessage(ctx, msg("prod"))
	b.handleCallback(ctx, cb("check-method:tcp"))
	b.handleMessage(ctx, msg("99999"))
	b.handleMessage(ctx, msg("443"))
	b.handleCallback(ctx, cb("check-method:http"))
	b.handleCallback(ctx, cb("manual-http:https"))
	b.handleCallback(ctx, cb("check-method:icmp"))
	b.handleMessage(ctx, msg("/check"))
	b.handleMessage(ctx, msg("/check example.com udp"))
	b.handleMessage(ctx, msg("/check edge icmp"))
	b.handleMessage(ctx, msg("/check edge tcp"))
	b.handleMessage(ctx, msg("/check nowhere.example.invalid"))
	b.handleMessage(ctx, msg("/check missing"))
	// Block checks, direct and guided.
	b.handleMessage(ctx, msg(buttonBlock))
	b.handleMessage(ctx, msg("prod"))
	b.handleMessage(ctx, msg("/block"))
	b.handleMessage(ctx, msg("/block edge"))
	b.handleMessage(ctx, msg("/block invalid/path"))
	// Add alert through all three methods and invalid state transitions.
	b.handleMessage(ctx, msg(buttonNewRule))
	b.handleCallback(ctx, cb("alert-target:99"))
	b.handleCallback(ctx, cb("alert-target:0"))
	b.handleCallback(ctx, cb("alert-method:bad"))
	b.handleCallback(ctx, cb("alert-method:tcp"))
	b.handleMessage(ctx, msg("nope"))
	b.handleMessage(ctx, msg("22"))
	b.handleMessage(ctx, msg("0s"))
	b.handleMessage(ctx, msg("2m"))
	b.handleMessage(ctx, msg(buttonRules))
	b.handleCallback(ctx, cb("alert-remove:missing"))
	b.handleCallback(ctx, cb("target-check:99"))
	b.handleCallback(ctx, cb("target-check:0"))
	b.handleCallback(ctx, cb("target-block:0"))
	b.handleCallback(ctx, cb("target-alert:0"))
	b.handleCallback(ctx, cb("alert-method:icmp"))
	b.handleMessage(ctx, msg("1m"))
	b.handleMessage(ctx, msg(buttonRules))
	b.handleMessage(ctx, msg("/alert prod http 30s https"))
	b.handleMessage(ctx, msg("/alerts"))
	b.handleMessage(ctx, msg("/alert_remove"))
	b.handleMessage(ctx, msg("/alert_remove invalid"))
	b.handleMessage(ctx, msg("/remove"))
	b.handleMessage(ctx, msg("/remove prod"))
	b.handleMessage(ctx, msg("/remove nonexistent"))
	b.handleCallback(ctx, cb("target-remove:0"))
	b.handleCallback(ctx, cb("bad"))
	b.handleCallback(ctx, CallbackQuery{ID: "orphan"})
	b.handleMessage(ctx, msg("/add"))
	b.handleMessage(ctx, msg("/start"))
}
func TestBotPendingEdgeCasesAndEmptyLists(t *testing.T) {
	b, _, _ := setupBot(t)
	ctx := context.Background()
	b.handleMessage(ctx, msg(buttonTargets))
	b.handleMessage(ctx, msg(buttonNewRule))
	b.handleMessage(ctx, msg(buttonRules))
	b.handlePending(ctx, 10, 1, interaction{Step: "add-name"}, "  ")
	b.handlePending(ctx, 10, 1, interaction{Step: "block-target"}, "no/path")
	b.handlePending(ctx, 10, 1, interaction{Step: "check-target"}, "no/path")
	b.handlePending(ctx, 10, 1, interaction{Step: "add-alert-interval", Name: "none"}, "bad")
	b.handlePending(ctx, 10, 1, interaction{Step: "add-alert-interval", Name: "none"}, "1m")
	b.handlePending(ctx, 10, 1, interaction{Step: "add-alert-tcp-port", Name: "none"}, "bad")
	b.handleCallback(ctx, cb("alert-http:http"))
	b.handleCallback(ctx, cb("target-remove:0"))
	b.handleCallback(ctx, cb("alert-remove:id"))
	b.handleMessage(ctx, msg("/remove absent"))
	b.handleMessage(ctx, msg("/check x y z more"))
	b.handleMessage(ctx, msg("/block x y"))
	b.handleMessage(ctx, msg("/alert x icmp 0"))
	b.handleMessage(ctx, msg("/alert missing icmp 5m"))
	b.handleMessage(ctx, msg("/alert x icmp nope"))
	b.handleMessage(ctx, msg("/alert x bad 1m"))
	b.handleMessage(ctx, msg("/remove x"))
	b.handleMessage(ctx, msg("/alert_remove x"))
	b.handleMessage(ctx, msg("/add name bad/path"))
	b.handleMessage(ctx, msg(buttonAdd))
	b.handleMessage(ctx, msg("broken"))
	b.handleMessage(ctx, msg("bad/path"))
}
func TestManualCheckRejectsStaleMethodButton(t *testing.T) {
	b, _, _ := setupBot(t)
	ctx := context.Background()
	b.setPending(1, interaction{Step: "check-method", Host: "example.com"})
	b.selectManualMethod(ctx, 10, 1, "icmp")
	if got := b.getPending(1); got.Step != "" {
		t.Fatalf("ICMP selection did not consume interaction: %+v", got)
	}
	b.handleCallback(ctx, cb("check-method:tcp"))
	if got := b.getPending(1); got.Step != "" {
		t.Fatalf("stale TCP button created pending interaction: %+v", got)
	}
	b.handleCallback(ctx, cb("manual-http:https"))
	if got := b.getPending(1); got.Step != "" {
		t.Fatalf("stale HTTP button created pending interaction: %+v", got)
	}
	b.setPending(1, interaction{Step: "check-method", Host: "example.com"})
	b.handleCallback(ctx, cb("check-method:tcp"))
	if got := b.getPending(1); got.Step != "manual-tcp-port" || got.Host != "example.com" {
		t.Fatalf("valid TCP selection lost its host: %+v", got)
	}
	b.setPending(1, interaction{Step: "check-method", Host: "example.com"})
	b.handleCallback(ctx, cb("check-method:http"))
	if got := b.getPending(1); got.Step != "manual-http" || got.Host != "example.com" {
		t.Fatalf("valid HTTP selection lost its host: %+v", got)
	}
	b.handleCallback(ctx, cb("manual-http:https"))
	if got := b.getPending(1); got.Step != "" {
		t.Fatalf("HTTPS selection did not consume interaction: %+v", got)
	}
	b.setPending(1, interaction{Step: "check-method", Host: "example.com"})
	b.selectManualMethod(ctx, 10, 1, "unsupported")
	b.setPending(1, interaction{Step: "check-method"})
	b.selectManualMethod(ctx, 10, 1, "tcp")
	if got := b.getPending(1); got.Step != "check-method" {
		t.Fatalf("invalid host interaction was accepted: %+v", got)
	}
}

func TestBotHelpersAndRunCancellation(t *testing.T) {
	b, _, _ := setupBot(t)
	if got := at([]string{"a"}, 3); got != "" {
		t.Fatal(got)
	}
	if got := methodKeyboard("m"); got[0][0].CallbackData != "m:icmp" {
		t.Fatal(got)
	}
	b.enqueue(context.Background(), 1, Update{Message: &Message{Text: "/start", Chat: Chat{ID: 1}, From: User{ID: 1}}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	if (Update{}).userID() != 0 {
		t.Fatal("empty update user id should be zero")
	}
	if (Update{CallbackQuery: &CallbackQuery{From: User{ID: 9}}}).userID() != 9 {
		t.Fatal("callback user id")
	}
	var payload map[string]any
	_ = json.Unmarshal([]byte(`{"ok":true}`), &payload)
}

func TestBotRunPollingAndFailure(t *testing.T) {
	var mu sync.Mutex
	var logs bytes.Buffer
	messages := 0
	var cancel context.CancelFunc
	ctx, cancelFn := context.WithCancel(context.Background())
	cancel = cancelFn
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/getUpdates") {
			time.AfterFunc(100*time.Millisecond, cancel)
			io.WriteString(w, `{"ok":true,"result":[{"update_id":5,"message":{"text":"/start","chat":{"id":10},"from":{"id":1}}},{"update_id":6,"message":{"text":"/start","chat":{"id":10},"from":{"id":99}}},{"update_id":7,"callback_query":{"id":"c","from":{"id":1},"message":{"chat":{"id":10}},"data":"invalid"}}]}`)
			return
		}
		mu.Lock()
		messages++
		mu.Unlock()
		io.WriteString(w, `{"ok":true,"result":true}`)
	}))
	defer srv.Close()
	api := &API{client: srv.Client(), baseURL: srv.URL}
	svc, err := monitor.New(context.Background(), &botRepo{state: domain.EmptyState()}, botNetwork{}, botBlock{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	bot := NewBot(api, svc, map[int64]struct{}{1: {}}, slog.New(slog.NewTextHandler(&logs, nil)))
	if err = bot.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	if !strings.Contains(logs.String(), "Telegram polling connected") {
		t.Fatalf("successful Telegram polling was not logged: %s", logs.String())
	}
	mu.Lock()
	got := messages
	mu.Unlock()
	if got == 0 {
		t.Fatal("authorized update did not produce a response")
	}
	// API failures are logged, then polling exits promptly when its context is canceled.
	failureCtx, failureCancel := context.WithCancel(context.Background())
	defer failureCancel()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.AfterFunc(30*time.Millisecond, failureCancel)
		io.WriteString(w, "not json")
	}))
	defer bad.Close()
	badAPI := &API{client: bad.Client(), baseURL: bad.URL}
	badSvc, _ := monitor.New(context.Background(), &botRepo{state: domain.EmptyState()}, botNetwork{}, botBlock{}, badAPI, nil)
	if err = NewBot(badAPI, badSvc, map[int64]struct{}{}, nil).Run(failureCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run after poll failure=%v", err)
	}
}

type botBlockFailure struct{}

func (botBlockFailure) Check(context.Context, string) (string, error) {
	return "", errors.New("block API failed")
}

type botFailWriter struct{}

func (botFailWriter) Write([]byte) (int, error) { return 0, errors.New("Telegram unavailable") }
func TestBotErrorResponsesAndStateDisplay(t *testing.T) {
	ctx := context.Background()
	api := &API{token: "x", client: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })}, baseURL: "http://test"}
	target := domain.Target{Name: "edge", Host: "example.com"}
	up, down := true, false
	repo := &botRepo{state: domain.State{Targets: map[string][]domain.Target{"1": {target}}, Alerts: []domain.Alert{{ID: "up", ChatID: 1, UserID: 1, Target: target, Method: "icmp", LastUp: &up}, {ID: "down", ChatID: 1, UserID: 1, Target: target, Method: "tcp", LastUp: &down}}}}
	svc, err := monitor.New(ctx, repo, botNetwork{}, botBlockFailure{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	b := NewBot(api, svc, map[int64]struct{}{1: {}}, nil)
	b.send(ctx, 1, "x")
	b.sendInline(ctx, 1, "x", nil)
	b.handleCallback(ctx, cb("target-check:0"))
	b.manualBlock(ctx, 1, "example.com")
	b.listAlerts(ctx, 1, 1)
	b.handleMessage(ctx, msg("/check example.com icmp extra too-many"))
	b.handleMessage(ctx, msg("/block nowhere.example.invalid"))
	b.handleTargetAction(ctx, 1, 1, "unknown", "0")
}

func TestBotPrimaryUserJourney(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var sent []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if strings.HasSuffix(r.URL.Path, "/sendMessage") {
			mu.Lock()
			sent = append(sent, payload)
			mu.Unlock()
		}
		io.WriteString(w, `{"ok":true,"result":true}`)
	}))
	defer server.Close()
	api := &API{token: "test", client: server.Client(), baseURL: server.URL}
	repo := &botRepo{state: domain.EmptyState()}
	service, err := monitor.New(ctx, repo, botNetwork{}, botBlock{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	bot := NewBot(api, service, map[int64]struct{}{1: {}}, nil)
	bot.handleMessage(ctx, msg("/start"))
	bot.handleMessage(ctx, msg("/add edge 2001:db8::7"))
	if target, ok := service.FindTarget(1, "edge"); !ok || target.Host != "2001:db8::7" {
		t.Fatalf("saved target = %#v, found=%v", target, ok)
	}
	bot.handleMessage(ctx, msg("/check edge tcp 443"))
	bot.handleMessage(ctx, msg("/block edge"))
	bot.handleMessage(ctx, msg("/alert edge tcp 5m 443"))
	alerts := service.Alerts(1)
	if len(alerts) != 1 || alerts[0].Interval != 5*time.Minute || alerts[0].Method != "tcp" || alerts[0].Argument != "443" {
		t.Fatalf("saved alert = %#v", alerts)
	}
	bot.handleMessage(ctx, msg("/alerts"))
	bot.handleCallback(ctx, cb("alert-remove:"+alerts[0].ID))
	if len(service.Alerts(1)) != 0 {
		t.Fatal("alert removal left a rule")
	}
	bot.handleMessage(ctx, msg("/remove edge"))
	if _, ok := service.FindTarget(1, "edge"); ok {
		t.Fatal("target removal left a server")
	}
	mu.Lock()
	defer mu.Unlock()
	texts := make([]string, 0, len(sent))
	hasKeyboard := false
	for _, payload := range sent {
		var text string
		_ = json.Unmarshal(payload["text"], &text)
		texts = append(texts, text)
		if len(payload["reply_markup"]) > 0 {
			hasKeyboard = true
		}
	}
	joined := strings.Join(texts, "\n")
	for _, want := range []string{"Выбери действие кнопкой", "Сохранил «edge»", "доступен", "Cheburcheck для 2001:db8::7: clear", "каждые 5m", "Алертов пока нет", "Сервер и его алерты удалены"} {
		if !strings.Contains(joined, want) {
			t.Errorf("bot responses missing %q; got:\n%s", want, joined)
		}
	}
	if !hasKeyboard {
		t.Fatal("/start did not provide a button menu")
	}
}
