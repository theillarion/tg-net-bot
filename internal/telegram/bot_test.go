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

type failedAlertInsertRepo struct{ *botRepo }

func (failedAlertInsertRepo) InsertAlert(context.Context, domain.Alert) error {
	return errors.New("alert database unavailable")
}

type testInteractionStore struct{ states map[int64]domain.Interaction }

type failedInteractionStore struct{ err error }

type synchronizedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func (s failedInteractionStore) LoadInteractions(context.Context) (map[int64]domain.Interaction, error) {
	return nil, s.err
}
func (s failedInteractionStore) SaveInteraction(context.Context, int64, domain.Interaction) error {
	return s.err
}
func (s failedInteractionStore) DeleteInteraction(context.Context, int64) error { return s.err }

func (s *testInteractionStore) LoadInteractions(context.Context) (map[int64]domain.Interaction, error) {
	loaded := make(map[int64]domain.Interaction, len(s.states))
	for userID, state := range s.states {
		loaded[userID] = state
	}
	return loaded, nil
}
func (s *testInteractionStore) SaveInteraction(_ context.Context, userID int64, state domain.Interaction) error {
	if s.states == nil {
		s.states = make(map[int64]domain.Interaction)
	}
	s.states[userID] = state
	return nil
}
func (s *testInteractionStore) DeleteInteraction(_ context.Context, userID int64) error {
	delete(s.states, userID)
	return nil
}

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
	store := &testInteractionStore{states: make(map[int64]domain.Interaction)}
	b.store = store
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
	if got := b.getPending(1); got.Step != "manual-tcp-port" || store.states[1] != got {
		t.Fatalf("invalid port discarded persisted form state: memory=%+v stored=%+v", got, store.states[1])
	}
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
	alertDraft := b.getPending(1)
	if alertDraft.Step != "add-alert-interval" || alertDraft.RequestID == "" {
		t.Fatalf("alert draft was not persisted at interval step: %+v", alertDraft)
	}
	b.handleCallback(ctx, cb("alert-target:0")) // Repeated click from the old server picker.
	if got := b.getPending(1); got != alertDraft {
		t.Fatalf("stale alert-target callback changed current draft: got=%+v want=%+v", got, alertDraft)
	}
	b.handleCallback(ctx, cb("alert-method:tcp")) // Repeated click from the old method picker.
	if got := b.getPending(1); got != alertDraft {
		t.Fatalf("stale alert-method callback changed current draft: got=%+v want=%+v", got, alertDraft)
	}
	b.handleMessage(ctx, msg("1m"))
	b.handleCallback(ctx, cb("target-alert:0"))
	b.handleCallback(ctx, cb("alert-method:http"))
	b.handleCallback(ctx, cb("alert-http:https"))
	b.handleMessage(ctx, msg("2m"))
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
	b.setPending(ctx, 10, 1, interaction{Step: "check-method", Host: "example.com"})
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
	b.setPending(ctx, 10, 1, interaction{Step: "check-method", Host: "example.com"})
	b.handleCallback(ctx, cb("check-method:tcp"))
	if got := b.getPending(1); got.Step != "manual-tcp-port" || got.Host != "example.com" {
		t.Fatalf("valid TCP selection lost its host: %+v", got)
	}
	b.setPending(ctx, 10, 1, interaction{Step: "check-method", Host: "example.com"})
	b.handleCallback(ctx, cb("check-method:http"))
	if got := b.getPending(1); got.Step != "manual-http" || got.Host != "example.com" {
		t.Fatalf("valid HTTP selection lost its host: %+v", got)
	}
	b.handleCallback(ctx, cb("manual-http:https"))
	if got := b.getPending(1); got.Step != "" {
		t.Fatalf("HTTPS selection did not consume interaction: %+v", got)
	}
	b.setPending(ctx, 10, 1, interaction{Step: "check-method", Host: "example.com"})
	b.selectManualMethod(ctx, 10, 1, "unsupported")
	b.setPending(ctx, 10, 1, interaction{Step: "check-method"})
	b.selectManualMethod(ctx, 10, 1, "tcp")
	if got := b.getPending(1); got.Step != "check-method" {
		t.Fatalf("invalid host interaction was accepted: %+v", got)
	}
}

func TestBotRestoresAndRetainsFormState(t *testing.T) {
	base, _, _ := setupBot(t)
	state := domain.Interaction{Step: "manual-tcp-port", Host: "illarion.tech", Method: "tcp"}
	store := &testInteractionStore{states: map[int64]domain.Interaction{1: state}}
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bot.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want cancellation", err)
	}
	if got := bot.getPending(1); got != state {
		t.Fatalf("restored state = %+v, want %+v", got, state)
	}
	bot.handleMessage(context.Background(), msg("70000"))
	if got := bot.getPending(1); got != state || store.states[1] != state {
		t.Fatalf("invalid input lost state: memory=%+v persisted=%+v", got, store.states[1])
	}
	bot.handleMessage(context.Background(), msg("443"))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("valid input did not finish form: %+v", got)
	}
	if _, ok := store.states[1]; ok {
		t.Fatalf("completed form remains persisted: %+v", store.states[1])
	}
}

func TestRepeatedAddHostSubmissionAfterRestartIsIdempotent(t *testing.T) {
	base, _, _ := setupBot(t)
	ctx := context.Background()
	if err := base.service.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	state := domain.Interaction{Step: "add-host", Name: "edge"}
	store := &testInteractionStore{states: map[int64]domain.Interaction{1: state}}
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, store)
	loadCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := bot.Run(loadCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want cancellation", err)
	}
	bot.handleMessage(ctx, msg("example.com"))
	if targets := base.service.Targets(1); len(targets) != 1 {
		t.Fatalf("duplicate input created %d targets, want 1", len(targets))
	}
	if _, ok := store.states[1]; ok {
		t.Fatalf("completed replay left dialog state: %+v", store.states[1])
	}
	if err := base.service.AddTarget(ctx, 1, "used", "old.example"); err != nil {
		t.Fatal(err)
	}
	if !bot.setPending(ctx, 10, 1, interaction{Step: "add-host", Name: "used"}) {
		t.Fatal("could not seed duplicate-name form")
	}
	bot.handleMessage(ctx, msg("new.example"))
	if got := bot.getPending(1); got.Step != "add-name" {
		t.Fatalf("duplicate name did not return to name entry: %+v", got)
	}
}

func TestAlertCreationRetryAfterRestartIsIdempotent(t *testing.T) {
	base, _, _ := setupBot(t)
	ctx := context.Background()
	if err := base.service.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	state := interaction{Step: "add-alert-interval", Name: "edge", Host: "example.com", Method: "tcp", Argument: "443", RequestID: "form-request-42"}
	// Simulate a crash after the alert insert committed but before the form was cleared.
	if _, err := base.service.AddAlertWithID(ctx, state.RequestID, 10, 1, state.Name, state.Method, state.Argument, time.Minute); err != nil {
		t.Fatal(err)
	}
	store := &testInteractionStore{states: map[int64]domain.Interaction{1: state}}
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, store)
	loadCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := bot.Run(loadCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want cancellation", err)
	}
	bot.handleMessage(ctx, msg("1m"))
	if alerts := base.service.Alerts(1); len(alerts) != 1 || alerts[0].ID != state.RequestID {
		t.Fatalf("retry created duplicate alert: %+v", alerts)
	}
	if _, ok := store.states[1]; ok {
		t.Fatalf("completed retry left dialog state: %+v", store.states[1])
	}
}

func TestLegacyAlertFormReceivesPersistedRequestIDBeforeCreate(t *testing.T) {
	base, _, _ := setupBot(t)
	ctx := context.Background()
	if err := base.service.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	store := &testInteractionStore{states: map[int64]domain.Interaction{}}
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, store)
	bot.createAlert(ctx, 10, 1, interaction{Step: "add-alert-interval", Name: "edge", Method: "tcp", Argument: "443"}, time.Minute)
	alerts := base.service.Alerts(1)
	if len(alerts) != 1 || alerts[0].ID == "" {
		t.Fatalf("alert created without a request ID: %+v", alerts)
	}
	if _, ok := store.states[1]; ok {
		t.Fatalf("completed legacy form remains persisted: %+v", store.states[1])
	}
}

func TestAlertFormRemainsAfterAlertStorageFailure(t *testing.T) {
	ctx := context.Background()
	state := domain.EmptyState()
	repo := &failedAlertInsertRepo{botRepo: &botRepo{state: state}}
	api := &API{token: "test", client: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":true}`))}, nil
	})}, baseURL: "http://test"}
	service, err := monitor.New(ctx, repo, botNetwork{}, botBlock{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	draft := interaction{Step: "add-alert-interval", Name: "edge", Host: "example.com", Method: "tcp", Argument: "443", RequestID: "form-stable-id"}
	store := &testInteractionStore{states: map[int64]domain.Interaction{1: draft}}
	bot := NewBot(api, service, map[int64]struct{}{1: {}}, slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	bot.pending[1] = draft
	bot.handlePending(ctx, 10, 1, draft, "5m")
	if got := bot.getPending(1); got != draft || store.states[1] != draft {
		t.Fatalf("storage error discarded alert form: memory=%+v persisted=%+v", got, store.states[1])
	}
	if len(service.Alerts(1)) != 0 {
		t.Fatalf("failed alert insert changed state: %+v", service.Alerts(1))
	}
}

func TestBotKeepsPreviousStateWhenPersistenceFails(t *testing.T) {
	base, _, _ := setupBot(t)
	want := interaction{Step: "add-name", Name: "preserve-me"}
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, failedInteractionStore{err: errors.New("disk full")})
	bot.pending[1] = want
	if bot.setPending(context.Background(), 10, 1, interaction{Step: "add-host", Name: "new"}) {
		t.Fatal("setPending succeeded with failed storage")
	}
	if got := bot.getPending(1); got != want {
		t.Fatalf("failed save replaced previous state: %+v", got)
	}
	if bot.clearPending(context.Background(), 10, 1) {
		t.Fatal("clearPending succeeded with failed storage")
	}
	if got := bot.getPending(1); got != want {
		t.Fatalf("failed clear discarded previous state: %+v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bot.Run(ctx); err == nil || !strings.Contains(err.Error(), "restore Telegram dialog state") {
		t.Fatalf("Run error after restore failure = %v", err)
	}
}

func TestPersistenceFailureIsReportedBeforeFormAdvances(t *testing.T) {
	base, _, _ := setupBot(t)
	var messages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		messages = append(messages, payload.Text)
		io.WriteString(w, `{"ok":true,"result":true}`)
	}))
	defer server.Close()
	api := &API{token: "test", client: server.Client(), baseURL: server.URL}
	bot := NewBot(api, base.service, map[int64]struct{}{1: {}}, base.logger, failedInteractionStore{err: errors.New("database unavailable")})
	bot.handleMessage(context.Background(), msg(buttonCheck))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("form advanced after failed persistence: %+v", got)
	}
	if len(messages) != 1 || !strings.Contains(messages[0], "Не удалось сохранить состояние формы") {
		t.Fatalf("persistence failure message = %q", messages)
	}
}

func TestFormTransitionsDoNotAdvanceWhenPersistenceFails(t *testing.T) {
	base, _, _ := setupBot(t)
	ctx := context.Background()
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, failedInteractionStore{err: errors.New("database unavailable")})
	for _, button := range []string{buttonCheck, buttonBlock, buttonAdd} {
		bot.handleMessage(ctx, msg(button))
		if got := bot.getPending(1); got.Step != "" {
			t.Fatalf("button %q advanced without persistence: %+v", button, got)
		}
	}
	bot.handlePending(ctx, 10, 1, interaction{Step: "check-target"}, "example.com")
	bot.pending[1] = interaction{Step: "check-method", Host: "example.com"}
	bot.selectManualMethod(ctx, 10, 1, "tcp")
	if got := bot.getPending(1); got.Step != "check-method" {
		t.Fatalf("TCP form advanced without persistence: %+v", got)
	}
	bot.selectManualMethod(ctx, 10, 1, "http")
	if got := bot.getPending(1); got.Step != "check-method" {
		t.Fatalf("HTTP form advanced without persistence: %+v", got)
	}
	if err := base.service.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	bot.chooseAlertTarget(ctx, 10, 1)
	bot.selectAlertTarget(ctx, 10, 1, "0")
	bot.pending[1] = interaction{Step: "alert-method", Name: "edge", Host: "example.com"}
	for _, method := range []string{"icmp", "tcp", "http"} {
		bot.selectAlertMethod(ctx, 10, 1, method)
		if got := bot.getPending(1); got.Step != "alert-method" {
			t.Fatalf("alert method %q advanced without persistence: %+v", method, got)
		}
	}
	bot.handleTargetAction(ctx, 10, 1, "target-check", "0")
	if got := bot.getPending(1); got.Step != "alert-method" {
		t.Fatalf("target check advanced without persistence: %+v", got)
	}
	bot.handleTargetAction(ctx, 10, 1, "target-alert", "0")
	if got := bot.getPending(1); got.Step != "alert-method" {
		t.Fatalf("target alert advanced without persistence: %+v", got)
	}
	bot.createAlert(ctx, 10, 1, interaction{Step: "add-alert-interval", Name: "edge", Method: "icmp"}, time.Minute)
	if len(base.service.Alerts(1)) != 0 {
		t.Fatalf("alert was created without persisting its request ID: %+v", base.service.Alerts(1))
	}
}

func TestCompletedFormStateIsRetainedWhenClearFails(t *testing.T) {
	base, _, _ := setupBot(t)
	ctx := context.Background()
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, failedInteractionStore{err: errors.New("database unavailable")})
	bot.pending[1] = interaction{Step: "add-host", Name: "edge"}
	bot.handlePending(ctx, 10, 1, bot.getPending(1), "example.com")
	if _, ok := base.service.FindTarget(1, "edge"); !ok {
		t.Fatal("server was not created")
	}
	if got := bot.getPending(1); got.Step != "add-host" {
		t.Fatalf("failed form clear discarded add-host state: %+v", got)
	}
	bot.pending[1] = interaction{Step: "block-target"}
	bot.handlePending(ctx, 10, 1, bot.getPending(1), "edge")
	if got := bot.getPending(1); got.Step != "block-target" {
		t.Fatalf("failed form clear discarded block state: %+v", got)
	}
	bot.pending[1] = interaction{Step: "add-alert-interval", Name: "edge", Method: "icmp", RequestID: "stable-request"}
	bot.handlePending(ctx, 10, 1, bot.getPending(1), "1m")
	if got := bot.getPending(1); got.Step != "add-alert-interval" {
		t.Fatalf("failed form clear discarded alert state: %+v", got)
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
	var logs synchronizedBuffer
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
	bot.handleCallback(ctx, cb("target-block:0"))
	bot.handleMessage(ctx, msg("/add remove-me remove.example"))
	bot.handleCallback(ctx, cb("target-remove:1"))
	if _, ok := service.FindTarget(1, "remove-me"); ok {
		t.Fatal("target-remove callback left the server")
	}
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
