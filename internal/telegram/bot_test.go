//go:build functional

package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/theillarion/tg-net-bot/internal/blockcheck"
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

type brokenLoadInteractionStore struct{}

func (brokenLoadInteractionStore) LoadInteractions(context.Context) (map[int64]domain.Interaction, error) {
	return nil, errors.New("interaction storage unavailable")
}
func (brokenLoadInteractionStore) SaveInteraction(context.Context, int64, domain.Interaction) error {
	return errors.New("interaction storage unavailable")
}
func (brokenLoadInteractionStore) DeleteInteraction(context.Context, int64) error {
	return errors.New("interaction storage unavailable")
}

type brokenSaveInteractionStore struct{}

func (brokenSaveInteractionStore) LoadInteractions(context.Context) (map[int64]domain.Interaction, error) {
	return map[int64]domain.Interaction{1: {Step: "alert-target"}}, nil
}
func (brokenSaveInteractionStore) SaveInteraction(context.Context, int64, domain.Interaction) error {
	return errors.New("interaction storage unavailable")
}
func (brokenSaveInteractionStore) DeleteInteraction(context.Context, int64) error { return nil }

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

type fixedNetwork struct {
	up     bool
	detail string
}

func (n fixedNetwork) Check(context.Context, string, checker.Method, string) (bool, string) {
	return n.up, n.detail
}

type botBlock struct{}

func (botBlock) Check(_ context.Context, target string) (blockcheck.Result, error) {
	return blockcheck.Result{Target: target}, nil
}
func (botBlock) Probe(context.Context, string) (blockcheck.ProbeResult, error) {
	return blockcheck.ProbeResult{}, nil
}
func (b botBlock) CheckBatch(ctx context.Context, targets []string, emit func(blockcheck.BatchItem)) error {
	for _, target := range targets {
		result, err := b.Check(ctx, target)
		item := blockcheck.BatchItem{Target: target, Result: result, Status: "complete"}
		if err != nil {
			item.Status, item.Error = "failed", err.Error()
		}
		emit(item)
	}
	return nil
}

type fixedBlockResult struct {
	result blockcheck.Result
	err    error
}

func (b fixedBlockResult) Check(_ context.Context, target string) (blockcheck.Result, error) {
	result := b.result
	if result.Target == "" {
		result.Target = target
	}
	return result, b.err
}
func (fixedBlockResult) Probe(context.Context, string) (blockcheck.ProbeResult, error) {
	return blockcheck.ProbeResult{}, nil
}
func (b fixedBlockResult) CheckBatch(ctx context.Context, targets []string, emit func(blockcheck.BatchItem)) error {
	for _, target := range targets {
		result := b.result
		result.Target = target
		emit(blockcheck.BatchItem{Target: target, Result: result, Status: "complete"})
	}
	return nil
}

type botProbeBlock struct {
	botBlock
	result blockcheck.ProbeResult
	err    error
}

func (b botProbeBlock) Probe(context.Context, string) (blockcheck.ProbeResult, error) {
	return b.result, b.err
}
func (b botProbeBlock) ProbeWithEvents(_ context.Context, _ string, emit func(blockcheck.ProbeResult)) (blockcheck.ProbeResult, error) {
	if b.err != nil {
		return blockcheck.ProbeResult{}, b.err
	}
	progress := b.result
	progress.Stats.Responded = 1
	progress.Stats.Unanswered = progress.Stats.Online - 1
	emit(progress)
	return b.result, nil
}

var _ monitor.Repository = (*botRepo)(nil)

func setupBot(t *testing.T) (*Bot, *int, *sync.Mutex) {
	return setupBotWithState(t, domain.EmptyState())
}

func setupBotWithState(t *testing.T, state domain.State) (*Bot, *int, *sync.Mutex) {
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
		if strings.HasSuffix(r.URL.Path, "/sendMessage") {
			io.WriteString(w, `{"ok":true,"result":{"message_id":1}}`)
			return
		}
		io.WriteString(w, `{"ok":true,"result":true}`)
	}))
	t.Cleanup(srv.Close)
	api := &API{token: "x", client: srv.Client(), baseURL: srv.URL}
	repo := &botRepo{state: state}
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
	if got := b.getPending(1); got.Step != "check-select" {
		t.Fatalf("check did not open multi-server picker: %+v", got)
	}
	b.handleCallback(ctx, cb("target-pick:99")) // Invalid selection must be rejected.
	b.handleCallback(ctx, cb("target-pick:0"))
	b.handleCallback(ctx, cb("target-run:continue"))
	b.handleCallback(ctx, cb("check-method:tcp"))
	b.handleMessage(ctx, msg("99999"))
	if got := b.getPending(1); got.Step != "manual-tcp-port-batch" || store.states[1] != got {
		t.Fatalf("invalid port discarded persisted form state: memory=%+v stored=%+v", got, store.states[1])
	}
	b.handleMessage(ctx, msg("443"))
	b.handleMessage(ctx, msg(buttonCheck))
	b.handleCallback(ctx, cb("target-all:toggle"))
	b.handleCallback(ctx, cb("target-run:continue"))
	b.handleCallback(ctx, cb("check-method:http"))
	b.handleCallback(ctx, cb("manual-http:https"))
	b.handleMessage(ctx, msg(buttonCheck))
	b.handleCallback(ctx, cb("target-all:toggle"))
	b.handleCallback(ctx, cb("target-run:continue"))
	b.handleCallback(ctx, cb("check-method:icmp"))
	b.handleMessage(ctx, msg("/check"))
	b.handleMessage(ctx, msg("/check example.com udp"))
	b.handleMessage(ctx, msg("/check edge icmp"))
	b.handleMessage(ctx, msg("/check edge tcp"))
	b.handleMessage(ctx, msg("/check nowhere.example.invalid"))
	b.handleMessage(ctx, msg("/check missing"))
	// Block checks, direct and guided.
	b.handleMessage(ctx, msg(buttonBlock))
	b.handleCallback(ctx, cb("target-pick:0"))
	b.handleCallback(ctx, cb("target-run:continue"))
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
	b.handleCallback(ctx, cb("alert-continue:next"))
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

func TestManualBlockKeyboardReflectsProbeExpiryAvailability(t *testing.T) {
	for _, test := range []struct {
		name      string
		result    blockcheck.Result
		err       error
		wantProbe bool
		wantText  string
	}{
		{name: "valid ID within bound", result: blockcheck.Result{ID: "jwt-id", CacheKey: "usable-key", ProbeAvailable: true, UpstreamExpiresAt: testTelegramTimePtr(time.Now().Add(time.Hour))}, wantProbe: true},
		{name: "expired ID", result: blockcheck.Result{ID: "jwt-id", ProbeAvailable: false, UpstreamExpiresAt: testTelegramTimePtr(time.Now().Add(-time.Second))}},
		{name: "cache save failure", result: blockcheck.Result{ID: "jwt-id", CacheKey: "failed-key", ProbeAvailable: true}, err: &blockcheck.CacheWriteError{Err: errors.New("Redis unavailable")}, wantText: "контекст probe не сохранён"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var payloads []map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
					mu.Lock()
					payloads = append(payloads, payload)
					mu.Unlock()
				}
				_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":1}}`)
			}))
			defer server.Close()
			api := &API{token: "x", client: server.Client(), baseURL: server.URL}
			repo := &botRepo{state: domain.EmptyState()}
			service, err := monitor.New(context.Background(), repo, botNetwork{}, fixedBlockResult{result: test.result, err: test.err}, api, nil)
			if err != nil {
				t.Fatal(err)
			}
			bot := NewBot(api, service, map[int64]struct{}{1: {}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			bot.manualBlock(context.Background(), 10, "edge.example")
			mu.Lock()
			defer mu.Unlock()
			if len(payloads) < 2 {
				t.Fatalf("expected initial and result screens, got %d", len(payloads))
			}
			last, err := json.Marshal(payloads[len(payloads)-1])
			if err != nil {
				t.Fatal(err)
			}
			hasProbe := strings.Contains(string(last), "block-probe:")
			if hasProbe != test.wantProbe {
				t.Fatalf("probe action present=%v want=%v screen=%s", hasProbe, test.wantProbe, last)
			}
			if test.wantText != "" && !strings.Contains(string(last), test.wantText) {
				t.Fatalf("primary result/cache error missing %q: %s", test.wantText, last)
			}
			if test.name == "cache save failure" && !strings.Contains(string(last), "edge.example") {
				t.Fatalf("successful primary result was lost: %s", last)
			}
		})
	}
}

func testTelegramTimePtr(value time.Time) *time.Time { return &value }
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

func TestAlertWizardShowsReviewWithoutSavingBatchRule(t *testing.T) {
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
	got := bot.getPending(1)
	if got.Step != "alert-review" || got.Name != "5m0s" || store.states[1] != got {
		t.Fatalf("alert draft did not persist at review step: memory=%+v persisted=%+v", got, store.states[1])
	}
	if len(service.Alerts(1)) != 0 {
		t.Fatalf("reviewing batch alert draft unexpectedly saved rules: %+v", service.Alerts(1))
	}
}

func TestLegacyAlertStorageFailureKeepsPersistedDraft(t *testing.T) {
	ctx := context.Background()
	repo := &failedAlertInsertRepo{botRepo: &botRepo{state: domain.EmptyState()}}
	api := &API{token: "test", client: &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":true}`))}, nil
	})}, baseURL: "http://test"}
	service, err := monitor.New(ctx, repo, botNetwork{}, botBlock{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	draft := interaction{Step: "add-alert-interval", Name: "edge", Host: "example.com", Method: "icmp", RequestID: "stable-id"}
	store := &testInteractionStore{states: map[int64]domain.Interaction{1: draft}}
	bot := NewBot(api, service, map[int64]struct{}{1: {}}, slog.New(slog.NewTextHandler(io.Discard, nil)), store)
	bot.pending[1] = draft
	bot.createAlert(ctx, 10, 1, draft, time.Minute)
	if bot.getPending(1) != draft || store.states[1] != draft || len(service.Alerts(1)) != 0 {
		t.Fatalf("failed alert insert changed draft/state: pending=%+v stored=%+v alerts=%+v", bot.getPending(1), store.states[1], service.Alerts(1))
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
	if err := base.service.AddTarget(context.Background(), 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
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
	for _, button := range []string{buttonServices, buttonServers, buttonAlerts, buttonTargets, buttonRules, buttonHelp} {
		bot.pending[1] = interaction{Step: "add-name", Name: "keep-me"}
		bot.handleMessage(ctx, msg(button))
		if got := bot.getPending(1); got.Step != "add-name" || got.Name != "keep-me" {
			t.Fatalf("menu button %q changed state after failed reset: %+v", button, got)
		}
	}
	delete(bot.pending, 1)
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

func TestRunResetsLegacyAlertFormsAndPreservesCurrentForms(t *testing.T) {
	bot, _, _ := setupBot(t)
	store := &testInteractionStore{states: map[int64]domain.Interaction{
		1: {Step: "alert-method", RequestID: "not-json"},
		2: {Step: "add-alert-interval", RequestID: `[]`},
		3: {Step: "add-alert-interval", RequestID: `["edge"]`},
	}}
	bot.store = store
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bot.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	if store.states[1].Step != "alert-stale" || store.states[2].Step != "alert-stale" || store.states[3].Step != "add-alert-interval" {
		t.Fatalf("legacy migration result = %+v", store.states)
	}
}

func TestRunReportsInteractionStoreRecoveryFailures(t *testing.T) {
	bot, _, _ := setupBot(t)
	bot.store = brokenLoadInteractionStore{}
	if err := bot.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "restore Telegram dialog state") {
		t.Fatalf("load failure = %v", err)
	}
	bot.store = brokenSaveInteractionStore{}
	if err := bot.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "reset legacy Telegram alert form") {
		t.Fatalf("migration failure = %v", err)
	}
}

func TestCallbackAndWizardRemainAtCurrentStepWhenStateSaveFails(t *testing.T) {
	base, _, _ := setupBot(t)
	ctx := context.Background()
	bot := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, failedInteractionStore{err: errors.New("database unavailable")})
	bot.handleCallback(ctx, cb("service:check-host"))
	bot.handleCallback(ctx, cb("server:add"))
	bot.pending[1] = interaction{Step: "alert-method", Name: "edge", RequestID: encodeNames([]string{"edge"})}
	bot.selectAlertMethod(ctx, 10, 1, "icmp")
	if got := bot.getPending(1); got.Step != "alert-method" {
		t.Fatalf("failed persistence advanced wizard: %+v", got)
	}
	bot.handlePending(ctx, 10, 1, interaction{Step: "manual-tcp-port-batch", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"})}, "443")
	bot.handlePending(ctx, 10, 1, interaction{Step: "add-alert-tcp-port", Name: "edge", RequestID: encodeNames([]string{"edge"})}, "443")
}

func TestRenderEditsExistingScreenAndRecoversFromTelegramEditErrors(t *testing.T) {
	for _, test := range []struct {
		name       string
		editBody   string
		wantSends  int
		wantScreen int
	}{
		{name: "edit succeeds", editBody: `{"ok":true,"result":true}`, wantScreen: 7},
		{name: "unchanged screen", editBody: `{"ok":false,"description":"Bad Request: message is not modified"}`, wantScreen: 7},
		{name: "edit failure sends replacement", editBody: `{"ok":false,"description":"Bad Request: message to edit not found"}`, wantSends: 1, wantScreen: 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			sends := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/editMessageText") {
					io.WriteString(w, test.editBody)
					return
				}
				sends++
				io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
			}))
			defer server.Close()
			base, _, _ := setupBot(t)
			base.api = &API{token: "test", client: server.Client(), baseURL: server.URL}
			base.screens[10] = 7
			base.render(context.Background(), 10, "<b>screen</b>", mainMenuKeyboard())
			if sends != test.wantSends || base.screens[10] != test.wantScreen {
				t.Fatalf("sends=%d screen=%d", sends, base.screens[10])
			}
		})
	}
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

func (botBlockFailure) Check(context.Context, string) (blockcheck.Result, error) {
	return blockcheck.Result{}, errors.New("block API failed")
}
func (botBlockFailure) Probe(context.Context, string) (blockcheck.ProbeResult, error) {
	return blockcheck.ProbeResult{}, errors.New("block API failed")
}
func (b botBlockFailure) CheckBatch(ctx context.Context, targets []string, emit func(blockcheck.BatchItem)) error {
	for _, target := range targets {
		result, err := b.Check(ctx, target)
		emit(blockcheck.BatchItem{Target: target, Result: result, Status: "failed", Error: err.Error()})
	}
	return nil
}

type botBatchStatuses struct{ botBlock }

func (botBatchStatuses) CheckBatch(_ context.Context, targets []string, emit func(blockcheck.BatchItem)) error {
	statuses := []blockcheck.BatchItem{
		{Target: targets[0], Status: "cache_failed", Result: blockcheck.Result{Target: targets[0], Blocked: false}, Error: "cache write failed"},
		{Target: targets[1], Status: "limited", Error: "rate limited"},
		{Target: targets[2], Status: "skipped", Error: "not sent"},
		{Target: targets[3], Status: "failed", Error: "provider failed"},
		{Target: targets[4], Status: "unknown", Error: "unexpected status"},
		{Target: targets[5], Status: "canceled", Error: "context canceled"},
	}
	for _, item := range statuses {
		emit(item)
	}
	emit(statuses[0]) // A defensive duplicate must not be assigned to another host.
	return errors.New("batch stopped")
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
		if strings.HasSuffix(r.URL.Path, "/sendMessage") || strings.HasSuffix(r.URL.Path, "/editMessageText") {
			mu.Lock()
			sent = append(sent, payload)
			mu.Unlock()
		}
		if strings.HasSuffix(r.URL.Path, "/sendMessage") {
			io.WriteString(w, `{"ok":true,"result":{"message_id":1}}`)
		} else {
			io.WriteString(w, `{"ok":true,"result":true}`)
		}
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
	for _, want := range []string{"Выбери раздел", "Сохранил «edge»", "доступен", "Проверка блокировок", "База:", "каждые 5m", "Пока нет активных алертов", "Сервер и его алерты удалены"} {
		if !strings.Contains(joined, want) {
			t.Errorf("bot responses missing %q; got:\n%s", want, joined)
		}
	}
	if !hasKeyboard {
		t.Fatal("/start did not provide a button menu")
	}
}

func TestBotMenuNavigationAndBatchAlertDraftFlow(t *testing.T) {
	ctx := context.Background()
	bot, _, _ := setupBot(t)
	for _, target := range []domain.Target{{Name: "edge", Host: "edge.example"}, {Name: "db", Host: "192.0.2.10"}} {
		if err := bot.service.AddTarget(ctx, 1, target.Name, target.Host); err != nil {
			t.Fatal(err)
		}
	}
	bot.handleMessage(ctx, msg(buttonServices))
	bot.handleMessage(ctx, msg(buttonAlerts))
	bot.handleCallback(ctx, cb("menu:services"))
	bot.handleCallback(ctx, cb("service:check"))
	bot.handleCallback(ctx, cb("nav:back"))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("back from server picker kept stale interaction: %+v", got)
	}
	bot.handleCallback(ctx, cb("menu:servers"))
	bot.handleCallback(ctx, cb("nav:home"))
	bot.handleCallback(ctx, cb("menu:alerts"))
	bot.handleCallback(ctx, cb("alert:new"))
	bot.handleCallback(ctx, cb("alert-pick:99"))
	bot.handleCallback(ctx, cb("alert-all:toggle"))
	bot.handleCallback(ctx, cb("alert-pick:0"))
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 1 || got[0] != "db" {
		t.Fatalf("multi-select did not preserve selected target: %v", got)
	}
	bot.handleCallback(ctx, cb("alert-continue:next"))
	bot.handleCallback(ctx, cb("alert-method:tcp"))
	bot.handleMessage(ctx, msg("443"))
	bot.handleMessage(ctx, msg("5m"))
	if got := bot.getPending(1); got.Step != "alert-review" || got.Name != "5m0s" {
		t.Fatalf("alert wizard did not reach review: %+v", got)
	}
	bot.handleCallback(ctx, cb("alert-back:targets"))
	bot.handleCallback(ctx, cb("alert-all:toggle"))
	bot.handleCallback(ctx, cb("alert-continue:next"))
	bot.handleCallback(ctx, cb("alert-method:http"))
	bot.handleCallback(ctx, cb("alert-http:https"))
	bot.handleCallback(ctx, cb("alert-interval:15m"))
	bot.handleCallback(ctx, cb("alert-review:show"))
	bot.handleCallback(ctx, cb("nav:back"))
	bot.handleCallback(ctx, cb("nav:home"))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("home navigation did not clear draft: %+v", got)
	}
	alert, err := bot.service.AddAlert(ctx, 10, 1, "edge", "icmp", "", time.Minute)
	if err != nil {
		t.Fatalf("alert insert for remaining targets %v failed: %v", bot.service.Targets(1), err)
	}
	bot.handleCallback(ctx, cb("alert-view:"+alert.ID))
	bot.handleCallback(ctx, cb("menu:alerts"))
	bot.handleCallback(ctx, cb("alert-remove:"+alert.ID))
	bot.handleCallback(ctx, cb("alert-view:missing"))
	if len(bot.service.Alerts(1)) != 0 {
		t.Fatalf("alert removal left active rules: %+v", bot.service.Alerts(1))
	}
}

func TestBackNavigationReturnsToTheParentStep(t *testing.T) {
	ctx := context.Background()
	bot, _, _ := setupBot(t)
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		state interaction
		want  string
	}{
		{interaction{Step: "add-name"}, ""},
		{interaction{Step: "add-host", Name: "edge"}, ""},
		{interaction{Step: "check-select", Host: encodeNames([]string{"edge"})}, ""},
		{interaction{Step: "block-select", Host: encodeNames([]string{"edge"})}, ""},
		{interaction{Step: "check-method", Host: "edge.example"}, ""},
		{interaction{Step: "manual-http", Host: "edge.example"}, ""},
		{interaction{Step: "manual-tcp-port", Host: "edge.example"}, ""},
		{interaction{Step: "manual-tcp-port-batch", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"})}, "check-select"},
		{interaction{Step: "alert-method", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"})}, "alert-select"},
		{interaction{Step: "alert-http", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"})}, "alert-method"},
		{interaction{Step: "add-alert-interval", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"})}, "alert-method"},
		{interaction{Step: "add-alert-tcp-port", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"})}, "alert-method"},
		{interaction{Step: "alert-select", Host: encodeNames([]string{"edge"})}, ""},
		{interaction{Step: "unknown"}, ""},
	}
	for _, test := range cases {
		bot.pending[1] = test.state
		bot.handleCallback(ctx, cb("nav:back"))
		if got := bot.getPending(1); got.Step != test.want {
			t.Fatalf("back from %q moved to %q, want %q", test.state.Step, got.Step, test.want)
		}
	}
	bot.pending[1] = interaction{Step: "alert-review", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"}), Method: "icmp", Name: "5m"}
	bot.handleCallback(ctx, cb("nav:back"))
	if got := bot.getPending(1); got.Step != "add-alert-interval" {
		t.Fatalf("back from review moved to %+v", got)
	}
	bot.pending[1] = interaction{Step: "check-method-batch", Host: encodeNames([]string{"edge"}), RequestID: encodeNames([]string{"edge"})}
	bot.handleCallback(ctx, cb("nav:back"))
	if got := bot.getPending(1); got.Step != "check-select" {
		t.Fatalf("back from batch method moved to %+v", got)
	}
}

func TestEmptyAndStaleBatchSelectionsAreRejected(t *testing.T) {
	bot, _, _ := setupBot(t)
	if err := bot.service.AddTarget(context.Background(), 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	bot.pending[1] = interaction{Step: "check-select", Host: encodeNames([]string{"edge"})}
	bot.runPickedTargets(ctx, 10, 1)
	if got := bot.getPending(1); got.Step != "check-select" || len(decodeNames(got.RequestID)) != 0 {
		t.Fatalf("empty check selection advanced: %+v", got)
	}
	bot.pending[1] = interaction{Step: "block-select", Host: encodeNames([]string{"edge"})}
	bot.runPickedTargets(ctx, 10, 1)
	if got := bot.getPending(1); got.Step != "block-select" {
		t.Fatalf("empty block selection advanced: %+v", got)
	}
	bot.pending[1] = interaction{Step: "stale"}
	bot.runPickedTargets(ctx, 10, 1)
	if got := bot.getPending(1); got.Step != "stale" {
		t.Fatalf("stale picker changed state: %+v", got)
	}
	bot.pending[1] = interaction{Step: "stale"}
	bot.continueAlertWizard(ctx, 10, 1)
	if got := bot.getPending(1); got.Step != "stale" {
		t.Fatalf("stale alert wizard changed state: %+v", got)
	}
	bot.toggleAllTargets(ctx, 10, 1)
	bot.toggleAllAlertTargets(ctx, 10, 1)
	if got := bot.getPending(1); got.Step != "stale" {
		t.Fatalf("stale select-all changed state: %+v", got)
	}
}

func TestPendingManualAddressAndExpiredFormFlows(t *testing.T) {
	ctx := context.Background()
	bot, _, _ := setupBot(t)
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	bot.handleCallback(ctx, cb("service:check-host"))
	bot.handleMessage(ctx, msg("2001:db8::5"))
	if got := bot.getPending(1); got.Step != "check-method" || got.Host != "2001:db8::5" {
		t.Fatalf("manual check target not accepted: %+v", got)
	}
	bot.pending[1] = interaction{Step: "block-host"}
	bot.handleMessage(ctx, msg("invalid/path"))
	if got := bot.getPending(1); got.Step != "block-host" {
		t.Fatalf("invalid block host advanced form: %+v", got)
	}
	bot.handleMessage(ctx, msg("edge"))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("valid block host did not finish form: %+v", got)
	}
	bot.pending[1] = interaction{Step: "check-target"}
	bot.handleMessage(ctx, msg("edge"))
	if got := bot.getPending(1); got.Step != "check-method" || got.Host != "edge.example" {
		t.Fatalf("legacy check target resolution failed: %+v", got)
	}
	bot.pending[1] = interaction{Step: "block-target"}
	bot.handleMessage(ctx, msg("no/path"))
	if got := bot.getPending(1); got.Step != "block-target" {
		t.Fatalf("invalid legacy block target advanced form: %+v", got)
	}
	bot.pending[1] = interaction{Step: "alert-stale"}
	bot.handleMessage(ctx, msg("anything"))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("stale alert form was not cleared: %+v", got)
	}
	bot.pending[1] = interaction{Step: "add-alert-tcp-port", Name: "edge", Method: "tcp", RequestID: "[\"edge\"]"}
	bot.handleMessage(ctx, msg("bad-port"))
	if got := bot.getPending(1); got.Step != "add-alert-tcp-port" {
		t.Fatalf("invalid alert port advanced form: %+v", got)
	}
	bot.handleMessage(ctx, msg("443"))
	if got := bot.getPending(1); got.Step != "add-alert-interval" || got.Argument != "443" {
		t.Fatalf("valid alert port did not advance to interval: %+v", got)
	}
}

func TestAlertDetailShowsEachStoredStateAndMissingAlert(t *testing.T) {
	up, down := true, false
	bot, _, _ := setupBotWithState(t, domain.State{
		Targets: map[string][]domain.Target{"1": {{Name: "edge", Host: "edge.example"}}},
		Alerts: []domain.Alert{
			{ID: "waiting", ChatID: 10, UserID: 1, Target: domain.Target{Name: "edge", Host: "edge.example"}, Method: "icmp", Interval: time.Minute},
			{ID: "up", ChatID: 10, UserID: 1, Target: domain.Target{Name: "edge", Host: "edge.example"}, Method: "icmp", Interval: time.Minute, LastUp: &up},
			{ID: "down", ChatID: 10, UserID: 1, Target: domain.Target{Name: "edge", Host: "edge.example"}, Method: "icmp", Interval: time.Minute, LastUp: &down},
		},
	})
	ctx := context.Background()
	for _, id := range []string{"waiting", "up", "down"} {
		bot.showAlertDetail(ctx, 10, 1, id)
	}
	bot.showAlertDetail(ctx, 10, 1, "missing")
}

func TestAlertWizardCompletesICMPAndHTTPReviewPaths(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	bot.pending[1] = interaction{Step: "alert-method", Name: "edge", RequestID: encodeNames([]string{"edge"})}
	bot.selectAlertMethod(ctx, 10, 1, "icmp")
	if got := bot.getPending(1); got.Step != "add-alert-interval" || got.Method != "icmp" {
		t.Fatalf("ICMP method state = %+v", got)
	}
	bot.handleCallback(ctx, cb("alert-interval:5m"))
	if got := bot.getPending(1); got.Step != "alert-review" || got.Name != "5m" {
		t.Fatalf("ICMP review state = %+v", got)
	}
	bot.handleCallback(ctx, cb("alert-review:show"))
	bot.handleCallback(ctx, cb("alert-back:servers"))
	if got := bot.getPending(1); got.Step != "alert-select" {
		t.Fatalf("back from review state = %+v", got)
	}
	bot.pending[1] = interaction{Step: "alert-method", Name: "edge", RequestID: encodeNames([]string{"edge"})}
	bot.selectAlertMethod(ctx, 10, 1, "http")
	if got := bot.getPending(1); got.Step != "alert-http" {
		t.Fatalf("HTTP choice state = %+v", got)
	}
	bot.handleCallback(ctx, cb("alert-http:https"))
	if got := bot.getPending(1); got.Step != "add-alert-interval" || got.Argument != "https" {
		t.Fatalf("HTTP interval state = %+v", got)
	}
	bot.handlePending(ctx, 10, 1, bot.getPending(1), "1m")
	if got := bot.getPending(1); got.Step != "alert-review" || got.Name != "1m0s" {
		t.Fatalf("HTTP review state = %+v", got)
	}
	bot.handleCallback(ctx, cb("alert-review:show"))
	if got := bot.getPending(1); got.Step != "alert-review" || len(bot.service.Alerts(1)) != 0 {
		t.Fatalf("saved alert state=%+v alerts=%+v", got, bot.service.Alerts(1))
	}
}

func TestCallbackStaleAndEmptyMenuPathsStayRecoverable(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	bot.handleCallback(ctx, CallbackQuery{ID: "no-message", From: User{ID: 1}, Data: "menu:services"})
	bot.handleCallback(ctx, cb("malformed"))
	bot.handleCallback(ctx, cb("unknown:action"))
	bot.handleCallback(ctx, cb("menu:unknown"))
	bot.handleCallback(ctx, cb("service:check"))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("empty target picker created state: %+v", got)
	}
	bot.handleCallback(ctx, cb("service:block-host"))
	if got := bot.getPending(1); got.Step != "block-host" {
		t.Fatalf("manual block form = %+v", got)
	}
	bot.handleCallback(ctx, cb("alert:new"))
	bot.handleCallback(ctx, cb("alert-interval:5m"))
	bot.handleCallback(ctx, cb("alert-http:https"))
	bot.handleCallback(ctx, cb("alert-method:unknown"))
	bot.renderAlertReview(ctx, 10, 1)
	bot.handleCallback(ctx, cb("target-remove:invalid"))
	bot.handleCallback(ctx, cb("alert-remove:missing"))
	bot.handleCallback(ctx, cb("alert-view:missing"))
	bot.handleCallback(ctx, cb("nav:home"))
	if got := bot.getPending(1); got.Step != "" {
		t.Fatalf("home navigation retained stale state: %+v", got)
	}
}

func TestLegacyCheckAndAlertCommandsValidateAndCreateSingleAlert(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	bot.manualCheckCommand(ctx, 10, 1, nil)
	bot.manualCheckCommand(ctx, 10, 1, []string{"not/a/host"})
	bot.manualCheckCommand(ctx, 10, 1, []string{"example.com", "udp"})
	bot.manualCheckCommand(ctx, 10, 1, []string{"example.com", "tcp", "bad-port"})
	bot.addAlertCommand(ctx, 10, 1, nil)
	bot.addAlertCommand(ctx, 10, 1, []string{"edge", "icmp", "invalid"})
	bot.addAlertCommand(ctx, 10, 1, []string{"missing", "icmp", "1m"})
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	bot.addAlertCommand(ctx, 10, 1, []string{"edge", "udp", "1m"})
	bot.addAlertCommand(ctx, 10, 1, []string{"edge", "tcp", "1m", "443"})
	alerts := bot.service.Alerts(1)
	if len(alerts) != 1 || alerts[0].Method != "tcp" || alerts[0].Argument != "443" {
		t.Fatalf("single alert command result = %+v", alerts)
	}
}

func TestCreateAlertPersistsAndSurfacesPersistenceFailures(t *testing.T) {
	ctx := context.Background()
	base, _, _ := setupBot(t)
	if err := base.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	base.pending[1] = interaction{Step: "add-alert-interval", RequestID: "alert-1"}
	base.createAlert(ctx, 10, 1, interaction{Step: "add-alert-interval", RequestID: "alert-1", Name: "edge", Method: "icmp"}, time.Minute)
	if alerts := base.service.Alerts(1); len(alerts) != 1 || alerts[0].ID != "alert-1" {
		t.Fatalf("created alerts = %+v", alerts)
	}
	base.createAlert(ctx, 10, 1, interaction{RequestID: "alert-1", Name: "edge", Method: "icmp"}, time.Minute)
	failedClear := NewBot(base.api, base.service, map[int64]struct{}{1: {}}, base.logger, failedInteractionStore{err: errors.New("database unavailable")})
	failedClear.createAlert(ctx, 10, 1, interaction{RequestID: "alert-2", Name: "edge", Method: "icmp"}, time.Minute)
	if len(base.service.Alerts(1)) != 2 {
		t.Fatalf("alert should be created even when draft cleanup fails: %+v", base.service.Alerts(1))
	}
	base.createAlert(ctx, 10, 1, interaction{Step: "add-alert-interval", Name: "edge", Method: "icmp"}, time.Minute)
	if len(base.service.Alerts(1)) != 3 || base.service.Alerts(1)[2].ID == "" {
		t.Fatalf("generated alert ID was not persisted: %+v", base.service.Alerts(1))
	}
	brokenRepo := &botRepo{state: domain.EmptyState()}
	brokenService, err := monitor.New(ctx, failedAlertInsertRepo{brokenRepo}, botNetwork{}, botBlock{}, base.api, base.logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := brokenService.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	failedInsert := NewBot(base.api, brokenService, map[int64]struct{}{1: {}}, base.logger)
	failedInsert.createAlert(ctx, 10, 1, interaction{RequestID: "alert-3", Name: "edge", Method: "icmp"}, time.Minute)
	if len(brokenService.Alerts(1)) != 0 {
		t.Fatalf("failed repository inserted an alert: %+v", brokenService.Alerts(1))
	}
}

func TestTargetPickRejectsRemovedTargets(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	bot.handleMessage(ctx, msg(buttonCheck))
	if err := bot.service.RemoveTarget(ctx, 1, "edge"); err != nil {
		t.Fatal(err)
	}
	bot.handleCallback(ctx, cb("target-pick:0"))
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 0 {
		t.Fatalf("removed target selected: %v", got)
	}
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	bot.chooseAlertTarget(ctx, 10, 1)
	if err := bot.service.RemoveTarget(ctx, 1, "edge"); err != nil {
		t.Fatal(err)
	}
	bot.handleCallback(ctx, cb("alert-pick:0"))
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 0 {
		t.Fatalf("removed alert target selected: %v", got)
	}
}

func TestMultiSelectCanToggleIndividualAndAllServersBothWays(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	for _, target := range []domain.Target{{Name: "edge", Host: "edge.example"}, {Name: "db", Host: "db.example"}} {
		if err := bot.service.AddTarget(ctx, 1, target.Name, target.Host); err != nil {
			t.Fatal(err)
		}
	}
	all := encodeNames([]string{"edge", "db"})
	bot.pending[1] = interaction{Step: "check-select", Host: all}
	bot.toggleAllTargets(ctx, 10, 1)
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 2 {
		t.Fatalf("select all = %v", got)
	}
	bot.toggleAllTargets(ctx, 10, 1)
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 0 {
		t.Fatalf("clear all = %v", got)
	}
	bot.toggleTargetSelection(ctx, 10, 1, "0")
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 1 || got[0] != "edge" {
		t.Fatalf("select one = %v", got)
	}
	bot.toggleTargetSelection(ctx, 10, 1, "0")
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 0 {
		t.Fatalf("deselect one = %v", got)
	}
	bot.pending[1] = interaction{Step: "alert-select", Host: all}
	bot.toggleAllAlertTargets(ctx, 10, 1)
	bot.toggleAlertTarget(ctx, 10, 1, "1")
	bot.toggleAllAlertTargets(ctx, 10, 1)
	bot.toggleAllAlertTargets(ctx, 10, 1)
	if got := decodeNames(bot.getPending(1).RequestID); len(got) != 0 {
		t.Fatalf("alert select all and clear = %v", got)
	}
}

func TestSelectionRejectsMalformedIndexesAndMissingMethod(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	encoded := encodeNames([]string{"edge"})
	bot.pending[1] = interaction{Step: "check-select", Host: encoded}
	bot.toggleTargetSelection(ctx, 10, 1, "not-an-index")
	if got := bot.getPending(1).RequestID; got != "" {
		t.Fatalf("invalid check selection changed draft: %s", got)
	}
	bot.pending[1] = interaction{Step: "alert-select", Host: encoded}
	bot.toggleAlertTarget(ctx, 10, 1, "-1")
	if got := bot.getPending(1).RequestID; got != "" {
		t.Fatalf("invalid alert selection changed draft: %s", got)
	}
	bot.pending[1] = interaction{Step: "check-select", Host: encoded, RequestID: encoded}
	bot.runPickedTargets(ctx, 10, 1)
	if got := bot.getPending(1); got.Step != "check-method-batch" {
		t.Fatalf("continue did not open method choice: %+v", got)
	}
	bot.pending[1] = interaction{Step: "check-method-batch", Host: encoded, RequestID: encoded}
	bot.runPickedTargets(ctx, 10, 1)
	if got := bot.getPending(1); got.Step != "check-method-batch" {
		t.Fatalf("missing method advanced batch: %+v", got)
	}
}

func TestPickersHideRemovedCandidatesAndKeepOtherTargetsVisible(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	if err := bot.service.AddTarget(ctx, 1, "edge", "edge.example"); err != nil {
		t.Fatal(err)
	}
	pending := interaction{Step: "check-select", Host: encodeNames([]string{"edge", "removed"}), RequestID: encodeNames([]string{"removed"})}
	bot.showTargetPicker(ctx, 10, 1, pending)
	pending.Step = "alert-select"
	bot.showAlertPicker(ctx, 10, 1, pending)
	if names := bot.targetsByName(1, []string{"edge", "removed"}); len(names) != 1 || names[0].Name != "edge" {
		t.Fatalf("target resolver returned removed entries: %+v", names)
	}
}

func TestStartOperationRejectsEmptyOrBusyWork(t *testing.T) {
	bot, _, _ := setupBot(t)
	ctx := context.Background()
	bot.startOperation(ctx, 10, "check", nil, "icmp", "")
	for len(bot.workSlots) < cap(bot.workSlots) {
		bot.workSlots <- struct{}{}
	}
	bot.startOperation(ctx, 10, "check", []domain.Target{{Name: "edge", Host: "edge.example"}}, "icmp", "")
	if len(bot.workSlots) != cap(bot.workSlots) {
		t.Fatalf("busy operation should not consume another slot: %d", len(bot.workSlots))
	}
}

func TestBlockBatchRendersPartialResultsLimitsAndStopReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var mu sync.Mutex
	var sent []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Error(readErr)
		}
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Error(err)
		}
		mu.Lock()
		sent = append(sent, string(body))
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/sendMessage") {
			io.WriteString(w, `{"ok":true,"result":{"message_id":77}}`)
		} else {
			io.WriteString(w, `{"ok":true,"result":true}`)
		}
	}))
	defer server.Close()
	api := &API{token: "test", client: server.Client(), baseURL: server.URL}
	service, err := monitor.New(ctx, &botRepo{state: domain.EmptyState()}, botNetwork{}, botBatchStatuses{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	bot := NewBot(api, service, map[int64]struct{}{1: {}}, nil)
	targets := make([]domain.Target, 6)
	for i := range targets {
		targets[i] = domain.Target{Name: fmt.Sprintf("server-%d", i+1), Host: fmt.Sprintf("s%d.example", i+1)}
	}
	bot.startOperation(ctx, 10, "block", targets, "", "")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bot.uiMu.Lock()
		active := bot.operations[10] != nil
		bot.uiMu.Unlock()
		if !active {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	joined := strings.Join(sent, "\n")
	mu.Unlock()
	for _, want := range []string{"лимит запросов", "пропущено из-за лимита", "ошибка", "кеш недоступен", "cache write failed", "отменено", "Пакетная проверка остановлена: batch stopped"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("partial batch output missing %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "block-probe:") {
		t.Fatalf("cache-failed result exposed a probe button: %s", joined)
	}
}

func TestAvailabilityBatchRendersDownAndInvalidMethodResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var mu sync.Mutex
	var sent []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		mu.Lock()
		sent = append(sent, payload.Text)
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/sendMessage") {
			io.WriteString(w, `{"ok":true,"result":{"message_id":77}}`)
		} else {
			io.WriteString(w, `{"ok":true,"result":true}`)
		}
	}))
	defer server.Close()
	api := &API{token: "test", client: server.Client(), baseURL: server.URL}
	service, err := monitor.New(ctx, &botRepo{state: domain.EmptyState()}, fixedNetwork{detail: "connection refused"}, botBlock{}, api, nil)
	if err != nil {
		t.Fatal(err)
	}
	bot := NewBot(api, service, map[int64]struct{}{1: {}}, nil)
	targets := []domain.Target{{Name: "down", Host: "down.example"}}
	bot.startOperation(ctx, 10, "check", targets, "icmp", "")
	waitOperation(t, ctx, bot, 10)
	bot.startOperation(ctx, 10, "check", targets, "unsupported", "")
	waitOperation(t, ctx, bot, 10)
	mu.Lock()
	joined := strings.Join(sent, "\n")
	mu.Unlock()
	if !strings.Contains(joined, "недоступен") || !strings.Contains(joined, "ошибка") || !strings.Contains(joined, "method must be icmp") {
		t.Fatalf("availability results = %s", joined)
	}
}

func waitOperation(t *testing.T, ctx context.Context, bot *Bot, chatID int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		bot.uiMu.Lock()
		active := bot.operations[chatID] != nil
		bot.uiMu.Unlock()
		if !active {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("operation did not finish before context deadline")
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatal("operation did not finish")
}

func TestManualProbeShowsStreamingProgressAndRateLimit(t *testing.T) {
	for _, test := range []struct {
		name  string
		block botProbeBlock
		want  string
	}{
		{name: "complete stream", block: botProbeBlock{result: blockcheck.ProbeResult{Target: "edge.example", Stats: blockcheck.ProbeStats{Online: 3, Responded: 2, Unanswered: 1}}}, want: "Категории вердиктов"},
		{name: "rate limited", block: botProbeBlock{err: &blockcheck.RateLimitError{Provider: "Cheburcheck", Route: "/api/v1/probe/", RetryAfter: time.Minute}}, want: "Ограничение запросов"},
		{name: "provider failure", block: botProbeBlock{err: errors.New("upstream unavailable")}, want: "Расширенная проверка не завершилась"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			texts := make(chan string, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Text string `json:"text"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				texts <- payload.Text
				if strings.HasSuffix(r.URL.Path, "/sendMessage") {
					io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
				} else {
					io.WriteString(w, `{"ok":true,"result":true}`)
				}
			}))
			defer server.Close()
			api := &API{token: "test", client: server.Client(), baseURL: server.URL}
			service, err := monitor.New(ctx, &botRepo{state: domain.EmptyState()}, botNetwork{}, test.block, api, nil)
			if err != nil {
				t.Fatal(err)
			}
			bot := NewBot(api, service, map[int64]struct{}{1: {}}, nil)
			bot.startBlockProbe(ctx, 10, "cache-key")
			var sawProgress bool
			for {
				select {
				case text := <-texts:
					sawProgress = sawProgress || strings.Contains(text, "получаю ответы узлов")
					if strings.Contains(text, test.want) {
						if test.name == "complete stream" && !sawProgress {
							t.Fatalf("completed without visible progress: %s", text)
						}
						return
					}
				case <-ctx.Done():
					t.Fatalf("probe screen did not show %q", test.want)
				}
			}
		})
	}
	bot, _, _ := setupBot(t)
	bot.startBlockProbe(context.Background(), 10, "")
}
