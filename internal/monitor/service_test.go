package monitor

import (
	"context"
	"errors"
	"github.com/theillarion/tg-net-bot/internal/checker"
	"github.com/theillarion/tg-net-bot/internal/domain"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type fakeRepo struct {
	state domain.State
	fail  string
}

func (r *fakeRepo) Load(context.Context) (domain.State, error) {
	if r.fail == "load" {
		return domain.State{}, errors.New("load")
	}
	return r.state, nil
}
func (r *fakeRepo) InsertTarget(_ context.Context, u int64, t domain.Target) error {
	if r.fail == "insert-target" {
		return errors.New("db")
	}
	k := userKey(u)
	r.state.Targets[k] = append(r.state.Targets[k], t)
	return nil
}
func (r *fakeRepo) DeleteTarget(_ context.Context, u int64, n string) error {
	if r.fail == "delete-target" {
		return errors.New("db")
	}
	return nil
}
func (r *fakeRepo) InsertAlert(_ context.Context, a domain.Alert) error {
	if r.fail == "insert-alert" {
		return errors.New("db")
	}
	r.state.Alerts = append(r.state.Alerts, a)
	return nil
}
func (r *fakeRepo) DeleteAlert(_ context.Context, u int64, id string) error {
	if r.fail == "delete-alert" {
		return errors.New("db")
	}
	return nil
}
func (r *fakeRepo) SetAlertLastCheck(context.Context, string, time.Time) error {
	if r.fail == "last-check" {
		return errors.New("db")
	}
	return nil
}
func (r *fakeRepo) SetAlertLastUp(context.Context, string, bool) error {
	if r.fail == "last-up" {
		return errors.New("db")
	}
	return nil
}

type fakeNetwork struct {
	up     bool
	detail string
	calls  int
}

func (n *fakeNetwork) Check(context.Context, string, checker.Method, string) (bool, string) {
	n.calls++
	return n.up, n.detail
}

type fakeBlock struct {
	verdict string
	err     error
	calls   int
}

func (b *fakeBlock) Check(context.Context, string) (string, error) {
	b.calls++
	return b.verdict, b.err
}

type fakeNotifier struct {
	messages []string
	err      error
}

func (n *fakeNotifier) Send(_ context.Context, _ int64, s string) error {
	n.messages = append(n.messages, s)
	return n.err
}
func newTestService(t *testing.T, r *fakeRepo, n *fakeNetwork, b *fakeBlock, no *fakeNotifier) *Service {
	t.Helper()
	s, e := New(context.Background(), r, n, b, no, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestNewAndCheckBlock(t *testing.T) {
	if _, err := New(context.Background(), &fakeRepo{fail: "load"}, nil, nil, nil, nil); err == nil {
		t.Fatal("expected load error")
	}
	r := &fakeRepo{state: domain.EmptyState()}
	n := &fakeNetwork{up: true, detail: "ok"}
	b := &fakeBlock{verdict: "clear"}
	s := newTestService(t, r, n, b, &fakeNotifier{})
	if _, err := s.Check(context.Background(), "bad host/path", "icmp", ""); err == nil {
		t.Fatal("invalid host accepted")
	}
	if _, err := s.Check(context.Background(), "example.com", "udp", ""); err == nil {
		t.Fatal("invalid method accepted")
	}
	res, err := s.Check(context.Background(), "example.com", "tcp", "443")
	if err != nil || !res.Up || res.Detail != "ok" || n.calls != 1 {
		t.Fatalf("check: %#v %v", res, err)
	}
	if _, err = s.Block(context.Background(), "bad/path"); err == nil {
		t.Fatal("invalid block target accepted")
	}
	v, err := s.Block(context.Background(), "example.com")
	if err != nil || v != "clear" || b.calls != 1 {
		t.Fatalf("block: %s %v", v, err)
	}
}
func TestTargetsAlertsCRUD(t *testing.T) {
	ctx := context.Background()
	r := &fakeRepo{state: domain.EmptyState()}
	s := newTestService(t, r, &fakeNetwork{}, &fakeBlock{}, &fakeNotifier{})
	if err := s.AddTarget(ctx, 1, " ", "example.com"); err == nil {
		t.Fatal("empty name accepted")
	}
	if err := s.AddTarget(ctx, 1, "bad", "bad/name"); err == nil {
		t.Fatal("bad host accepted")
	}
	r.fail = "insert-target"
	if err := s.AddTarget(ctx, 1, "edge", "example.com"); err == nil {
		t.Fatal("expected insert failure")
	}
	r.fail = ""
	if err := s.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTarget(ctx, 1, "EDGE", "other.com"); err == nil {
		t.Fatal("case-insensitive duplicate accepted")
	}
	targets := s.Targets(1)
	targets[0].Name = "mutated"
	if got, _ := s.FindTarget(1, "EDGE"); got.Name != "edge" {
		t.Fatalf("FindTarget = %#v", got)
	}
	if _, ok := s.FindTarget(1, "none"); ok {
		t.Fatal("found missing target")
	}
	for _, in := range []time.Duration{0, -time.Second} {
		if _, err := s.AddAlert(ctx, 1, 1, "edge", "icmp", "", in); err == nil {
			t.Fatalf("accepted interval %s", in)
		}
	}
	if _, err := s.AddAlert(ctx, 1, 1, "missing", "icmp", "", time.Minute); err == nil {
		t.Fatal("accepted missing target")
	}
	if _, err := s.AddAlert(ctx, 1, 1, "edge", "tcp", "bad", time.Minute); err == nil {
		t.Fatal("accepted bad argument")
	}
	r.fail = "insert-alert"
	if _, err := s.AddAlert(ctx, 1, 1, "edge", "icmp", "", time.Minute); err == nil {
		t.Fatal("expected insert alert failure")
	}
	r.fail = ""
	a, err := s.AddAlert(ctx, 10, 1, "edge", "tcp", "443", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Alerts(1)) != 1 || len(s.Alerts(2)) != 0 {
		t.Fatal("alert user filtering failed")
	}
	if err = s.RemoveAlert(ctx, 2, a.ID); err == nil {
		t.Fatal("removed other user's alert")
	}
	r.fail = "delete-alert"
	if err = s.RemoveAlert(ctx, 1, a.ID); err == nil {
		t.Fatal("expected delete alert failure")
	}
	r.fail = ""
	if err = s.RemoveAlert(ctx, 1, a.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.RemoveAlert(ctx, 1, a.ID); err == nil {
		t.Fatal("removed missing alert")
	}
	// Alert tied to target gets dropped from in-memory state when target is removed.
	a, err = s.AddAlert(ctx, 10, 1, "edge", "icmp", "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r.fail = "delete-target"
	if err = s.RemoveTarget(ctx, 1, "edge"); err == nil {
		t.Fatal("expected target delete failure")
	}
	r.fail = ""
	if err = s.RemoveTarget(ctx, 1, "edge"); err != nil {
		t.Fatal(err)
	}
	if len(s.Alerts(1)) != 0 || len(s.Targets(1)) != 0 {
		t.Fatal("target removal did not clear state")
	}
	if err = s.RemoveTarget(ctx, 1, "edge"); err == nil {
		t.Fatal("removed missing target")
	}
	_ = a
}

func TestAddAlertWithIDIsIdempotent(t *testing.T) {
	ctx := context.Background()
	r := &fakeRepo{state: domain.EmptyState()}
	s := newTestService(t, r, &fakeNetwork{}, &fakeBlock{}, &fakeNotifier{})
	if err := s.AddTarget(ctx, 1, "edge", "example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddAlertWithID(ctx, "", 10, 1, "edge", "tcp", "443", time.Minute); err == nil {
		t.Fatal("accepted an empty alert request ID")
	}
	first, err := s.AddAlertWithID(ctx, "form-id", 10, 1, "edge", "tcp", "443", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := s.AddAlertWithID(ctx, "form-id", 10, 1, "edge", "tcp", "443", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if retry != first || len(s.Alerts(1)) != 1 {
		t.Fatalf("idempotent retry = %+v; stored alerts = %+v", retry, s.Alerts(1))
	}
	if _, err = s.AddAlertWithID(ctx, "form-id", 10, 1, "edge", "icmp", "", time.Minute); err == nil {
		t.Fatal("reused request ID for a different alert")
	}
	if len(s.Alerts(1)) != 1 {
		t.Fatalf("request ID conflict changed alerts: %+v", s.Alerts(1))
	}
}

func TestRecordTransitionsAndErrors(t *testing.T) {
	ctx := context.Background()
	a := domain.Alert{ID: "a", ChatID: 7, UserID: 1, Target: domain.Target{Name: "edge", Host: "edge.example"}, Method: "tcp"}
	r := &fakeRepo{state: domain.State{Targets: map[string][]domain.Target{"1": {a.Target}}, Alerts: []domain.Alert{a}}}
	n := &fakeNetwork{}
	b := &fakeBlock{verdict: "listed"}
	notifier := &fakeNotifier{}
	s := newTestService(t, r, n, b, notifier)
	s.record(ctx, a, true, "fine")
	if len(notifier.messages) != 0 || r.state.Alerts[0].LastUp == nil {
		t.Fatal("initial up should persist without alert")
	}
	s.record(ctx, a, true, "fine")
	if len(notifier.messages) != 0 {
		t.Fatal("unchanged state notified")
	}
	b.err = errors.New("chebur down")
	s.record(ctx, a, false, "timeout")
	if len(notifier.messages) != 1 || !strings.Contains(notifier.messages[0], "Cheburcheck failed") || b.calls != 1 {
		t.Fatalf("down notification=%v calls=%d", notifier.messages, b.calls)
	}
	s.record(ctx, a, false, "timeout")
	if len(notifier.messages) != 1 {
		t.Fatal("unchanged down notified")
	}
	s.record(ctx, a, true, "restored")
	if len(notifier.messages) != 2 || !strings.Contains(notifier.messages[1], "available again") {
		t.Fatalf("recovery notification=%v", notifier.messages)
	}
	s.record(ctx, domain.Alert{ID: "missing"}, false, "")
	if len(notifier.messages) != 2 {
		t.Fatal("missing alert emitted notification")
	}
	r.fail = "last-up"
	s.record(ctx, a, false, "timeout")
	if len(notifier.messages) != 2 {
		t.Fatal("persist failure notified")
	}
}
func TestCheckDueAndRun(t *testing.T) {
	ctx := context.Background()
	a := domain.Alert{ID: "a", ChatID: 1, UserID: 1, Target: domain.Target{Name: "edge", Host: "example.com"}, Method: "icmp", Interval: time.Hour}
	r := &fakeRepo{state: domain.State{Targets: domain.EmptyState().Targets, Alerts: []domain.Alert{a}}}
	n := &fakeNetwork{up: true, detail: "ok"}
	s := newTestService(t, r, n, &fakeBlock{}, &fakeNotifier{})
	s.checkDue(ctx)
	if n.calls != 1 || s.state.Alerts[0].LastCheck.IsZero() {
		t.Fatal("initial alert not checked")
	}
	s.checkDue(ctx)
	if n.calls != 1 {
		t.Fatal("not-yet-due alert checked")
	}
	s.state.Alerts[0].LastCheck = time.Time{}
	r.fail = "last-check"
	s.checkDue(ctx)
	if n.calls != 1 {
		t.Fatal("check ran when schedule persistence failed")
	}
	c, cancel := context.WithCancel(ctx)
	cancel()
	s.Run(c)
}

func TestRunTicksAndPreservesOtherUsersAlerts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := domain.Target{Name: "edge", Host: "example.com"}
	r := &fakeRepo{state: domain.State{Targets: map[string][]domain.Target{"1": {target}, "2": {target}}, Alerts: []domain.Alert{{ID: "other", UserID: 2, Target: target}}}}
	s := newTestService(t, r, &fakeNetwork{}, &fakeBlock{}, &fakeNotifier{})
	if err := s.RemoveTarget(context.Background(), 1, "edge"); err != nil {
		t.Fatal(err)
	}
	if len(s.Alerts(2)) != 1 {
		t.Fatal("removed another user's alert")
	}
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	time.Sleep(1050 * time.Millisecond)
	cancel()
	<-done
}
func TestRecordNotifierFailures(t *testing.T) {
	ctx := context.Background()
	target := domain.Target{Name: "edge", Host: "example.com"}
	up := false
	a := domain.Alert{ID: "a", ChatID: 1, UserID: 1, Target: target, Method: "icmp", LastUp: &up}
	r := &fakeRepo{state: domain.State{Targets: map[string][]domain.Target{"1": {target}}, Alerts: []domain.Alert{a}}}
	n := &fakeNotifier{err: errors.New("send failed")}
	s := newTestService(t, r, &fakeNetwork{}, &fakeBlock{verdict: "blocked"}, n)
	s.record(ctx, a, true, "restored")
	s.record(ctx, a, false, "down")
	if len(n.messages) != 2 {
		t.Fatalf("messages=%v", n.messages)
	}
}
