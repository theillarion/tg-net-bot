package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/theillarion/tg-net-bot/internal/blockcheck"
	"github.com/theillarion/tg-net-bot/internal/checker"
	"github.com/theillarion/tg-net-bot/internal/domain"
)

type Repository interface {
	Load(context.Context) (domain.State, error)
	InsertTarget(context.Context, int64, domain.Target) error
	DeleteTarget(context.Context, int64, string) error
	InsertAlert(context.Context, domain.Alert) error
	DeleteAlert(context.Context, int64, string) error
	SetAlertLastCheck(context.Context, string, time.Time) error
	SetAlertLastUp(context.Context, string, bool) error
}
type NetworkChecker interface {
	Check(context.Context, string, checker.Method, string) (bool, string)
}
type BlockChecker interface {
	Check(context.Context, string) (blockcheck.Result, error)
	Probe(context.Context, string) (blockcheck.ProbeResult, error)
	CheckBatch(context.Context, []string, func(blockcheck.BatchItem)) error
}
type Notifier interface {
	Send(context.Context, int64, string) error
}

type Service struct {
	mu       sync.Mutex
	repo     Repository
	network  NetworkChecker
	block    BlockChecker
	notifier Notifier
	state    domain.State
	logger   *slog.Logger
}

func New(ctx context.Context, repo Repository, network NetworkChecker, block BlockChecker, notifier Notifier, logger *slog.Logger) (*Service, error) {
	state, err := repo.Load(ctx)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{repo: repo, network: network, block: block, notifier: notifier, state: state, logger: logger}, nil
}
func userKey(user int64) string { return strconv.FormatInt(user, 10) }
func (s *Service) Check(ctx context.Context, host, method, argument string) (domain.CheckResult, error) {
	if !checker.ValidHost(host) {
		return domain.CheckResult{}, fmt.Errorf("invalid server: expected an IP or domain")
	}
	m, arg, err := checker.Parse(method, argument)
	if err != nil {
		return domain.CheckResult{}, err
	}
	up, detail := s.network.Check(ctx, host, m, arg)
	return domain.CheckResult{Up: up, Detail: detail}, nil
}
func (s *Service) Block(ctx context.Context, host string) (blockcheck.Result, error) {
	if !checker.ValidHost(host) {
		return blockcheck.Result{}, fmt.Errorf("invalid server: expected an IP or domain")
	}
	return s.block.Check(ctx, host)
}

func (s *Service) BlockProbe(ctx context.Context, id string) (blockcheck.ProbeResult, error) {
	return s.block.Probe(ctx, id)
}

func (s *Service) BlockProbeWithEvents(ctx context.Context, id string, emit func(blockcheck.ProbeResult)) (blockcheck.ProbeResult, error) {
	if streamer, ok := s.block.(interface {
		ProbeWithEvents(context.Context, string, func(blockcheck.ProbeResult)) (blockcheck.ProbeResult, error)
	}); ok {
		return streamer.ProbeWithEvents(ctx, id, emit)
	}
	result, err := s.block.Probe(ctx, id)
	if err == nil && emit != nil {
		emit(result)
	}
	return result, err
}

func (s *Service) BlockBatch(ctx context.Context, targets []string, emit func(blockcheck.BatchItem)) error {
	return s.block.CheckBatch(ctx, targets, emit)
}
func (s *Service) AddTarget(ctx context.Context, user int64, name, host string) error {
	if strings.TrimSpace(name) == "" || !checker.ValidHost(host) {
		return fmt.Errorf("server needs a name and a valid IP or domain")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.state.Targets[userKey(user)]
	for _, t := range items {
		if strings.EqualFold(t.Name, name) {
			return fmt.Errorf("target name already exists")
		}
	}
	target := domain.Target{Name: name, Host: host}
	if err := s.repo.InsertTarget(ctx, user, target); err != nil {
		return fmt.Errorf("save target: %w", err)
	}
	s.state.Targets[userKey(user)] = append(items, target)
	return nil
}
func (s *Service) Targets(user int64) []domain.Target {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Target(nil), s.state.Targets[userKey(user)]...)
}
func (s *Service) FindTarget(user int64, name string) (domain.Target, bool) {
	for _, t := range s.Targets(user) {
		if strings.EqualFold(t.Name, name) {
			return t, true
		}
	}
	return domain.Target{}, false
}
func (s *Service) RemoveTarget(ctx context.Context, user int64, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := userKey(user)
	items := s.state.Targets[key]
	kept := make([]domain.Target, 0, len(items))
	removed := false
	for _, t := range items {
		if strings.EqualFold(t.Name, name) {
			removed = true
		} else {
			kept = append(kept, t)
		}
	}
	if !removed {
		return fmt.Errorf("saved target not found")
	}
	if err := s.repo.DeleteTarget(ctx, user, name); err != nil {
		return fmt.Errorf("remove target from database: %w", err)
	}
	s.state.Targets[key] = kept
	alerts := make([]domain.Alert, 0, len(s.state.Alerts))
	for _, a := range s.state.Alerts {
		if a.UserID != user || !strings.EqualFold(a.Target.Name, name) {
			alerts = append(alerts, a)
		}
	}
	s.state.Alerts = alerts
	return nil
}
func (s *Service) AddAlert(ctx context.Context, chat, user int64, name, method, argument string, interval time.Duration) (domain.Alert, error) {
	return s.AddAlertWithID(ctx, NewAlertID(), chat, user, name, method, argument, interval)
}

// NewAlertID creates an ID that can be persisted with a form before creating an alert.
func NewAlertID() string { return strconv.FormatInt(time.Now().UnixNano(), 36) }

func (s *Service) AddAlertWithID(ctx context.Context, id string, chat, user int64, name, method, argument string, interval time.Duration) (domain.Alert, error) {
	if id == "" {
		return domain.Alert{}, fmt.Errorf("alert ID must not be empty")
	}
	m, arg, err := checker.Parse(method, argument)
	if err != nil {
		return domain.Alert{}, err
	}
	if interval <= 0 {
		return domain.Alert{}, fmt.Errorf("alert interval must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.state.Alerts {
		if existing.ID != id {
			continue
		}
		if existing.ChatID == chat && existing.UserID == user && strings.EqualFold(existing.Target.Name, name) && existing.Method == string(m) && existing.Argument == arg && existing.Interval == interval {
			return existing, nil
		}
		return domain.Alert{}, fmt.Errorf("alert ID already belongs to a different request")
	}
	var target *domain.Target
	for i := range s.state.Targets[userKey(user)] {
		if strings.EqualFold(s.state.Targets[userKey(user)][i].Name, name) {
			target = &s.state.Targets[userKey(user)][i]
			break
		}
	}
	if target == nil {
		return domain.Alert{}, fmt.Errorf("saved target not found")
	}
	alert := domain.Alert{ID: id, ChatID: chat, UserID: user, Target: *target, Method: string(m), Argument: arg, Interval: interval}
	if err = s.repo.InsertAlert(ctx, alert); err != nil {
		return domain.Alert{}, fmt.Errorf("save alert: %w", err)
	}
	s.state.Alerts = append(s.state.Alerts, alert)
	return alert, nil
}
func (s *Service) Alerts(user int64) []domain.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := []domain.Alert{}
	for _, a := range s.state.Alerts {
		if a.UserID == user {
			items = append(items, a)
		}
	}
	return items
}
func (s *Service) RemoveAlert(ctx context.Context, user int64, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := -1
	for i, alert := range s.state.Alerts {
		if alert.UserID == user && alert.ID == id {
			index = i
			break
		}
	}
	if index < 0 {
		return fmt.Errorf("alert not found")
	}
	if err := s.repo.DeleteAlert(ctx, user, id); err != nil {
		return fmt.Errorf("remove alert from database: %w", err)
	}
	s.state.Alerts = append(s.state.Alerts[:index], s.state.Alerts[index+1:]...)
	return nil
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkDue(ctx)
		}
	}
}
func (s *Service) checkDue(ctx context.Context) {
	now := time.Now()
	s.mu.Lock()
	due := []domain.Alert{}
	for i := range s.state.Alerts {
		a := &s.state.Alerts[i]
		if a.LastCheck.IsZero() || now.Sub(a.LastCheck) >= a.Interval {
			if err := s.repo.SetAlertLastCheck(ctx, a.ID, now); err != nil {
				s.logger.Error("persist alert schedule", "alert_id", a.ID, "error", err)
				continue
			}
			a.LastCheck = now
			due = append(due, *a)
		}
	}
	s.mu.Unlock()
	for _, a := range due {
		checkCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		up, detail := s.network.Check(checkCtx, a.Target.Host, checker.Method(a.Method), a.Argument)
		cancel()
		s.record(ctx, a, up, detail)
	}
}
func (s *Service) record(ctx context.Context, alert domain.Alert, up bool, detail string) {
	s.mu.Lock()
	idx := -1
	for i := range s.state.Alerts {
		if s.state.Alerts[i].ID == alert.ID {
			idx = i
			break
		}
	}
	if idx < 0 {
		s.mu.Unlock()
		return
	}
	previous := s.state.Alerts[idx].LastUp
	first := previous == nil
	changed := previous != nil && *previous != up || first && !up
	if err := s.repo.SetAlertLastUp(ctx, alert.ID, up); err != nil {
		s.logger.Error("persist alert result", "alert_id", alert.ID, "error", err)
		s.mu.Unlock()
		return
	}
	value := up
	s.state.Alerts[idx].LastUp = &value
	s.mu.Unlock()
	if !changed {
		return
	}
	if up {
		if err := s.notifier.Send(ctx, alert.ChatID, fmt.Sprintf("✅ %s is available again (%s). %s", alert.Target.Name, alert.Method, detail)); err != nil {
			s.logger.Error("send recovery alert", "alert_id", alert.ID, "error", err)
		}
		return
	}
	blockCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	verdict, err := s.block.Check(blockCtx, alert.Target.Host)
	blockSummary := "database: not found in blocklists"
	if err != nil {
		blockSummary = "Cheburcheck failed: " + err.Error()
	} else if verdict.Blocked {
		blockSummary = "database: listed as blocked"
	}
	if err := s.notifier.Send(ctx, alert.ChatID, fmt.Sprintf("🔴 %s is unavailable (%s).\nCheck: %s\nCheburcheck: %s", alert.Target.Name, alert.Method, detail, blockSummary)); err != nil {
		s.logger.Error("send unavailable alert", "alert_id", alert.ID, "error", err)
	}
}
