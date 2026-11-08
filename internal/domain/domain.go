package domain

import "time"

type Target struct {
	Name string `json:"name"`
	Host string `json:"host"`
}

type Alert struct {
	ID        string        `json:"id"`
	ChatID    int64         `json:"chat_id"`
	UserID    int64         `json:"user_id"`
	Target    Target        `json:"target"`
	Method    string        `json:"method"`
	Argument  string        `json:"argument,omitempty"`
	Interval  time.Duration `json:"interval"`
	LastCheck time.Time     `json:"last_check,omitempty"`
	LastUp    *bool         `json:"last_up,omitempty"`
}

// Interaction stores an in-progress Telegram form so it can resume after restart.
type Interaction struct {
	Step      string
	Name      string
	Host      string
	Method    string
	Argument  string
	RequestID string
}

type State struct {
	Targets map[string][]Target `json:"targets"`
	Alerts  []Alert             `json:"alerts"`
}

type CheckResult struct {
	Up     bool
	Detail string
}

func EmptyState() State { return State{Targets: make(map[string][]Target)} }
