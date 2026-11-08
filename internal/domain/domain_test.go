package domain

import "testing"

func TestEmptyState(t *testing.T) {
	s := EmptyState()
	if s.Targets == nil || len(s.Targets) != 0 || len(s.Alerts) != 0 {
		t.Fatalf("unexpected empty state: %#v", s)
	}
	s.Targets["1"] = nil
	if len(EmptyState().Targets) != 0 {
		t.Fatal("EmptyState shares target map")
	}
}
