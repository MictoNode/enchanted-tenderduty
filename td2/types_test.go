package tenderduty

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestChainConfigTypeDefaultsCosmos(t *testing.T) {
	cc := &ChainConfig{}
	fatal, _ := validateConfig(&Config{Chains: map[string]*ChainConfig{"x": cc}})
	if fatal {
		t.Fatal("unexpected fatal for default config")
	}
	if cc.Type != "cosmos" {
		t.Fatalf("expected default type cosmos, got %q", cc.Type)
	}
}

func TestChainConfigTypeRejectsUnknown(t *testing.T) {
	cc := &ChainConfig{Type: "cardano"}
	_, problems := validateConfig(&Config{Chains: map[string]*ChainConfig{"x": cc}})
	found := false
	for _, p := range problems {
		if strings.Contains(p, "unknown chain type") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected unknown-type warning, got %v", problems)
	}
}

func TestSavedStateGnoCountersRoundTrip(t *testing.T) {
	s := savedState{
		GnoCounters: map[string]*gnoSavedStats{
			"gno": {TotalSigns: 90, TotalProps: 5, TotalMiss: 7, ConsecutiveMiss: 3},
		},
	}
	b, err := json.Marshal(&s)
	if err != nil {
		t.Fatal(err)
	}
	var back savedState
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	g := back.GnoCounters["gno"]
	if g == nil || g.TotalSigns != 90 || g.TotalMiss != 7 || g.ConsecutiveMiss != 3 {
		t.Fatalf("round-trip lost gno counters: %+v", back.GnoCounters)
	}
}
