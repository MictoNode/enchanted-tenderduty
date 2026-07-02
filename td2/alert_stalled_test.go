package tenderduty

import (
	"testing"
	"time"
)

// TestStalledTransition covers the stalled-alarm state machine shared by watch()
// for both the cosmos and gnoland providers. The key behavior added over the old
// inline if/else is the "resolve on recover" branch: when blocks are flowing
// again within the threshold, an already-active stalled alarm must be resolved so
// it clears from the alarm cache and the dashboard stops blinking — previously it
// only cleared when lastBlockTime was the zero value, which never happened in the
// normal poll path, so a recovered stalled alarm lingered (and survived restart).
func TestStalledTransition(t *testing.T) {
	now := time.Date(2026, 6, 28, 9, 50, 0, 0, time.UTC)
	stalledMin := 3
	threshold := time.Duration(stalledMin) * time.Minute
	recent := now.Add(-1 * time.Minute)      // within threshold, healthy
	stale := now.Add(-threshold).Add(-time.Second) // just past threshold, stalled

	cases := []struct {
		name      string
		alerts    bool
		alarm     bool
		blockTime time.Time
		want      string
	}{
		{"fire when stalled and no alarm", true, false, stale, "fire"},
		{"no-op when stalled and already alarmed", true, true, stale, ""},
		{"resolve on recover within threshold", true, true, recent, "resolve"},
		{"no-op when healthy and no alarm", true, false, recent, ""},
		{"resolve on zero block time (defensive)", true, true, time.Time{}, "resolve"},
		{"disabled: no fire", false, false, stale, ""},
		{"disabled: no resolve", false, true, recent, ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := stalledTransition(c.alerts, c.alarm, c.blockTime, stalledMin, now)
			if got != c.want {
				t.Fatalf("stalledTransition(%v,%v,%v,%d) = %q, want %q",
					c.alerts, c.alarm, c.blockTime, stalledMin, got, c.want)
			}
		})
	}
}

// TestInactiveTransition covers the validator-inactive (jailed/tombstoned) alarm
// state machine in watch(). It mirrors stalledTransition: the resolve decision is
// driven by whether the alarm is actually present in the cache (alarmActive), not
// by the bonded-state diff alone. Without this gate, while a bonded-state diff
// persists between gnoMonitorHealth refreshes (up to a minute), watch() re-fires
// the resolve every 2s tick and shouldNotify spams "no corresponding alert" for
// every sink — exactly the prod log noise seen when a gno validator was
// transiently misread as inactive.
func TestInactiveTransition(t *testing.T) {
	cases := []struct {
		name        string
		alerts      bool
		lastBonded  bool
		curBonded   bool
		alarmActive bool
		want        string
	}{
		{"fire when newly inactive and not already alarmed", true, true, false, false, "fire"},
		{"no-op when inactive but already alarmed", true, true, false, true, ""},
		{"resolve when back to active and alarm present", true, false, true, true, "resolve"},
		{"no-op resolve when active but no prior alarm (the spam bug)", true, false, true, false, ""},
		{"no-op when bonded unchanged (both bonded)", true, true, true, false, ""},
		{"no-op when never bonded", true, false, false, false, ""},
		{"disabled: no fire", false, true, false, false, ""},
		{"disabled: no resolve", false, false, true, true, ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := inactiveTransition(c.alerts, c.lastBonded, c.curBonded, c.alarmActive)
			if got != c.want {
				t.Fatalf("inactiveTransition(%v,%v,%v,%v) = %q, want %q",
					c.alerts, c.lastBonded, c.curBonded, c.alarmActive, got, c.want)
			}
		})
	}
}

// TestAlarmCacheHasMessage verifies the cache lookup the stalled resolver relies
// on to decide whether a stalled alarm is still active (and thus needs clearing).
func TestAlarmCacheHasMessage(t *testing.T) {
	a := &alarmCache{AllAlarms: map[string]map[string]time.Time{}}
	const chain = "c"
	const msg = "stalled: have not seen a new block on x in 3 minutes"
	if a.hasMessage(chain, msg) {
		t.Fatal("hasMessage true before any alarm set")
	}
	a.AllAlarms[chain] = map[string]time.Time{msg: time.Now()}
	if !a.hasMessage(chain, msg) {
		t.Fatal("hasMessage false after alarm set")
	}
	if a.hasMessage(chain, "other") {
		t.Fatal("hasMessage true for an unset message")
	}
}
