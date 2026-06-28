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
