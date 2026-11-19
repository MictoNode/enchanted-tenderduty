package tenderduty

import (
	"fmt"
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
	recent := now.Add(-1 * time.Minute)            // within threshold, healthy
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
		{"disabled with active alarm resolves", false, true, recent, "resolve"},
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
		{"disabled with active alarm resolves", false, false, true, true, "resolve"},
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

func TestPercentageMissedTransition(t *testing.T) {
	cases := []struct {
		name   string
		alerts bool
		alarm  bool
		missed int64
		window int64
		limit  int
		want   string
	}{
		{"fire when over threshold and no alarm", true, false, 6, 10, 50, "fire"},
		{"no-op when over threshold and already alarmed", true, true, 6, 10, 50, ""},
		{"resolve when recovered under threshold", true, true, 4, 10, 50, "resolve"},
		{"no-op when healthy and no alarm", true, false, 4, 10, 50, ""},
		{"equal threshold is not over", true, false, 5, 10, 50, ""},
		{"zero window is ignored", true, false, 1, 0, 50, ""},
		{"disabled: no fire", false, false, 6, 10, 50, ""},
		{"disabled with active alarm resolves", false, true, 4, 10, 50, "resolve"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := percentageMissedTransition(c.alerts, c.alarm, c.missed, c.window, c.limit)
			if got != c.want {
				t.Fatalf("percentageMissedTransition(%v,%v,%d,%d,%d) = %q, want %q",
					c.alerts, c.alarm, c.missed, c.window, c.limit, got, c.want)
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

// TestObsoleteAlarmMessage: the alarm cache is keyed by message text, so an
// alarm whose type was disabled or whose embedded threshold/moniker changed can
// never be resolved by the normal transition logic. clearObsoleteAlarms uses
// this classification at watch() start; without it a state-restored false
// percentage alarm stayed on the dashboard forever (prod 2026-11-19).
func TestObsoleteAlarmMessage(t *testing.T) {
	const moniker, chainID = "M", "c"
	on := AlertConfig{StalledAlerts: true, AlertIfInactive: true, ConsecutiveAlerts: true, PercentageAlerts: true, Stalled: 3, ConsecutiveMissed: 10, Window: 10}
	off := AlertConfig{Stalled: 3, ConsecutiveMissed: 10, Window: 10} // every alert type disabled
	disabledPct := on
	disabledPct.PercentageAlerts = false
	disabledPct.Window = 0 // percentage_missed omitted, like the prod config
	stalled5 := on
	stalled5.Stalled = 5
	consec5 := on
	consec5.ConsecutiveMissed = 5
	pct5 := on
	pct5.Window = 5

	msgs := func(a AlertConfig) currentAlarmMessages {
		return currentAlarmMessages{
			stalled:     fmt.Sprintf("stalled: have not seen a new block on %s in %d minutes", chainID, a.Stalled),
			consecutive: fmt.Sprintf("%s has missed %d blocks on %s", moniker, a.ConsecutiveMissed, chainID),
			percentage:  fmt.Sprintf("%s has missed > %d%% of the signing-health window's blocks on %s", moniker, a.Window, chainID),
		}
	}
	base := msgs(on)

	cases := []struct {
		name string
		msg  string
		a    AlertConfig
		want bool
	}{
		{"stalled exactly current", base.stalled, on, false},
		{"stalled disabled", base.stalled, off, true},
		{"stalled minutes changed", base.stalled, stalled5, true},
		{"consecutive exactly current", base.consecutive, on, false},
		{"consecutive disabled", base.consecutive, off, true},
		{"consecutive threshold changed", base.consecutive, consec5, true},
		{"percentage exactly current", base.percentage, on, false},
		{"percentage disabled, old threshold cached", base.percentage, disabledPct, true},
		{"percentage threshold changed", base.percentage, pct5, true},
		{"inactive disabled", "M is no longer active: validator is jailed", off, true},
		{"inactive enabled", "M is no longer active: validator is jailed", on, false},
		{"node down untouched", "Severity: critical\nRPC node https://x has been down for > 3 minutes on c", off, false},
		{"no servers untouched", "no RPC endpoints are working for c", off, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := msgs(c.a).obsolete(c.msg, c.a); got != c.want {
				t.Fatalf("obsolete(%q) = %v, want %v", c.msg, got, c.want)
			}
		})
	}
}
