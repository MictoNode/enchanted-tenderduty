package tenderduty

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestStatusUpdateReportsActiveAlarm: watch() pushes a dashboard status when the
// stalled alarm fires, because no block updates arrive while the chain is halted.
// Without it the dashboard kept showing the last pre-halt snapshot (2/2 healthy,
// 0 alerts) for the entire 2026-11-17 onyx-1 halt.
func TestStatusUpdateReportsActiveAlarm(t *testing.T) {
	origAlarms := alarms
	t.Cleanup(func() { alarms = origAlarms })
	alarms = &alarmCache{AllAlarms: map[string]map[string]time.Time{
		"Gnoland-T": {"stalled: have not seen a new block on onyx-1 in 3 minutes": time.Now()},
	}}

	cc := &ChainConfig{
		name:          "Gnoland-T",
		ChainId:       "onyx-1",
		Network:       "testnet",
		Nodes:         []*NodeConfig{{Url: "https://a"}, {Url: "https://b", down: true}},
		valInfo:       &ValInfo{Moniker: "MictoNode", Bonded: true, Missed: 2, Window: 100},
		lastBlockNum:  1203401,
		blocksResults: []int{3, 3, 3},
	}
	st := cc.statusUpdate()

	if st.ActiveAlerts != 1 || cc.activeAlerts != 1 {
		t.Fatalf("active alerts = %d/%d, want 1", st.ActiveAlerts, cc.activeAlerts)
	}
	if st.MsgType != "status" || st.Name != "Gnoland-T" || st.ChainId != "onyx-1" || st.Network != "testnet" {
		t.Fatalf("bad identity fields: %+v", st)
	}
	if st.Height != 1203401 || st.Moniker != "MictoNode" || !st.Bonded || st.Missed != 2 || st.Window != 100 {
		t.Fatalf("stale runtime state in update: %+v", st)
	}
	if st.Nodes != 2 || st.HealthyNodes != 1 {
		t.Fatalf("nodes = %d/%d, want 2/1", st.HealthyNodes, st.Nodes)
	}
	if !strings.Contains(st.LastError, "stalled") {
		t.Fatalf("active alarm missing from LastError: %q", st.LastError)
	}
	// The grid must be a snapshot: the dashboard goroutine consumes it after the
	// chain lock is released.
	cc.blocksResults[0] = 4
	if len(st.Blocks) != 3 || st.Blocks[0] != 3 {
		t.Fatalf("Blocks is not a copy: %v", st.Blocks)
	}
}

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

func TestValidateConfigCountsPushoverAsNotification(t *testing.T) {
	cc := &ChainConfig{
		ChainId:    "test-chain",
		ValAddress: "validator",
		Alerts: AlertConfig{
			AlertIfInactive: true,
			Pushover:        PushoverConfig{Enabled: true, Token: "token", UserKey: "user"},
		},
	}
	_, problems := validateConfig(&Config{
		Pushover: PushoverConfig{Enabled: true},
		Chains:   map[string]*ChainConfig{"x": cc},
	})
	for _, p := range problems {
		if strings.Contains(p, "has no notifications configured") {
			t.Fatalf("pushover-only chain should count as notified, got problems: %v", problems)
		}
	}
}

func TestLoadConfigRestoresPushoverAlarmState(t *testing.T) {
	oldAlarms := alarms
	oldLogs := logs
	defer func() {
		alarms = oldAlarms
		logs = oldLogs
	}()
	logs = make(chan interface{}, 20)
	alarms = &alarmCache{
		SentPdAlarms:  make(map[string]time.Time),
		SentTgAlarms:  make(map[string]time.Time),
		SentDiAlarms:  make(map[string]time.Time),
		SentSlkAlarms: make(map[string]time.Time),
		SentPoAlarms:  make(map[string]time.Time),
		AllAlarms:     make(map[string]map[string]time.Time),
	}

	dir := t.TempDir()
	configFile := filepath.Join(dir, "config.yml")
	stateFile := filepath.Join(dir, "state.json")
	chainsDir := filepath.Join(dir, "chains.d")
	if err := os.Mkdir(chainsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configFile, []byte(`
chains:
  x:
    chain_id: test-chain
    valoper_address: validator
    alerts:
      if_inactive: true
      pushover:
        enabled: true
`), 0o600); err != nil {
		t.Fatal(err)
	}
	sentAt := time.Now().Add(-time.Minute).UTC()
	state := savedState{
		Alarms: &alarmCache{
			SentPoAlarms: map[string]time.Time{"critical": sentAt},
		},
	}
	b, err := json.Marshal(&state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateFile, b, 0o600); err != nil {
		t.Fatal(err)
	}
	password := ""
	if _, err := loadConfig(configFile, stateFile, chainsDir, &password); err != nil {
		t.Fatal(err)
	}
	if alarms.SentPoAlarms["critical"].IsZero() {
		t.Fatalf("SentPoAlarms was not restored: %#v", alarms.SentPoAlarms)
	}
}

func TestClearStalePagerDutyUsesCaseInsensitiveNotice(t *testing.T) {
	oldLogs := logs
	defer func() { logs = oldLogs }()
	logs = make(chan interface{}, 10)
	clearStale(map[string]time.Time{"old": time.Now().Add(-2 * time.Hour)}, "PagerDuty", true, 1)

	found := false
	for len(logs) > 0 {
		entry := <-logs
		fields, ok := entry.([]any)
		if !ok {
			continue
		}
		for _, field := range fields {
			if strings.Contains(field.(string), "manually cleared from PagerDuty") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected stale PagerDuty manual-clear notice")
	}
}
