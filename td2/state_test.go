package tenderduty

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWriteStateFileAtomic: writeStateFile must persist atomically — the
// destination file appears with 0600 perms, no .tmp is left behind, the
// content round-trips through JSON, and a second write replaces it.
func TestWriteStateFileAtomic(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state.json")

	first := time.Now().UTC()
	s := &savedState{
		Alarms: &alarmCache{
			SentPoAlarms: map[string]time.Time{"x": first},
		},
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}

	if err := writeStateFile(stateFile, b); err != nil {
		t.Fatalf("writeStateFile: %v", err)
	}

	info, err := os.Stat(stateFile)
	if err != nil {
		t.Fatalf("state file not created: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Fatalf("state file mode = %v, want 0600", perm)
	}
	if _, err := os.Stat(stateFile + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind after write: %v", err)
	}

	back, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	var got savedState
	if err := json.Unmarshal(back, &got); err != nil {
		t.Fatalf("unmarshal state file: %v", err)
	}
	if got.Alarms == nil {
		t.Fatal("Alarms is nil after round trip")
	}
	if gotTime := got.Alarms.SentPoAlarms["x"]; !gotTime.Equal(first) {
		t.Fatalf("SentPoAlarms[x] = %v, want %v", gotTime, first)
	}

	// Second write over the existing file must replace the content.
	second := time.Now().UTC().Add(-time.Hour)
	s2 := &savedState{
		Alarms: &alarmCache{
			SentPoAlarms: map[string]time.Time{"y": second},
		},
	}
	b2, err := json.Marshal(s2)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStateFile(stateFile, b2); err != nil {
		t.Fatalf("second writeStateFile: %v", err)
	}

	back2, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("read state file after overwrite: %v", err)
	}
	var got2 savedState
	if err := json.Unmarshal(back2, &got2); err != nil {
		t.Fatalf("unmarshal overwritten state file: %v", err)
	}
	if got2.Alarms == nil {
		t.Fatal("Alarms is nil after overwrite round trip")
	}
	if gotTime := got2.Alarms.SentPoAlarms["y"]; !gotTime.Equal(second) {
		t.Fatalf("SentPoAlarms[y] = %v, want %v", gotTime, second)
	}
	if gotTime := got2.Alarms.SentPoAlarms["x"]; !gotTime.IsZero() {
		t.Fatalf("SentPoAlarms[x] = %v, want absent after overwrite", gotTime)
	}
	if _, err := os.Stat(stateFile + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind after overwrite: %v", err)
	}
}

// TestSnapshotStateDoesNotDeadlockWatch: watch() holds a chain's stateMux and
// then takes chainsMux.RLock inside Config.alert, while snapshotState used to
// hold chainsMux.Lock and only then take each chain's stateMux.RLock. That
// lock-order inversion deadlocks the whole process as soon as the 60s periodic
// save overlaps an alert — no alerts, no saves, no dashboard updates.
func TestSnapshotStateDoesNotDeadlockWatch(t *testing.T) {
	origAlarms := alarms
	t.Cleanup(func() { alarms = origAlarms })

	cfg := &Config{
		alertChan: make(chan *alertMsg, 8),
		Chains:    map[string]*ChainConfig{"Gnoland-T": {name: "Gnoland-T", ChainId: "onyx-1"}},
	}
	alarms = &alarmCache{
		SentPdAlarms:   map[string]time.Time{},
		SentTgAlarms:   map[string]time.Time{},
		SentDiAlarms:   map[string]time.Time{},
		SentSlkAlarms:  map[string]time.Time{},
		SentPoAlarms:   map[string]time.Time{},
		AllAlarms:      map[string]map[string]time.Time{},
		flappingAlarms: map[string]map[string]time.Time{},
	}

	cc := cfg.Chains["Gnoland-T"]
	cc.stateMux.Lock() // what watch() does before raising an alarm

	snapDone := make(chan struct{})
	go func() { _, _ = snapshotStateOf(cfg); close(snapDone) }()
	// Give snapshotState time to reach its per-chain lock wait (the old code held
	// chainsMux.Lock at this point).
	time.Sleep(100 * time.Millisecond)

	alertDone := make(chan struct{})
	go func() {
		cfg.alert("Gnoland-T", "deadlock probe", "critical", false, nil)
		close(alertDone)
	}()

	select {
	case <-alertDone:
	case <-time.After(2 * time.Second):
		cc.stateMux.Unlock()
		t.Fatal("deadlock: snapshotState waits for stateMux while holding chainsMux")
	}

	cc.stateMux.Unlock()
	select {
	case <-snapDone:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshotState did not complete after the chain lock was released")
	}
}

// TestSaveOnExitPeriodicSave: the core new contract — saveOnExit must persist
// the state while running (periodic tick), before cancel() is ever called.
func TestSaveOnExitPeriodicSave(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state.json")

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	origInterval := stateSaveInterval
	stateSaveInterval = 20 * time.Millisecond
	t.Cleanup(func() { stateSaveInterval = origInterval })

	saved := make(chan interface{})
	go saveOnExit(stateFile, saved)

	// Poll until the file exists with valid JSON — proof a periodic save fired
	// before we cancelled. Atomic writes guarantee the file is never partial.
	deadline := time.Now().Add(2 * time.Second)
	for {
		b, err := os.ReadFile(stateFile)
		if err == nil && json.Valid(b) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic save did not produce a valid state file within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-saved:
	case <-time.After(2 * time.Second):
		t.Fatal("saveOnExit did not close saved after ctx cancel")
	}

	final, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("final state file: %v", err)
	}
	if !json.Valid(final) {
		t.Fatal("final state file is not valid JSON")
	}
	if _, err := os.Stat(stateFile + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind: %v", err)
	}
}

// TestSaveOnExitPeriodicSaveConcurrentAlarmWrites: periodic snapshots must
// marshal the alarm cache under notifyMux (RLock) — this must pass clean under
// -race while a production writer (clearAll) mutates the same maps.
func TestSaveOnExitPeriodicSaveConcurrentAlarmWrites(t *testing.T) {
	dir := t.TempDir()
	stateFile := filepath.Join(dir, "state.json")

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	origInterval := stateSaveInterval
	stateSaveInterval = 15 * time.Millisecond
	t.Cleanup(func() { stateSaveInterval = origInterval })

	// Seed the cache under the lock so clearAll/shouldNotify do real work.
	alarms.notifyMux.Lock()
	alarms.AllAlarms["race-chain"] = make(map[string]time.Time)
	alarms.notifyMux.Unlock()

	saved := make(chan interface{})
	go saveOnExit(stateFile, saved)

	// Concurrent production writers, exactly the access the RLock snapshot
	// protects against. clearAll takes notifyMux.Lock and rewrites the map.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			alarms.clearAll("race-chain")
			// alternating resolved keeps shouldNotify writing (add then delete)
			_ = shouldNotify(&alertMsg{chain: "race-chain", message: "race-msg", resolved: i%2 == 0}, di)
			i++
			time.Sleep(time.Millisecond)
		}
	}()

	// Let several periodic saves fire while the writer is running.
	time.Sleep(100 * time.Millisecond)

	cancel()
	select {
	case <-saved:
	case <-time.After(2 * time.Second):
		t.Fatal("saveOnExit did not close saved after ctx cancel")
	}
	<-writerDone

	b, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("state file: %v", err)
	}
	if !json.Valid(b) {
		t.Fatal("state file is not valid JSON")
	}
	if _, err := os.Stat(stateFile + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file left behind: %v", err)
	}
}
