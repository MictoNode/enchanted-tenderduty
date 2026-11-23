package tenderduty

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const (
	espressoTestKey = "BLS_VER_KEY~N0OiqRFrqQswtyicd5u9_7c519K-j7UXJd-iVxD-LwhSeF4XfgLWrRXw7jyTpikbFT11aYDZr4--M-W6O0MWGANO5W_FpnsSnQ0k5YNdIPINLIqkCKTpjckIIwVEyoMUpihBmQxlij_MNnlDcSMAKyVZ7fbX9T7Q_sQsDWlMVJ_M"
	espressoTestEth = "0x9d8d7866aec0ef09e19d0c458623280f00ac37b8"
)

func espressoMetricsFixture(assigned, missed, current, voted int64, key, moniker string) string {
	return fmt.Sprintf(`# HELP consensus_number_of_timeouts_as_leader views where this node was leader and failed to propose
# TYPE consensus_number_of_timeouts_as_leader counter
consensus_number_of_timeouts_as_leader %d
# HELP consensus_view_duration_as_leader view duration as leader
# TYPE consensus_view_duration_as_leader histogram
consensus_view_duration_as_leader_bucket{le="1"} 42
consensus_view_duration_as_leader_count %d
# HELP consensus_current_view current view
# TYPE consensus_current_view gauge
consensus_current_view %d
# HELP consensus_last_voted_view last voted view
# TYPE consensus_last_voted_view gauge
consensus_last_voted_view %d
# HELP consensus_last_decided_view last decided view
# TYPE consensus_last_decided_view gauge
consensus_last_decided_view %d
# HELP consensus_node node
# TYPE consensus_node gauge
consensus_node{key=%q} 1
# HELP consensus_node_identity_general node_identity_general
# TYPE consensus_node_identity_general gauge
consensus_node_identity_general{company_name="MictoNode",name=%q,operating_system="Linux"} 1
# HELP consensus_version version
# TYPE consensus_version gauge
consensus_version{desc="0.7.0.6",rev="abc"} 1
`, missed, assigned, current, voted, current, key, moniker)
}

// espressoMock serves the validator node Status API. Values are swapped
// between poll ticks to drive the counter deltas.
type espressoMock struct {
	mu       sync.Mutex
	assigned int64
	missed   int64
	current  int64
	voted    int64
	height   int64
}

func (m *espressoMock) set(assigned, missed, height int64) {
	m.mu.Lock()
	m.assigned, m.missed, m.height = assigned, missed, height
	m.mu.Unlock()
}

func (m *espressoMock) serve(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	assigned, missed, current, voted, height := m.assigned, m.missed, m.current, m.voted, m.height
	m.mu.Unlock()
	switch r.URL.Path {
	case "/v1/status/metrics":
		_, _ = w.Write([]byte(espressoMetricsFixture(assigned, missed, current, voted, espressoTestKey, "MictoNode")))
	case "/v1/status/block-height":
		_, _ = fmt.Fprint(w, height)
	case "/v1/status/time-since-last-decide":
		_, _ = fmt.Fprint(w, 1)
	case "/v1/status/keys":
		_, _ = fmt.Fprintf(w, `{"eth_account":%q,"consensus_key":%q,"state_ver_key":"SCHNORR_VER_KEY~x","x25519_key":"X25519_PK~x","p2p_addr":"1.2.3.4:9977"}`,
			espressoTestEth, espressoTestKey)
	case "/v1/node/stake-table/current":
		_, _ = fmt.Fprintf(w, `{"epoch":845,"stake_table":[{"stake_table_entry":{"stake_key":%q,"stake_amount":"0x42"}}]}`, espressoTestKey)
	default:
		http.NotFound(w, r)
	}
}

func newEspressoTestChain(url string) *ChainConfig {
	blocks := make([]int, showBLocks)
	for i := range blocks {
		blocks[i] = -1
	}
	cc := &ChainConfig{
		name:               "Espresso-M",
		ChainId:            "espresso-mainnet",
		Type:               "espresso",
		EspressoNodeURL:    url,
		EspressoStakingKey: espressoTestKey,
		EspressoDutyWindow: 100,
		Nodes:              []*NodeConfig{{Url: url, AlertIfDown: true}},
		valInfo:            &ValInfo{Moniker: "MictoNode", Bonded: true, Valcons: espressoTestKey},
		blocksResults:      blocks,
	}
	return cc
}

func shortenEspressoPollTimers(t *testing.T, interval, idle time.Duration) {
	t.Helper()
	oi, oe := espressoPollInterval, espressoPollIdleExit
	espressoPollInterval, espressoPollIdleExit = interval, idle
	t.Cleanup(func() { espressoPollInterval, espressoPollIdleExit = oi, oe })
}

// allowEveryStakeTableFetch disables the production rate limit so a test can
// drive two consecutive stake-table answers.
func allowEveryStakeTableFetch(t *testing.T) {
	t.Helper()
	orig := espressoStakeTableInterval
	espressoStakeTableInterval = 0
	t.Cleanup(func() { espressoStakeTableInterval = orig })
}

func TestEspressoParseMetrics(t *testing.T) {
	st := espressoParseMetrics([]byte(espressoMetricsFixture(44803, 15, 34667904, 34667903, espressoTestKey, "MictoNode")))
	if st.Assigned != 44803 || st.Missed != 15 {
		t.Fatalf("counters: assigned=%d missed=%d", st.Assigned, st.Missed)
	}
	if st.CurrentView != 34667904 || st.LastVotedView != 34667903 || st.LastDecidedView != 34667904 {
		t.Fatalf("views: current=%d voted=%d decided=%d", st.CurrentView, st.LastVotedView, st.LastDecidedView)
	}
	if st.StakingKey != espressoTestKey {
		t.Fatalf("staking key not parsed: %q", st.StakingKey)
	}
	if st.Moniker != "MictoNode" || st.Version != "0.7.0.6" {
		t.Fatalf("identity: moniker=%q version=%q", st.Moniker, st.Version)
	}
	// The HELP line for consensus_node must not be mistaken for the sample.
	if st.StakingKey == "" {
		t.Fatal("metric sample line not found")
	}
}

// TestEspressoStatusLegacyPaths: older builds serve /status/* without the /v1
// prefix; the provider must fall back instead of reporting the node down.
func TestEspressoStatusLegacyPaths(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status/metrics":
			_, _ = w.Write([]byte(espressoMetricsFixture(10, 1, 100, 99, espressoTestKey, "MictoNode")))
		case "/status/block-height":
			_, _ = fmt.Fprint(w, "1234")
		case "/status/time-since-last-decide":
			_, _ = fmt.Fprint(w, "2")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	st, err := espressoFetchStatus(srv.URL)
	if err != nil {
		t.Fatalf("legacy path fetch: %v", err)
	}
	if st.Height != 1234 || st.Assigned != 10 || st.Missed != 1 || st.TimeSinceDecide != 2 {
		t.Fatalf("legacy status: %+v", st)
	}
}

// TestEspressoPollRecordsDutyOutcomes drives three counter samples through
// PollRun and verifies the grid, counters, streak and sliding window.
func TestEspressoPollRecordsDutyOutcomes(t *testing.T) {
	shortenEspressoPollTimers(t, 10*time.Millisecond, time.Minute)

	m := &espressoMock{current: 1000, voted: 999, height: 500}
	m.set(10, 1, 500) // baseline sample: no events
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newEspressoTestChain(srv.URL)
	done := make(chan struct{})
	go func() { cc.EspressoPollRun(); close(done) }()

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for {
			cc.stateMux.RLock()
			ok := cond()
			cc.stateMux.RUnlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				cancel()
				<-done
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Start from a known height so the first tick does not prime the grid.
	cc.lastBlockNum = 500

	// Wait for the first (baseline) sample before changing the counters: the
	// poller may start after the test's set() call, and a baseline of 12 would
	// swallow the first delta.
	waitFor("baseline sample", func() bool { return cc.espressoCountersValid && cc.espressoAssignedTotal == 10 })

	// 2 more duties assigned, 1 missed → one miss cell + one proposed cell.
	m.set(12, 2, 501)
	waitFor("second sample", func() bool { return cc.espressoAssignedTotal == 12 })

	// 3 more duties, all missed → streak of 3.
	m.set(15, 5, 500)
	waitFor("third sample", func() bool { return cc.espressoAssignedTotal == 15 })

	cancel()
	<-done

	cc.stateMux.RLock()
	defer cc.stateMux.RUnlock()
	if cc.statTotalProps != 1 || cc.statTotalSigns != 1 {
		t.Fatalf("proposal counters: props=%v signs=%v, want 1/1", cc.statTotalProps, cc.statTotalSigns)
	}
	if cc.statTotalMiss != 4 {
		t.Fatalf("total missed: %v, want 4", cc.statTotalMiss)
	}
	if cc.statConsecutiveMiss != 3 {
		t.Fatalf("consecutive misses: %v, want 3", cc.statConsecutiveMiss)
	}
	if cc.valInfo == nil || cc.valInfo.Missed != 4 || cc.valInfo.Window != 5 {
		t.Fatalf("duty window: %+v, want missed=4 window=5", cc.valInfo)
	}
	// Grid is newest-first and per BLOCK, with one cell per missed duty inserted
	// in front of that tick's block cells. Tick2 had one successful proposal, so
	// the newest signed block cell was painted green (proposed); tick3 (lower
	// head, no new blocks) pushed three missed cells.
	want := []int{int(Statusmissed), int(Statusmissed), int(Statusmissed), int(Statusmissed), int(StatusProposed)}
	for i, w := range want {
		if cc.blocksResults[i] != w {
			t.Fatalf("grid[%d]=%d, want %d (grid=%v)", i, cc.blocksResults[i], w, cc.blocksResults[:6])
		}
	}
	if cc.espressoVoteLag != 1 {
		t.Fatalf("vote lag: %d, want 1", cc.espressoVoteLag)
	}
	if cc.lastBlockNum != 501 {
		t.Fatalf("lastBlockNum: %d, want 501 (a lower head must not rewind)", cc.lastBlockNum)
	}
}

// TestEspressoPollRebaselinesOnCounterReset: the node counters are
// process-lifetime Prometheus counters. A drop means the node restarted, and
// must re-seed the baseline instead of producing bogus negative deltas.
func TestEspressoPollRebaselinesOnCounterReset(t *testing.T) {
	shortenEspressoPollTimers(t, 10*time.Millisecond, time.Minute)

	m := &espressoMock{current: 1000, voted: 999, height: 700}
	m.set(100, 5, 700)
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newEspressoTestChain(srv.URL)
	cc.lastBlockNum = 700
	done := make(chan struct{})
	go func() { cc.EspressoPollRun(); close(done) }()

	waitForVal := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for {
			cc.stateMux.RLock()
			ok := cond()
			cc.stateMux.RUnlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				cancel()
				<-done
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	waitForVal("baseline", func() bool { return cc.espressoAssignedTotal == 100 && cc.espressoCountersValid })

	// Node restart: counters reset to near zero.
	m.set(3, 0, 701)
	waitForVal("reset baseline", func() bool { return cc.espressoAssignedTotal == 3 })

	cancel()
	<-done

	cc.stateMux.RLock()
	defer cc.stateMux.RUnlock()
	if cc.statTotalMiss != 0 || cc.statTotalProps != 0 {
		t.Fatalf("counter reset produced phantom events: missed=%v props=%v", cc.statTotalMiss, cc.statTotalProps)
	}
	if cc.valInfo.Window != 0 {
		t.Fatalf("counter reset pushed %d phantom duties into the window", cc.valInfo.Window)
	}
	if cc.lastBlockNum != 701 {
		t.Fatalf("lastBlockNum: %d, want 701", cc.lastBlockNum)
	}
	// The height advance is still recorded as a signed block cell; only duty
	// accounting must stay untouched.
	if cc.valInfo.Window != 0 || cc.blocksResults[0] != int(StatusSigned) {
		t.Fatalf("grid after reset: window=%d blocks[0]=%d, want 0/signed", cc.valInfo.Window, cc.blocksResults[0])
	}
}

// TestEspressoPollChainOnlyWithoutNodeURL: without the node Status API the
// chain is still watched for liveness (height / stall clock), duties are not.
func TestEspressoPollChainOnlyWithoutNodeURL(t *testing.T) {
	shortenEspressoPollTimers(t, 10*time.Millisecond, time.Minute)

	m := &espressoMock{height: 900}
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newEspressoTestChain(srv.URL)
	cc.EspressoNodeURL = ""
	done := make(chan struct{})
	go func() { cc.EspressoPollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		h := cc.lastBlockNum
		cc.stateMux.RUnlock()
		if h == 900 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("chain-only mode never advanced height: %d", h)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The first sample only seeds the height; the next advance must paint
	// "No Data" (-1) cells, not green ones: without the node API our
	// participation is unknown.
	m.set(0, 0, 901)
	deadline = time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		h := cc.lastBlockNum
		cc.stateMux.RUnlock()
		if h == 901 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("chain-only mode never advanced past the primed height: %d", h)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	cc.stateMux.RLock()
	defer cc.stateMux.RUnlock()
	if cc.blocksResults[0] != -1 {
		t.Fatalf("chain-only grid cell: %d, want -1 (No Data)", cc.blocksResults[0])
	}
	if cc.valInfo.Window != 0 {
		t.Fatalf("chain-only mode pushed duties: %d", cc.valInfo.Window)
	}
}

// TestEspressoGetValInfoStakeTableAndKeyDiscovery: identity comes from
// /v1/status/keys (fallback: the metrics label) and Bonded from the stake table,
// with a two-tick debounce before a not-in-set verdict flips.
func TestEspressoGetValInfoStakeTableAndKeyDiscovery(t *testing.T) {
	allowEveryStakeTableFetch(t)
	m := &espressoMock{current: 10, voted: 9, height: 1}
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	cc := newEspressoTestChain(srv.URL)
	cc.espressoWin = newGnoWindow(10)
	cc.EspressoStakingKey = ""
	cc.valInfo = &ValInfo{Moniker: "not connected"}

	if err := cc.EspressoGetValInfo(true); err != nil {
		t.Fatalf("GetValInfo: %v", err)
	}
	if cc.valInfo.Valcons != espressoTestKey {
		t.Fatalf("key discovery: %q", cc.valInfo.Valcons)
	}
	if cc.valInfo.Moniker != "MictoNode" {
		t.Fatalf("moniker: %q", cc.valInfo.Moniker)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("validator in the stake table must be bonded")
	}
	if cc.espressoEpoch != 845 {
		t.Fatalf("epoch: %d, want 845", cc.espressoEpoch)
	}

	// Stake table no longer lists the key: first definitive answer must NOT flip
	// Bonded (debounce), the second one must.
	absent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/node/stake-table/current":
			_, _ = fmt.Fprint(w, `{"epoch":845,"stake_table":[]}`)
		case "/v1/status/keys":
			_, _ = fmt.Fprintf(w, `{"consensus_key":%q}`, espressoTestKey)
		default:
			http.NotFound(w, r)
		}
	}))
	defer absent.Close()
	cc.Nodes = []*NodeConfig{{Url: absent.URL}}
	cc.EspressoNodeURL = absent.URL
	cc.stateMux.RLock()
	prev := cc.valInfo.Bonded
	cc.stateMux.RUnlock()

	if err := cc.EspressoGetValInfo(false); err != nil {
		t.Fatalf("GetValInfo 1: %v", err)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("bonded flipped to false after a single not-in-set answer (debounce broken)")
	}
	if prev != cc.valInfo.Bonded {
		t.Fatal("state changed unexpectedly")
	}
	if err := cc.EspressoGetValInfo(false); err != nil {
		t.Fatalf("GetValInfo 2: %v", err)
	}
	if cc.valInfo.Bonded {
		t.Fatal("bonded stayed true after two consecutive not-in-set answers")
	}
}

// TestEspressoGetValInfoPreservesStateOnQueryFailure: a failing stake table
// query must keep the resolved identity and Bonded state (the false-jailed
// failure class from gnoland), instead of wiping them.
func TestEspressoGetValInfoPreservesStateOnQueryFailure(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()

	cc := newEspressoTestChain(failing.URL)
	cc.espressoWin = newGnoWindow(10)
	cc.EspressoStakingKey = ""

	if err := cc.EspressoGetValInfo(false); err != nil {
		t.Fatalf("a query failure must preserve the resolved identity, got error: %v", err)
	}
	if !cc.valInfo.Bonded || cc.valInfo.Valcons != espressoTestKey {
		t.Fatalf("state was wiped on query failure: bonded=%v valcons=%q", cc.valInfo.Bonded, cc.valInfo.Valcons)
	}
	if cc.valInfo.Moniker != "MictoNode" {
		t.Fatalf("moniker was wiped on query failure: %q", cc.valInfo.Moniker)
	}
}

// TestEspressoDutyWindowEntriesRoundTrip: the persisted window must restore
// oldest-first so the sliding order (and therefore which entry gets evicted)
// survives a restart.
func TestEspressoDutyWindowEntriesRoundTrip(t *testing.T) {
	w := newGnoWindow(4)
	for _, miss := range []bool{false, true, false, true, true} {
		w.Push(miss)
	}
	got := w.Entries()
	want := []bool{true, false, true, true} // oldest evicted (the false)
	if len(got) != len(want) {
		t.Fatalf("entries len: %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entries[%d]=%v, want %v (%v)", i, got[i], want[i], got)
		}
	}
	restored := newGnoWindow(4)
	for _, miss := range got {
		restored.Push(miss)
	}
	if restored.Missed() != w.Missed() || restored.Window() != w.Window() {
		t.Fatalf("restore mismatch: missed %d/%d window %d/%d", restored.Missed(), w.Missed(), restored.Window(), w.Window())
	}
}

// TestEspressoSavedStateRoundTrip: the duty window and counters must survive a
// snapshot/restore cycle, otherwise percentage_missed needs hours of warm-up
// after every tenderduty restart at a small stake share.
func TestEspressoSavedStateRoundTrip(t *testing.T) {
	origAlarms := alarms
	t.Cleanup(func() { alarms = origAlarms })
	alarms = &alarmCache{
		SentPdAlarms:   map[string]time.Time{},
		SentTgAlarms:   map[string]time.Time{},
		SentDiAlarms:   map[string]time.Time{},
		SentSlkAlarms:  map[string]time.Time{},
		SentPoAlarms:   map[string]time.Time{},
		AllAlarms:      map[string]map[string]time.Time{},
		flappingAlarms: map[string]map[string]time.Time{},
	}

	cc := newEspressoTestChain("http://127.0.0.1:1")
	cc.espressoWin = newGnoWindow(8)
	for _, miss := range []bool{true, false, true, false} {
		cc.espressoWin.Push(miss)
	}
	cc.valInfo.Missed = 2
	cc.valInfo.Window = 4
	cc.espressoCountersValid = true
	cc.espressoAssignedTotal = 42
	cc.espressoMissedTotal = 3
	cc.espressoEpoch = 845
	cc.espressoEpochAssigned = 7
	cc.espressoEpochMissed = 1
	cc.lastBlockNum = 1234

	cfg := &Config{Chains: map[string]*ChainConfig{"Espresso-M": cc}, EnableDash: true}
	b, err := snapshotStateOf(cfg)
	if err != nil {
		t.Fatalf("snapshotStateOf: %v", err)
	}
	var saved savedState
	if err = json.Unmarshal(b, &saved); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	es := saved.EspressoCounters["Espresso-M"]
	if es == nil {
		t.Fatalf("espresso counters missing from state: %s", string(b))
	}
	if !es.CountersValid || es.AssignedTotal != 42 || es.MissedTotal != 3 {
		t.Fatalf("counters not persisted: %+v", es)
	}
	if es.Epoch != 845 || es.EpochAssigned != 7 || es.EpochMissed != 1 || es.LastBlockNum != 1234 {
		t.Fatalf("epoch/height not persisted: %+v", es)
	}
	if len(es.Duties) != 4 {
		t.Fatalf("duties not persisted: %v", es.Duties)
	}
	restored := newGnoWindow(8)
	for _, miss := range es.Duties {
		restored.Push(miss)
	}
	if restored.Missed() != 2 || restored.Window() != 4 {
		t.Fatalf("restored window: missed=%d window=%d, want 2/4", restored.Missed(), restored.Window())
	}
}

// TestEspressoStatusIncludesEpochTally: the dashboard row exposes the current
// epoch's assigned/missed leader duties (the delegation-program bot format).
// Non-espresso chains must not report an epoch at all.
func TestEspressoStatusIncludesEpochTally(t *testing.T) {
	orig := alarms
	t.Cleanup(func() { alarms = orig })
	alarms = &alarmCache{AllAlarms: map[string]map[string]time.Time{}}

	cc := newEspressoTestChain("http://127.0.0.1:1")
	cc.espressoEpoch = 845
	cc.espressoEpochAssigned = 412
	cc.espressoEpochMissed = 1
	cc.valInfo.Missed = 1
	cc.valInfo.Window = 1000

	cc.stateMux.Lock()
	st := cc.statusUpdate()
	cc.stateMux.Unlock()
	if st.Epoch != 845 || st.EpochAssigned != 412 || st.EpochMissed != 1 {
		t.Fatalf("epoch tally: %+v", st)
	}
	if st.Missed != 1 || st.Window != 1000 {
		t.Fatalf("window counters changed: missed=%d window=%d", st.Missed, st.Window)
	}

	cc.Type = "gnoland"
	cc.stateMux.Lock()
	st = cc.statusUpdate()
	cc.stateMux.Unlock()
	if st.Epoch != 0 || st.EpochAssigned != 0 || st.EpochMissed != 0 {
		t.Fatalf("non-espresso chain reported an epoch: %+v", st)
	}
}

// TestValidateConfigPublishesRestoredDutyWindow: loadConfig restores the duty
// window before validateConfig resets valInfo; the window counts must be
// published again or the dashboard shows "N/A" until the next duty (prod
// observation: restored duties=32 but /state window=0).
func TestValidateConfigPublishesRestoredDutyWindow(t *testing.T) {
	cc := newEspressoTestChain("http://127.0.0.1:1")
	cc.espressoWin = newGnoWindow(10)
	cc.espressoWin.Push(true)
	cc.espressoWin.Push(false)
	cc.espressoWin.Push(false)
	cc.valInfo = &ValInfo{Moniker: "not connected"}

	c := &Config{Chains: map[string]*ChainConfig{"Espresso-M": cc}}
	if fatal, problems := validateConfig(c); fatal {
		t.Fatalf("validateConfig fatal: %v", problems)
	}
	if cc.valInfo.Window != 3 || cc.valInfo.Missed != 1 {
		t.Fatalf("restored window not published: missed=%d window=%d, want 1/3", cc.valInfo.Missed, cc.valInfo.Window)
	}
}

// TestEspressoPollVoteLagWithoutDuties: a validator with no leader duty yet
// (Assigned==0) must still expose the vote-lag signal — gating "not voting" on
// the duty counters would leave it dead for hours at a small stake share.
func TestEspressoPollVoteLagWithoutDuties(t *testing.T) {
	shortenEspressoPollTimers(t, 10*time.Millisecond, time.Minute)

	m := &espressoMock{current: 1000, voted: 890, height: 500}
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newEspressoTestChain(srv.URL)
	cc.lastBlockNum = 500
	done := make(chan struct{})
	go func() { cc.EspressoPollRun(); close(done) }()

	deadline := time.Now().Add(4 * time.Second)
	for {
		cc.stateMux.RLock()
		ok := cc.espressoMetricsValid && cc.espressoVoteLag == 110
		valid := cc.espressoCountersValid
		cc.stateMux.RUnlock()
		if ok {
			if valid {
				t.Fatal("duty baseline seeded without any assigned duty")
			}
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("vote lag never recorded without duties (valid=%v lag=%d)", cc.espressoMetricsValid, cc.espressoVoteLag)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

// TestEspressoPollKeepsMissWhenTimeoutLeadsAssigned: the leader-duty histogram
// for an abandoned view is observed one view after the timeout counter, so a
// real miss can lead the assigned count by one. Clamping it away would lose the
// miss forever once the baseline advances.
func TestEspressoPollKeepsMissWhenTimeoutLeadsAssigned(t *testing.T) {
	shortenEspressoPollTimers(t, 10*time.Millisecond, time.Minute)

	m := &espressoMock{current: 1000, voted: 999, height: 500}
	m.set(10, 0, 500)
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newEspressoTestChain(srv.URL)
	cc.lastBlockNum = 500
	done := make(chan struct{})
	go func() { cc.EspressoPollRun(); close(done) }()

	waitCond := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for {
			cc.stateMux.RLock()
			ok := cond()
			cc.stateMux.RUnlock()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				cancel()
				<-done
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	waitCond("baseline", func() bool { return cc.espressoCountersValid && cc.espressoAssignedTotal == 10 })

	// Timeout counter advanced, assigned count not yet: a real miss.
	m.set(10, 1, 500)
	waitCond("miss-led sample", func() bool { return cc.espressoMissedTotal == 1 })

	cancel()
	<-done

	cc.stateMux.RLock()
	defer cc.stateMux.RUnlock()
	if cc.espressoWin.Missed() != 1 || cc.espressoWin.Window() != 1 {
		t.Fatalf("window: missed=%d window=%d, want 1/1", cc.espressoWin.Missed(), cc.espressoWin.Window())
	}
	if cc.statTotalMiss != 1 || cc.statConsecutiveMiss != 1 {
		t.Fatalf("counters: totalMiss=%v consecutive=%v, want 1/1", cc.statTotalMiss, cc.statConsecutiveMiss)
	}
	if cc.valInfo.Missed != 1 || cc.valInfo.Window != 1 {
		t.Fatalf("valInfo window: %+v", cc.valInfo)
	}
	if cc.espressoEpochAssigned < cc.espressoEpochMissed {
		t.Fatalf("epoch tally went negative: assigned=%d missed=%d", cc.espressoEpochAssigned, cc.espressoEpochMissed)
	}
	if cc.blocksResults[0] != int(Statusmissed) {
		t.Fatalf("grid[0]=%d, want missed", cc.blocksResults[0])
	}
}

// TestEspressoPrependGridOrder: the batched prepend must produce exactly the
// same newest-first order as one prepend per cell.
func TestEspressoPrependGridOrder(t *testing.T) {
	cc := &ChainConfig{blocksResults: []int{-1, -1, -1, -1}}
	cc.espressoPrependGrid([]int{10, 11})
	want := []int{11, 10, -1, -1}
	for i := range want {
		if cc.blocksResults[i] != want[i] {
			t.Fatalf("after 2 cells: %v, want %v", cc.blocksResults, want)
		}
	}
	cc.espressoPrependGrid([]int{20, 21, 22, 23, 24})
	want = []int{24, 23, 22, 21}
	for i := range want {
		if cc.blocksResults[i] != want[i] {
			t.Fatalf("after overflow: %v, want %v", cc.blocksResults, want)
		}
	}
}

// TestEspressoPollStartsDutiesWhenKeyResolvesLater: the duty switch must be
// evaluated per tick. A poller that started while the node was unreachable (key
// not yet resolved) used to latch duty monitoring off for the whole process.
func TestEspressoPollStartsDutiesWhenKeyResolvesLater(t *testing.T) {
	shortenEspressoPollTimers(t, 10*time.Millisecond, time.Minute)

	m := &espressoMock{current: 1000, voted: 999, height: 500}
	m.set(10, 0, 500)
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newEspressoTestChain(srv.URL)
	cc.EspressoStakingKey = ""
	cc.espressoKey = ""
	cc.lastBlockNum = 500
	done := make(chan struct{})
	go func() { cc.EspressoPollRun(); close(done) }()

	// Let the poller run a few ticks without a key: no duty baseline may appear.
	time.Sleep(40 * time.Millisecond)
	cc.stateMux.RLock()
	validBefore := cc.espressoCountersValid
	cc.stateMux.RUnlock()
	if validBefore {
		cancel()
		<-done
		t.Fatal("duty baseline seeded while the key was unknown")
	}

	// The health path resolves the key; the next tick must start duty polling.
	if err := cc.EspressoGetValInfo(false); err != nil {
		cancel()
		<-done
		t.Fatalf("GetValInfo: %v", err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		cc.stateMux.RLock()
		ok := cc.espressoCountersValid && cc.espressoAssignedTotal == 10
		cc.stateMux.RUnlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("duty polling never started after the key resolved")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

// TestEspressoGetValInfoDetectsForeignNode: the duty counters are node-local, so
// pointing espresso_node_url at another operator's node would monitor THEIR
// proposals. With espresso_staking_key pinned, a key mismatch must disable duty
// accounting (and log), not silently track the foreign validator.
func TestEspressoGetValInfoDetectsForeignNode(t *testing.T) {
	shortenEspressoPollTimers(t, 10*time.Millisecond, time.Minute)

	m := &espressoMock{current: 1000, voted: 999, height: 500}
	m.set(10, 0, 500)
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	defer srv.Close()

	cc := newEspressoTestChain(srv.URL) // pin == the node's key
	if err := cc.EspressoGetValInfo(false); err != nil {
		t.Fatalf("GetValInfo: %v", err)
	}
	if cc.espressoKeyMismatch {
		t.Fatal("matching node key flagged as a mismatch")
	}

	// Pin a different key: the node belongs to someone else.
	cc.EspressoStakingKey = "BLS_VER_KEY~some-other-operator-key"
	cc.espressoKey = ""
	if err := cc.EspressoGetValInfo(false); err != nil {
		t.Fatalf("GetValInfo after mismatch: %v", err)
	}
	if !cc.espressoKeyMismatch {
		t.Fatal("foreign node not detected")
	}

	// The poller must not account duties from the foreign node.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)
	cc.lastBlockNum = 500
	done := make(chan struct{})
	go func() { cc.EspressoPollRun(); close(done) }()
	time.Sleep(60 * time.Millisecond)
	cancel()
	<-done

	cc.stateMux.RLock()
	defer cc.stateMux.RUnlock()
	if cc.espressoCountersValid || cc.valInfo.Window != 0 {
		t.Fatalf("foreign node contributed duty data: valid=%v window=%d", cc.espressoCountersValid, cc.valInfo.Window)
	}
	if cc.blocksResults[0] != -1 {
		t.Fatalf("grid painted as signed while duty monitoring is disabled: %d", cc.blocksResults[0])
	}
}
