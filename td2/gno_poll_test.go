package tenderduty

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func gnoStatusJSON(height int64) string {
	return fmt.Sprintf(`{"result":{"node_info":{"network":"t"},"sync_info":{"latest_block_height":"%d","catching_up":false}}}`, height)
}

func gnoCommitJSON(height int64, proposer, signedBy string) string {
	return fmt.Sprintf(`{"result":{"signed_header":{"header":{"height":"%d","proposer_address":%q},"commit":{"precommits":[{"validator_address":%q}]}}}}`, height, proposer, signedBy)
}

// gnoNodeHandler serves the two endpoints PollRun uses: /status for head
// detection and /commit?height=N for the per-height signing outcome.
func gnoNodeHandler(head func() int64, sign func(int64) (proposer, signedBy string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_, _ = w.Write([]byte(gnoStatusJSON(head())))
		case "/commit":
			h, _ := strconv.ParseInt(r.URL.Query().Get("height"), 10, 64)
			proposer, signed := sign(h)
			_, _ = w.Write([]byte(gnoCommitJSON(h, proposer, signed)))
		default:
			http.NotFound(w, r)
		}
	}
}

func TestGnoSignState(t *testing.T) {
	val := "ABCDEF0123456789ABCDEF0123456789ABCDEF01"
	other := "1111111123456789ABCDEF0123456789ABCDEF01"

	// proposed
	cr := &gnoCommitResult{}
	_ = json.Unmarshal([]byte(gnoCommitJSON(100, val, other)), cr)
	if got := gnoSignState(strings.ToLower(val), cr); got != StatusProposed {
		t.Fatalf("proposed: got %v", got)
	}
	// signed (in precommits, not proposer)
	cr2 := &gnoCommitResult{}
	_ = json.Unmarshal([]byte(gnoCommitJSON(100, other, val)), cr2)
	if got := gnoSignState(strings.ToLower(val), cr2); got != StatusSigned {
		t.Fatalf("signed: got %v", got)
	}
	// missed
	cr3 := &gnoCommitResult{}
	_ = json.Unmarshal([]byte(gnoCommitJSON(100, other, other)), cr3)
	if got := gnoSignState(strings.ToLower(val), cr3); got != Statusmissed {
		t.Fatalf("missed: got %v", got)
	}
}

// TestGnoSignStateNullPrecommit: a validator that failed to precommit is
// serialized as null in the commit; it must count as missed, not crash or match.
func TestGnoSignStateNullPrecommit(t *testing.T) {
	body := `{"result":{"signed_header":{"header":{"height":"1","proposer_address":"9999999999999999999999999999999999999999"},"commit":{"precommits":[null]}}}}`
	cr := &gnoCommitResult{}
	if err := json.Unmarshal([]byte(body), cr); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := gnoSignState("1111111123456789abcdef0123456789abcdef01", cr); got != Statusmissed {
		t.Fatalf("null precommit: got %v", got)
	}
}

// withCtx swaps td.ctx for the duration of a PollRun test and restores it after.
func withCtx(t *testing.T, ctx context.Context) {
	t.Helper()
	orig := td.ctx
	td.ctx = ctx
	t.Cleanup(func() { td.ctx = orig })
}

// shortenPollTimers makes PollRun's ticker and idle-exit window test-fast and
// restores the production values after the test.
func shortenPollTimers(t *testing.T, interval, idle time.Duration) {
	t.Helper()
	oi, oe := gnoPollInterval, gnoPollIdleExit
	gnoPollInterval, gnoPollIdleExit = interval, idle
	t.Cleanup(func() { gnoPollInterval, gnoPollIdleExit = oi, oe })
}

func newGnoTestChain(name, chainID, rpcURL string, valcons string, bonded bool, lastBlockNum int64) *ChainConfig {
	blocks := make([]int, showBLocks)
	for i := range blocks {
		blocks[i] = -1
	}
	cc := &ChainConfig{
		name:           name,
		ChainId:        chainID,
		Type:           "gnoland",
		gnoRpcEndpoint: rpcURL,
		Nodes:          []*NodeConfig{{Url: rpcURL}},
		valInfo:        &ValInfo{Moniker: "MictoNode", Valcons: valcons, Bonded: bonded},
		blocksResults:  blocks,
	}
	cc.lastBlockNum = lastBlockNum
	return cc
}

// TestPollRunRestartDoesNotAdvanceOnStaleHead is the 2026-11-17 onyx-1 prod
// regression: the chain halted, PollRun returned on its idle timer, and every
// restart re-seeded lastHeight to 0 — so the frozen head was processed as a new
// block, lastBlockTime kept being refreshed, and the stalled alarm (3 min) never
// fired once in 50+ minutes. A restart must resume from the last processed height.
func TestPollRunRestartDoesNotAdvanceOnStaleHead(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, 150*time.Millisecond)

	signing := "1111111123456789abcdef0123456789abcdef01"
	srv := httptest.NewServer(gnoNodeHandler(
		func() int64 { return 100 },
		func(int64) (string, string) { return signing, signing },
	))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newGnoTestChain("Gnoland-T", "t", srv.URL, signing, true, 100)
	haltedAt := time.Now().Add(-10 * time.Minute)
	cc.lastBlockTime = haltedAt

	// First run exits on the idle timer, then the run loop reconnects and starts
	// the poller again. Neither run may treat the frozen head as a new block.
	cc.PollRun()
	cc.PollRun()

	if !cc.lastBlockTime.Equal(haltedAt) {
		t.Fatalf("stale head refreshed lastBlockTime across a poller restart (stalled alarm would never fire): got %s, want %s",
			cc.lastBlockTime, haltedAt)
	}
	if cc.lastBlockNum != 100 {
		t.Fatalf("lastBlockNum changed on a stale head: %d", cc.lastBlockNum)
	}
	if cc.blocksResults[0] != -1 {
		t.Fatalf("stale head was recorded as a signing result in the grid: %d", cc.blocksResults[0])
	}
	if cc.gnoWin == nil || cc.gnoWin.Window() != 0 {
		t.Fatalf("stale head was pushed into the signing window")
	}
}

// TestPollRunAcceptsChainHeightReset: a re-genesis (upgrade with fresh heights)
// makes the head lower than the last processed height. That must not freeze
// monitoring — after the idle window the new head is accepted and polling resumes.
func TestPollRunAcceptsChainHeightReset(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, 80*time.Millisecond)

	signing := "1111111123456789abcdef0123456789abcdef01"
	srv := httptest.NewServer(gnoNodeHandler(
		func() int64 { return 50 },
		func(int64) (string, string) { return signing, signing },
	))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("Gnoland-T", "t", srv.URL, signing, true, 500)
	cc.lastBlockTime = time.Now().Add(-time.Hour)

	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		h := cc.lastBlockNum
		cc.stateMux.RUnlock()
		if h == 50 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("lower head after a re-genesis was never accepted, lastBlockNum=%d", h)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if !cc.lastBlockTime.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("accepted re-genesis head did not refresh lastBlockTime: %s", cc.lastBlockTime)
	}
}

// TestPollRunReturnsWhenSigningAddrUnresolved: when the signing address could
// not be derived (Valcons empty), PollRun must return immediately rather than
// enter the poll loop where it could never match a precommit.
func TestPollRunReturnsWhenSigningAddrUnresolved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", "http://127.0.0.1:1", "", false, 0)
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PollRun did not return when signing address was unresolved")
	}
}

// TestPollRunCountsSignedBySigningAddress: the commit's precommit carries the
// SIGNING address while config holds the OPERATOR address. PollRun must match
// by Valcons (signing) and count the block as signed — it would be missed if it
// matched by the operator address.
func TestPollRunCountsSignedBySigningAddress(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, time.Minute)

	operator := "abcdef0123456789abcdef0123456789abcdef01" // config valoper_address
	signing := "1111111123456789abcdef0123456789abcdef01"  // consensus addr in /commit
	srv := httptest.NewServer(gnoNodeHandler(
		func() int64 { return 100 },
		func(int64) (string, string) {
			return "0000000000000000000000000000000000000000", signing
		},
	))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", srv.URL, signing, true, 99)
	cc.ValAddress = operator // operator != signing
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		signs, miss := cc.statTotalSigns, cc.statTotalMiss
		cc.stateMux.RUnlock()
		if signs >= 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("expected block counted as signed; signs=%v miss=%v", signs, miss)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if cc.statTotalMiss != 0 {
		t.Fatalf("operator != signing but got misses=%v (matched by wrong address)", cc.statTotalMiss)
	}
}

// TestPollRunSkipsCountersWhenNotBonded: a validator that is resolved but NOT
// in the active set (Bonded=false) must not accrue miss/consecutive counters —
// it is not expected to sign, so counting its misses would produce false
// consecutive-missed alerts.
func TestPollRunSkipsCountersWhenNotBonded(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, time.Minute)

	signing := "1111111123456789abcdef0123456789abcdef01"
	someoneElse := "9999999999999999999999999999999999999999"
	srv := httptest.NewServer(gnoNodeHandler(
		func() int64 { return 100 },
		func(int64) (string, string) { return someoneElse, someoneElse },
	))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", srv.URL, signing, false, 99) // resolved but not active
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	<-done

	if cc.statTotalMiss != 0 || cc.statConsecutiveMiss != 0 || cc.valInfo.Missed != 0 {
		t.Fatalf("non-bonded validator must not accrue miss counters: totalMiss=%v consec=%v valInfo.Missed=%v",
			cc.statTotalMiss, cc.statConsecutiveMiss, cc.valInfo.Missed)
	}
	if cc.blocksResults[0] != -1 {
		t.Fatalf("non-bonded validator must paint No Data (-1), got %d", cc.blocksResults[0])
	}
}

// TestPollRunAdvancesViaHealthyNodeWhenOneStale: PollRun must poll every node and
// advance to the max height, so a stale-but-valid endpoint cannot freeze
// monitoring (the 2026-06-28 false-stall bug).
func TestPollRunAdvancesViaHealthyNodeWhenOneStale(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, time.Minute)

	signing := "1111111123456789abcdef0123456789abcdef01"
	someoneElse := "9999999999999999999999999999999999999999"

	// stale node: always the same old height
	srvStale := httptest.NewServer(gnoNodeHandler(
		func() int64 { return 100 },
		func(int64) (string, string) { return someoneElse, signing },
	))
	defer srvStale.Close()

	// healthy node: advances on every status call
	var h int64 = 100
	srvHealthy := httptest.NewServer(gnoNodeHandler(
		func() int64 { return atomic.AddInt64(&h, 1) },
		func(int64) (string, string) { return someoneElse, signing },
	))
	defer srvHealthy.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", srvStale.URL, signing, true, 100) // pinned endpoint is the stale one
	cc.Nodes = []*NodeConfig{{Url: srvStale.URL}, {Url: srvHealthy.URL}}
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		last := cc.lastBlockNum
		cc.stateMux.RUnlock()
		if last > 100 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("stale endpoint pinned PollRun: lastBlockNum=%d, expected healthy node to carry past 100", last)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}

// TestPollRunProcessesEveryHeight: gno.land blocks (~3.5s) arrive faster than the
// poll interval (5s), so a head-only poller silently skips ~30% of blocks and
// their signing outcomes never reach the counters, window or grid. Every height
// between the last processed one and the head must be evaluated.
func TestPollRunProcessesEveryHeight(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, time.Minute)

	signing := "1111111123456789abcdef0123456789abcdef01"
	var head int64 = 100
	srv := httptest.NewServer(gnoNodeHandler(
		func() int64 { return atomic.AddInt64(&head, 3) }, // +3 blocks per tick, like 3.5s blocks at 5s polls
		func(int64) (string, string) { return signing, signing },
	))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", srv.URL, signing, true, 100)
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		last, signs := cc.lastBlockNum, cc.statTotalSigns
		cc.stateMux.RUnlock()
		if last >= 110 {
			if signs != float64(last-100) {
				cancel()
				<-done
				t.Fatalf("skipped heights: processed %d blocks but counted %v signatures (heights 101..%d)", last-100, signs, last)
			}
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("poller did not advance past 110, lastBlockNum=%d", last)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}

// TestPollRunUsesCommitForHeightNotBlockLastCommit: block H's own last_commit
// carries the votes for H-1, so reading it shifts every signing outcome by one
// height (and never evaluates the head). The signing state must come from
// /commit?height=H. Here /block says "signed" while /commit for the same height
// says "missed": the miss must be counted.
func TestPollRunUsesCommitForHeightNotBlockLastCommit(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, time.Minute)

	signing := "1111111123456789abcdef0123456789abcdef01"
	someoneElse := "9999999999999999999999999999999999999999"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_, _ = w.Write([]byte(gnoStatusJSON(100)))
		case "/commit":
			_, _ = w.Write([]byte(gnoCommitJSON(100, someoneElse, someoneElse))) // we did not sign
		case "/block":
			// Old code read this: it says "signed" (votes for height 99).
			_, _ = w.Write([]byte(gnoCommitJSON(100, someoneElse, signing)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", srv.URL, signing, true, 99)
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		miss, signs := cc.statTotalMiss, cc.statTotalSigns
		cc.stateMux.RUnlock()
		if miss >= 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("commit outcome ignored (read block.last_commit instead): miss=%v signs=%v", miss, signs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if cc.statTotalSigns != 0 {
		t.Fatalf("block.last_commit was counted as a signature for the wrong height: signs=%v", cc.statTotalSigns)
	}
}

// TestPollRunFallsBackToSecondNodeForCommit: the head may come from one endpoint
// while its /commit is broken or pruned. The commit fetch must fall back to the
// other configured node instead of stalling the poller.
func TestPollRunFallsBackToSecondNodeForCommit(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, time.Minute)

	signing := "1111111123456789abcdef0123456789abcdef01"
	someoneElse := "9999999999999999999999999999999999999999"
	srvBrokenCommit := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_, _ = w.Write([]byte(gnoStatusJSON(100)))
		default:
			http.Error(w, "commit disabled", http.StatusNotFound)
		}
	}))
	defer srvBrokenCommit.Close()
	srvGood := httptest.NewServer(gnoNodeHandler(
		func() int64 { return 100 },
		func(int64) (string, string) { return someoneElse, signing },
	))
	defer srvGood.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", srvBrokenCommit.URL, signing, true, 99)
	cc.Nodes = []*NodeConfig{{Url: srvBrokenCommit.URL}, {Url: srvGood.URL}}
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		signs := cc.statTotalSigns
		cc.stateMux.RUnlock()
		if signs >= 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("commit fetch never fell back to the second node")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}

// TestPollRunIgnoresWrongChainNode: a /status from another network must not
// supply the head (or commits) for this chain. Otherwise a stray testnet
// endpoint with a higher height would make the poller evaluate thousands of
// foreign heights as misses.
func TestPollRunIgnoresWrongChainNode(t *testing.T) {
	shortenPollTimers(t, 20*time.Millisecond, time.Minute)

	signing := "1111111123456789abcdef0123456789abcdef01"
	someoneElse := "9999999999999999999999999999999999999999"
	srvWrong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_, _ = w.Write([]byte(`{"result":{"node_info":{"network":"other-1"},"sync_info":{"latest_block_height":"999999","catching_up":false}}}`))
		case "/commit":
			h, _ := strconv.ParseInt(r.URL.Query().Get("height"), 10, 64)
			_, _ = w.Write([]byte(gnoCommitJSON(h, someoneElse, someoneElse)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srvWrong.Close()
	srvRight := httptest.NewServer(gnoNodeHandler(
		func() int64 { return 100 },
		func(int64) (string, string) { return someoneElse, signing },
	))
	defer srvRight.Close()

	ctx, cancel := context.WithCancel(context.Background())
	withCtx(t, ctx)

	cc := newGnoTestChain("gno", "t", srvWrong.URL, signing, true, 99)
	cc.Nodes = []*NodeConfig{{Url: srvWrong.URL}, {Url: srvRight.URL}}
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		last, signs := cc.lastBlockNum, cc.statTotalSigns
		cc.stateMux.RUnlock()
		if signs >= 1 {
			if last != 100 {
				cancel()
				<-done
				t.Fatalf("wrong-chain head was used: lastBlockNum=%d, want 100", last)
			}
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("right node was never used, lastBlockNum=%d signs=%v", last, signs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}
