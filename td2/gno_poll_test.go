package tenderduty

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func blockJSON(proposer, signedBy string) string {
	return fmt.Sprintf(`{"result":{"block":{"header":{"height":"100","proposer_address":%q},"last_commit":{"precommits":[{"validator_address":%q}]}}}}`, proposer, signedBy)
}

func TestGnoSignState(t *testing.T) {
	val := "ABCDEF0123456789ABCDEF0123456789ABCDEF01"
	other := "1111111123456789ABCDEF0123456789ABCDEF01"

	// proposed
	br := &pollBlockResult{}
	_ = json.Unmarshal([]byte(blockJSON(val, other)), br)
	if got := gnoSignState(strings.ToLower(val), br); got != StatusProposed {
		t.Fatalf("proposed: got %v", got)
	}
	// signed (in precommits, not proposer)
	br2 := &pollBlockResult{}
	_ = json.Unmarshal([]byte(blockJSON(other, val)), br2)
	if got := gnoSignState(strings.ToLower(val), br2); got != StatusSigned {
		t.Fatalf("signed: got %v", got)
	}
	// missed
	br3 := &pollBlockResult{}
	_ = json.Unmarshal([]byte(blockJSON(other, other)), br3)
	if got := gnoSignState(strings.ToLower(val), br3); got != Statusmissed {
		t.Fatalf("missed: got %v", got)
	}
}

// withCtx swaps td.ctx for the duration of a PollRun test and restores it after.
func withCtx(t *testing.T, ctx context.Context) {
	t.Helper()
	orig := td.ctx
	td.ctx = ctx
	t.Cleanup(func() { td.ctx = orig })
}

// TestPollRunReturnsWhenSigningAddrUnresolved: when the signing address could
// not be derived (Valcons empty), PollRun must return immediately rather than
// enter the poll loop where it could never match a precommit.
func TestPollRunReturnsWhenSigningAddrUnresolved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := &ChainConfig{
		name:           "gno",
		ChainId:        "t",
		Type:           "gnoland",
		gnoRpcEndpoint: "http://127.0.0.1:1",   // non-empty so the wait-loop breaks; never contacted
		valInfo:        &ValInfo{Moniker: "x"}, // Valcons == "" : unresolved
	}
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PollRun did not return when signing address was unresolved")
	}
}

// TestPollRunCountsSignedBySigningAddress: the block's precommit carries the
// SIGNING address while config holds the OPERATOR address. PollRun must match
// by Valcons (signing) and count the block as signed — it would be missed if it
// matched by the operator address.
func TestPollRunCountsSignedBySigningAddress(t *testing.T) {
	operator := "abcdef0123456789abcdef0123456789abcdef01" // config valoper_address
	signing := "1111111123456789abcdef0123456789abcdef01"  // consensus addr in /block
	blockBody := fmt.Sprintf(`{"result":{"block":{"header":{"height":"100","proposer_address":"0000000000000000000000000000000000000000"},"last_commit":{"precommits":[{"validator_address":%q}]}}}}`, signing)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(blockBody))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := &ChainConfig{
		name:           "gno",
		ChainId:        "t",
		Type:           "gnoland",
		ValAddress:     operator, // operator != signing
		gnoRpcEndpoint: srv.URL,
		Nodes:          []*NodeConfig{{Url: srv.URL}},
		valInfo:        &ValInfo{Moniker: "x", Valcons: signing, Bonded: true},
		blocksResults:  make([]int, 100),
	}
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()

	// The poller fires on a 5s ticker; wait for one block, then cancel and JOIN
	// the goroutine before reading its writes (avoids a concurrent-access race).
	time.Sleep(8 * time.Second) // >5s ticker fire + margin against scheduler jitter
	cancel()
	<-done

	if cc.statTotalSigns < 1 {
		t.Fatalf("expected block counted as signed; signs=%v miss=%v", cc.statTotalSigns, cc.statTotalMiss)
	}
	if cc.statTotalMiss != 0 {
		t.Fatalf("operator != signing but got misses=%v (matched by wrong address)", cc.statTotalMiss)
	}
}

// TestPollRunSkipsCountersWhenNotBonded: a validator that is resolved but NOT
// in the active set (Bonded=false) must not accrue miss/consecutive counters —
// it is not expected to sign, so counting its misses would produce false
// consecutive-missed alerts.
func TestPollRunSkipsCountersWhenNotBonded(t *testing.T) {
	signing := "1111111123456789abcdef0123456789abcdef01"
	someoneElse := "9999999999999999999999999999999999999999"
	// block signed by someone else → our validator misses it
	blockBody := fmt.Sprintf(`{"result":{"block":{"header":{"height":"100","proposer_address":%q},"last_commit":{"precommits":[{"validator_address":%q}]}}}}`, someoneElse, someoneElse)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(blockBody))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := &ChainConfig{
		name:           "gno",
		ChainId:        "t",
		Type:           "gnoland",
		gnoRpcEndpoint: srv.URL,
		Nodes:          []*NodeConfig{{Url: srv.URL}},
		valInfo:        &ValInfo{Moniker: "x", Valcons: signing, Bonded: false}, // resolved but not active
		blocksResults:  make([]int, 100),
	}
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()
	time.Sleep(8 * time.Second) // >5s ticker fire + margin against scheduler jitter
	cancel()
	<-done

	if cc.statTotalMiss != 0 || cc.statConsecutiveMiss != 0 || cc.valInfo.Missed != 0 {
		t.Fatalf("non-bonded validator must not accrue miss counters: totalMiss=%v consec=%v valInfo.Missed=%v",
			cc.statTotalMiss, cc.statConsecutiveMiss, cc.valInfo.Missed)
	}
}

// blockJSONAt renders a /block response body at a specific height signed by `signedBy`.
func blockJSONAt(height int64, proposer, signedBy string) string {
	return fmt.Sprintf(`{"result":{"block":{"header":{"height":"%d","proposer_address":%q},"last_commit":{"precommits":[{"validator_address":%q}]}}}}`, height, proposer, signedBy)
}

// TestPollRunAdvancesViaHealthyNodeWhenOneStale: the production bug — PollRun was
// pinned to a single RPC endpoint. When that endpoint stalls (keeps serving a
// stale height), PollRun never consulted the second node and lastBlockTime froze,
// producing a false "stalled chain" alert even though the network was healthy.
// The fix polls every configured node each tick and advances to the max height,
// so a healthy node carries the chain past a stale one.
func TestPollRunAdvancesViaHealthyNodeWhenOneStale(t *testing.T) {
	signing := "1111111123456789abcdef0123456789abcdef01"
	someoneElse := "9999999999999999999999999999999999999999"

	// stale node: always the same old block
	srvStale := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(blockJSONAt(100, someoneElse, signing)))
	}))
	defer srvStale.Close()

	// healthy node: advances on every call (101, 102, 103, ...)
	var h int64 = 100
	srvHealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(blockJSONAt(atomic.AddInt64(&h, 1), someoneElse, signing)))
	}))
	defer srvHealthy.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	withCtx(t, ctx)

	cc := &ChainConfig{
		name:           "gno",
		ChainId:        "t",
		Type:           "gnoland",
		gnoRpcEndpoint: srvStale.URL, // the pinned endpoint is the stale one
		Nodes: []*NodeConfig{
			{Url: srvStale.URL},
			{Url: srvHealthy.URL},
		},
		valInfo:       &ValInfo{Moniker: "x", Valcons: signing, Bonded: true},
		blocksResults: make([]int, 100),
	}
	done := make(chan struct{})
	go func() { cc.PollRun(); close(done) }()
	time.Sleep(8 * time.Second) // >5s ticker fire + jitter margin
	cancel()
	<-done

	if cc.lastBlockNum <= 100 {
		t.Fatalf("stale endpoint pinned PollRun: lastBlockNum=%d, expected healthy node to carry past 100", cc.lastBlockNum)
	}
}
