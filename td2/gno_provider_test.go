package tenderduty

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func TestCanonicalAddrHex(t *testing.T) {
	want := []byte{0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01}
	for _, in := range []string{"abcdef0123456789abcdef0123456789abcdef01", "ABCDEF0123456789ABCDEF0123456789ABCDEF01"} {
		out, err := canonicalAddr(in)
		if err != nil {
			t.Fatalf("%q: unexpected err: %v", in, err)
		}
		if !bytes.Equal(out, want) {
			t.Fatalf("%q: decode mismatch %x", in, out)
		}
	}
}

func TestCanonicalAddrBech32RoundTrip(t *testing.T) {
	bz := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa}
	s, err := bech32.ConvertAndEncode("g", bz)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := canonicalAddr(s)
	if err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	if !bytes.Equal(out, bz) {
		t.Fatalf("round-trip mismatch: %x", out)
	}
}

func TestCanonicalAddrInvalid(t *testing.T) {
	for _, in := range []string{"", "not an address", "abcd"} {
		if _, err := canonicalAddr(in); err == nil {
			t.Fatalf("expected error for %q", in)
		}
	}
}

func TestAddrEqualCrossEncoding(t *testing.T) {
	if !addrEqual("abcdef0123456789abcdef0123456789abcdef01", "ABCDEF0123456789ABCDEF0123456789ABCDEF01") {
		t.Fatal("hex lower/upper of same bytes must be equal")
	}
	if addrEqual("abcdef0123456789abcdef0123456789abcdef01", "0000000000000000000000000000000000000000") {
		t.Fatal("different addresses must not be equal")
	}
}

func TestGnoGetStatus(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":-1,"result":{"node_info":{"network":"test11"},"sync_info":{"latest_block_height":"123","catching_up":false}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/status") {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		w.Write([]byte(body))
	}))
	defer srv.Close()
	cid, h, catching, err := GnoGetStatus(srv.URL)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if cid != "test11" || h != "123" || catching {
		t.Fatalf("got cid=%q h=%q catching=%v", cid, h, catching)
	}
}

func TestGnoIsValidatorActive(t *testing.T) {
	target := "ABCDEF0123456789ABCDEF0123456789ABCDEF01"
	body := `{"jsonrpc":"2.0","result":{"validators":[{"address":"` + target + `","voting_power":"1"}]}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()
	got, err := GnoIsValidatorActive(srv.URL, strings.ToLower(target))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !got {
		t.Fatal("expected validator active")
	}
}

// TestGnoGetValidatorsErrorOnJSONRPCError proves a JSON-RPC error body
// ({"error":{...}}) is surfaced as an error instead of silently unmarshaling
// into a zero-value struct and reporting "definitely not in the active set".
// That silent (nil, nil) was the 2026-08-10 prod incident root cause: an
// intermittent error body flipped a bonded validator to Bonded=false, painted
// ~10 "No Data" grid cells, and raised a false "jailed" critical alert.
func TestGnoGetValidatorsErrorOnJSONRPCError(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":-1,"error":{"code":-32603,"message":"internal error"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()
	if _, err := GnoGetValidators(srv.URL); err == nil {
		t.Fatal("expected error for JSON-RPC error body")
	}
}

// TestGnoGetValidatorsErrorOnEmptyResult proves an empty /validators answer is
// an error, not a definitive "not in the active set": a healthy live chain
// always lists its active set, so an empty set means the response was
// indeterminate and the caller must preserve prior state (2026-08-10 incident:
// result:null / missing validators unmarshaled into a zero struct -> (nil, nil)
// -> silent Bonded=false -> false jailed).
func TestGnoGetValidatorsErrorOnEmptyResult(t *testing.T) {
	for _, body := range []string{
		`{}`,
		`{"jsonrpc":"2.0","result":null}`,
		`{"jsonrpc":"2.0","result":{"validators":[]}}`,
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(body))
		}))
		_, err := GnoGetValidators(srv.URL)
		srv.Close()
		if err == nil {
			t.Fatalf("expected error for body %s", body)
		}
	}
}

// TestGnoIsValidatorActiveAbsentHealthyResponse proves the real-unbond path is
// preserved: a HEALTHY populated response that lacks the signing address still
// returns (false, nil) — only empty/error responses are indeterminate now.
func TestGnoIsValidatorActiveAbsentHealthyResponse(t *testing.T) {
	body := `{"jsonrpc":"2.0","result":{"validators":[{"address":"0000000000000000000000000000000000000000"}]}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()
	active, err := GnoIsValidatorActive(srv.URL, "1111111123456789abcdef0123456789abcdef01")
	if err != nil {
		t.Fatalf("healthy populated response must not error: %v", err)
	}
	if active {
		t.Fatal("address absent from healthy set must report inactive, not error")
	}
}

// TestGnoGetValInfoDebouncesNotBondedTicks proves the single-tick false-flip
// debounce (2026-08-10 prod incident class, same as 2026-07-02/2026-07-28): a
// definitive not-bonded /validators verdict must be confirmed across two
// consecutive ticks before Bonded flips to false (and Jailed to true); a
// bonded verdict resets the streak; an indeterminate verdict (all endpoints
// failed) leaves the streak unchanged. One transient tick used to flip
// immediately, painting ~10 "No Data" grid cells and raising a false "jailed"
// critical that self-recovered ~60s later.
func TestGnoGetValInfoDebouncesNotBondedTicks(t *testing.T) {
	mux := http.NewServeMux()
	operator := "abcdef0123456789abcdef0123456789abcdef01" // realm key (config valoper_address)
	signing := "1111111123456789abcdef0123456789abcdef01"  // consensus addr (/validators, /block)
	includeSigning := true
	validatorsDown := false
	mux.HandleFunc("/abci_query", func(w http.ResponseWriter, r *http.Request) {
		decoded := gnoValoperStruct("GnoVal", operator, "gpub1pk", signing)
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"response":{"ResponseBase":{"Data":"` + b64(decoded) + `"}}}}`))
	})
	mux.HandleFunc("/validators", func(w http.ResponseWriter, r *http.Request) {
		if validatorsDown {
			w.Write([]byte(`<html>bad gateway</html>`))
			return
		}
		if includeSigning {
			w.Write([]byte(`{"jsonrpc":"2.0","result":{"validators":[{"address":"` + signing + `"}]}}`))
			return
		}
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"validators":[{"address":"0000000000000000000000000000000000000000"}]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cc := &ChainConfig{
		name: "gno", ChainId: "test11", Type: "gnoland",
		ValAddress: operator, Nodes: []*NodeConfig{{Url: srv.URL}},
	}
	// Healthy baseline: bonded.
	if err := cc.GnoGetValInfo(true); err != nil {
		t.Fatalf("baseline err: %v", err)
	}
	if !cc.valInfo.Bonded || cc.valInfo.Jailed {
		t.Fatalf("baseline wrong: bonded=%v jailed=%v", cc.valInfo.Bonded, cc.valInfo.Jailed)
	}
	cc.lastValInfo = &ValInfo{Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded}

	// Tick 1: definitive not-found. One tick must NOT flip the state.
	includeSigning = false
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("tick1 err: %v", err)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("single not-bonded tick must not flip Bonded (debounce)")
	}
	if cc.valInfo.Jailed {
		t.Fatal("single not-bonded tick must not set Jailed")
	}
	cc.stateMux.RLock()
	streak := cc.gnoBondMissStreak
	cc.stateMux.RUnlock()
	if streak != 1 {
		t.Fatalf("streak after tick1: want 1, got %d", streak)
	}

	// Tick 2: second consecutive not-found -> definitive unbond, flip happens.
	cc.lastValInfo = &ValInfo{Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded}
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("tick2 err: %v", err)
	}
	if cc.valInfo.Bonded {
		t.Fatal("second consecutive not-bonded tick must flip Bonded=false")
	}
	if !cc.valInfo.Jailed {
		t.Fatal("bonded->unbonded transition must set Jailed=true")
	}

	// Indeterminate tick (all endpoints fail): preserve state, streak unchanged.
	cc.lastValInfo = &ValInfo{Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded}
	validatorsDown = true
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("indeterminate err: %v", err)
	}
	if cc.valInfo.Bonded {
		t.Fatal("indeterminate must preserve prior Bonded=false")
	}
	cc.stateMux.RLock()
	streak = cc.gnoBondMissStreak
	cc.stateMux.RUnlock()
	if streak != 2 {
		t.Fatalf("indeterminate must leave streak unchanged: want 2, got %d", streak)
	}

	// Bonded verdict: streak resets and state flips back.
	cc.lastValInfo = &ValInfo{Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded}
	validatorsDown = false
	includeSigning = true
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("recover err: %v", err)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("bonded verdict must flip Bonded back to true")
	}
	if cc.valInfo.Jailed {
		t.Fatal("bonded verdict must clear Jailed")
	}
	cc.stateMux.RLock()
	streak = cc.gnoBondMissStreak
	cc.stateMux.RUnlock()
	if streak != 0 {
		t.Fatalf("bonded verdict must reset streak: want 0, got %d", streak)
	}
}

// gnoValoperStruct builds a decoded vm/qeval Data payload shaped like the real
// gno.land/r/gnops/valopers Valoper struct: field order is moniker, description,
// tag, operator(.uverse.address), pubkey(gpub), signing(.uverse.address),
// created(int64), active(bool).
func gnoValoperStruct(moniker, operator, pubkey, signing string) string {
	return `(struct{("` + moniker + `" string),("desc" string),("tag" string),("` +
		operator + `" .uverse.address),("` + pubkey + `" string),("` +
		signing + `" .uverse.address),(100 int64),(true bool),(&<nil> *foo)} gno.land/r/gnops/valopers.Valoper)`
}

func TestGnoResolveValoper(t *testing.T) {
	op := "abcdef0123456789abcdef0123456789abcdef01" // operator (realm key)
	sg := "1111111123456789abcdef0123456789abcdef01" // signing (consensus) — distinct
	pk := "gpub1testpubkey"
	decoded := gnoValoperStruct("TestMoniker", op, pk, sg)
	body := `{"jsonrpc":"2.0","result":{"response":{"ResponseBase":{"Data":"` + b64(decoded) + `"}}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "vm/qeval") {
			t.Fatalf("expected vm/qeval query, got %q", r.URL.RawQuery)
		}
		w.Write([]byte(body))
	}))
	defer srv.Close()
	moniker, signing, pubkey, err := GnoResolveValoper(srv.URL, "gno.land/r/gnops/valopers", op)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if moniker != "TestMoniker" {
		t.Fatalf("moniker: want TestMoniker, got %q", moniker)
	}
	if !addrEqual(sg, signing) {
		t.Fatalf("signing: want %s, got %q", sg, signing)
	}
	if addrEqual(op, signing) {
		t.Fatal("signing must differ from operator")
	}
	if pubkey != pk {
		t.Fatalf("pubkey: want %s, got %q", pk, pubkey)
	}
}

// TestGnoResolveValoperPicksSigningNotOperator proves the signing address is
// selected by value (the non-operator .uverse.address), not by field position,
// by putting the signing address in the operator slot and vice-versa.
func TestGnoResolveValoperPicksSigningNotOperator(t *testing.T) {
	op := "2000000000000000000000000000000000000001"
	sg := "3000000000000000000000000000000000000001"
	// swapped field positions: signing first, operator second
	decoded := `(struct{("M" string),("d" string),("t" string),("` + sg +
		`" .uverse.address),("gpub1x" string),("` + op + `" .uverse.address),(1 int64),(true bool),(&<nil> *f)} x)`
	body := `{"jsonrpc":"2.0","result":{"response":{"ResponseBase":{"Data":"` + b64(decoded) + `"}}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()
	_, signing, _, err := GnoResolveValoper(srv.URL, "gno.land/r/gnops/valopers", op)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !addrEqual(sg, signing) {
		t.Fatalf("signing: want %s (non-operator), got %q", sg, signing)
	}
}

func TestGnoResolveValoperErrorOnMissingSigning(t *testing.T) {
	op := "4000000000000000000000000000000000000001"
	// struct with only ONE .uverse.address (operator) — signing absent
	decoded := `(struct{("M" string),("d" string),("` + op + `" .uverse.address),(1 int64)} x)`
	body := `{"jsonrpc":"2.0","result":{"response":{"ResponseBase":{"Data":"` + b64(decoded) + `"}}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()
	_, _, _, err := GnoResolveValoper(srv.URL, "gno.land/r/gnops/valopers", op)
	if err == nil {
		t.Fatal("expected error when signing address absent from struct")
	}
}

func TestGnoGetValInfoBondedAndJailed(t *testing.T) {
	mux := http.NewServeMux()
	operator := "abcdef0123456789abcdef0123456789abcdef01" // realm key (config valoper_address)
	signing := "1111111123456789abcdef0123456789abcdef01"  // consensus addr (/validators, /block)
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"node_info":{"network":"test11"},"sync_info":{"latest_block_height":"1","catching_up":false}}}`))
	})
	mux.HandleFunc("/validators", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"validators":[{"address":"` + signing + `"}]}}`))
	})
	mux.HandleFunc("/abci_query", func(w http.ResponseWriter, r *http.Request) {
		decoded := gnoValoperStruct("GnoVal", operator, "gpub1pk", signing)
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"response":{"ResponseBase":{"Data":"` + b64(decoded) + `"}}}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cc := &ChainConfig{
		name: "gno", ChainId: "test11", Type: "gnoland",
		ValAddress: operator, Nodes: []*NodeConfig{{Url: srv.URL}},
	}
	if err := cc.GnoGetValInfo(true); err != nil {
		t.Fatalf("err: %v", err)
	}
	if cc.valInfo.Moniker != "GnoVal" {
		t.Fatalf("moniker: want GnoVal, got %q", cc.valInfo.Moniker)
	}
	// Valcons must hold the derived signing address, NOT the operator address.
	if !addrEqual(signing, cc.valInfo.Valcons) {
		t.Fatalf("valcons: want signing %s, got %q", signing, cc.valInfo.Valcons)
	}
	if addrEqual(operator, cc.valInfo.Valcons) {
		t.Fatal("valcons must be the signing address, not the operator address")
	}
	if !cc.valInfo.Bonded {
		t.Fatal("expected bonded: signing addr is in active set")
	}
	if cc.valInfo.Jailed {
		t.Fatal("should not be jailed on first refresh")
	}
}

// TestGnoGetValInfoPreservesStateOnRealmFailure reproduces the 2026-07-28 prod
// false "validator is jailed" critical alarm: the single topaz RPC started
// serving an HTML error page, GnoResolveValoper failed, and the refresh wiped
// Bonded to false (and Valcons to "") because the active-set check is skipped
// when the signing address is unknown. The next refresh with the realm back —
// but /validators still failing — preserved the already-wiped false state, and
// watch() saw a true->false transition: false jailed -> Pushover emergency.
// A realm/RPC failure must preserve the previously resolved identity and bonded
// state; only a healthy /validators answer may change them.
func TestGnoGetValInfoPreservesStateOnRealmFailure(t *testing.T) {
	operator := "abcdef0123456789abcdef0123456789abcdef01" // realm key (config valoper_address)
	signing := "1111111123456789abcdef0123456789abcdef01"  // consensus addr (/validators, /block)
	realmDown := false
	validatorsDown := false
	mux := http.NewServeMux()
	mux.HandleFunc("/abci_query", func(w http.ResponseWriter, r *http.Request) {
		if realmDown {
			w.Write([]byte(`<html>bad gateway</html>`)) // the prod HTML error page
			return
		}
		decoded := gnoValoperStruct("GnoVal", operator, "gpub1pk", signing)
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"response":{"ResponseBase":{"Data":"` + b64(decoded) + `"}}}}`))
	})
	mux.HandleFunc("/validators", func(w http.ResponseWriter, r *http.Request) {
		if validatorsDown {
			w.Write([]byte(`<html>bad gateway</html>`))
			return
		}
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"validators":[{"address":"` + signing + `"}]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cc := &ChainConfig{
		name: "gno", ChainId: "test11", Type: "gnoland",
		ValAddress: operator, Nodes: []*NodeConfig{{Url: srv.URL}},
	}
	// Healthy baseline: realm resolves, validator bonded.
	if err := cc.GnoGetValInfo(true); err != nil {
		t.Fatalf("baseline err: %v", err)
	}
	if !cc.valInfo.Bonded || !addrEqual(signing, cc.valInfo.Valcons) || cc.valInfo.Moniker != "GnoVal" {
		t.Fatalf("baseline wrong: bonded=%v valcons=%q moniker=%q", cc.valInfo.Bonded, cc.valInfo.Valcons, cc.valInfo.Moniker)
	}

	// Whole RPC flaps: realm + /validators both fail. Known-good state must survive.
	realmDown, validatorsDown = true, true
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("refresh err: %v", err)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("realm failure must not wipe bonded state (false jailed root cause)")
	}
	if !addrEqual(signing, cc.valInfo.Valcons) {
		t.Fatalf("realm failure must not wipe signing addr, got %q", cc.valInfo.Valcons)
	}
	if cc.valInfo.Moniker != "GnoVal" {
		t.Fatalf("realm failure should keep the known moniker, got %q", cc.valInfo.Moniker)
	}
	if cc.valInfo.Jailed {
		t.Fatal("transient realm failure must not mark the validator jailed")
	}

	// Realm recovers while /validators is still down (prod 07:15:42): the preserved
	// bonded state must be kept (all-endpoints-failed path), not flipped to false.
	cc.lastValInfo = &ValInfo{Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded}
	realmDown = false
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("refresh err: %v", err)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("realm-recovered refresh must preserve bonded when /validators is unreachable")
	}
	if cc.valInfo.Jailed {
		t.Fatal("no jailed transition allowed on preserved state")
	}

	// /validators recovers (realm still failing): the real check must run against
	// the preserved signing addr and confirm bonded — state stays accurate.
	realmDown, validatorsDown = true, false
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("refresh err: %v", err)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("healthy /validators must confirm bonded via the preserved signing addr")
	}
}

// TestGnoCheckBondedPollsAllNodes proves the active-set check polls every
// non-down node and treats the validator as bonded when ANY healthy node reports
// it active. This is the poll-all philosophy: a single stale/lagging endpoint —
// or a transient /validators error on the one sticky endpoint the old code used —
// must not flip a bonded validator to "jailed" (the false-alarm root cause).
func TestGnoCheckBondedPollsAllNodes(t *testing.T) {
	signing := "1111111123456789abcdef0123456789abcdef01"
	withoutAddr := `{"jsonrpc":"2.0","result":{"validators":[{"address":"0000000000000000000000000000000000000000"}]}}`
	withAddr := `{"jsonrpc":"2.0","result":{"validators":[{"address":"` + signing + `"}]}}`

	t.Run("one node errors, other confirms bonded -> bonded", func(t *testing.T) {
		errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer errSrv.Close()
		okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(withAddr))
		}))
		defer okSrv.Close()

		cc := &ChainConfig{Nodes: []*NodeConfig{{Url: errSrv.URL}, {Url: okSrv.URL}}}
		bonded, ok := cc.gnoCheckBonded(signing)
		if !ok {
			t.Fatal("expected ok=true: at least one node answered")
		}
		if !bonded {
			t.Fatal("expected bonded: healthy node has the signing addr")
		}
	})

	t.Run("all healthy nodes report absent -> not bonded", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(withoutAddr))
		}))
		defer srv.Close()
		cc := &ChainConfig{Nodes: []*NodeConfig{{Url: srv.URL}}}
		bonded, ok := cc.gnoCheckBonded(signing)
		if !ok {
			t.Fatal("expected ok=true: node answered")
		}
		if bonded {
			t.Fatal("expected not bonded: signing addr absent from set")
		}
	})

	t.Run("all nodes down or failing -> ok=false (indeterminate)", func(t *testing.T) {
		errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer errSrv.Close()
		// one down flag, one erroring
		cc := &ChainConfig{Nodes: []*NodeConfig{{Url: errSrv.URL, down: true}, {Url: errSrv.URL}}}
		bonded, ok := cc.gnoCheckBonded(signing)
		if ok {
			t.Fatal("expected ok=false: every endpoint failed or was down")
		}
		if bonded {
			t.Fatal("expected bonded=false when indeterminate")
		}
	})
}

// TestGnoGetValInfoResolvesRealmViaSecondNode proves realm resolution is
// poll-all across cc.Nodes, mirroring gnoCheckBonded/PollRun: the first node's
// abci_query (vm/qeval) fails fast (500 + HTML error page, the same error class
// as the prod timeout loop) and GnoGetValInfo must fall through to the second
// node, resolve the realm there, and confirm bonded via that same node. A
// single node whose abci_query times out — but whose /status still works, so it
// is never marked down — must not disable monitoring (prod 2026-08-10 19:39
// episode: topaz-1 looped ~50s with "could not resolve valoper ... from realm"
// while the second node was never tried).
func TestGnoGetValInfoResolvesRealmViaSecondNode(t *testing.T) {
	operator := "abcdef0123456789abcdef0123456789abcdef01" // realm key (config valoper_address)
	signing := "1111111123456789abcdef0123456789abcdef01"  // consensus addr (/validators, /block)

	// First node: every path fails fast (500 + HTML error page). No sleep
	// handlers — a fast error body exercises the exact same fallback path as a
	// timeout without slowing the test.
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`<html>bad gateway</html>`))
	}))
	defer badSrv.Close()

	goodMux := http.NewServeMux()
	goodMux.HandleFunc("/abci_query", func(w http.ResponseWriter, r *http.Request) {
		decoded := gnoValoperStruct("GnoVal", operator, "gpub1pk", signing)
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"response":{"ResponseBase":{"Data":"` + b64(decoded) + `"}}}}`))
	})
	goodMux.HandleFunc("/validators", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"validators":[{"address":"` + signing + `"}]}}`))
	})
	goodSrv := httptest.NewServer(goodMux)
	defer goodSrv.Close()

	cc := &ChainConfig{
		name: "gno", ChainId: "test11", Type: "gnoland",
		ValAddress: operator, Nodes: []*NodeConfig{{Url: badSrv.URL}, {Url: goodSrv.URL}},
	}
	if err := cc.GnoGetValInfo(true); err != nil {
		t.Fatalf("err: %v", err)
	}
	if cc.valInfo.Moniker != "GnoVal" {
		t.Fatalf("moniker: want GnoVal (via second node), got %q", cc.valInfo.Moniker)
	}
	if !addrEqual(signing, cc.valInfo.Valcons) {
		t.Fatalf("valcons: want signing %s, got %q", signing, cc.valInfo.Valcons)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("expected bonded: second node's /validators lists the signing addr")
	}
	if cc.valInfo.Jailed {
		t.Fatal("should not be jailed on first refresh")
	}
}

// TestGnoGetValInfoAllNodesRealmFailPreservesState proves the poll-all realm
// loop degrades exactly like the old single-endpoint path when EVERY node's
// abci_query fails: on first boot the signing address stays unresolved, Bonded
// stays false and the moniker falls back to the truncated operator address;
// with prior state set the prior Moniker/Valcons are preserved (the 2026-07-28
// false-jailed regression guard). Both nodes must be queried — the loop keeps
// trying past a failure — unlike the old sticky gnoRPCUrl, which stopped at the
// first node.
func TestGnoGetValInfoAllNodesRealmFailPreservesState(t *testing.T) {
	operator := "abcdef0123456789abcdef0123456789abcdef01" // realm key (config valoper_address)
	signing := "1111111123456789abcdef0123456789abcdef01"  // consensus addr (/validators, /block)

	var hitsA, hitsB int32
	failSrv := func(hits *int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "abci_query") {
				atomic.AddInt32(hits, 1)
			}
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`<html>bad gateway</html>`))
		}))
	}
	srvA := failSrv(&hitsA)
	defer srvA.Close()
	srvB := failSrv(&hitsB)
	defer srvB.Close()

	cc := &ChainConfig{
		name: "gno", ChainId: "test11", Type: "gnoland",
		ValAddress: operator, Nodes: []*NodeConfig{{Url: srvA.URL}, {Url: srvB.URL}},
	}

	// First boot: realm never resolves; both nodes must have been tried.
	if err := cc.GnoGetValInfo(true); err != nil {
		t.Fatalf("first boot err: %v", err)
	}
	if atomic.LoadInt32(&hitsA) == 0 || atomic.LoadInt32(&hitsB) == 0 {
		t.Fatalf("both nodes must be queried for realm resolution: hitsA=%d hitsB=%d", atomic.LoadInt32(&hitsA), atomic.LoadInt32(&hitsB))
	}
	if cc.valInfo.Valcons != "" {
		t.Fatalf("first boot: signing must stay unresolved, got %q", cc.valInfo.Valcons)
	}
	if cc.valInfo.Bonded {
		t.Fatal("first boot: must stay unbonded when realm never resolves")
	}
	if cc.valInfo.Moniker != operator[:20]+"..." {
		t.Fatalf("first boot moniker: want truncated operator %q, got %q", operator[:20]+"...", cc.valInfo.Moniker)
	}
	if cc.valInfo.Jailed {
		t.Fatal("first boot: must not be jailed")
	}

	// Prior state set: a later all-realm-fail refresh must preserve it.
	cc.stateMux.Lock()
	cc.valInfo.Moniker = "PriorMoniker"
	cc.valInfo.Valcons = signing
	cc.valInfo.Bonded = true
	cc.lastValInfo = &ValInfo{Moniker: "PriorMoniker", Bonded: true}
	cc.stateMux.Unlock()
	if err := cc.GnoGetValInfo(false); err != nil {
		t.Fatalf("refresh err: %v", err)
	}
	if cc.valInfo.Moniker != "PriorMoniker" {
		t.Fatalf("realm failure must preserve prior moniker, got %q", cc.valInfo.Moniker)
	}
	if !addrEqual(signing, cc.valInfo.Valcons) {
		t.Fatalf("realm failure must preserve prior signing addr, got %q", cc.valInfo.Valcons)
	}
	if !cc.valInfo.Bonded {
		t.Fatal("realm failure must preserve prior bonded state (false-jailed regression guard)")
	}
	if cc.valInfo.Jailed {
		t.Fatal("realm failure must not mark the validator jailed")
	}
}

// TestGnoMonitorHealthFlagsWrongChain: a /status that answers with another
// network must mark that node down. Otherwise a mixed testnet/mainnet endpoint
// list would let the poller read head heights (and fetch commits) from the
// wrong chain.
func TestGnoMonitorHealthFlagsWrongChain(t *testing.T) {
	og, od := gnoHealthInterval, gnoHealthRetryDelay
	gnoHealthInterval, gnoHealthRetryDelay = 20*time.Millisecond, time.Millisecond
	defer func() { gnoHealthInterval, gnoHealthRetryDelay = og, od }()

	statusFor := func(network string, height int64) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/status" {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(fmt.Sprintf(
				`{"result":{"node_info":{"network":%q},"sync_info":{"latest_block_height":"%d","catching_up":false}}}`, network, height)))
		}
	}
	srvWrong := httptest.NewServer(statusFor("other-1", 500))
	defer srvWrong.Close()
	srvRight := httptest.NewServer(statusFor("t", 400))
	defer srvRight.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cc := &ChainConfig{
		name:    "gno",
		ChainId: "t",
		Type:    "gnoland",
		Nodes: []*NodeConfig{
			{Url: srvWrong.URL, AlertIfDown: true},
			{Url: srvRight.URL, AlertIfDown: true},
		},
		valInfo:       &ValInfo{Moniker: "x"},
		blocksResults: make([]int, showBLocks),
	}
	done := make(chan struct{})
	go func() { cc.gnoMonitorHealth(ctx, "gno"); close(done) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		cc.stateMux.RLock()
		wrongDown := cc.Nodes[0].down
		rightDown := cc.Nodes[1].down
		msg := cc.Nodes[0].lastMsg
		cc.stateMux.RUnlock()
		if wrongDown {
			if rightDown {
				cancel()
				<-done
				t.Fatalf("matching node was marked down too: %q", msg)
			}
			if !strings.Contains(msg, "chain id other-1 does not match t") {
				cancel()
				<-done
				t.Fatalf("wrong-chain node marked down for the wrong reason: %q", msg)
			}
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("wrong-chain node was never marked down")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}
