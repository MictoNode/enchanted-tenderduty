package tenderduty

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
