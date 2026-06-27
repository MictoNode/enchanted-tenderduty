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
