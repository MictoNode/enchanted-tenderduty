package tenderduty

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// resetAlarms gives tests a clean shared alarm cache.
func resetAlarms() {
	alarms = &alarmCache{
		SentPdAlarms:   map[string]time.Time{},
		SentTgAlarms:   map[string]time.Time{},
		SentDiAlarms:   map[string]time.Time{},
		SentSlkAlarms:  map[string]time.Time{},
		SentPoAlarms:   map[string]time.Time{},
		AllAlarms:      map[string]map[string]time.Time{},
		flappingAlarms: map[string]map[string]time.Time{},
		notifyMux:      sync.RWMutex{},
	}
}

func TestShouldNotifyPushoverDedup(t *testing.T) {
	resetAlarms()
	msg := &alertMsg{chain: "c", message: "missed", po: true}
	if !shouldNotify(msg, po) {
		t.Fatal("first pushover notify should be allowed")
	}
	if shouldNotify(msg, po) {
		t.Fatal("second pushover notify for same message should be suppressed (dedup)")
	}
}

func TestNotifyPushoverPostsEmergency(t *testing.T) {
	resetAlarms()
	var got url.Values
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		got, _ = url.ParseQuery(string(b))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	msg := &alertMsg{
		po: true, chain: "AtomOne-M", message: "missed 10 blocks", severity: "critical",
		poToken: "tok", poUser: "user", poPriority: 2, poRetry: 30, poExpire: 3600, poSound: "persistent",
	}
	// point the function at the test server
	pushoverEndpoint = srv.URL + "/messages.json"
	if err := notifyPushover(msg); err != nil {
		t.Fatalf("notifyPushover: %v", err)
	}
	if path != "/messages.json" {
		t.Fatalf("unexpected path %q", path)
	}
	if got.Get("token") != "tok" || got.Get("user") != "user" {
		t.Fatalf("missing creds: %v", got)
	}
	if got.Get("priority") != "2" || got.Get("retry") != "30" || got.Get("expire") != "3600" {
		t.Fatalf("emergency params missing: %v", got)
	}
	if !strings.HasPrefix(got.Get("title"), "🚨") {
		t.Fatalf("expected alert title prefix, got %q", got.Get("title"))
	}
}

func TestNotifyPushoverResolvedClampsPriority(t *testing.T) {
	resetAlarms()
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got, _ = url.ParseQuery(string(b))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	pushoverEndpoint = srv.URL + "/messages.json"
	// seed the alert (non-resolved) so the resolve is actually delivered
	seed := &alertMsg{po: true, chain: "c", message: "ok", poToken: "t", poUser: "u", poPriority: 2, poRetry: 30, poExpire: 3600}
	if err := notifyPushover(seed); err != nil {
		t.Fatalf("seed notifyPushover: %v", err)
	}
	// now resolve — priority must clamp to 0
	msg := &alertMsg{po: true, resolved: true, chain: "c", message: "ok", poToken: "t", poUser: "u", poPriority: 2, poRetry: 30, poExpire: 3600}
	if err := notifyPushover(msg); err != nil {
		t.Fatalf("resolve notifyPushover: %v", err)
	}
	if got.Get("priority") != "0" {
		t.Fatalf("resolved must clamp priority to 0, got %q", got.Get("priority"))
	}
}
