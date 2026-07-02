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

func TestNotifyPushoverSkipsResolveAndNonCritical(t *testing.T) {
	resetAlarms()
	hits := 0
	var last url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		b, _ := io.ReadAll(r.Body)
		last, _ = url.ParseQuery(string(b))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	pushoverEndpoint = srv.URL + "/messages.json"

	poCfg := func(sev string, resolved bool) *alertMsg {
		return &alertMsg{
			po: true, chain: "c", message: "m", severity: sev, resolved: resolved,
			poToken: "t", poUser: "u", poPriority: 2, poRetry: 30, poExpire: 3600,
		}
	}

	// non-critical fire -> not delivered
	if err := notifyPushover(poCfg("warning", false)); err != nil {
		t.Fatalf("warning notifyPushover: %v", err)
	}
	if hits != 0 {
		t.Fatalf("non-critical must not be delivered to pushover, hits=%d", hits)
	}

	// resolve -> not delivered, even of a critical alarm
	if err := notifyPushover(poCfg("critical", true)); err != nil {
		t.Fatalf("resolve notifyPushover: %v", err)
	}
	if hits != 0 {
		t.Fatalf("resolve must not be delivered to pushover, hits=%d", hits)
	}

	// critical fire -> delivered
	if err := notifyPushover(poCfg("critical", false)); err != nil {
		t.Fatalf("critical fire notifyPushover: %v", err)
	}
	if hits != 1 {
		t.Fatalf("critical fire must be delivered, hits=%d", hits)
	}
	if last.Get("priority") != "2" {
		t.Fatalf("critical fire keeps emergency priority, got %q", last.Get("priority"))
	}

	// resolve (suppressed) must still clear dedup so a re-fire is delivered again
	if err := notifyPushover(poCfg("critical", true)); err != nil {
		t.Fatalf("second resolve notifyPushover: %v", err)
	}
	if hits != 1 {
		t.Fatalf("resolve still not delivered, hits=%d", hits)
	}
	if err := notifyPushover(poCfg("critical", false)); err != nil {
		t.Fatalf("re-fire notifyPushover: %v", err)
	}
	if hits != 2 {
		t.Fatalf("re-fire after suppressed resolve must be delivered (dedup cleared), hits=%d", hits)
	}
}
