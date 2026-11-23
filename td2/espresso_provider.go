package tenderduty

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Espresso provider.
//
// Espresso (HotShot consensus) has no cosmos staking/slashing module and no
// tendermint websocket, so neither of the existing paths applies. The public
// query service only exposes per-epoch *participation rates* (no counts, no
// leader schedule, no DRB), which is too slow and too coarse to alert on. The
// validator's own Status API is the authoritative source for leader duty: it
// counts the slots assigned to the node (consensus_view_duration_as_leader_count)
// and the ones it failed to propose (consensus_number_of_timeouts_as_leader).
// Missed slots = the difference over time, i.e. proposals = assigned - missed.
// Verified against three public validators (LinkPool/Omakase/01node): the
// counter ratio matches the delegation program's published weekly miss rate.
//
// Chain head, node health and liveness come from the query service and the
// node's /v1/status/block-height, mirroring the gnoland poll-all philosophy
// (every configured node is asked; the highest height wins, so one stale
// endpoint cannot freeze monitoring).

const (
	// espressoDefaultDutyWindow is the number of recent leader duties the
	// dashboard uptime % and the percentage_missed alert are computed over.
	espressoDefaultDutyWindow = 1000
	// espressoDefaultVoteLag is how many views the node's last vote may trail
	// the current view before a "not voting" alarm fires (~45s at 450ms/view).
	espressoDefaultVoteLag = 100
	// espressoBondedConfirmTicks debounces a definitive not-in-active-set stake
	// table answer, mirroring gnoBondedConfirmTicks: a single transient query
	// failure must not raise a false "not in the active set" critical.
	espressoBondedConfirmTicks = 2
	espressoQueryTimeout       = 10 * time.Second
)

var (
	espressoPollInterval    = 5 * time.Second
	espressoPollIdleExit    = 2 * time.Minute
	espressoHealthInterval  = time.Minute
	espressoHealthRetryWait = 2 * time.Second
)

// espressoStatus is a snapshot of the validator node's Status API.
type espressoStatus struct {
	Height          int64
	TimeSinceDecide int64
	// Assigned is the number of views the node was leader for (leader duties).
	Assigned int64
	// Missed is how many of those leader duties failed to produce a proposal.
	Missed          int64
	CurrentView     int64
	LastVotedView   int64
	LastDecidedView int64
	Moniker         string
	Version         string
	StakingKey      string
}

// espressoKeys is the /v1/status/keys response: the node's identity.
type espressoKeys struct {
	EthAccount   string `json:"eth_account"`
	ConsensusKey string `json:"consensus_key"`
	StateVerKey  string `json:"state_ver_key"`
	X25519Key    string `json:"x25519_key"`
	P2PAddr      string `json:"p2p_addr"`
}

// espressoStakeTable is the /v1/node/stake-table/current response. Only the
// epoch and the member keys are needed: membership is the active-set check.
type espressoStakeTable struct {
	Epoch      int64 `json:"epoch"`
	StakeTable []struct {
		StakeTableEntry struct {
			StakeKey string `json:"stake_key"`
			// StakeAmount is a hex quantity; not needed for monitoring.
			StakeAmount string `json:"stake_amount"`
		} `json:"stake_table_entry"`
	} `json:"stake_table"`
}

// espressoRawGET performs a GET and returns the status code plus the body. A
// non-2xx response is not an error at this level: the Status API path moved
// between releases (/status/* vs /v1/status/*) and the caller falls back.
func espressoRawGET(url string, timeout time.Duration) (int, []byte, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

// espressoNodeGET fetches a Status API path, preferring the /v1-prefixed form
// served by current releases and falling back to the legacy unprefixed one.
func espressoNodeGET(base, path string, timeout time.Duration) ([]byte, error) {
	base = strings.TrimRight(base, "/")
	code, body, err := espressoRawGET(base+"/v1"+path, timeout)
	if err == nil && code == http.StatusOK {
		return body, nil
	}
	legacyCode, legacyBody, legacyErr := espressoRawGET(base+path, timeout)
	if legacyErr == nil && legacyCode == http.StatusOK {
		return legacyBody, nil
	}
	if err != nil {
		return nil, err
	}
	if legacyErr != nil {
		return nil, legacyErr
	}
	return nil, fmt.Errorf("http %d from %s%s", legacyCode, base, path)
}

// espressoMetricLine returns the first sample line for a metric name, ignoring
// the "# HELP"/"# TYPE" comment lines that share the name.
func espressoMetricLine(body, name string) string {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# ") {
			continue
		}
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			return line
		}
	}
	return ""
}

// espressoMetricFloat parses the sample value (last field) of a metric.
func espressoMetricFloat(body, name string) float64 {
	line := espressoMetricLine(body, name)
	if line == "" {
		return 0
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
	if err != nil {
		return 0
	}
	return v
}

// espressoMetricLabel extracts a quoted label value from a metric sample.
func espressoMetricLabel(body, name, label string) string {
	line := espressoMetricLine(body, name)
	key := label + `="`
	i := strings.Index(line, key)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// espressoParseMetrics turns the Prometheus text of /status/metrics into a
// status snapshot. Missing metrics stay zero; callers must treat
// Assigned==0 && Missed==0 && CurrentView==0 as "metrics not served yet"
// (the status module registers a few seconds after boot on 0.7.x).
func espressoParseMetrics(body []byte) *espressoStatus {
	txt := string(body)
	return &espressoStatus{
		Assigned:        int64(espressoMetricFloat(txt, "consensus_view_duration_as_leader_count")),
		Missed:          int64(espressoMetricFloat(txt, "consensus_number_of_timeouts_as_leader")),
		CurrentView:     int64(espressoMetricFloat(txt, "consensus_current_view")),
		LastVotedView:   int64(espressoMetricFloat(txt, "consensus_last_voted_view")),
		LastDecidedView: int64(espressoMetricFloat(txt, "consensus_last_decided_view")),
		StakingKey:      espressoMetricLabel(txt, "consensus_node", "key"),
		Moniker:         espressoMetricLabel(txt, "consensus_node_identity_general", "name"),
		Version:         espressoMetricLabel(txt, "consensus_version", "desc"),
	}
}

// espressoFetchStatus reads the counters, identity and height from a node's
// Status API. The block height is a separate endpoint from the metrics.
func espressoFetchStatus(base string) (*espressoStatus, error) {
	mb, err := espressoNodeGET(base, "/status/metrics", espressoQueryTimeout)
	if err != nil {
		return nil, fmt.Errorf("status metrics: %w", err)
	}
	st := espressoParseMetrics(mb)
	hb, err := espressoNodeGET(base, "/status/block-height", espressoQueryTimeout)
	if err != nil {
		return nil, fmt.Errorf("status block-height: %w", err)
	}
	h, err := strconv.ParseInt(strings.TrimSpace(string(hb)), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("status block-height %q: %w", strings.TrimSpace(string(hb)), err)
	}
	st.Height = h
	if tb, terr := espressoNodeGET(base, "/status/time-since-last-decide", espressoQueryTimeout); terr == nil {
		if v, perr := strconv.ParseInt(strings.TrimSpace(string(tb)), 10, 64); perr == nil {
			st.TimeSinceDecide = v
		}
	}
	return st, nil
}

// espressoFetchKeys reads the node's registered identity keys.
func espressoFetchKeys(base string) (*espressoKeys, error) {
	b, err := espressoNodeGET(base, "/status/keys", espressoQueryTimeout)
	if err != nil {
		return nil, err
	}
	var k espressoKeys
	if err = json.Unmarshal(b, &k); err != nil {
		return nil, err
	}
	if k.ConsensusKey == "" {
		return nil, errors.New("empty consensus_key")
	}
	return &k, nil
}

// espressoFetchBlockHeight asks a query service for the chain height as that
// node sees it. A 200 with a non-integer body is an error, not a height.
func espressoFetchBlockHeight(base string) (int64, error) {
	base = strings.TrimRight(base, "/")
	code, body, err := espressoRawGET(base+"/v1/status/block-height", espressoQueryTimeout)
	if err != nil {
		return 0, err
	}
	if code != http.StatusOK {
		return 0, fmt.Errorf("http %d from %s", code, base)
	}
	h, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("block-height %q: %w", strings.TrimSpace(string(body)), err)
	}
	return h, nil
}

// espressoFetchStakeTable reads the current epoch's active set.
func espressoFetchStakeTable(base string) (*espressoStakeTable, error) {
	base = strings.TrimRight(base, "/")
	code, body, err := espressoRawGET(base+"/v1/node/stake-table/current", espressoQueryTimeout)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("http %d from %s", code, base)
	}
	var st espressoStakeTable
	if err = json.Unmarshal(body, &st); err != nil {
		return nil, err
	}
	if st.Epoch <= 0 {
		return nil, errors.New("empty stake table response")
	}
	return &st, nil
}

// espressoStakeTableHasKey reports whether the BLS key is in the active set.
func espressoStakeTableHasKey(st *espressoStakeTable, key string) bool {
	if st == nil || key == "" {
		return false
	}
	for _, e := range st.StakeTable {
		if e.StakeTableEntry.StakeKey == key {
			return true
		}
	}
	return false
}

// espressoShortKey renders a BLS key compactly for logs and dashboards.
func espressoShortKey(key string) string {
	body := strings.TrimPrefix(key, "BLS_VER_KEY~")
	if len(body) <= 12 {
		return key
	}
	return body[:8] + "…" + body[len(body)-4:]
}

// EspressoGetValInfo refreshes the validator identity and active-set state.
// It mirrors GnoGetValInfo: a failed query service must never wipe a resolved
// identity or flip Bonded — a false "not in the active set" critical is the
// exact failure class that hit gnoland three times.
func (cc *ChainConfig) EspressoGetValInfo(first bool) error {
	cc.stateMux.Lock()
	if cc.valInfo == nil {
		cc.valInfo = &ValInfo{}
	}
	cc.stateMux.Unlock()

	// Identity: the node's /v1/status/keys is authoritative (it is what the
	// operator registered); the metrics label is the fallback for builds that
	// do not serve the keys endpoint.
	var nodeKey, nodeMoniker, nodeVersion string
	if cc.EspressoNodeURL != "" {
		if keys, err := espressoFetchKeys(cc.EspressoNodeURL); err == nil {
			nodeKey = keys.ConsensusKey
		} else if first {
			l(fmt.Sprintf("⚠️ %s could not read espresso node keys from %s: %s", cc.ChainId, cc.EspressoNodeURL, err))
		}
		if st, err := espressoFetchStatus(cc.EspressoNodeURL); err == nil {
			nodeMoniker = st.Moniker
			nodeVersion = st.Version
			if nodeKey == "" {
				nodeKey = st.StakingKey
			}
		}
	}

	cc.stateMux.RLock()
	prevMoniker, prevValcons := "", ""
	if cc.valInfo != nil {
		prevMoniker, prevValcons = cc.valInfo.Moniker, cc.valInfo.Valcons
	}
	cc.stateMux.RUnlock()

	key := cc.EspressoStakingKey
	if key == "" {
		key = nodeKey
	}
	if key == "" {
		key = prevValcons
	}
	moniker := nodeMoniker
	if moniker == "" {
		if prevMoniker != "" && prevMoniker != "not connected" {
			moniker = prevMoniker
		} else if key != "" {
			moniker = espressoShortKey(key)
		} else {
			moniker = "not connected"
		}
	}
	if key == "" {
		if first {
			l(fmt.Sprintf("🛑 %s no BLS staking key: set espresso_staking_key or espresso_node_url; duty monitoring inactive", cc.ChainId))
		}
		return errors.New("espresso staking key unknown")
	}

	// Active set: poll every non-down query node and take the first healthy
	// stake table (poll-all, same reasoning as GnoGetValInfo).
	var st *espressoStakeTable
	var lastErr error
	for _, n := range cc.Nodes {
		cc.stateMux.RLock()
		down, url := n.down, n.Url
		cc.stateMux.RUnlock()
		if down {
			continue
		}
		tbl, err := espressoFetchStakeTable(url)
		if err != nil {
			lastErr = err
			continue
		}
		st = tbl
		lastErr = nil
		break
	}

	cc.stateMux.Lock()
	defer cc.stateMux.Unlock()
	bonded := cc.valInfo.Bonded
	if st != nil {
		if espressoStakeTableHasKey(st, key) {
			cc.espressoBondMissStreak = 0
			bonded = true
		} else {
			cc.espressoBondMissStreak++
			// Preserve the prior verdict until the absence is confirmed.
			if cc.espressoBondMissStreak >= espressoBondedConfirmTicks {
				bonded = false
			}
		}
		if st.Epoch != cc.espressoEpoch {
			if cc.espressoEpoch != 0 {
				l(fmt.Sprintf("🗓 %-12s epoch %d → %d (epoch miss counters reset)", cc.ChainId, cc.espressoEpoch, st.Epoch))
			}
			cc.espressoEpoch = st.Epoch
			cc.espressoEpochAssigned = 0
			cc.espressoEpochMissed = 0
		}
	} else if lastErr != nil && first {
		l(fmt.Sprintf("⚠️ %s could not read the active stake table: %s", cc.ChainId, lastErr))
	}

	cc.espressoKey = key
	cc.valInfo.Moniker = moniker
	cc.valInfo.Valcons = key
	cc.valInfo.Bonded = bonded
	cc.valInfo.Jailed = false
	cc.valInfo.Tombstoned = false
	if first {
		node := "unknown"
		if cc.EspressoNodeURL != "" {
			node = cc.EspressoNodeURL
		}
		state := "ACTIVE"
		if !bonded {
			state = "NOT in active validator set"
		}
		l(fmt.Sprintf("⚙️ %-12s espresso validator %s (%s) %s; node %s version %s epoch %d",
			cc.ChainId, moniker, espressoShortKey(key), state, node, nodeVersion, cc.espressoEpoch))
	}
	return nil
}

// espressoNewRpc validates that at least one query endpoint answers. The node's
// own Status API is optional at this stage: liveness monitoring keeps working
// without it, only leader-duty accounting needs it.
func (cc *ChainConfig) espressoNewRpc() error {
	working := 0
	for _, ep := range cc.Nodes {
		h, err := espressoFetchBlockHeight(ep.Url)
		cc.stateMux.Lock()
		if err != nil {
			if !ep.down {
				ep.down = true
				ep.downSince = time.Now()
			}
			ep.lastMsg = fmt.Sprintf("❌ espresso query failed %s: %s", ep.Url, err)
			msg := ep.lastMsg
			cc.stateMux.Unlock()
			l(msg)
			continue
		}
		ep.down = false
		ep.downSince = time.Unix(0, 0)
		ep.lastMsg = ""
		cc.stateMux.Unlock()
		if h > 0 {
			working++
		}
	}
	if working == 0 {
		return errors.New("no working Espresso query endpoints")
	}
	return nil
}

// espressoMonitorHealth polls every configured query endpoint plus the
// validator's own Status API. Lag is measured against the highest height any
// endpoint reports, never against a single sticky endpoint (the gnoland
// lesson: a lagging endpoint made node-lag alerting silently dead).
func (cc *ChainConfig) espressoMonitorHealth(ctx context.Context, chainName string) {
	tick := time.NewTicker(espressoHealthInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			type qStatus struct {
				node   *NodeConfig
				height int64
				err    error
			}
			statuses := make([]qStatus, 0, len(cc.Nodes))
			var headH int64
			for _, node := range cc.Nodes {
				h, err := espressoFetchBlockHeight(node.Url)
				if err != nil {
					// One failed request must not mark the node down for a whole
					// health tick; retry once first.
					time.Sleep(espressoHealthRetryWait)
					h, err = espressoFetchBlockHeight(node.Url)
				}
				st := qStatus{node: node, height: h, err: err}
				if err == nil && h > headH {
					headH = h
				}
				statuses = append(statuses, st)
			}
			for _, st := range statuses {
				if st.err != nil {
					nodeLagCheck(cc, st.node, 0)
					alertNodeDown(cc, st.node, "down: "+st.err.Error())
					continue
				}
				nodeLagCheck(cc, st.node, headH-st.height)
				cc.stateMux.Lock()
				if st.node.down {
					st.node.lastMsg = ""
					st.node.wasDown = true
				}
				st.node.down = false
				st.node.syncing = false
				st.node.downSince = time.Unix(0, 0)
				cc.noNodes = false
				cc.stateMux.Unlock()
				if td.Prom {
					td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, 0, st.node.Url)
				}
			}

			// Validator node: down detection + lag against the chain head.
			if cc.espressoNode != nil {
				st, err := espressoFetchStatus(cc.EspressoNodeURL)
				if err != nil {
					time.Sleep(espressoHealthRetryWait)
					st, err = espressoFetchStatus(cc.EspressoNodeURL)
				}
				if err != nil {
					nodeLagCheck(cc, cc.espressoNode, 0)
					alertNodeDown(cc, cc.espressoNode, "down: "+err.Error())
				} else {
					if headH > 0 && st.Height > 0 {
						nodeLagCheck(cc, cc.espressoNode, headH-st.Height)
					}
					cc.stateMux.Lock()
					lag := st.CurrentView - st.LastVotedView
					if lag < 0 {
						lag = 0
					}
					cc.espressoVoteLag = lag
					if cc.espressoNode.down {
						cc.espressoNode.lastMsg = ""
						cc.espressoNode.wasDown = true
					}
					cc.espressoNode.down = false
					cc.espressoNode.syncing = false
					cc.espressoNode.downSince = time.Unix(0, 0)
					cc.stateMux.Unlock()
					if td.Prom {
						td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, 0, cc.espressoNode.Url)
					}
				}
			}

			// Snapshot the previous identity before refreshing, exactly like
			// gnoMonitorHealth: watch() compares lastValInfo against valInfo to
			// detect a real bonded→unbonded transition.
			cc.stateMux.Lock()
			if cc.valInfo != nil {
				cc.lastValInfo = &ValInfo{
					Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded, Jailed: cc.valInfo.Jailed,
					Tombstoned: cc.valInfo.Tombstoned, Missed: cc.valInfo.Missed, Window: cc.valInfo.Window,
					Conspub: cc.valInfo.Conspub, Valcons: cc.valInfo.Valcons,
				}
			}
			cc.stateMux.Unlock()
			if e := cc.EspressoGetValInfo(false); e != nil {
				l("❓ refreshing espresso validator info for", chainName, e)
			}
		}
	}
}
