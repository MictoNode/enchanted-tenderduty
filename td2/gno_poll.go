package tenderduty

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	dash "github.com/MictoNode/enchanted-tenderduty/td2/dashboard"
)

// gnoCommitResult is the /commit RPC response. The commit AT height H carries
// the precommits cast on block H; a block's own last_commit carries the votes
// for H-1. Per-height signing state must therefore come from /commit — reading
// /block's last_commit attributes every signature to the following height and
// leaves the newest head height unevaluated until the next block arrives.
type gnoCommitResult struct {
	JSONRPC string `json:"jsonrpc"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error"`
	Result struct {
		SignedHeader struct {
			Header struct {
				Height          string `json:"height"`
				ProposerAddress string `json:"proposer_address"`
			} `json:"header"`
			Commit struct {
				Precommits []struct {
					ValidatorAddress string `json:"validator_address"`
				} `json:"precommits"`
			} `json:"commit"`
		} `json:"signed_header"`
	} `json:"result"`
}

// gnoGetCommit fetches the commit (and signed header) for exactly `height`.
// A JSON-RPC error, an empty body, or a height that does not match the request
// is an error: callers must never process another height's signatures.
func gnoGetCommit(rpcURL string, height int64) (*gnoCommitResult, error) {
	url := fmt.Sprintf("%s/commit?height=%d", strings.TrimRight(rpcURL, "/"), height)
	body, err := gnoHTTPGet(url, 10*time.Second)
	if err != nil {
		return nil, err
	}
	var res gnoCommitResult
	if err = json.Unmarshal(body, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("commit error: %s", res.Error.Message)
	}
	got, err := strconv.ParseInt(res.Result.SignedHeader.Header.Height, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("empty commit response for height %d", height)
	}
	if got != height {
		return nil, fmt.Errorf("commit height mismatch: asked %d got %d", height, got)
	}
	return &res, nil
}

// gnoSignState derives the validator's signing state for a single committed
// height using canonical address bytes.
func gnoSignState(valAddr string, cr *gnoCommitResult) StatusType {
	if addrEqual(cr.Result.SignedHeader.Header.ProposerAddress, valAddr) {
		return StatusProposed
	}
	for _, pc := range cr.Result.SignedHeader.Commit.Precommits {
		if addrEqual(pc.ValidatorAddress, valAddr) {
			return StatusSigned
		}
	}
	return Statusmissed
}

// gnoPollInterval is how often PollRun polls every configured node for the head
// block. gnoPollIdleExit is how long PollRun waits with no height advance before
// returning, so the run loop can re-evaluate the endpoints.
var (
	gnoPollInterval = 5 * time.Second
	gnoPollIdleExit = 2 * time.Minute
	// gnoMaxCatchUpBlocks bounds how many heights are backfilled per tick.
	// gno.land blocks are ~3.5s but PollRun ticks every 5s, so a single tick
	// normally advances 1-2 heights; short outages can leave a larger gap.
	gnoMaxCatchUpBlocks int64 = 100
	// gnoCatchUpJump is the backlog beyond which polling skips ahead instead of
	// walking every height (e.g. after hours of downtime or a pruned /commit
	// history). Without it the poller would spend a very long time replaying.
	gnoCatchUpJump int64 = 1000
	// gnoCommitFetchWorkers bounds the concurrent /commit requests used when a
	// tick must backfill more than one height.
	gnoCommitFetchWorkers = 8
)

// PollRun replaces the websocket subscription for gnoland: every tick it reads
// the head from all configured nodes and evaluates EVERY height in between via
// /commit, so a block time shorter than the poll interval does not hide blocks
// and every evaluated height carries its own signing outcome.
func (cc *ChainConfig) PollRun() {
	started := time.Now()
	for {
		cc.stateMux.RLock()
		waiting := cc.gnoRpcEndpoint == "" || cc.valInfo == nil
		cc.stateMux.RUnlock()
		if waiting {
			if started.Before(time.Now().Add(-2 * time.Minute)) {
				l(cc.name, "poller timed out waiting for endpoint")
				return
			}
			l("⏰ waiting for a healthy client for", cc.ChainId)
			time.Sleep(30 * time.Second)
			continue
		}
		break
	}

	// Without the signing (consensus) address, resolved from the valopers realm
	// into valInfo.Valcons, precommits can never be matched. Block monitoring is
	// impossible in that state; node-down/stalled alerts still work via watch().
	cc.stateMux.RLock()
	valcons := ""
	// Start from the last height actually processed instead of 0. During a chain
	// halt every poller restart would otherwise treat the frozen head as a brand
	// new block, refreshing lastBlockTime and silently suppressing the stalled
	// alarm forever (prod 2026-11-17: onyx-1 halted, no alert for 50+ minutes,
	// gno window stuck at 1 cell per poller restart).
	lastHeight := cc.lastBlockNum
	if cc.valInfo != nil {
		valcons = cc.valInfo.Valcons
	}
	cc.stateMux.RUnlock()
	if valcons == "" {
		l(fmt.Sprintf("🛑 %-12s no signing address resolved from realm; block monitoring inactive", cc.ChainId))
		return
	}

	l(fmt.Sprintf("⚙️ %-12s polling %d node(s) for new blocks", cc.ChainId, len(cc.Nodes)))
	windowSize := cc.GnoSignedBlocksWindow
	if windowSize <= 0 {
		windowSize = 10000
	}
	cc.stateMux.Lock()
	cc.gnoWin = newGnoWindow(windowSize)
	cc.stateMux.Unlock()
	noBlockSince := time.Now()
	tick := time.NewTicker(gnoPollInterval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			// Head detection polls every configured node each tick and takes the
			// highest height seen. A single stale/lagging endpoint can no longer
			// freeze monitoring — a healthy node carries the chain past it.
			// (Previously PollRun was pinned to one endpoint and stalled whenever
			// it kept serving a stale /block, producing false "stalled chain"
			// alerts while the network was healthy.)
			var bestHeight int64 = -1
			var bestURL string
			var healthy []string
			for i := range cc.Nodes {
				node := cc.Nodes[i]
				cc.stateMux.RLock()
				down, nodeURL := node.down, node.Url
				cc.stateMux.RUnlock()
				if down {
					continue
				}
				cid, hs, _, err := GnoGetStatus(nodeURL)
				if err != nil || cid != cc.ChainId {
					// A node on another network must not contribute a head height
					// (or serve commits) for this chain; gnoMonitorHealth marks it
					// down on its next tick.
					continue
				}
				h, perr := strconv.ParseInt(hs, 10, 64)
				if perr != nil || h <= 0 {
					continue
				}
				healthy = append(healthy, nodeURL)
				if h > bestHeight {
					bestHeight, bestURL = h, nodeURL
				}
			}
			// A commit is only final once the NEXT block exists: /commit?height=head
			// returns an in-progress precommit set (dozens of nulls) that briefly
			// lacks validators that did sign. Treating those nulls as misses raised
			// false "missed block" lines and percentage-alarm churn on onyx-1
			// (2026-11-19; the same heights evaluate as signed once final).
			finalHead := bestHeight - 1
			if finalHead <= lastHeight {
				// Nothing new is finalized this tick, so the head is stale for
				// evaluation and must not refresh lastBlockTime (that would suppress
				// the stalled alarm). A head that stays BELOW the last processed
				// height for the whole idle window is a re-genesis (heights reset),
				// not staleness: accept it so monitoring recovers without a restart.
				if bestHeight > 0 && bestHeight < lastHeight &&
					time.Since(noBlockSince) > gnoPollIdleExit {
					l(fmt.Sprintf("♻️ %-12s chain height reset to %d (was %d), resuming", cc.ChainId, bestHeight, lastHeight))
					lastHeight = 0
				} else {
					// If this persists across every node for the idle window, force a
					// reconnect so gnoNewRpc re-evaluates the endpoints (mirrors the
					// cosmos websocket's idle exit). The stalled-chain alarm itself is
					// raised by watch() from lastBlockTime.
					if time.Since(noBlockSince) > gnoPollIdleExit {
						l("🛑", cc.ChainId, "no blocks for 2 min, exiting")
						return
					}
					continue
				}
			}
			noBlockSince = time.Now()

			// Evaluate every finalized height the head advanced by, not just one,
			// so no block goes uncounted when block time is shorter than the poll
			// interval (~3.5s vs 5s on gno.land). Backfill is bounded per tick;
			// a very large backlog (long outage) is skipped rather than replayed.
			start := lastHeight + 1
			if finalHead-lastHeight > gnoCatchUpJump {
				skipped := finalHead - lastHeight - gnoMaxCatchUpBlocks
				start = finalHead - gnoMaxCatchUpBlocks + 1
				// A fresh start (no persisted height) intentionally begins at the
				// recent history instead of replaying the chain from genesis — only
				// log the skip when resuming from a known height.
				if lastHeight > 0 {
					l(fmt.Sprintf("⏩ %-12s %d heights behind, skipping %d unprocessed heights", cc.ChainId, finalHead-lastHeight, skipped))
				}
			}
			end := finalHead
			if end-start+1 > gnoMaxCatchUpBlocks {
				end = start + gnoMaxCatchUpBlocks - 1
			}
			cc.stateMux.RLock()
			valcons = ""
			if cc.valInfo != nil {
				valcons = cc.valInfo.Valcons
			}
			cc.stateMux.RUnlock()
			if valcons == "" {
				l(fmt.Sprintf("🛑 %-12s no signing address resolved from realm; block monitoring inactive", cc.ChainId))
				return
			}

			// Fetch the commits concurrently (bounded) so a cold start or a
			// post-outage catch-up does not serialize ~200ms round trips: 100
			// heights sequentially would delay the first dashboard update by tens
			// of seconds. Results are processed in height order below.
			commits := make([]*gnoCommitResult, end-start+1)
			cerrs := make([]error, len(commits))
			var wg sync.WaitGroup
			sem := make(chan struct{}, gnoCommitFetchWorkers)
			for i := range commits {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					commits[i], cerrs[i] = cc.gnoFetchCommit(bestURL, healthy, start+int64(i))
				}(i)
			}
			wg.Wait()

			info := getAlarms(cc.name)
			advanced := false
			for i, cr := range commits {
				h := start + int64(i)
				if cerrs[i] != nil {
					if i == len(commits)-1 {
						// Retry this height next tick instead of advancing past a
						// block whose signatures were never read.
						l(fmt.Sprintf("⚠️ %-12s could not fetch commit %d: %s", cc.ChainId, h, cerrs[i]))
						break
					}
					// Older heights may be pruned. Skip so the poller still reaches
					// the head; the skipped height is not counted either way.
					l(fmt.Sprintf("⚠️ %-12s could not fetch commit %d, skipping: %s", cc.ChainId, h, cerrs[i]))
				} else {
					cc.stateMux.Lock()
					signState := gnoSignState(valcons, cr)
					// Grid slot: the actual signing state when bonded; a -1 "No Data"
					// sentinel otherwise (grid.js renders values outside 0-4 as No
					// Data), so a resolved-but-inactive validator doesn't paint every
					// block missed.
					gridBlock := int(signState)
					if !cc.valInfo.Bonded {
						gridBlock = -1
					}
					cc.blocksResults = append([]int{gridBlock}, cc.blocksResults[:len(cc.blocksResults)-1]...)
					if signState < 3 && cc.valInfo.Bonded {
						warn := fmt.Sprintf("❌ %s missed block %d on %s", cc.valInfo.Moniker, h, cc.ChainId)
						info += warn + "\n"
						cc.lastError = time.Now().UTC().String() + " " + info
						l(warn)
					}
					// Only accrue sign/miss counters for bonded validators. A validator
					// that is resolved but not in the active set is not expected to sign,
					// so counting its misses would produce false consecutive/percentage
					// alerts (e.g. a node that has not started validating yet).
					if cc.valInfo.Bonded {
						isMiss := false
						switch signState {
						case Statusmissed:
							cc.statTotalMiss += 1
							cc.statConsecutiveMiss += 1
							isMiss = true
						case StatusSigned:
							cc.statTotalSigns += 1
							cc.statConsecutiveMiss = 0
						case StatusProposed:
							cc.statTotalProps += 1
							cc.statTotalSigns += 1
							cc.statConsecutiveMiss = 0
						}
						// Sliding signing-health window (NOT a slashing window; gno has none).
						// valInfo.Missed/Window are bounded + sliding (cosmos-like), so the
						// dashboard uptime % and percentage_missed alert stay meaningful.
						cc.gnoWin.Push(isMiss)
						cc.valInfo.Missed = int64(cc.gnoWin.Missed())
						cc.valInfo.Window = int64(cc.gnoWin.Window())
					}
					cc.lastBlockNum = h
					cc.stateMux.Unlock()
				}
				lastHeight = h
				advanced = true
			}
			if !advanced {
				continue
			}

			cc.stateMux.Lock()
			if lastHeight%20 == 0 {
				l(fmt.Sprintf("🧊 %-12s block %d", cc.ChainId, lastHeight))
			}
			sinceLast := time.Since(cc.lastBlockTime).Seconds()
			cc.lastBlockTime = time.Now()
			cc.lastBlockAlarm = false
			if td.Prom {
				td.statsChan <- cc.mkUpdateWithMoniker(metricLastBlockSeconds, sinceLast, "", cc.valInfo.Moniker)
			}
			healthyNodes := 0
			for i := range cc.Nodes {
				if !cc.Nodes[i].down {
					healthyNodes += 1
				}
			}
			cc.activeAlerts = alarms.getCount(cc.name)
			if td.EnableDash {
				td.updateChan <- &dash.ChainStatus{
					MsgType: "status", Name: cc.name, ChainId: cc.ChainId, Network: cc.Network, Moniker: cc.valInfo.Moniker,
					Bonded: cc.valInfo.Bonded, Jailed: cc.valInfo.Jailed, Tombstoned: cc.valInfo.Tombstoned,
					Missed: cc.valInfo.Missed, Window: cc.valInfo.Window, Nodes: len(cc.Nodes),
					HealthyNodes: healthyNodes, ActiveAlerts: cc.activeAlerts, Height: lastHeight,
					LastError: info, Blocks: append([]int(nil), cc.blocksResults...),
				}
			}
			if td.Prom {
				td.statsChan <- cc.mkUpdateWithMoniker(metricSigned, cc.statTotalSigns, "", cc.valInfo.Moniker)
				td.statsChan <- cc.mkUpdateWithMoniker(metricProposed, cc.statTotalProps, "", cc.valInfo.Moniker)
				td.statsChan <- cc.mkUpdateWithMoniker(metricMissed, cc.statTotalMiss, "", cc.valInfo.Moniker)
				td.statsChan <- cc.mkUpdateWithMoniker(metricConsecutive, cc.statConsecutiveMiss, "", cc.valInfo.Moniker)
				td.statsChan <- cc.mkUpdateWithMoniker(metricUnealthyNodes, float64(len(cc.Nodes)-healthyNodes), "", cc.valInfo.Moniker)
			}
			cc.stateMux.Unlock()
		case <-td.ctx.Done():
			return
		}
	}
}

// gnoFetchCommit fetches the commit for `height` from the node that reported the
// current head, falling back to the other nodes that answered /status with the
// matching chain id this tick. A single endpoint with a broken or pruned
// /commit must not stall the advance, and a node on another network must never
// supply signatures for this chain.
func (cc *ChainConfig) gnoFetchCommit(preferred string, healthy []string, height int64) (*gnoCommitResult, error) {
	var lastErr error
	if preferred != "" {
		cr, err := gnoGetCommit(preferred, height)
		if err == nil {
			return cr, nil
		}
		lastErr = err
	}
	for _, url := range healthy {
		if url == preferred {
			continue
		}
		cr, err := gnoGetCommit(url, height)
		if err == nil {
			return cr, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no healthy gno RPC node for commit")
	}
	return nil, lastErr
}
