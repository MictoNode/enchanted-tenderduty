package tenderduty

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	dash "github.com/MictoNode/enchanted-tenderduty/td2/dashboard"
)

type pollBlockResult struct {
	Result struct {
		Block struct {
			Header struct {
				Height          string `json:"height"`
				ProposerAddress string `json:"proposer_address"`
			} `json:"header"`
			LastCommit struct {
				Precommits []struct {
					ValidatorAddress string `json:"validator_address"`
				} `json:"precommits"`
			} `json:"last_commit"`
		} `json:"block"`
	} `json:"result"`
}

// gnoSignState derives the validator's signing state for a polled block using canonical address bytes.
func gnoSignState(valAddr string, br *pollBlockResult) StatusType {
	if addrEqual(br.Result.Block.Header.ProposerAddress, valAddr) {
		return StatusProposed
	}
	for _, pc := range br.Result.Block.LastCommit.Precommits {
		if addrEqual(pc.ValidatorAddress, valAddr) {
			return StatusSigned
		}
	}
	return Statusmissed
}

// gnoPollInterval is how often PollRun polls every configured node for the head
// block. gnoPollIdleExit is how long PollRun waits with no height advance before
// returning, so the run loop can re-evaluate the endpoints. Both are vars (not
// consts) so tests can shorten them.
var (
	gnoPollInterval = 5 * time.Second
	gnoPollIdleExit = 2 * time.Minute
)

// PollRun replaces the websocket subscription for gnoland: HTTP-poll /block every 5s.
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
			// Poll every configured node each tick and advance to the highest height
			// seen. A single stale/lagging endpoint can no longer freeze monitoring —
			// a healthy node carries the chain past it. Previously PollRun was pinned
			// to one endpoint (cc.gnoRpcEndpoint) and stalled whenever it kept serving
			// a stale /block, producing false "stalled chain" alerts while the network
			// was actually healthy.
			var bestBR *pollBlockResult
			var bestHeight int64 = -1
			for i := range cc.Nodes {
				node := cc.Nodes[i]
				cc.stateMux.RLock()
				down := node.down
				nodeURL := node.Url
				cc.stateMux.RUnlock()
				if down {
					continue
				}
				url := strings.TrimRight(nodeURL, "/") + "/block"
				client := &http.Client{Timeout: 10 * time.Second}
				resp, err := client.Get(url)
				if err != nil {
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				var br pollBlockResult
				if err = json.Unmarshal(body, &br); err != nil {
					continue
				}
				var h int64
				fmt.Sscanf(br.Result.Block.Header.Height, "%d", &h)
				if h > bestHeight {
					bestHeight = h
					bestBR = &br
				}
			}
			if bestBR == nil || bestHeight <= lastHeight {
				// No node produced an advance this tick, so this head is stale and must
				// not be processed as a new block (that would refresh lastBlockTime and
				// suppress the stalled alarm). A head that stays BELOW the last
				// processed height for the whole idle window is a re-genesis (heights
				// reset), not staleness: accept it so monitoring recovers without a
				// process restart.
				if bestBR != nil && bestHeight > 0 && bestHeight < lastHeight &&
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
			lastHeight = bestHeight

			cc.stateMux.Lock()
			signState := gnoSignState(cc.valInfo.Valcons, bestBR)
			if bestHeight%20 == 0 {
				l(fmt.Sprintf("🧊 %-12s block %d", cc.ChainId, bestHeight))
			}
			cc.lastBlockNum = bestHeight
			if td.Prom {
				td.statsChan <- cc.mkUpdateWithMoniker(metricLastBlockSeconds, time.Since(cc.lastBlockTime).Seconds(), "", cc.valInfo.Moniker)
			}
			cc.lastBlockTime = time.Now()
			cc.lastBlockAlarm = false
			info := getAlarms(cc.name)
			// Grid slot: the actual signing state when bonded; a -1 "No Data"
			// sentinel otherwise (grid.js renders values outside 0-4 as No Data),
			// so a resolved-but-inactive validator doesn't paint every block missed.
			gridBlock := int(signState)
			if !cc.valInfo.Bonded {
				gridBlock = -1
			}
			cc.blocksResults = append([]int{gridBlock}, cc.blocksResults[:len(cc.blocksResults)-1]...)
			if signState < 3 && cc.valInfo.Bonded {
				warn := fmt.Sprintf("❌ %s missed block %d on %s", cc.valInfo.Moniker, bestHeight, cc.ChainId)
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
				// valInfo.Missed/Window are now bounded + sliding (cosmos-like), so the
				// dashboard uptime % and percentage_missed alert stay meaningful.
				cc.gnoWin.Push(isMiss)
				cc.valInfo.Missed = int64(cc.gnoWin.Missed())
				cc.valInfo.Window = int64(cc.gnoWin.Window())
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
					HealthyNodes: healthyNodes, ActiveAlerts: cc.activeAlerts, Height: bestHeight,
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
