package tenderduty

import (
	"fmt"
	"time"
)

// espressoPushGrid prepends a leader-duty outcome to the dashboard block grid.
// The grid holds the most recent duty slots (newest first), which is the same
// meaning it has for cosmos/gno: did the validator do its job for that slot.
func (cc *ChainConfig) espressoPushGrid(cell int) {
	if len(cc.blocksResults) == 0 {
		return
	}
	cc.blocksResults = append([]int{cell}, cc.blocksResults[:len(cc.blocksResults)-1]...)
}

// EspressoPollRun replaces the cosmos websocket / gno commit poller for Espresso
// chains. Every tick it reads:
//   - the chain head from ALL configured query endpoints (max height wins, so a
//     stale endpoint cannot freeze the height / suppress the stalled alarm),
//   - the validator node's Status API counters (leader duties assigned/missed),
//   - the node's current/last-voted view for a "not voting" signal.
//
// The counters are process-lifetime Prometheus counters: a drop means the node
// restarted, so the baseline is re-seeded instead of reporting a negative delta.
// Duty outcomes are appended to the grid and to the sliding duty window, which
// feeds the dashboard uptime % and the percentage_missed alert with real counts.
//
// The loop never requires the node API for liveness: without espresso_node_url
// the chain is still watched for stalls and node lag, only duty accounting is
// inactive (logged once at start).
func (cc *ChainConfig) EspressoPollRun() {
	windowSize := cc.EspressoDutyWindow
	if windowSize <= 0 {
		windowSize = espressoDefaultDutyWindow
	}
	cc.stateMux.Lock()
	if cc.espressoWin == nil {
		cc.espressoWin = newGnoWindow(windowSize)
	}
	cc.stateMux.Unlock()

	duties := cc.EspressoNodeURL != ""
	if duties {
		cc.stateMux.RLock()
		key := cc.espressoKey
		cc.stateMux.RUnlock()
		if key == "" {
			key = cc.EspressoStakingKey
		}
		if key == "" {
			l(fmt.Sprintf("🛑 %-12s no BLS staking key; leader-duty monitoring inactive", cc.ChainId))
			duties = false
		} else {
			l(fmt.Sprintf("⚙️ %-12s polling %d query node(s) and node %s for leader duties", cc.ChainId, len(cc.Nodes), cc.EspressoNodeURL))
		}
	}
	if !duties && cc.EspressoNodeURL == "" {
		l(fmt.Sprintf("⚠️ %-12s no espresso_node_url: chain liveness only, leader duties are not tracked", cc.ChainId))
	}

	noBlockSince := time.Now()
	tick := time.NewTicker(espressoPollInterval)
	defer tick.Stop()

	for {
		select {
		case <-td.ctx.Done():
			return
		case <-tick.C:
			// Chain head: poll-all, highest height wins.
			head := int64(-1)
			for _, n := range cc.Nodes {
				cc.stateMux.RLock()
				down, url := n.down, n.Url
				cc.stateMux.RUnlock()
				if down {
					continue
				}
				h, err := espressoFetchBlockHeight(url)
				if err != nil {
					continue
				}
				if h > head {
					head = h
				}
			}

			var st *espressoStatus
			var nodeErr error
			if duties {
				st, nodeErr = espressoFetchStatus(cc.EspressoNodeURL)
			}

			var (
				logMsgs  []string
				cells    []int
				advanced bool
				events   int
			)

			cc.stateMux.Lock()

			// Leader-duty accounting from the node counters.
			if st != nil && cc.valInfo != nil && (st.Assigned > 0 || st.Missed > 0) {
				switch {
				case !cc.espressoCountersValid:
					// First sample (or first after an upgrade): establish the
					// baseline; history before this process started is unknown.
					cc.espressoAssignedTotal = st.Assigned
					cc.espressoMissedTotal = st.Missed
					cc.espressoCountersValid = true
				case st.Assigned < cc.espressoAssignedTotal || st.Missed < cc.espressoMissedTotal:
					// Prometheus counters are process-lifetime: a drop means the
					// node restarted. Re-baseline instead of reporting a huge
					// negative or bogus delta (which would look like a recovery
					// followed by impossible misses).
					logMsgs = append(logMsgs, fmt.Sprintf("🔄 %-12s espresso counters reset (node restart); re-baselining duty counters", cc.ChainId))
					cc.espressoAssignedTotal = st.Assigned
					cc.espressoMissedTotal = st.Missed
				default:
					dAssigned := st.Assigned - cc.espressoAssignedTotal
					dMissed := st.Missed - cc.espressoMissedTotal
					if dMissed < 0 {
						dMissed = 0
					}
					if dMissed > dAssigned {
						dMissed = dAssigned
					}
					successes := dAssigned - dMissed
					cc.espressoAssignedTotal = st.Assigned
					cc.espressoMissedTotal = st.Missed
					if dAssigned > 0 {
						events++
						cc.espressoEpochAssigned += dAssigned
						cc.statTotalProps += float64(successes)
						cc.statTotalSigns += float64(successes)
						if dMissed > 0 {
							cc.espressoEpochMissed += dMissed
							cc.statTotalMiss += float64(dMissed)
							if successes > 0 {
								// A success broke the streak within this sample;
								// per-slot ordering is not observable at poll rate.
								cc.statConsecutiveMiss = 0
							} else {
								cc.statConsecutiveMiss += float64(dMissed)
							}
						} else if successes > 0 {
							cc.statConsecutiveMiss = 0
						}

						// Sliding duty window (real counts, so the dashboard uptime
						// % and percentage_missed are exact over the window).
						pushTotal := dAssigned
						if pushTotal > int64(windowSize) {
							pushTotal = int64(windowSize)
						}
						pushMissed := dMissed
						if pushMissed > pushTotal {
							pushMissed = pushTotal
						}
						pushOK := pushTotal - pushMissed
						for i := int64(0); i < pushMissed; i++ {
							cc.espressoWin.Push(true)
						}
						for i := int64(0); i < pushOK; i++ {
							cc.espressoWin.Push(false)
						}
						cc.valInfo.Missed = int64(cc.espressoWin.Missed())
						cc.valInfo.Window = int64(cc.espressoWin.Window())

						// Grid cells: exact while the tick saw few duties, sampled
						// proportionally after a long outage so 500 missed duties
						// do not flood the 512-cell ring with one poll.
						gridTotal := dAssigned
						if gridTotal > 32 {
							gridTotal = 32
						}
						gridMissed := dMissed
						if gridMissed > gridTotal {
							gridMissed = gridTotal
						}
						for i := int64(0); i < gridMissed; i++ {
							cells = append(cells, int(Statusmissed))
						}
						for i := int64(0); i < gridTotal-gridMissed; i++ {
							cells = append(cells, int(StatusProposed))
						}

						if dMissed > 0 && cc.valInfo.Bonded {
							warn := fmt.Sprintf("❌ %s missed leader duty on %s (epoch %d, %d this epoch, proposals %d/%d)",
								cc.valInfo.Moniker, cc.ChainId, cc.espressoEpoch, cc.espressoEpochMissed,
								cc.espressoEpochAssigned-cc.espressoEpochMissed, cc.espressoEpochAssigned)
							cc.lastError = time.Now().UTC().String() + " " + warn
							logMsgs = append(logMsgs, warn)
						}
					}
				}
			}

			if st != nil {
				lag := st.CurrentView - st.LastVotedView
				if lag < 0 {
					lag = 0
				}
				cc.espressoVoteLag = lag
			}

			// Chain height and the stall clock. A head below the last processed
			// height is only accepted after the idle window (re-genesis), exactly
			// like the gno poller: a stale head must never refresh lastBlockTime.
			switch {
			case head > cc.lastBlockNum:
				cc.lastBlockNum = head
				advanced = true
				cc.lastBlockTime = time.Now()
				cc.lastBlockAlarm = false
			case head > 0 && head < cc.lastBlockNum && time.Since(noBlockSince) > espressoPollIdleExit:
				cc.lastBlockNum = head
				advanced = true
				cc.lastBlockTime = time.Now()
				cc.lastBlockAlarm = false
				logMsgs = append(logMsgs, fmt.Sprintf("♻️ %-12s chain height reset to %d, resuming", cc.ChainId, head))
			}
			for _, c := range cells {
				cc.espressoPushGrid(c)
			}
			height := cc.lastBlockNum
			moniker := cc.valInfo.Moniker
			status := cc.statusUpdate()
			cc.stateMux.Unlock()

			for _, m := range logMsgs {
				l(m)
			}
			if advanced {
				noBlockSince = time.Now()
				if height%200 == 0 {
					l(fmt.Sprintf("🧊 %-12s block %d", cc.ChainId, height))
				}
			}

			if advanced || events > 0 {
				if td.EnableDash {
					td.updateChan <- status
				}
				if td.Prom {
					cc.stateMux.RLock()
					var props, miss, consec, signs float64
					if cc.valInfo != nil {
						props, miss, consec = cc.statTotalProps, cc.statTotalMiss, cc.statConsecutiveMiss
						signs = cc.statTotalSigns
					}
					unhealthy := 0
					for i := range cc.Nodes {
						if cc.Nodes[i].down {
							unhealthy++
						}
					}
					if cc.espressoNode != nil && cc.espressoNode.down {
						unhealthy++
					}
					lastBlock := time.Since(cc.lastBlockTime).Seconds()
					cc.stateMux.RUnlock()
					td.statsChan <- cc.mkUpdateWithMoniker(metricSigned, signs, "", moniker)
					td.statsChan <- cc.mkUpdateWithMoniker(metricProposed, props, "", moniker)
					td.statsChan <- cc.mkUpdateWithMoniker(metricMissed, miss, "", moniker)
					td.statsChan <- cc.mkUpdateWithMoniker(metricConsecutive, consec, "", moniker)
					td.statsChan <- cc.mkUpdateWithMoniker(metricLastBlockSeconds, lastBlock, "", moniker)
					td.statsChan <- cc.mkUpdateWithMoniker(metricUnealthyNodes, float64(unhealthy), "", moniker)
				}
			}
			if !advanced && nodeErr != nil && duties {
				// The node API is transiently unreadable; monitorHealth owns the
				// down alarm. Nothing to advance here.
				continue
			}
			if !advanced && time.Since(noBlockSince) > espressoPollIdleExit {
				l("🛑", cc.ChainId, "no new espresso blocks for", espressoPollIdleExit.String(), ", exiting poller")
				return
			}
		}
	}
}
