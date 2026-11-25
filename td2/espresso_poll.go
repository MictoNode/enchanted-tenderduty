package tenderduty

import (
	"fmt"
	"strings"
	"time"
)

// espressoPrependGrid inserts one tick's new cells at the front of the grid,
// newest first, matching per-cell prepends without allocating a slice per cell
// (a 512-cell catch-up would otherwise allocate ~2MB and copy 262k ints).
func (cc *ChainConfig) espressoPrependGrid(cells []int) {
	n := len(cc.blocksResults)
	if n == 0 || len(cells) == 0 {
		return
	}
	if len(cells) >= n {
		tail := cells[len(cells)-n:]
		for i := 0; i < n; i++ {
			cc.blocksResults[i] = tail[n-1-i]
		}
		return
	}
	// Shift the old cells right by len(cells) (overlapping copy is safe), then
	// write the new ones reversed into the freed front.
	copy(cc.blocksResults[len(cells):], cc.blocksResults[:n-len(cells)])
	for i, c := range cells {
		cc.blocksResults[len(cells)-1-i] = c
	}
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
	partEvery := espressoParticipationEveryTicks
	if partEvery < 1 {
		partEvery = 1
	}
	cc.stateMux.Lock()
	if cc.espressoWin == nil {
		cc.espressoWin = newGnoWindow(windowSize)
	}
	cc.stateMux.Unlock()

	if cc.EspressoNodeURL == "" {
		l(fmt.Sprintf("⚠️ %-12s no espresso_node_url: chain liveness only, leader duties are not tracked", cc.ChainId))
	} else {
		cc.stateMux.RLock()
		key := cc.espressoKey
		mismatch := cc.espressoKeyMismatch
		cc.stateMux.RUnlock()
		if key == "" {
			key = cc.EspressoStakingKey
		}
		switch {
		case mismatch:
			l(fmt.Sprintf("🛑 %-12s node %s is not this validator (BLS key mismatch); leader-duty monitoring stays off until the config is fixed", cc.ChainId, cc.EspressoNodeURL))
		case key == "":
			// Not fatal: the health tick resolves the key from /v1/status/keys and
			// a later tick starts duty polling. Latching this off would disable
			// duty monitoring for the whole process if the node was briefly down
			// when the poller started.
			l(fmt.Sprintf("⚠️ %-12s no BLS staking key yet; leader-duty polling starts once the node answers /v1/status/keys", cc.ChainId))
		default:
			l(fmt.Sprintf("⚙️ %-12s polling %d query node(s) and node %s for leader duties", cc.ChainId, len(cc.Nodes), cc.EspressoNodeURL))
		}
	}

	noBlockSince := time.Now()
	tickN := 0
	tick := time.NewTicker(espressoPollInterval)
	defer tick.Stop()

	for {
		select {
		case <-td.ctx.Done():
			return
		case <-tick.C:
			tickN++
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
			cc.stateMux.RLock()
			key := cc.espressoKey
			mismatch := cc.espressoKeyMismatch
			cc.stateMux.RUnlock()
			if key == "" {
				key = cc.EspressoStakingKey
			}
			duties := cc.EspressoNodeURL != "" && key != "" && !mismatch
			if duties && tickN%espressoMetricsEveryTicks == 1 {
				st, nodeErr = espressoFetchStatus(cc.EspressoNodeURL)
				if st != nil && st.Moniker != "" {
					cc.stateMux.Lock()
					cc.espressoMoniker = st.Moniker
					cc.stateMux.Unlock()
				}
			}

			// Chain-side epoch truth. Primary source: the staking API's exact
			// per-validator counters (keyed by the node's L1 account) — the same
			// numbers stake.espresso.network shows. Fallback: the query service's
			// participation ratio in lowest terms (one good answer is enough).
			var part map[string]float64
			var chainTally *espressoChainTally
			var partEpoch int64
			if duties && (tickN-1)%partEvery == 0 {
				cc.stateMux.RLock()
				partEpoch = cc.espressoEpoch
				account := cc.espressoAccount
				stakingAPI := cc.EspressoStakingAPI
				cc.stateMux.RUnlock()
				if partEpoch > 0 {
					if stakingAPI != "" && account != "" {
						if na, err := espressoFetchNodesActive(stakingAPI); err == nil && na.EspressoBlock.Epoch == partEpoch {
							for _, n := range na.Nodes {
								if !strings.EqualFold(n.Address, account) {
									continue
								}
								slots := n.Slots
								if slots < 0 {
									slots = 0
								}
								missed := slots - n.Proposals
								if missed < 0 {
									missed = 0
								}
								chainTally = &espressoChainTally{slots: slots, missed: missed}
								break
							}
						}
					}
					if chainTally == nil {
						for _, n := range cc.Nodes {
							cc.stateMux.RLock()
							down, url := n.down, n.Url
							cc.stateMux.RUnlock()
							if down {
								continue
							}
							m, err := espressoFetchParticipation(url, partEpoch)
							if err != nil || len(m) == 0 {
								continue
							}
							part = m
							break
						}
					}
				}
			}

			var (
				logMsgs      []string
				cells        []int
				missCells    []int
				proposedCell int
				advanced     bool
				events       int
			)

			cc.stateMux.Lock()

			// A served metrics snapshot refreshes the vote-lag signal even before
			// the first leader duty (Assigned==0): gating this on the duty
			// counters would make "not voting" dead until the validator happens to
			// be assigned a slot (hours at a small stake share).
			if st != nil && st.CurrentView > 0 {
				cc.espressoMetricsValid = true
				lag := st.CurrentView - st.LastVotedView
				if lag < 0 {
					lag = 0
				}
				cc.espressoVoteLag = lag
			}

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
					// The leader-duty histogram for an abandoned view is observed
					// one view after the timeout counter, so a genuine miss can
					// briefly lead the assigned count. Clamp only the success side:
					// capping dMissed at dAssigned and then advancing the baseline
					// would drop that miss forever.
					successes := dAssigned - dMissed
					if successes < 0 {
						successes = 0
					}
					// Successful leader duties produced blocks: paint that many of
					// this tick's block cells green (proposed) instead of signed.
					if successes > 0 {
						proposedCell = int(successes)
						if proposedCell > 32 {
							proposedCell = 32
						}
					}
					cc.espressoAssignedTotal = st.Assigned
					cc.espressoMissedTotal = st.Missed
					if dAssigned > 0 || dMissed > 0 {
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
						if cc.espressoEpochAssigned < cc.espressoEpochMissed {
							// Miss-led sample: keep the displayed tally sane until the
							// lagging assigned counter catches up.
							cc.espressoEpochAssigned = cc.espressoEpochMissed
						}
						proposals := cc.espressoEpochAssigned - cc.espressoEpochMissed
						if proposals < 0 {
							proposals = 0
						}

						// Sliding duty window (real counts, so the dashboard uptime
						// % and percentage_missed are exact over the window).
						remaining := int64(windowSize)
						missPush := dMissed
						if missPush > remaining {
							missPush = remaining
						}
						okPush := successes
						if okPush > remaining-missPush {
							okPush = remaining - missPush
						}
						for i := int64(0); i < missPush; i++ {
							cc.espressoWin.Push(true)
						}
						for i := int64(0); i < okPush; i++ {
							cc.espressoWin.Push(false)
						}
						cc.valInfo.Missed = int64(cc.espressoWin.Missed())
						cc.valInfo.Window = int64(cc.espressoWin.Window())
						// Local counter-only tally: the chain cross-check subtracts
						// this before crediting misses the counters could not see.
						cc.espressoEpochSeenSlots += dAssigned
						cc.espressoEpochSeenMissed += dMissed

						// Grid cells for missed duties. Decided blocks are added
						// separately below, so the grid keeps flowing at block rate
						// (a 0.5% stake share only gets a leader duty every ~90s) and
						// a missed duty stands out as an orange cell in that stream.
						gridMissed := dMissed
						if gridMissed > 32 {
							gridMissed = 32
						}
						for i := int64(0); i < gridMissed; i++ {
							missCells = append(missCells, int(Statusmissed))
						}

						if dMissed > 0 && cc.valInfo.Bonded {
							warn := fmt.Sprintf("❌ %s missed leader duty on %s (epoch %d, %d this epoch, proposals %d/%d)",
								cc.valInfo.Moniker, cc.ChainId, cc.espressoEpoch, cc.espressoEpochMissed,
								proposals, cc.espressoEpochAssigned)
							cc.lastError = time.Now().UTC().String() + " " + warn
							logMsgs = append(logMsgs, warn)
						} else if successes > 0 && cc.valInfo.Bonded {
							// Low-stake validators get a duty only every ~1-2 minutes;
							// one line per duty makes the poller's progress visible in
							// the journal (the dashboard grid alone moves too slowly).
							logMsgs = append(logMsgs, fmt.Sprintf("✅ %s proposed leader duty on %s (epoch %d, proposals %d/%d)",
								cc.valInfo.Moniker, cc.ChainId, cc.espressoEpoch, proposals, cc.espressoEpochAssigned))
						}
					}
				}
			}

			// Chain-side credit: the node's process-lifetime counters miss a leader
			// slot that expired while it was down for an upgrade (and one whose
			// proposal never reached quorum), so the chain truth — exact counts from
			// the staking API, or the participation-ratio fallback — repairs the
			// epoch tally, the duty window and the consecutive-miss streak.
			if chainTally != nil && partEpoch == cc.espressoEpoch {
				if msg, credited := cc.applyEspressoChainCounts(chainTally.slots, chainTally.missed); credited {
					if msg != "" {
						logMsgs = append(logMsgs, msg)
					}
					events++
				}
			} else if part != nil && partEpoch == cc.espressoEpoch {
				ccKey := cc.espressoKey
				if ccKey == "" {
					ccKey = cc.EspressoStakingKey
				}
				if r, ok := part[ccKey]; ok {
					if msg, credited := cc.applyEspressoParticipation(r); credited {
						if msg != "" {
							logMsgs = append(logMsgs, msg)
						}
						events++
					}
				}
			}

			// Chain height and the stall clock. A head below the last processed
			// height is only accepted after the idle window (re-genesis), exactly
			// like the gno poller: a stale head must never refresh lastBlockTime.
			switch {
			case head > cc.lastBlockNum:
				// Every decided block gets a cell, so the grid moves at block rate
				// like the cosmos/gno grids. The very first sample only seeds the
				// height: backfilling from 0 would paint a wall of cells for blocks
				// that were never observed by this process.
				if cc.lastBlockNum > 0 {
					blockCell := int(StatusSigned)
					if !duties {
						// No node API: our participation is unknown, so do not paint
						// the new blocks as signed by us.
						blockCell = -1
					}
					delta := head - cc.lastBlockNum
					if delta > int64(showBLocks) {
						delta = int64(showBLocks)
					}
					for i := int64(0); i < delta; i++ {
						cells = append(cells, blockCell)
					}
				}
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
			// Missed duties are prepended last so they land in front of the block
			// cells from the same tick and stay visible for a full grid cycle.
			cells = append(cells, missCells...)
			cc.espressoPrependGrid(cells)
			// Successful leader duties produced blocks. The counters do not say
			// which block, so paint the newest signed cell(s) green (proposed);
			// doing it after the prepend puts the green cell at the leading edge.
			// Miss cells sit in front, so skip them while looking for signed cells.
			painted := 0
			for i := 0; i < len(cc.blocksResults) && painted < proposedCell; i++ {
				if cc.blocksResults[i] == int(StatusSigned) {
					cc.blocksResults[i] = int(StatusProposed)
					painted++
				}
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

// espressoChainTally is one chain-side epoch snapshot: leader slots assigned
// and missed (slots - proposals).
type espressoChainTally struct {
	slots  int64
	missed int64
}

// applyEspressoChainCounts credits chain-derived epoch totals into the
// dashboard tally, the sliding duty window and the consecutive-miss streak.
// Only events the node's process-lifetime counters could not see are credited
// (chain total minus the local seen tally), so re-polling an epoch never double
// counts and switching between the staking API and the participation-ratio
// fallback stays idempotent. Caller must hold stateMux.
func (cc *ChainConfig) applyEspressoChainCounts(slots, missed int64) (msg string, credited bool) {
	if cc.valInfo == nil || cc.espressoWin == nil || slots < 0 || missed < 0 {
		return "", false
	}
	if missed > slots {
		missed = slots
	}
	changed := slots != cc.espressoEpochAssigned || missed != cc.espressoEpochMissed
	// First chain observation of the epoch (no local event credited yet) is a
	// silent baseline: adopt the dashboard truth, but do not alert for misses
	// that predate our watch (same ground rule as the counter path).
	firstSample := cc.espressoEpochSeenSlots == 0 && cc.espressoEpochSeenMissed == 0
	// Adopt the chain tally even when it only confirms the local one: this
	// repairs the assigned count after a mid-epoch counter reset (restart).
	cc.espressoEpochAssigned = slots
	cc.espressoEpochMissed = missed
	dSlots := slots - cc.espressoEpochSeenSlots
	dMissed := missed - cc.espressoEpochSeenMissed
	if cc.espressoEpochSeenSlots < slots {
		cc.espressoEpochSeenSlots = slots
	}
	if cc.espressoEpochSeenMissed < missed {
		cc.espressoEpochSeenMissed = missed
	}
	if dSlots < 0 {
		dSlots = 0
	}
	if dMissed < 0 {
		dMissed = 0
	}
	if dSlots < dMissed {
		dSlots = dMissed
	}
	if dSlots == 0 && dMissed == 0 {
		return "", changed
	}
	for i := int64(0); i < dMissed; i++ {
		cc.espressoWin.Push(true)
	}
	for i := int64(0); i < dSlots-dMissed; i++ {
		cc.espressoWin.Push(false)
	}
	// Streak semantics: new misses extend the streak; a batch with proposals and
	// no misses means the duty is being met again (streak 0). The first sample of
	// an epoch is baseline-only.
	switch {
	case firstSample:
		cc.statConsecutiveMiss = 0
	case dMissed > 0:
		cc.statConsecutiveMiss += float64(dMissed)
	case dSlots > 0:
		cc.statConsecutiveMiss = 0
	}
	cc.valInfo.Missed = int64(cc.espressoWin.Missed())
	cc.valInfo.Window = int64(cc.espressoWin.Window())
	if dMissed == 0 {
		return "", true
	}
	return fmt.Sprintf("⚖️ %-12s chain participation %d/%d (epoch %d): %d missed duty(s) invisible to the node counters; credited",
		cc.ChainId, slots-missed, slots, cc.espressoEpoch, dMissed), true
}

// applyEspressoParticipation is the fallback path: the query service reports
// proposals/slots in lowest terms, so q is scaled by k (the nearest multiple of
// the local assignment tally) before the shared credit runs. Caller must hold
// stateMux.
func (cc *ChainConfig) applyEspressoParticipation(r float64) (msg string, credited bool) {
	p, q, ok := espressoRatioFraction(r)
	if !ok || q <= 0 || cc.valInfo == nil || cc.espressoWin == nil {
		return "", false
	}
	// A perfect ratio with no locally observed slot only says "nothing missed
	// yet"; adopting it would fabricate a 1-slot epoch at epoch start.
	if p == q && cc.espressoEpochSeenSlots == 0 {
		return "", false
	}
	k := int64(1)
	if base := cc.espressoEpochAssigned; base >= q {
		k = (base + q/2) / q
		if k < 1 {
			k = 1
		}
	}
	slots := k * q
	missed := k * (q - p)
	if slots < missed {
		return "", false
	}
	return cc.applyEspressoChainCounts(slots, missed)
}
