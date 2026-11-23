package tenderduty

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	dash "github.com/MictoNode/enchanted-tenderduty/td2/dashboard"
)

var td = &Config{}

func Run(configFile, stateFile, chainConfigDirectory string, password *string) error {
	var err error
	td, err = loadConfig(configFile, stateFile, chainConfigDirectory, password)
	if err != nil {
		return err
	}
	fatal, problems := validateConfig(td)
	for _, p := range problems {
		fmt.Println(p)
	}
	if fatal {
		log.Fatal("tenderduty the configuration is invalid, refusing to start")
	}
	log.Println("tenderduty config is valid, starting tenderduty with", len(td.Chains), "chains")

	defer td.cancel()

	go func() {
		for {
			select {
			case alert := <-td.alertChan:
				go func(msg *alertMsg) {
					var e error
					e = notifyPagerduty(msg)
					if e != nil {
						l(msg.chain, "error sending alert to pagerduty", e.Error())
					}
					e = notifyDiscord(msg)
					if e != nil {
						l(msg.chain, "error sending alert to discord", e.Error())
					}
					e = notifyTg(msg)
					if e != nil {
						l(msg.chain, "error sending alert to telegram", e.Error())
					}
					e = notifySlack(msg)
					if e != nil {
						l(msg.chain, "error sending alert to slack", e.Error())
					}
					e = notifyPushover(msg)
					if e != nil {
						l(msg.chain, "error sending alert to pushover", e.Error())
					}
				}(alert)
			case <-td.ctx.Done():
				return
			}
		}
	}()

	if td.EnableDash {
		dash.Content = staticContent
		go dash.Serve(td.Listen, td.updateChan, td.logChan, td.HideLogs)
		l("starting dashboard on", td.Listen)
	} else {
		go func() {
			for {
				<-td.updateChan
			}
		}()
	}
	if td.Prom {
		go prometheusExporter(td.ctx, td.statsChan)
	} else {
		go func() {
			for {
				<-td.statsChan
			}
		}()
	}

	// tenderduty health checks:
	if td.Healthcheck.Enabled {
		td.pingHealthcheck()
	}

	for k := range td.Chains {
		cc := td.Chains[k]

		go func(cc *ChainConfig, name string) {
			// alert worker
			go cc.watch()

			// node health checks:
			go func() {
				for {
					cc.monitorHealth(td.ctx, name)
				}
			}()

			// websocket subscription and occasional validator info refreshes
			for {
				e := cc.newRpc()
				if e != nil {
					l(cc.ChainId, e)
					time.Sleep(5 * time.Second)
					continue
				}
				e = cc.GetValInfo(true)
				if e != nil {
					l("🛑", cc.ChainId, e)
				}
				cc.WsRun()
				l(cc.ChainId, "🌀 websocket exited! Restarting monitoring")
				time.Sleep(5 * time.Second)
			}
		}(cc, k)
	}

	// attempt to save state on exit, only a best-effort ...
	saved := make(chan interface{})
	go saveOnExit(stateFile, saved)

	<-td.ctx.Done()
	<-saved

	return err
}

// stateSaveInterval is how often runtime state is persisted while running, so
// SIGKILL/OOM/power loss loses at most this much state.
var stateSaveInterval = 60 * time.Second

// snapshotState gathers the global runtime state and marshals it to JSON.
func snapshotState() ([]byte, error) {
	return snapshotStateOf(td)
}

// snapshotStateOf gathers c's runtime state and marshals it to JSON. The alarm
// cache is marshaled under alarms.notifyMux.RLock so concurrent writers can
// never be observed mid-update.
func snapshotStateOf(c *Config) ([]byte, error) {
	// Copy the chain set and release chainsMux before taking any per-chain
	// stateMux. watch() holds a chain's stateMux and then asks for
	// chainsMux.RLock (inside Config.alert), so waiting for stateMux while
	// holding chainsMux is a lock-order inversion that deadlocks both: the
	// periodic save (60s) would silently stop every alert, save and dashboard
	// update for the whole process.
	c.chainsMux.RLock()
	chains := make(map[string]*ChainConfig, len(c.Chains))
	for k, v := range c.Chains {
		chains[k] = v
	}
	c.chainsMux.RUnlock()

	blocks := make(map[string][]int)
	// only need to save counts if the dashboard exists
	if c.EnableDash {
		for k, v := range chains {
			v.stateMux.RLock()
			blocks[k] = append([]int(nil), v.blocksResults...)
			v.stateMux.RUnlock()
		}
	}
	nodesDown := make(map[string]map[string]time.Time)
	for k, v := range chains {
		v.stateMux.RLock()
		for _, node := range v.Nodes {
			if node.down {
				if nodesDown[k] == nil {
					nodesDown[k] = make(map[string]time.Time)
				}
				nodesDown[k][node.Url] = node.downSince
			}
		}
		v.stateMux.RUnlock()
	}
	gnoCounters := make(map[string]*gnoSavedStats)
	for k, v := range chains {
		v.stateMux.RLock()
		if v.Type == "gnoland" && v.valInfo != nil {
			gnoCounters[k] = &gnoSavedStats{
				TotalSigns: v.statTotalSigns, TotalProps: v.statTotalProps,
				TotalMiss: v.statTotalMiss, ConsecutiveMiss: v.statConsecutiveMiss,
				LastBlockNum: v.lastBlockNum, LastBlockTime: v.lastBlockTime,
			}
		}
		v.stateMux.RUnlock()
	}
	espressoCounters := make(map[string]*espressoSavedStats)
	for k, v := range chains {
		v.stateMux.RLock()
		if v.Type == "espresso" && v.valInfo != nil {
			es := &espressoSavedStats{
				TotalSigns: v.statTotalSigns, TotalProps: v.statTotalProps,
				TotalMiss: v.statTotalMiss, ConsecutiveMiss: v.statConsecutiveMiss,
				CountersValid: v.espressoCountersValid,
				AssignedTotal: v.espressoAssignedTotal, MissedTotal: v.espressoMissedTotal,
				Epoch: v.espressoEpoch, EpochAssigned: v.espressoEpochAssigned, EpochMissed: v.espressoEpochMissed,
				LastBlockNum: v.lastBlockNum, LastBlockTime: v.lastBlockTime,
			}
			if v.espressoWin != nil {
				es.Duties = v.espressoWin.Entries()
			}
			espressoCounters[k] = es
		}
		v.stateMux.RUnlock()
	}
	alarms.notifyMux.RLock()
	defer alarms.notifyMux.RUnlock()
	return json.Marshal(&savedState{
		Alarms:           alarms,
		Blocks:           blocks,
		NodesDown:        nodesDown,
		GnoCounters:      gnoCounters,
		EspressoCounters: espressoCounters,
	})
}

// writeStateFile writes b to stateFile atomically: the temp file lives in the
// same directory, so the rename is a same-filesystem atomic replace and a
// mid-write crash or disk-full never leaves a truncated state file.
func writeStateFile(stateFile string, b []byte) error {
	//#nosec -- variable specified on command line
	f, e := os.OpenFile(stateFile+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	removeTmp := func() {
		_ = f.Close()
		_ = os.Remove(stateFile + ".tmp")
	}
	if _, e = f.Write(b); e != nil {
		removeTmp()
		return e
	}
	if e = f.Sync(); e != nil {
		removeTmp()
		return e
	}
	if e = f.Close(); e != nil {
		_ = os.Remove(stateFile + ".tmp")
		return e
	}
	return os.Rename(stateFile+".tmp", stateFile)
}

func saveOnExit(stateFile string, saved chan interface{}) {
	quitting := make(chan os.Signal, 1)
	signal.Notify(quitting, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(quitting)

	ticker := time.NewTicker(stateSaveInterval)
	defer ticker.Stop()

	persist := func(exit bool) {
		if exit {
			log.Println("saving state...")
		}
		b, e := snapshotState()
		if e != nil {
			log.Println(e)
			return
		}
		if e := writeStateFile(stateFile, b); e != nil {
			log.Println(e)
			return
		}
		if exit {
			log.Println("tenderduty exiting.")
		}
	}
	for {
		select {
		case <-td.ctx.Done():
			persist(true)
			close(saved)
			return
		case <-quitting:
			persist(true)
			close(saved)
			td.cancel()
			return
		case <-ticker.C:
			persist(false)
		}
	}
}
