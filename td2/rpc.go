package tenderduty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"

	dash "github.com/MictoNode/enchanted-tenderduty/td2/dashboard"
	rpchttp "github.com/tendermint/tendermint/rpc/client/http"
)

// newRpc sets up the rpc client used for monitoring. It will try nodes in order until a working node is found.
// it will also get some initial info on the validator's status.
func (cc *ChainConfig) newRpc() error {
	if cc.Type == "gnoland" {
		return cc.gnoNewRpc()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var anyWorking bool // if healthchecks are running, we will skip to the first known good node.
	for _, endpoint := range cc.Nodes {
		cc.stateMux.RLock()
		down := endpoint.down
		cc.stateMux.RUnlock()
		anyWorking = anyWorking || !down
	}
	// grab the first working endpoint
	tryUrl := func(u string) (msg string, down, syncing bool) {
		_, err := url.Parse(u)
		if err != nil {
			msg = fmt.Sprintf("❌ could not parse url %s: (%s) %s", cc.name, u, err)
			l(msg)
			down = true
			return
		}
		client, err := rpchttp.New(u, "/websocket")
		if err != nil {
			msg = fmt.Sprintf("❌ could not connect client for %s: (%s) %s", cc.name, u, err)
			l(msg)
			down = true
			return
		}
		status, err := client.Status(ctx)
		if err != nil {
			msg = fmt.Sprintf("❌ could not get status for %s: (%s) %s", cc.name, u, err)
			down = true
			l(msg)
			return
		}
		if status.NodeInfo.Network != cc.ChainId {
			msg = fmt.Sprintf("chain id %s on %s does not match, expected %s, skipping", status.NodeInfo.Network, u, cc.ChainId)
			down = true
			l(msg)
			return
		}
		if status.SyncInfo.CatchingUp {
			msg = fmt.Sprint("🐢 node is not synced, skipping ", u)
			syncing = true
			down = true
			l(msg)
			return
		}
		cc.stateMux.Lock()
		cc.client = client
		cc.noNodes = false
		cc.stateMux.Unlock()
		return
	}
	down := func(endpoint *NodeConfig, msg string) {
		cc.stateMux.Lock()
		defer cc.stateMux.Unlock()
		if !endpoint.down {
			endpoint.down = true
			endpoint.downSince = time.Now()
		}
		endpoint.lastMsg = msg
	}
	for _, endpoint := range cc.Nodes {
		cc.stateMux.RLock()
		endpointDown := endpoint.down
		cc.stateMux.RUnlock()
		if anyWorking && endpointDown {
			continue
		}
		if msg, failed, syncing := tryUrl(endpoint.Url); failed {
			endpoint.syncing = syncing
			down(endpoint, msg)
			continue
		}
		return nil
	}
	if cc.PublicFallback {
		if u, ok := getRegistryUrl(cc.ChainId); ok {
			node := guessPublicEndpoint(u)
			l(cc.ChainId, "⛑ attemtping to use public fallback node", node)
			if _, kk, _ := tryUrl(node); !kk {
				l(cc.ChainId, "⛑ connected to public endpoint", node)
				return nil
			}
		} else {
			l("could not find a public endpoint for", cc.ChainId)
		}
	}
	alarms.clearAll(cc.name)
	cc.stateMux.Lock()
	cc.noNodes = true
	cc.lastError = "no usable RPC endpoints available for " + cc.ChainId
	status := &dash.ChainStatus{
		MsgType: "status",
		Name:    cc.name,
		ChainId: cc.ChainId, Network: cc.Network,
		Nodes:        len(cc.Nodes),
		HealthyNodes: 0,
		ActiveAlerts: 1,
		Height:       0,
		LastError:    cc.lastError,
		Blocks:       append([]int(nil), cc.blocksResults...),
	}
	if cc.valInfo != nil {
		status.Moniker = cc.valInfo.Moniker
		status.Bonded = cc.valInfo.Bonded
		status.Jailed = cc.valInfo.Jailed
		status.Tombstoned = cc.valInfo.Tombstoned
		status.Missed = cc.valInfo.Missed
		status.Window = cc.valInfo.Window
	}
	cc.stateMux.Unlock()
	if td.EnableDash {
		td.updateChan <- status
	}
	return errors.New("no usable endpoints available for " + cc.ChainId)
}

// nodeLagCheck feeds the node-lag gauge and fires an alert when a node falls
// more than td.NodeLagAlertBlocks behind the head (0 disables alerting).
func nodeLagCheck(cc *ChainConfig, node *NodeConfig, lag int64) {
	if lag < 0 {
		lag = 0
	}
	if td.Prom {
		td.statsChan <- cc.mkUpdate(metricNodeLagSeconds, float64(lag), node.Url)
	}
	if td.NodeLagAlertBlocks > 0 && lag > int64(td.NodeLagAlertBlocks) && node.AlertIfDown {
		id := node.Url + "lag"
		td.alert(cc.name, fmt.Sprintf("RPC node %s is %d blocks behind on %s", node.Url, lag, cc.ChainId), td.NodeDownSeverity, false, &id)
	}
}

func (cc *ChainConfig) monitorHealth(ctx context.Context, chainName string) {
	if cc.Type == "gnoland" {
		cc.gnoMonitorHealth(ctx, chainName)
		return
	}
	tick := time.NewTicker(time.Minute)
	cc.stateMux.RLock()
	clientNil := cc.client == nil
	cc.stateMux.RUnlock()
	if clientNil {
		_ = cc.newRpc()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			var err error
			for _, node := range cc.Nodes {
				go func(node *NodeConfig) {
					alert := func(msg string) {
						cc.stateMux.Lock()
						node.lastMsg = fmt.Sprintf("%-12s node %s is %s", chainName, node.Url, msg)
						if !node.AlertIfDown {
							// even if we aren't alerting, we want to display the status in the dashboard.
							node.down = true
							cc.stateMux.Unlock()
							return
						}
						if !node.down {
							node.down = true
							node.downSince = time.Now()
						}
						downSince := node.downSince
						lastMsg := node.lastMsg
						cc.stateMux.Unlock()
						if td.Prom {
							td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, time.Since(downSince).Seconds(), node.Url)
						}
						l("⚠️ " + lastMsg)
					}
					c, e := rpchttp.New(node.Url, "/websocket")
					if e != nil {
						alert(e.Error())
					}
					cwt, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					status, e := c.Status(cwt)
					cancel()
					if e != nil {
						alert("down")
						return
					}
					if status.NodeInfo.Network != cc.ChainId {
						alert("on the wrong network")
						return
					}
					if status.SyncInfo.CatchingUp {
						alert("not synced")
						cc.stateMux.Lock()
						node.syncing = true
						cc.stateMux.Unlock()
						return
					}

					// lag detection against the head we know from the websocket stream
					cc.stateMux.RLock()
					lastBlockNum := cc.lastBlockNum
					cc.stateMux.RUnlock()
					nodeLagCheck(cc, node, lastBlockNum-status.SyncInfo.LatestBlockHeight)

					// node's OK, clear the note
					cc.stateMux.Lock()
					if node.down {
						node.lastMsg = ""
						node.wasDown = true
					}
					node.down = false
					node.syncing = false
					node.downSince = time.Unix(0, 0)
					cc.noNodes = false
					cc.stateMux.Unlock()
					td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, 0, node.Url)
					l(fmt.Sprintf("🟢 %-12s node %s is healthy", chainName, node.Url))
				}(node)
			}

			cc.stateMux.RLock()
			clientNil = cc.client == nil
			cc.stateMux.RUnlock()
			if clientNil {
				e := cc.newRpc()
				if e != nil {
					l("💥", cc.ChainId, e)
				}
			}
			cc.stateMux.Lock()
			if cc.valInfo != nil {
				cc.lastValInfo = &ValInfo{
					Moniker:    cc.valInfo.Moniker,
					Bonded:     cc.valInfo.Bonded,
					Jailed:     cc.valInfo.Jailed,
					Tombstoned: cc.valInfo.Tombstoned,
					Missed:     cc.valInfo.Missed,
					Window:     cc.valInfo.Window,
					Conspub:    cc.valInfo.Conspub,
					Valcons:    cc.valInfo.Valcons,
				}
			}
			cc.stateMux.Unlock()
			err = cc.GetValInfo(false)
			if err != nil {
				l("❓ refreshing signing info for", cc.ValAddress, err)
			}
		}
	}
}

func (c *Config) pingHealthcheck() {
	if !c.Healthcheck.Enabled {
		return
	}

	ticker := time.NewTicker(c.Healthcheck.PingRate * time.Second)

	go func() {
		for range ticker.C {
			_, err := http.Get(c.Healthcheck.PingURL)
			if err != nil {
				l(fmt.Sprintf("❌ Failed to ping healthcheck URL: %s", err.Error()))
			} else {
				l(fmt.Sprintf("🏓 Successfully pinged healthcheck URL: %s", c.Healthcheck.PingURL))
			}
		}
	}()
}

// endpointRex matches the first a tag's hostname and port if present.
var endpointRex = regexp.MustCompile(`//([^/:]+)(:\d+)?`)

// guessPublicEndpoint attempts to deal with a shortcoming in the tendermint RPC client that doesn't allow path prefixes.
// The cosmos.directory requires them. This is a workaround to get the actual URL for the server behind their proxy.
// The RPC base URL will return links endpoints, and we can parse this to guess the original URL.
func guessPublicEndpoint(u string) string {
	resp, err := http.Get(u + "/")
	if err != nil {
		return u
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return u
	}
	_ = resp.Body.Close()
	matches := endpointRex.FindStringSubmatch(string(b))
	if len(matches) < 2 {
		// didn't work
		return u
	}
	proto := "https://"
	port := ":443"
	// will be 3 elements if there is a port no port means listening on https
	if len(matches) == 3 && matches[2] != "" && matches[2] != ":443" {
		proto = "http://"
		port = matches[2]
	}
	return proto + matches[1] + port
}
