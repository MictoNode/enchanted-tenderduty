package tenderduty

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/types/bech32"

	dash "github.com/MictoNode/enchanted-tenderduty/td2/dashboard"
)

var hexAddrRe = regexp.MustCompile(`^[0-9a-f]{40}$`)

// canonicalAddr decodes a validator address given in hex (40 hex chars) or bech32
// form into its 20-byte consensus address. gno.land endpoints mix encodings
// (/validators may be bech32 g1..., /block proposer/precommits are hex), so all
// comparisons go through this to avoid silent match failures.
func canonicalAddr(s string) ([]byte, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return nil, errors.New("empty address")
	}
	if hexAddrRe.MatchString(s) {
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, err
		}
		if len(b) != 20 {
			return nil, fmt.Errorf("hex address not 20 bytes: %s", s)
		}
		return b, nil
	}
	if _, b, err := bech32.DecodeAndConvert(s); err == nil {
		if len(b) != 20 {
			return nil, fmt.Errorf("bech32 decoded address not 20 bytes: %s", s)
		}
		return b, nil
	}
	return nil, fmt.Errorf("not a hex or bech32 address: %s", s)
}

// addrEqual compares two addresses by canonical bytes; returns false on any decode error.
func addrEqual(a, b string) bool {
	ab, err := canonicalAddr(a)
	if err != nil {
		return false
	}
	bb, err := canonicalAddr(b)
	if err != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}

// gnoHTTPGet performs a GET with a timeout and returns the raw body.
func gnoHTTPGet(url string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// GnoStatusResult is the /status RPC response from a TM2 node.
type GnoStatusResult struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  struct {
		NodeInfo struct {
			Network string `json:"network"`
		} `json:"node_info"`
		SyncInfo struct {
			LatestBlockHeight string `json:"latest_block_height"`
			CatchingUp        bool   `json:"catching_up"`
		} `json:"sync_info"`
	} `json:"result"`
}

// GnoValidatorsResult is the /validators RPC response.
type GnoValidatorsResult struct {
	JSONRPC string `json:"jsonrpc"`
	Result  struct {
		Validators []GnoValidator `json:"validators"`
	} `json:"result"`
}

// GnoValidator is a single validator entry.
type GnoValidator struct {
	Address          string `json:"address"`
	VotingPower      string `json:"voting_power"`
	ProposerPriority string `json:"proposer_priority"`
}

func GnoGetStatus(rpcURL string) (chainID, height string, catchingUp bool, err error) {
	url := strings.TrimRight(rpcURL, "/") + "/status?"
	body, err := gnoHTTPGet(url, 10*time.Second)
	if err != nil {
		return "", "", false, fmt.Errorf("status request failed: %w", err)
	}
	var res GnoStatusResult
	if err = json.Unmarshal(body, &res); err != nil {
		return "", "", false, fmt.Errorf("status unmarshal failed: %w", err)
	}
	return res.Result.NodeInfo.Network, res.Result.SyncInfo.LatestBlockHeight, res.Result.SyncInfo.CatchingUp, nil
}

func GnoGetValidators(rpcURL string) ([]GnoValidator, error) {
	url := strings.TrimRight(rpcURL, "/") + "/validators?per_page=100"
	body, err := gnoHTTPGet(url, 10*time.Second)
	if err != nil {
		return nil, err
	}
	var res GnoValidatorsResult
	if err = json.Unmarshal(body, &res); err != nil {
		return nil, err
	}
	return res.Result.Validators, nil
}

// GnoIsValidatorActive reports whether `address` is in the active validator set.
func GnoIsValidatorActive(rpcURL, address string) (bool, error) {
	vals, err := GnoGetValidators(rpcURL)
	if err != nil {
		return false, err
	}
	for _, v := range vals {
		if addrEqual(v.Address, address) {
			return true, nil
		}
	}
	return false, nil
}

// GnoABCIResult is the abci_query (vm/qeval) response. TM2 nests data under response.ResponseBase.Data.
type GnoABCIResult struct {
	JSONRPC string `json:"jsonrpc"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error"`
	Result struct {
		Response struct {
			ResponseBase struct {
				Data string `json:"Data"`
			} `json:"ResponseBase"`
		} `json:"response"`
	} `json:"result"`
}

var (
	// valoperMonikerRex matches the first ("..." string) field of the Valoper
	// struct, which is the moniker.
	valoperMonikerRex = regexp.MustCompile(`\("([^"]*)" string\)`)
	// gnoAddrFieldRex matches struct fields carrying a gno address, e.g.
	// ("g1..." .uverse.address). The valopers Valoper struct has two: the
	// operator address (the realm key) and the signing/consensus address. They
	// are distinguished by value, not field position.
	gnoAddrFieldRex = regexp.MustCompile(`\("([A-Za-z0-9]+)" \.uverse\.address\)`)
	// gnoPubkeyRex matches the signing pubkey field, e.g. ("gpub1..." string).
	gnoPubkeyRex = regexp.MustCompile(`\("(gpub[A-Za-z0-9]+)" string\)`)
)

// GnoResolveValoper resolves a gnoland validator's moniker, signing (consensus)
// address, and pubkey by evaluating realmPath.GetByAddr("operatorAddr") via
// vm/qeval.
//
// gno.land has two addresses per validator: the operator address is the realm
// key (what gnoweb shows, what config valoper_address holds); the
// signing/consensus address is what /validators and /block precommits use. The
// signing address is embedded in the Valoper struct as the .uverse.address that
// is NOT the operator address, so callers pass the operator address and receive
// the signing address needed for consensus-level matching.
func GnoResolveValoper(rpcURL, realmPath, operatorAddr string) (moniker, signingAddr, pubkey string, err error) {
	expr := fmt.Sprintf(`%s.GetByAddr("%s")`, realmPath, operatorAddr)
	url := fmt.Sprintf(`%s/abci_query?path=%%22vm/qeval%%22&data=0x%s`,
		strings.TrimRight(rpcURL, "/"), hex.EncodeToString([]byte(expr)))
	body, e := gnoHTTPGet(url, 10*time.Second)
	if e != nil {
		return "", "", "", e
	}
	var res GnoABCIResult
	if e = json.Unmarshal(body, &res); e != nil {
		return "", "", "", e
	}
	if res.Error != nil {
		return "", "", "", fmt.Errorf("qeval error: %s", res.Error.Message)
	}
	data := res.Result.Response.ResponseBase.Data
	if data == "" {
		return "", "", "", errors.New("empty qeval response")
	}
	decoded, e := base64.StdEncoding.DecodeString(data)
	if e != nil {
		return "", "", "", e
	}
	s := string(decoded)

	if m := valoperMonikerRex.FindStringSubmatch(s); len(m) >= 2 {
		moniker = m[1]
	}
	// signing = the .uverse.address that is not the operator
	for _, m := range gnoAddrFieldRex.FindAllStringSubmatch(s, -1) {
		if len(m) >= 2 && !addrEqual(m[1], operatorAddr) {
			signingAddr = m[1]
			break
		}
	}
	if m := gnoPubkeyRex.FindStringSubmatch(s); len(m) >= 2 {
		pubkey = m[1]
	}
	if moniker == "" {
		return "", "", "", errors.New("could not parse moniker from qeval response")
	}
	if signingAddr == "" {
		return "", "", "", errors.New("could not parse signing address from qeval response")
	}
	return moniker, signingAddr, pubkey, nil
}

// gnoRPCUrl returns the first node URL that isn't marked down (fallback: first node).
func (cc *ChainConfig) gnoRPCUrl() string {
	for _, n := range cc.Nodes {
		if !n.down {
			return n.Url
		}
	}
	if len(cc.Nodes) > 0 {
		return cc.Nodes[0].Url
	}
	return ""
}

// GnoGetValInfo populates valInfo for a gnoland chain via HTTP RPC. No cosmos-sdk dependency.
//
// Config valoper_address holds the OPERATOR address (the realm key shown on
// gnoweb). The signing/consensus address used by /validators and /block is
// resolved from the valopers realm and stored in valInfo.Valcons; all
// consensus-level matching then goes through Valcons, not the operator address.
func (cc *ChainConfig) GnoGetValInfo(first bool) error {
	if cc.valInfo == nil {
		cc.valInfo = &ValInfo{}
	}
	rpcURL := cc.gnoRPCUrl()
	if rpcURL == "" {
		return errors.New("no RPC URL available")
	}

	realm := cc.GnoValopersRealm
	if realm == "" {
		realm = "gno.land/r/gnops/valopers"
	}

	moniker, signingAddr, _, rErr := GnoResolveValoper(rpcURL, realm, cc.ValAddress)
	if rErr != nil {
		if first {
			l(fmt.Sprintf("⚠️ could not resolve valoper %s from realm: %s", cc.ValAddress, rErr))
			l(fmt.Sprintf("⚠️ %s: signing address unknown — missed-block monitoring inactive (set valoper_address to the operator address shown on gnoweb)", cc.ChainId))
		}
		if len(cc.ValAddress) >= 20 {
			moniker = cc.ValAddress[:20] + "..."
		} else {
			moniker = cc.ValAddress
		}
	}
	cc.valInfo.Moniker = moniker
	cc.valInfo.Tombstoned = false // gno has no evidence/tombstone path
	cc.valInfo.Valcons = signingAddr
	if signingAddr != "" {
		cc.valInfo.Conspub = []byte(signingAddr)
	}

	// Active-set check uses the signing (consensus) address — that is what
	// /validators lists. Skipped when the signing address is unknown.
	bonded := false
	if signingAddr != "" {
		b, err := GnoIsValidatorActive(rpcURL, signingAddr)
		if err != nil {
			if first {
				l(fmt.Sprintf("⚠️ could not check active set for %s: %s", signingAddr, err))
			}
		} else {
			bonded = b
		}
	}
	// jailed detection: validator left the active set since last refresh
	if cc.lastValInfo != nil && cc.lastValInfo.Bonded && !bonded {
		cc.valInfo.Jailed = true
	} else if bonded {
		cc.valInfo.Jailed = false
	}
	cc.valInfo.Bonded = bonded

	if first {
		switch {
		case cc.valInfo.Bonded:
			l(fmt.Sprintf("⚙️ found %s (operator %s, signing %s) in active validator set", cc.valInfo.Moniker, cc.ValAddress, signingAddr))
		case signingAddr == "":
			l(fmt.Sprintf("❌ %s: signing address unresolved — not monitoring missed blocks", cc.ChainId))
		default:
			l(fmt.Sprintf("❌ %s (operator %s, signing %s) NOT in active validator set", cc.valInfo.Moniker, cc.ValAddress, signingAddr))
		}
	}
	return nil
}

// gnoNewRpc selects a working gno RPC endpoint and records it on cc.gnoRpcEndpoint.
func (cc *ChainConfig) gnoNewRpc() error {
	for _, ep := range cc.Nodes {
		cid, _, catching, err := GnoGetStatus(ep.Url)
		if err != nil {
			if !ep.down {
				ep.down = true
				ep.downSince = time.Now()
			}
			ep.lastMsg = fmt.Sprintf("❌ gno status failed %s: %s", ep.Url, err)
			l(ep.lastMsg)
			continue
		}
		if cid != cc.ChainId {
			if !ep.down {
				ep.down = true
				ep.downSince = time.Now()
			}
			ep.lastMsg = fmt.Sprintf("chain id %s on %s does not match %s", cid, ep.Url, cc.ChainId)
			l(ep.lastMsg)
			continue
		}
		if catching {
			if !ep.down {
				ep.down = true
				ep.downSince = time.Now()
			}
			ep.syncing = true
			ep.lastMsg = "🐢 gno node not synced: " + ep.Url
			l(ep.lastMsg)
			continue
		}
		ep.down = false
		ep.syncing = false
		ep.downSince = time.Unix(0, 0)
		ep.lastMsg = ""
		cc.gnoRpcEndpoint = ep.Url
		cc.noNodes = false
		return nil
	}
	if cc.PublicFallback {
		l("no public fallback available for gnoland chain", cc.ChainId)
	}
	cc.noNodes = true
	alarms.clearAll(cc.name)
	cc.lastError = "no usable RPC endpoints available for " + cc.ChainId
	if td.EnableDash {
		td.updateChan <- &dash.ChainStatus{
			MsgType: "status", Name: cc.name, ChainId: cc.ChainId, Network: cc.Network,
			Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded, Jailed: cc.valInfo.Jailed,
			Nodes: len(cc.Nodes), HealthyNodes: 0, ActiveAlerts: 1, LastError: cc.lastError,
			Blocks: cc.blocksResults,
		}
	}
	return errors.New("no usable endpoints available for " + cc.ChainId)
}

// gnoMonitorHealth periodically re-checks each gno node and refreshes valInfo.
func (cc *ChainConfig) gnoMonitorHealth(ctx context.Context, chainName string) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			head, _, _, _ := GnoGetStatus(cc.gnoRpcEndpoint)
			headH, _ := strconv.ParseInt(head, 10, 64)
			for _, node := range cc.Nodes {
				_, nh, catching, err := GnoGetStatus(node.Url)
				if err != nil {
					nodeLagCheck(cc, node, 0)
					alertNodeDown(cc, node, "down")
					continue
				}
				h, _ := strconv.ParseInt(nh, 10, 64)
				nodeLagCheck(cc, node, headH-h)
				if catching {
					node.syncing = true
					continue
				}
				if node.down {
					node.lastMsg = ""
					node.wasDown = true
				}
				node.down = false
				node.syncing = false
				node.downSince = time.Unix(0, 0)
				cc.noNodes = false
				if td.Prom {
					td.statsChan <- cc.mkUpdate(metricNodeDownSeconds, 0, node.Url)
				}
			}
			if cc.valInfo != nil {
				cc.lastValInfo = &ValInfo{
					Moniker: cc.valInfo.Moniker, Bonded: cc.valInfo.Bonded, Jailed: cc.valInfo.Jailed,
					Tombstoned: cc.valInfo.Tombstoned, Missed: cc.valInfo.Missed, Window: cc.valInfo.Window,
					Conspub: cc.valInfo.Conspub, Valcons: cc.valInfo.Valcons,
				}
			}
			if e := cc.GnoGetValInfo(false); e != nil {
				l("❓ refreshing gno validator info for", cc.ValAddress, e)
			}
		}
	}
}

// alertNodeDown marks a gno node down so the shared watch() loop fires the node-down alert.
func alertNodeDown(cc *ChainConfig, node *NodeConfig, msg string) {
	if !node.AlertIfDown {
		node.down = true
		return
	}
	if !node.down {
		node.down = true
		node.downSince = time.Now()
	}
	node.lastMsg = fmt.Sprintf("%-12s node %s is %s", cc.name, node.Url, msg)
	l("⚠️ " + node.lastMsg)
}
