package tenderduty

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	dash "github.com/MictoNode/enchanted-tenderduty/td2/dashboard"
	"github.com/go-yaml/yaml"
	rpchttp "github.com/tendermint/tendermint/rpc/client/http"
)

const (
	showBLocks = 512
	staleHours = 24
)

// Config holds both the settings for tenderduty to monitor and state information while running.
type Config struct {
	alertChan  chan *alertMsg // channel used for outgoing notifications
	updateChan chan *dash.ChainStatus
	logChan    chan dash.LogMessage
	statsChan  chan *promUpdate
	ctx        context.Context
	cancel     context.CancelFunc
	alarms     *alarmCache

	// EnableDash enables the web dashboard
	EnableDash bool `yaml:"enable_dashboard"`
	// Listen is the URL for the dashboard to listen on, must be a valid/parsable URL
	Listen string `yaml:"listen_port"`
	// HideLogs controls whether logs are sent to the dashboard. It will also suppress many alarm details.
	// This is useful if the dashboard will be public.
	HideLogs bool `yaml:"hide_logs"`

	// NodeDownMin controls how long we wait before sending an alert that a node is not responding or has
	// fallen behind.
	NodeDownMin int `yaml:"node_down_alert_minutes"`
	// NodeDownSeverity controls the Pagerduty severity when notifying if a node is down.
	NodeDownSeverity string `yaml:"node_down_alert_severity"`

	// NodeLagAlertBlocks, if > 0, alerts when an RPC node is more than this many blocks behind the head.
	NodeLagAlertBlocks int `yaml:"node_lag_alert_blocks"`

	// Prom controls if the prometheus exporter is enabled.
	Prom bool `yaml:"prometheus_enabled"`
	// PrometheusListenPort is the port number used by the prometheus web server
	PrometheusListenPort int `yaml:"prometheus_listen_port"`

	// Pagerduty configuration values
	Pagerduty PDConfig `yaml:"pagerduty"`
	// Discord webhook information
	Discord DiscordConfig `yaml:"discord"`
	// Telegram api information
	Telegram TeleConfig `yaml:"telegram"`
	// Slack webhook information
	Slack SlackConfig `yaml:"slack"`
	// Pushover push-notification information
	Pushover PushoverConfig `yaml:"pushover"`
	// Healthcheck information
	Healthcheck HealthcheckConfig `yaml:"healthcheck"`

	chainsMux sync.RWMutex // prevents concurrent map access for Chains
	// Chains has settings for each validator to monitor. The map's name does not need to match the chain-id.
	Chains map[string]*ChainConfig `yaml:"chains"`
}

// savedState is dumped to a JSON file at exit time, and is loaded at start. If successful it will prevent
// duplicate alerts, and will show old blocks in the dashboard.
type savedState struct {
	Alarms           *alarmCache                     `json:"alarms"`
	Blocks           map[string][]int                `json:"blocks"`
	NodesDown        map[string]map[string]time.Time `json:"nodes_down"`
	GnoCounters      map[string]*gnoSavedStats       `json:"gno_counters,omitempty"`
	EspressoCounters map[string]*espressoSavedStats  `json:"espresso_counters,omitempty"`
}

// espressoSavedStats persists the espresso provider across restart. The two
// node counters (Assigned/Missed) are process-lifetime on the node, so they are
// re-baselined after a node restart; the persisted copies only exist so a
// tenderduty restart resumes with the last observed values instead of
// treating every historic duty as new. Duties is the sliding window itself
// (exact counts, so percentage_missed stays meaningful right after a restart
// instead of warming up for hours at a small stake share).
type espressoSavedStats struct {
	TotalSigns      float64 `json:"total_signs"`
	TotalProps      float64 `json:"total_props"`
	TotalMiss       float64 `json:"total_miss"`
	ConsecutiveMiss float64 `json:"consecutive_miss"`

	CountersValid bool  `json:"counters_valid,omitempty"`
	AssignedTotal int64 `json:"assigned_total,omitempty"`
	MissedTotal   int64 `json:"missed_total,omitempty"`

	Epoch         int64 `json:"epoch,omitempty"`
	EpochAssigned int64 `json:"epoch_assigned,omitempty"`
	EpochMissed   int64 `json:"epoch_missed,omitempty"`

	LastBlockNum  int64     `json:"last_block_num,omitempty"`
	LastBlockTime time.Time `json:"last_block_time,omitempty"`

	Duties []bool `json:"duties,omitempty"`
}

// gnoSavedStats persists gnoland cumulative counters (prometheus gauges +
// consecutive-miss streak) across restart. valInfo.Missed/Window are NOT
// persisted — they come from the ephemeral gnoWin sliding ring (warm-up).
type gnoSavedStats struct {
	TotalSigns      float64 `json:"total_signs"`
	TotalProps      float64 `json:"total_props"`
	TotalMiss       float64 `json:"total_miss"`
	ConsecutiveMiss float64 `json:"consecutive_miss"`
	// LastBlockNum and LastBlockTime let a restarted process resume where it
	// stopped. Without them a restart during a chain halt re-evaluates the
	// frozen head as fresh blocks (refreshing lastBlockTime and masking the
	// stalled alarm) and every restart re-backfills the catch-up window.
	LastBlockNum  int64     `json:"last_block_num,omitempty"`
	LastBlockTime time.Time `json:"last_block_time,omitempty"`
}

// ChainConfig represents a validator to be monitored on a chain, it is somewhat of a misnomer since multiple
// validators can be monitored on a single chain.
type ChainConfig struct {
	stateMux       sync.RWMutex
	name           string
	wsclient       *TmConn       // custom websocket client to work around wss:// bugs in tendermint
	client         *rpchttp.HTTP // legit tendermint client
	noNodes        bool          // tracks if all nodes are down
	valInfo        *ValInfo      // recent validator state, only refreshed every few minutes
	lastValInfo    *ValInfo      // use for detecting newly-jailed/tombstone
	blocksResults  []int
	lastError      string
	lastBlockTime  time.Time
	lastBlockAlarm bool
	lastBlockNum   int64
	gnoRpcEndpoint string     // gno: the working RPC base URL chosen by gnoNewRpc
	gnoWin         *gnoWindow // gno: sliding signing-health window (gno_signed_blocks_window)
	// gnoBondMissStreak counts consecutive definitive not-bonded /validators
	// ticks. GnoGetValInfo flips Bonded=false only after gnoBondedConfirmTicks
	// consecutive verdicts, so one transient tick cannot paint the grid with
	// "No Data" and raise a false "jailed" critical (2026-08-10 prod incident
	// class, same as 2026-07-02/2026-07-28). Guarded by stateMux.
	gnoBondMissStreak int
	activeAlerts      int

	// espresso provider runtime state (all guarded by stateMux).
	espressoNode           *NodeConfig // the validator's own Status API, health-checked separately from the query nodes
	espressoWin            *gnoWindow  // sliding leader-duty window (dashboard uptime %, percentage alarm)
	espressoKey            string      // resolved BLS staking public key
	espressoEpoch          int64       // current epoch reported by the stake table
	espressoEpochAssigned  int64       // leader duties assigned in this epoch (from counter deltas)
	espressoEpochMissed    int64       // leader duties missed in this epoch
	espressoCountersValid  bool        // false until the first counter sample seeds the baseline
	espressoAssignedTotal  int64       // last observed consensus_view_duration_as_leader_count
	espressoMissedTotal    int64       // last observed consensus_number_of_timeouts_as_leader
	espressoVoteLag        int64       // current_view - last_voted_view at the last health tick
	espressoBondMissStreak int         // consecutive definitive not-in-active-set stake table answers

	statTotalSigns      float64
	statTotalProps      float64
	statTotalMiss       float64
	statPrevoteMiss     float64
	statPrecommitMiss   float64
	statConsecutiveMiss float64

	// ChainId is used to ensure any endpoints contacted claim to be on the correct chain. This is a weak verification,
	// no light client validation is performed, so caution is advised when using public endpoints.
	ChainId string `yaml:"chain_id"`
	// Network classifies the chain for the dashboard's mainnet/testnet tabs: "mainnet" or "testnet".
	// Optional — when empty the chain appears only in the "all" tab. Free-form otherwise.
	Network string `yaml:"network"`
	// ValAddress is the validator operator address to be monitored. Tenderduty v1 required the consensus address,
	// this is no longer needed. The operator address is much easier to find in explorers etc.
	ValAddress string `yaml:"valoper_address"`
	// ValconsOverride allows skipping the lookup of the consensus public key and setting it directly.
	ValconsOverride string `yaml:"valcons_override"`
	// ExtraInfo will be appended to the alert data. This is useful for pagerduty because multiple tenderduty instances
	// can be pointed at pagerduty and duplicate alerts will be filtered by using a key. The first alert will win, this
	// can be useful for knowing what tenderduty instance sent the alert.
	ExtraInfo string `yaml:"extra_info"` // FIXME not used yet!
	// Alerts defines the types of alerts to send for this chain.
	Alerts AlertConfig `yaml:"alerts"`
	// PublicFallback determines if tenderduty should attempt to use public RPC endpoints in the situation that not
	// explicitly defined RPC servers are available. Not recommended.
	PublicFallback bool `yaml:"public_fallback"`
	// Nodes defines what RPC servers to connect to.
	Nodes []*NodeConfig `yaml:"nodes"`

	// Type selects the monitoring provider. "cosmos" (default) uses the websocket + cosmos-sdk
	// staking/slashing path. "gnoland" uses the gno.land/TM2 HTTP-poll provider.
	Type string `yaml:"type"`

	// GnoValopersRealm is the gno realm path used to resolve validator monikers via vm/qeval.
	// Only used when Type == "gnoland". Defaults to "gno.land/r/gnops/valopers".
	GnoValopersRealm string `yaml:"gno_valopers_realm"`

	// GnoSignedBlocksWindow is the size of the LOCAL sliding signing-health
	// window for gnoland (dashboard uptime % + percentage_missed denominator).
	// gno.land has NO on-chain slashing module, so this is NOT a slashing window
	// — it is a local health metric. Only used when Type == "gnoland". Default 10000.
	GnoSignedBlocksWindow int `yaml:"gno_signed_blocks_window"`

	// EspressoNodeURL is the base URL of the validator's own Espresso Status API
	// (ESPRESSO_NODE_API_PORT). Only that node counts the leader slots assigned
	// to it (consensus_view_duration_as_leader_count) and the ones it failed to
	// propose (consensus_number_of_timeouts_as_leader); the public query service
	// exposes per-epoch rates only, which are too coarse to alert on. Without
	// this URL the chain is still watched for stalls and node lag, but leader
	// duties are not tracked.
	EspressoNodeURL string `yaml:"espresso_node_url"`
	// EspressoStakingKey is the validator BLS staking public key
	// (BLS_VER_KEY~...). Optional: discovered from the node's /v1/status/keys
	// (falling back to the consensus_node metric) when empty.
	EspressoStakingKey string `yaml:"espresso_staking_key"`
	// EspressoDutyWindow is how many recent leader duties the dashboard uptime %
	// and the percentage_missed alert are computed over. Default 1000.
	EspressoDutyWindow int `yaml:"espresso_duty_window"`
	// EspressoVoteLagViews is how many views the node's last vote may trail the
	// current view before a "not voting" alarm fires. Default 100 (~45s).
	EspressoVoteLagViews int `yaml:"espresso_vote_lag_views"`
}

// mkUpdate returns the info needed by prometheus for a gauge.
func (cc *ChainConfig) mkUpdate(t metricType, v float64, node string) *promUpdate {
	cc.stateMux.RLock()
	moniker := ""
	if cc.valInfo != nil {
		moniker = cc.valInfo.Moniker
	}
	cc.stateMux.RUnlock()
	return cc.mkUpdateWithMoniker(t, v, node, moniker)
}

func (cc *ChainConfig) mkUpdateWithMoniker(t metricType, v float64, node string, moniker string) *promUpdate {
	return &promUpdate{
		metric:   t,
		counter:  v,
		name:     cc.name,
		chainId:  cc.ChainId,
		moniker:  moniker,
		endpoint: node,
	}
}

// statusUpdate builds the chain's dashboard status from its current runtime
// state. Callers hold cc.stateMux.
func (cc *ChainConfig) statusUpdate() *dash.ChainStatus {
	healthy := 0
	for i := range cc.Nodes {
		if !cc.Nodes[i].down {
			healthy++
		}
	}
	nodeCount := len(cc.Nodes)
	// The espresso validator's own Status API is monitored as well (node down /
	// lag alarms), so count it in the dashboard's node row; otherwise 1/1 would
	// look healthy while the validator endpoint is unreachable.
	if cc.Type == "espresso" && cc.espressoNode != nil {
		nodeCount++
		if !cc.espressoNode.down {
			healthy++
		}
	}
	cc.activeAlerts = alarms.getCount(cc.name)
	st := &dash.ChainStatus{
		MsgType:      "status",
		Name:         cc.name,
		ChainId:      cc.ChainId,
		Network:      cc.Network,
		Nodes:        nodeCount,
		HealthyNodes: healthy,
		ActiveAlerts: cc.activeAlerts,
		Height:       cc.lastBlockNum,
		LastError:    getAlarms(cc.name),
		Blocks:       append([]int(nil), cc.blocksResults...),
	}
	if cc.valInfo != nil {
		st.Moniker = cc.valInfo.Moniker
		st.Bonded = cc.valInfo.Bonded
		st.Jailed = cc.valInfo.Jailed
		st.Tombstoned = cc.valInfo.Tombstoned
		st.Missed = cc.valInfo.Missed
		st.Window = cc.valInfo.Window
	}
	if cc.Type == "espresso" {
		st.Epoch = cc.espressoEpoch
		st.EpochAssigned = cc.espressoEpochAssigned
		st.EpochMissed = cc.espressoEpochMissed
	}
	return st
}

// pushStatus sends a dashboard status update. PollRun/WsRun only send updates
// when a block arrives, so a stalled chain leaves the dashboard frozen on its
// last pre-halt snapshot (healthy, zero active alerts) even while an alarm is
// firing. watch() calls this on alarm transitions so the stall and the alert
// become visible. Callers hold cc.stateMux.
func (cc *ChainConfig) pushStatus() {
	if !td.EnableDash {
		return
	}
	td.updateChan <- cc.statusUpdate()
}

// AlertConfig defines the type of alerts to send for a ChainConfig
type AlertConfig struct {
	// How many minutes to wait before alerting that no new blocks have been seen
	Stalled int `yaml:"stalled_minutes"`
	// Whether to alert when no new blocks are seen
	StalledAlerts bool `yaml:"stalled_enabled"`

	// How many missed blocks are acceptable before alerting
	ConsecutiveMissed int `yaml:"consecutive_missed"`
	// Tag for pagerduty to set the alert priority
	ConsecutivePriority string `yaml:"consecutive_priority"`
	// Whether to alert on consecutive missed blocks
	ConsecutiveAlerts bool `yaml:"consecutive_enabled"`

	// Window is how many blocks missed as a percentage of the slashing window to trigger an alert
	Window int `yaml:"percentage_missed"`
	// PercentagePriority is a tag for pagerduty to route on priority
	PercentagePriority string `yaml:"percentage_priority"`
	// PercentageAlerts is whether to alert on percentage based misses
	PercentageAlerts bool `yaml:"percentage_enabled"`

	// AlertIfInactive decides if tenderduty send an alert if the validator is not in the active set?
	AlertIfInactive bool `yaml:"alert_if_inactive"`
	// AlertIfNoServers: should an alert be sent if no servers are reachable?
	AlertIfNoServers bool `yaml:"alert_if_no_servers"`

	// PagerdutyAlerts: Should pagerduty alerts be sent for this chain? Both 'config.pagerduty.enabled: yes' and this must be set.
	//Deprecated: use Pagerduty.Enabled instead
	PagerdutyAlerts bool `yaml:"pagerduty_alerts"`
	// DiscordAlerts: Should discord alerts be sent for this chain? Both 'config.discord.enabled: yes' and this must be set.
	//Deprecated: use Discord.Enabled instead
	DiscordAlerts bool `yaml:"discord_alerts"`
	// TelegramAlerts: Should telegram alerts be sent for this chain? Both 'config.telegram.enabled: yes' and this must be set.
	//Deprecated: use Telegram.Enabled instead
	TelegramAlerts bool `yaml:"telegram_alerts"`

	// chain specific overrides for alert destinations.
	// Pagerduty configuration values
	Pagerduty PDConfig `yaml:"pagerduty"`
	// Discord webhook information
	Discord DiscordConfig `yaml:"discord"`
	// Telegram webhook information
	Telegram TeleConfig `yaml:"telegram"`
	// Slack webhook information
	Slack SlackConfig `yaml:"slack"`
	// Pushover push-notification information
	Pushover PushoverConfig `yaml:"pushover"`
}

// NodeConfig holds the basic information for a node to connect to.
type NodeConfig struct {
	Url         string `yaml:"url"`
	AlertIfDown bool   `yaml:"alert_if_down"`

	down      bool
	wasDown   bool
	syncing   bool
	lastMsg   string
	downSince time.Time
}

// PDConfig is the information required to send alerts to PagerDuty
type PDConfig struct {
	Enabled         bool   `yaml:"enabled"`
	ApiKey          string `yaml:"api_key"`
	DefaultSeverity string `yaml:"default_severity"`
}

// DiscordConfig holds the information needed to publish to a Discord webhook for sending alerts
type DiscordConfig struct {
	Enabled  bool     `yaml:"enabled"`
	Webhook  string   `yaml:"webhook"`
	Mentions []string `yaml:"mentions"`
}

// TeleConfig holds the information needed to publish to a Telegram webhook for sending alerts
type TeleConfig struct {
	Enabled  bool     `yaml:"enabled"`
	ApiKey   string   `yaml:"api_key"`
	Channel  string   `yaml:"channel"`
	Mentions []string `yaml:"mentions"`
}

// SlackConfig holds the information needed to publish to a Slack webhook for sending alerts
type SlackConfig struct {
	Enabled  bool     `yaml:"enabled"`
	Webhook  string   `yaml:"webhook"`
	Mentions []string `yaml:"mentions"`
}

// PushoverConfig holds the information needed to send alerts via Pushover (push notifications).
type PushoverConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Token    string `yaml:"token"`    // application API token
	UserKey  string `yaml:"user_key"` // user/group key
	Priority int    `yaml:"priority"` // -2..2; 2 = emergency (breaks DND)
	Sound    string `yaml:"sound"`    // optional, e.g. "persistent"
	Retry    int    `yaml:"retry"`    // required when priority == 2 (seconds, min 30)
	Expire   int    `yaml:"expire"`   // required when priority == 2 (seconds, max 10800)
}

// HealthcheckConfig holds the information needed to send pings to a healthcheck endpoint
type HealthcheckConfig struct {
	Enabled  bool          `yaml:"enabled"`
	PingURL  string        `yaml:"ping_url"`
	PingRate time.Duration `yaml:"ping_rate"`
}

// validateConfig is a non-exhaustive check for common problems with the configuration. Needs love.
func validateConfig(c *Config) (fatal bool, problems []string) {
	problems = make([]string, 0)
	var err error

	if c.EnableDash {
		_, err = url.Parse(c.Listen)
		if err != nil {
			fatal = true
			problems = append(problems, fmt.Sprintf("error: The listen URL %s does not appear to be valid", c.Listen))
		}
	}

	if c.Pagerduty.Enabled {
		rex := regexp.MustCompile(`[+_-]`)
		if rex.MatchString(c.Pagerduty.ApiKey) {
			fatal = true
			problems = append(problems, "error: The Pagerduty key provided appears to be an Oauth token, not a V2 Events API key.")
		}
	}

	if c.NodeDownMin < 3 {
		problems = append(problems, "warning: setting 'node_down_alert_minutes' to less than three minutes might result in false alarms")
	}

	var wantsPublic bool
	for k, v := range c.Chains {
		if v.blocksResults == nil {
			v.blocksResults = make([]int, showBLocks)
			for i := range v.blocksResults {
				v.blocksResults[i] = -1
			}
		}
		if v.name == "" {
			v.name = k
		}
		if v.PublicFallback {
			wantsPublic = true
		}

		v.valInfo = &ValInfo{Moniker: "not connected"}

		// default the provider type
		if v.Type == "" {
			v.Type = "cosmos"
		}
		switch v.Type {
		case "cosmos", "gnoland", "espresso":
			// ok
		default:
			problems = append(problems, fmt.Sprintf("warn: %20s has unknown chain type %q (expected cosmos, gnoland or espresso)", k, v.Type))
		}
		if v.Type == "gnoland" && v.ValAddress == "" {
			problems = append(problems, fmt.Sprintf("warn: %20s gnoland chain has no valoper_address; the signing address cannot be resolved and block monitoring stays inactive", k))
		}
		if v.Type == "espresso" {
			if v.EspressoDutyWindow <= 0 {
				v.EspressoDutyWindow = espressoDefaultDutyWindow
			}
			if v.EspressoVoteLagViews <= 0 {
				v.EspressoVoteLagViews = espressoDefaultVoteLag
			}
			if v.espressoWin == nil {
				v.espressoWin = newGnoWindow(v.EspressoDutyWindow)
			}
			if v.EspressoNodeURL == "" {
				problems = append(problems, fmt.Sprintf("warn: %20s espresso chain has no espresso_node_url; leader duties (missed proposals) cannot be tracked, only chain liveness", k))
			} else {
				v.EspressoNodeURL = strings.TrimRight(v.EspressoNodeURL, "/")
				v.espressoNode = &NodeConfig{Url: v.EspressoNodeURL, AlertIfDown: true}
			}
			if len(v.Nodes) == 0 {
				problems = append(problems, fmt.Sprintf("warn: %20s espresso chain has no query service URLs under nodes", k))
			}
		}
		switch strings.ToLower(v.Network) {
		case "", "mainnet", "testnet":
			// ok (empty = appears only in the "all" tab)
		default:
			problems = append(problems, fmt.Sprintf("warn: %20s has unknown network %q (expected mainnet or testnet)", k, v.Network))
		}

		// copy global pushover creds into per-chain override when blank (mirrors discord/telegram/slack)
		if v.Alerts.Pushover.Token == "" {
			v.Alerts.Pushover.Token = c.Pushover.Token
			v.Alerts.Pushover.UserKey = c.Pushover.UserKey
			if v.Alerts.Pushover.Priority == 0 && c.Pushover.Priority != 0 {
				v.Alerts.Pushover.Priority = c.Pushover.Priority
			}
			if v.Alerts.Pushover.Sound == "" {
				v.Alerts.Pushover.Sound = c.Pushover.Sound
			}
			if v.Alerts.Pushover.Retry == 0 {
				v.Alerts.Pushover.Retry = c.Pushover.Retry
			}
			if v.Alerts.Pushover.Expire == 0 {
				v.Alerts.Pushover.Expire = c.Pushover.Expire
			}
		}
		// emergency priority requires retry+expire
		if v.Alerts.Pushover.Enabled && v.Alerts.Pushover.Priority == 2 && (v.Alerts.Pushover.Retry < 30 || v.Alerts.Pushover.Expire <= 0) {
			problems = append(problems, fmt.Sprintf("warn: %20s pushover priority 2 needs retry>=30 and expire>0, dropping to priority 1", k))
			v.Alerts.Pushover.Priority = 1
		}

		// the bools for enabling alerts are deprecated with full configs preferred,
		// don't break if someone is still using them:
		if v.Alerts.DiscordAlerts && !v.Alerts.Discord.Enabled {
			v.Alerts.Discord.Enabled = true
		}
		if v.Alerts.TelegramAlerts && !v.Alerts.Telegram.Enabled {
			v.Alerts.Telegram.Enabled = true
		}
		if v.Alerts.PagerdutyAlerts && !v.Alerts.Pagerduty.Enabled {
			v.Alerts.Pagerduty.Enabled = true
		}

		// if the settings are blank, copy in the defaults:
		if v.Alerts.Discord.Webhook == "" {
			v.Alerts.Discord.Webhook = c.Discord.Webhook
			v.Alerts.Discord.Mentions = c.Discord.Mentions
		}
		if v.Alerts.Slack.Webhook == "" {
			v.Alerts.Slack.Webhook = c.Slack.Webhook
			v.Alerts.Slack.Mentions = c.Slack.Mentions
		}
		if v.Alerts.Telegram.ApiKey == "" {
			v.Alerts.Telegram.ApiKey = c.Telegram.ApiKey
			v.Alerts.Telegram.Mentions = c.Telegram.Mentions
		}
		if v.Alerts.Telegram.Channel == "" {
			v.Alerts.Telegram.Channel = c.Telegram.Channel
		}
		if v.Alerts.Pagerduty.ApiKey == "" {
			v.Alerts.Pagerduty.ApiKey = c.Pagerduty.ApiKey
			v.Alerts.Pagerduty.DefaultSeverity = c.Pagerduty.DefaultSeverity
		}

		switch {
		case v.Alerts.Slack.Enabled && !c.Slack.Enabled:
			problems = append(problems, fmt.Sprintf("warn: %20s is configured for slack alerts, but it is not enabled", k))
			fallthrough
		case v.Alerts.Discord.Enabled && !c.Discord.Enabled:
			problems = append(problems, fmt.Sprintf("warn: %20s is configured for discord alerts, but it is not enabled", k))
			fallthrough
		case v.Alerts.Pagerduty.Enabled && !c.Pagerduty.Enabled:
			problems = append(problems, fmt.Sprintf("warn: %20s is configured for pagerduty alerts, but it is not enabled", k))
			fallthrough
		case v.Alerts.Telegram.Enabled && !c.Telegram.Enabled:
			problems = append(problems, fmt.Sprintf("warn: %20s is configured for telegram alerts, but it is not enabled", k))
		case v.Alerts.Pushover.Enabled && !c.Pushover.Enabled:
			problems = append(problems, fmt.Sprintf("warn: %20s is configured for pushover alerts, but it is not enabled", k))
		case !v.Alerts.ConsecutiveAlerts && !v.Alerts.PercentageAlerts && !v.Alerts.AlertIfInactive && !v.Alerts.AlertIfNoServers:
			problems = append(problems, fmt.Sprintf("warn: %20s has no alert types configured", k))
			fallthrough
		case !v.Alerts.Pagerduty.Enabled && !v.Alerts.Discord.Enabled && !v.Alerts.Telegram.Enabled && !v.Alerts.Slack.Enabled && !v.Alerts.Pushover.Enabled:
			problems = append(problems, fmt.Sprintf("warn: %20s has no notifications configured", k))
		}
		if td.EnableDash {
			td.updateChan <- &dash.ChainStatus{
				MsgType: "status",
				Name:    v.name,
				ChainId: v.ChainId, Network: v.Network,
				Moniker:      v.valInfo.Moniker,
				Bonded:       v.valInfo.Bonded,
				Jailed:       v.valInfo.Jailed,
				Tombstoned:   v.valInfo.Tombstoned,
				Missed:       v.valInfo.Missed,
				Window:       v.valInfo.Window,
				Nodes:        len(v.Nodes),
				HealthyNodes: 0,
				ActiveAlerts: 0,
				Blocks:       v.blocksResults,
			}
		}
	}

	// if public endpoints are enabled we do our best to keep the list refreshed. Immediate, then every 12 hours.
	if wantsPublic {
		go func() {
			e := refreshRegistry()
			if e != nil {
				l("could not fetch chain registry paths, using defaults")
			}
			for {
				time.Sleep(12 * time.Hour)
				l("refreshing cosmos.registry paths")
				e = refreshRegistry()
				if e != nil {
					l("could not refresh registry paths -", e)
				}
			}
		}()
	}
	return
}

func loadChainConfig(yamlFile string) (*ChainConfig, error) {
	//#nosec -- variable specified on command line
	f, e := os.OpenFile(yamlFile, os.O_RDONLY, 0600)
	if e != nil {
		return nil, e
	}
	i, e := f.Stat()
	if e != nil {
		_ = f.Close()
		return nil, e
	}
	b := make([]byte, int(i.Size()))
	_, e = f.Read(b)
	_ = f.Close()
	if e != nil {
		return nil, e
	}
	c := &ChainConfig{}
	e = yaml.Unmarshal(b, c)
	if e != nil {
		return nil, e
	}
	return c, nil
}

// loadConfig creates a new Config from a file.
func loadConfig(yamlFile, stateFile, chainConfigDirectory string, password *string) (*Config, error) {

	c := &Config{}
	if strings.HasPrefix(yamlFile, "http://") || strings.HasPrefix(yamlFile, "https://") {
		if *password == "" {
			return nil, errors.New("a password is required if loading a remote configuration")
		}
		//#nosec -- url is specified on command line
		resp, err := http.Get(yamlFile)
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()
		log.Printf("downloaded %d bytes from %s", len(b), yamlFile)
		decrypted, err := decrypt(b, *password)
		if err != nil {
			return nil, err
		}
		empty := ""
		password = &empty             // let gc get password out of memory, it's still referenced in main()
		_ = os.Setenv("PASSWORD", "") // also clear the ENV var
		err = yaml.Unmarshal(decrypted, c)
		if err != nil {
			return nil, err
		}
	} else {
		//#nosec -- variable specified on command line
		f, e := os.OpenFile(yamlFile, os.O_RDONLY, 0600)
		if e != nil {
			return nil, e
		}
		i, e := f.Stat()
		if e != nil {
			_ = f.Close()
			return nil, e
		}
		b := make([]byte, int(i.Size()))
		_, e = f.Read(b)
		_ = f.Close()
		if e != nil {
			return nil, e
		}
		e = yaml.Unmarshal(b, c)
		if e != nil {
			return nil, e
		}
	}

	// Load additional chain configuration files
	chainConfigFiles, e := os.ReadDir(chainConfigDirectory)
	if e != nil {
		l("Failed to scan chainConfigDirectory", e)
	}

	for _, chainConfigFile := range chainConfigFiles {
		if chainConfigFile.IsDir() {
			l("Skipping Directory: ", chainConfigFile.Name())
			continue
		}
		if !strings.HasSuffix(chainConfigFile.Name(), ".yml") {
			l("Skipping non .yml file: ", chainConfigFile.Name())
			continue
		}
		fmt.Println("Reading Chain Config File: ", chainConfigFile.Name())
		chainConfig, e := loadChainConfig(path.Join(chainConfigDirectory, chainConfigFile.Name()))
		if e != nil {
			l(fmt.Sprintf("Failed to read %s", chainConfigFile), e)
			return nil, e
		}

		chainName := strings.Split(chainConfigFile.Name(), ".")[0]

		// Create map if it didnt exist in config.yml
		if c.Chains == nil {
			c.Chains = make(map[string]*ChainConfig)
		}
		c.Chains[chainName] = chainConfig
		l(fmt.Sprintf("Added %s from ", chainName), chainConfigFile.Name())
	}

	if len(c.Chains) == 0 {
		return nil, errors.New("no chains configured")
	}

	c.alertChan = make(chan *alertMsg)
	c.logChan = make(chan dash.LogMessage)
	// buffer enough to get through validateConfig()
	c.updateChan = make(chan *dash.ChainStatus, len(c.Chains)*2)
	c.statsChan = make(chan *promUpdate, len(c.Chains)*2)
	c.ctx, c.cancel = context.WithCancel(context.Background())

	// handle cached data. FIXME: incomplete.
	c.alarms = &alarmCache{
		SentPdAlarms:  make(map[string]time.Time),
		SentTgAlarms:  make(map[string]time.Time),
		SentDiAlarms:  make(map[string]time.Time),
		SentSlkAlarms: make(map[string]time.Time),
		SentPoAlarms:  make(map[string]time.Time),
		AllAlarms:     make(map[string]map[string]time.Time),
		notifyMux:     sync.RWMutex{},
	}

	//#nosec -- variable specified on command line
	sf, e := os.OpenFile(stateFile, os.O_RDONLY, 0600)
	if e != nil {
		l("could not load saved state", e.Error())
	}
	b, e := io.ReadAll(sf)
	_ = sf.Close()
	if e != nil {
		l("could not read saved state", e.Error())
	}
	saved := &savedState{}
	e = json.Unmarshal(b, saved)
	if e != nil {
		l("could not unmarshal saved state", e.Error())
	}
	for k, v := range saved.Blocks {
		if c.Chains[k] != nil {
			c.Chains[k].blocksResults = v
		}
	}

	// restore gnoland local counters
	for k, v := range saved.GnoCounters {
		if c.Chains[k] != nil && c.Chains[k].Type == "gnoland" {
			if c.Chains[k].valInfo == nil {
				c.Chains[k].valInfo = &ValInfo{}
			}
			c.Chains[k].statTotalSigns = v.TotalSigns
			c.Chains[k].statTotalProps = v.TotalProps
			c.Chains[k].statTotalMiss = v.TotalMiss
			c.Chains[k].statConsecutiveMiss = v.ConsecutiveMiss
			c.Chains[k].lastBlockNum = v.LastBlockNum
			// Resume the stall clock only when the saved time is still fresh
			// enough to be meaningful. A process that was down longer than the
			// stall threshold would otherwise fire a false "stalled" critical the
			// moment monitoring starts, while a chain that is already halted must
			// still alarm: in that case start the clock now, so the alarm fires
			// after the configured threshold instead of never (zero time).
			stalled := c.Chains[k].Alerts.Stalled
			if stalled <= 0 {
				stalled = 10
			}
			if !v.LastBlockTime.IsZero() && time.Since(v.LastBlockTime) <= time.Duration(stalled)*time.Minute {
				c.Chains[k].lastBlockTime = v.LastBlockTime
			} else {
				c.Chains[k].lastBlockTime = time.Now()
			}
			// valInfo.Missed/Window are NOT restored: they come from the ephemeral
			// gnoWin sliding ring (warm-up after restart), not a persisted counter.
			l(fmt.Sprintf("📂 restored gno counters for %s", k))
		}
	}

	// gno: without a saved timestamp the stall clock (and the prometheus
	// last-block gauge) would start at the zero time — a chain that is already
	// halted at first boot would never raise its stalled alarm.
	for _, v := range c.Chains {
		if v.Type == "gnoland" && v.lastBlockTime.IsZero() {
			v.lastBlockTime = time.Now()
		}
	}

	// restore espresso counters + the sliding duty window
	for k, v := range saved.EspressoCounters {
		cc := c.Chains[k]
		if cc == nil || cc.Type != "espresso" {
			continue
		}
		if cc.valInfo == nil {
			cc.valInfo = &ValInfo{}
		}
		cc.statTotalSigns = v.TotalSigns
		cc.statTotalProps = v.TotalProps
		cc.statTotalMiss = v.TotalMiss
		cc.statConsecutiveMiss = v.ConsecutiveMiss
		cc.espressoCountersValid = v.CountersValid
		cc.espressoAssignedTotal = v.AssignedTotal
		cc.espressoMissedTotal = v.MissedTotal
		cc.espressoEpoch = v.Epoch
		cc.espressoEpochAssigned = v.EpochAssigned
		cc.espressoEpochMissed = v.EpochMissed
		cc.lastBlockNum = v.LastBlockNum
		if cc.EspressoDutyWindow <= 0 {
			cc.EspressoDutyWindow = espressoDefaultDutyWindow
		}
		cc.espressoWin = newGnoWindow(cc.EspressoDutyWindow)
		for _, missed := range v.Duties {
			cc.espressoWin.Push(missed)
		}
		cc.valInfo.Missed = int64(cc.espressoWin.Missed())
		cc.valInfo.Window = int64(cc.espressoWin.Window())
		// Same stall-clock guard as gnoland: a saved timestamp older than the
		// threshold means the process was down for a long time — start the clock
		// now instead of firing a false "stalled" critical on boot. A fresh
		// timestamp resumes as-is so a real halt still alarms on time.
		stalled := cc.Alerts.Stalled
		if stalled <= 0 {
			stalled = 10
		}
		if !v.LastBlockTime.IsZero() && time.Since(v.LastBlockTime) <= time.Duration(stalled)*time.Minute {
			cc.lastBlockTime = v.LastBlockTime
		} else {
			cc.lastBlockTime = time.Now()
		}
		l(fmt.Sprintf("📂 restored espresso counters for %s (assigned=%d missed=%d duties=%d)",
			k, v.AssignedTotal, v.MissedTotal, cc.espressoWin.Window()))
	}
	for _, v := range c.Chains {
		if v.Type == "espresso" && v.lastBlockTime.IsZero() {
			v.lastBlockTime = time.Now()
		}
	}

	// restore alarm state to prevent duplicate alerts
	if saved.Alarms != nil {
		if saved.Alarms.SentTgAlarms != nil {
			alarms.SentTgAlarms = saved.Alarms.SentTgAlarms
			clearStale(alarms.SentTgAlarms, "telegram", c.Pagerduty.Enabled, staleHours)
		}
		if saved.Alarms.SentPdAlarms != nil {
			alarms.SentPdAlarms = saved.Alarms.SentPdAlarms
			clearStale(alarms.SentPdAlarms, "PagerDuty", c.Pagerduty.Enabled, staleHours)
		}
		if saved.Alarms.SentDiAlarms != nil {
			alarms.SentDiAlarms = saved.Alarms.SentDiAlarms
			clearStale(alarms.SentDiAlarms, "Discord", c.Pagerduty.Enabled, staleHours)
		}
		if saved.Alarms.SentSlkAlarms != nil {
			alarms.SentSlkAlarms = saved.Alarms.SentSlkAlarms
			clearStale(alarms.SentSlkAlarms, "Slack", c.Pagerduty.Enabled, staleHours)
		}
		if saved.Alarms.SentPoAlarms != nil {
			alarms.SentPoAlarms = saved.Alarms.SentPoAlarms
			clearStale(alarms.SentPoAlarms, "Pushover", c.Pagerduty.Enabled, staleHours)
		}
		if saved.Alarms.AllAlarms != nil {
			alarms.AllAlarms = saved.Alarms.AllAlarms
			for _, alrm := range saved.Alarms.AllAlarms {
				clearStale(alrm, "dashboard", c.Pagerduty.Enabled, staleHours)
			}
		}
	}

	// we need to know if the node was already down to clear alarms
	if saved.NodesDown != nil {
		for k, v := range saved.NodesDown {
			for nodeUrl := range v {
				if !v[nodeUrl].IsZero() {
					if c.Chains[k] != nil {
						for j := range c.Chains[k].Nodes {
							if c.Chains[k].Nodes[j].Url == nodeUrl {
								c.Chains[k].Nodes[j].down = true
								c.Chains[k].Nodes[j].wasDown = true
								c.Chains[k].Nodes[j].downSince = v[nodeUrl]
							}
						}
					}
				}
			}
		}
		// now we need to know if all RPC endpoints were down.
		for k, v := range c.Chains {
			downCount := 0
			for j := range v.Nodes {
				if v.Nodes[j].down {
					downCount += 1
				}
			}
			if downCount == len(c.Chains[k].Nodes) {
				c.Chains[k].noNodes = true
			}
		}
	}

	return c, nil
}

func clearStale(alarms map[string]time.Time, what string, hasPagerduty bool, hours float64) {
	for k := range alarms {
		if time.Since(alarms[k]).Hours() >= hours {
			l(fmt.Sprintf("🗑 not restoring old alarm (%v >%.2f hours) from cache - %s", alarms[k], hours, k))
			if hasPagerduty && strings.EqualFold(what, "pagerduty") {
				l("NOTE: stale alarms may need to be manually cleared from PagerDuty!")
			}
			delete(alarms, k)
			continue
		}
		l(fmt.Sprintf("📂 restored %s alarm state - %s", what, k))
	}
}
