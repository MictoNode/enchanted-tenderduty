package dash

type ChainStatus struct {
	MsgType    string `json:"msgType"`
	Name       string `json:"name"`
	ChainId    string `json:"chain_id"`
	Network    string `json:"network"`
	Moniker    string `json:"moniker"`
	Bonded     bool   `json:"bonded"`
	Jailed     bool   `json:"jailed"`
	Tombstoned bool   `json:"tombstoned"`
	Missed     int64  `json:"missed"`
	Window     int64  `json:"window"`
	// Espresso only: current epoch and its leader-duty tally. The window
	// counters above are a duty window; these make "assigned this epoch" visible
	// the way the delegation-program bots report it.
	Epoch         int64  `json:"epoch,omitempty"`
	EpochAssigned int64  `json:"epoch_assigned,omitempty"`
	EpochMissed   int64  `json:"epoch_missed,omitempty"`
	Nodes         int    `json:"nodes"`
	HealthyNodes  int    `json:"healthy_nodes"`
	ActiveAlerts  int    `json:"active_alerts"`
	Height        int64  `json:"height"`
	LastError     string `json:"last_error"`

	Blocks []int `json:"blocks"`
}

type LogMessage struct {
	MsgType string `json:"msgType"`
	Ts      int64  `json:"ts"`
	Msg     string `json:"msg"`
}
