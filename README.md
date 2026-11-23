# enchanted-tenderduty

A validator monitoring and alerting service with a web dashboard and a
Prometheus exporter. It watches Cosmos-SDK / Evmos chains, gnoland
(gno.land / Tendermint 2) chains and Espresso (HotShot) validators from a
single binary, and alerts on missed blocks, stalled chains, jailed/inactive
validators, unreachable nodes, and validators that stopped voting.

> Forked from [blockpane/tenderduty](https://github.com/blockpane/tenderduty).
> The gnoland (gno.land / TM2) provider is ported from the
> [GnoDuty](https://github.com/aviaone/gnoduty) reference and hardened with
> canonical address normalization and a dual operator/signing address model.

## Features

- **Tri-provider monitoring** in one binary:
  - `cosmos` (default) — Cosmos-SDK / Evmos chains via websocket + staking/slashing queries.
  - `gnoland` — gno.land / TM2 chains via HTTP polling (`/block`, `/validators`, `vm/qeval`), for public nodes that expose no websocket.
  - `espresso` — Espresso (HotShot) validators: leader duties assigned/missed are read from the validator's own Status API, so a missed proposal alerts within seconds (the public query service only exposes per-epoch rates).
- **Alerts** to Discord, Telegram, Slack, PagerDuty, and **Pushover** (emergency priority that breaks through Do Not Disturb).
- **Web dashboard** — every chain (cosmos, gnoland *and* espresso, testnet *and* mainnet) shows up the same way: bonded/jailed status, missed-block grid (leader duties for espresso), height, active alerts, with mainnet/testnet tabs driven by a per-chain `network` field.
- **Prometheus exporter** — gauges prefixed `tenderduty_*`.
- **State persistence** — missed-block counters and alarm state survive restarts; written atomically every 60s while running, so a crash (SIGKILL/OOM/power loss) loses at most ~1 minute of dedup/counter state.
- Backward compatible — existing cosmos configs keep working unchanged.

## Requirements

- A Linux server (the dashboard + alerts run as a long-lived service).
- **Go 1.21+** to build (developed on Go 1.26).
- A build toolchain: `git` and a C compiler (`build-essential` + `libssl-dev` on Debian/Ubuntu, `base-devel` on Arch, etc.) — some dependencies use cgo.
- Outbound HTTPS to your chains' RPC endpoints and to whichever alert services you enable.

---

## Quick start

1. [Install Go](#1-install-go) (if not already present).
2. [Build the binary](#2-get-the-code-and-build).
3. [Create your config](#3-create-your-config).
4. [Run it as a systemd service](#4-run-as-a-systemd-service).
5. [Put the dashboard behind a reverse proxy](#dashboard--reverse-proxy).

---

## 1. Install Go

Check whether Go is installed and recent enough:

```bash
go version      # need 1.21+; if missing or older, install below
```

**Option A — official tarball (works on any Linux distro).** Grab the latest
`linux-amd64` tarball from <https://go.dev/dl>, then:

```bash
GOVER=1.23.4   # ← replace with the latest version from go.dev/dl
wget "https://go.dev/dl/go${GOVER}.linux-amd64.tar.gz"
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf "go${GOVER}.linux-amd64.tar.gz"
echo 'export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin' | sudo tee /etc/profile.d/go.sh
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin     # for the current shell
go version
```

**Option B — distro package** (simpler, may lag the latest):

```bash
sudo apt install -y golang-go      # Debian/Ubuntu
sudo dnf install -y go             # Fedora
sudo pacman -S go                  # Arch
```

## 2. Get the code and build

```bash
cd $HOME
git clone https://github.com/MictoNode/enchanted-tenderduty.git
cd enchanted-tenderduty
# The binary reports its source commit; check with `tenderduty --version` or the startup log.
go build -ldflags "-X main.version=$(git describe --tags --always --dirty) -X main.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o "$(go env GOPATH)/bin/tenderduty" .   # installs the binary to $HOME/go/bin/tenderduty
$HOME/go/bin/tenderduty --help
```

## 3. Create your config

`example-config.yml` is a complete, commented reference (every sink, both chain
types, all alert fields). Copy it and edit:

```bash
cp example-config.yml config.yml
$EDITOR config.yml
```

At minimum set: dashboard/listen options, the alert sinks you want, and a
`chains:` entry per validator (see [Configuration](#configuration)).

You can also print a starter config with `./tenderduty -example-config`.

## 4. Run as a systemd service

The binary and config stay in the clone directory under your home
(`$HOME/enchanted-tenderduty`) — no separate system user and no `/opt` layout.
The service runs as your own user, and reads `config.yml` from the working
directory by default. The heredoc expands `$HOME` and `$(whoami)` to your real
paths:

```bash
sudo tee /etc/systemd/system/tenderduty.service > /dev/null <<EOF
[Unit]
Description=Tenderduty validator monitor
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=5
TimeoutSec=180
User=$(whoami)
WorkingDirectory=$HOME/enchanted-tenderduty
ExecStart=$HOME/go/bin/tenderduty

LimitNOFILE=infinity
NoNewPrivileges=true
ProtectSystem=strict
RestrictSUIDSGID=true
LockPersonality=true
PrivateUsers=true
PrivateDevices=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF
```

Make sure the `-o` output path of your build matches `ExecStart` above: the
unit runs the binary from `$HOME/go/bin/tenderduty`, so build with the
ldflags command from section 2 (`-o "$(go env GOPATH)/bin/tenderduty"`).
Building to `./tenderduty` instead silently keeps the old binary running —
the service stays on the previous build until the paths are aligned.

Enable, start, and follow logs:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tenderduty
sudo systemctl status tenderduty
journalctl -u tenderduty -f
```

The dashboard is at `http://<your-server-ip>:8888` (or whichever `listen_port`
you set).

**Updating** later:

```bash
cd $HOME/enchanted-tenderduty
git pull
go build -ldflags "-X main.version=$(git describe --tags --always --dirty) -X main.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o "$(go env GOPATH)/bin/tenderduty" .
sudo systemctl restart tenderduty
```

---

## Dashboard & reverse proxy

The dashboard listens on the port set by `listen_port` in your config (e.g.
`8888`). Every chain you configure — cosmos or gnoland, testnet or mainnet —
appears on it the same way (status cards + missed-block grid).

Put it behind a reverse proxy with TLS. With Caddy:

```
monitor.example.com {
    reverse_proxy 127.0.0.1:8888
}
```

Equivalent one-liners for nginx/Traefik: route `monitor.example.com` →
`127.0.0.1:8888`. The Prometheus exporter listens on `prometheus_listen_port`
(e.g. `28686`) at `/metrics`.

> Tip: don't expose the dashboard port directly to the internet without
> TLS/auth — it can reveal validator addresses and (unless `hide_logs: yes`)
> RPC node details.

---

## Configuration

`config.yml` is YAML: global settings + alert sinks, then a `chains:` map with
one entry per validator. A sink delivers a notification only when **both** its
global flag and the chain-level flag are `enabled`.

Minimal example:

```yaml
enable_dashboard: yes
listen_port: 8888
hide_logs: yes
node_down_alert_minutes: 3
node_down_alert_severity: critical
node_lag_alert_blocks: 0        # 0 = off; alert when an RPC node is >N blocks behind head

prometheus_enabled: yes
prometheus_listen_port: 28686

discord:
  enabled: yes
  webhook: "https://discord.com/api/webhooks/XX/YY"

telegram:
  enabled: yes
  api_key: "123:ABC"
  channel: "-1001234567890"

pushover:                       # optional; emergency pushes break DND
  enabled: no                   # flip to yes + fill token/user_key to activate
  token: "APP_TOKEN_HERE"
  user_key: "USER_KEY_HERE"
  priority: 2                   # 2 = emergency (requires retry+expire)
  retry: 30
  expire: 3600

chains:

  # ---- Cosmos-SDK / Evmos chain (default provider) ----
  "Example-Mainnet":
    chain_id: example-1
    network: mainnet
    valoper_address: examplevaloper1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    alerts:
      stalled_enabled: yes
      stalled_minutes: 3
      consecutive_enabled: yes
      consecutive_missed: 10
      consecutive_priority: critical
      percentage_enabled: yes
      percentage_missed: 10
      percentage_priority: warning
      alert_if_inactive: yes
      alert_if_no_servers: yes
    nodes:
      - url: https://rpc.example.com:443
        alert_if_down: no

  # ---- gnoland chain ----
  "Gno-Testnet":
    chain_id: test11
    network: testnet
    type: gnoland                                   # selects the gno provider
    valoper_address: g1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx   # OPERATOR addr (gnoweb); signing is auto-derived
    gno_valopers_realm: "gno.land/r/gnops/valopers"
    alerts:
      consecutive_enabled: yes                      # primary gno alert
      consecutive_missed: 10
      consecutive_priority: critical
      alert_if_inactive: yes
      alert_if_no_servers: yes
      pushover:                                     # per-chain override
        enabled: yes
        priority: 2
        retry: 30
        expire: 3600
    nodes:
      - url: https://rpc.gno.example:443
        alert_if_down: yes

  # ---- Espresso validator ----
  "Espresso-M":
    chain_id: espresso-mainnet-1        # cosmetic; identity is the BLS key
    network: mainnet
    type: espresso                      # selects the Espresso/HotShot provider
    espresso_node_url: https://espresso-node.example.com:28181   # own Status API
    # espresso_staking_key: BLS_VER_KEY~...   # blank = auto-discover from the node
    espresso_duty_window: 1000          # leader duties for uptime % + percentage
    alerts:
      consecutive_enabled: yes          # 1 = alert on every missed leader duty
      consecutive_missed: 1
      consecutive_priority: critical
      percentage_enabled: yes
      percentage_missed: 1              # >1% of the duty window
      percentage_priority: warning
      alert_if_inactive: yes
      alert_if_no_servers: yes
    nodes:                              # query-service endpoints (head, poll-all)
      - url: https://query.main.net.espresso.network
        alert_if_down: yes
```

See `example-config.yml` for the full set of fields (all five sinks with
per-chain overrides, healthcheck, etc.).

### Per-chain fields

| Field | Description |
|---|---|
| `chain_id` | Chain ID; must match what the RPC `/status` reports. |
| `network` | `mainnet` or `testnet` — drives the dashboard's tabs. Optional; empty = appears only in "all". |
| `type` | `cosmos` (default), `gnoland`, or `espresso`. |
| `valoper_address` | Validator **operator** address. Cosmos: `...valoper1...`. Gno: the operator `g1...` shown on gnoweb (the signing/consensus address is auto-derived). Not used by espresso. |
| `gno_valopers_realm` | (gnoland only) Realm path for moniker/signing lookup. Default `gno.land/r/gnops/valopers`. |
| `espresso_node_url` | (espresso only) Base URL of the validator's own Status API (`/v1/status/*`). Required for missed-proposal monitoring; without it only chain liveness is tracked. |
| `espresso_staking_key` | (espresso only) BLS staking public key. Optional — discovered from the node's `/v1/status/keys`. |
| `espresso_duty_window` | (espresso only) Recent leader duties used for uptime % + `percentage_missed`. Default 1000. |
| `espresso_vote_lag_views` | (espresso only) `current_view - last_voted_view` before a "not voting" warning. Default 100 (~45s). |
| `alerts` | Alert thresholds + per-sink overrides (see `example-config.yml`). |
| `nodes[]` | RPC endpoints; `alert_if_down` toggles node-down alerts. |

### gnoland notes

- gno.land has **two addresses per validator**: an operator address (the valopers realm key, shown on gnoweb) and a signing/consensus address (used by `/validators` and `/commit`). Put the **operator** address in `valoper_address`; the signing address is resolved automatically from the realm. The operator must have a profile in the valopers realm, otherwise no signing address can be derived and block monitoring stays inactive until the profile exists.
- Public gno.land nodes generally **don't support websocket subscriptions**, so the gno provider works over HTTP: every 5s it reads the head from **all** configured nodes (`/status`, max height wins) and evaluates **every height in between** via `/commit?height=N`. That keeps block-level accuracy on chains whose block time (~3.5s on gno.land mainnet) is shorter than the poll interval; a large backlog after downtime is bounded (last 100 heights per tick, skipped beyond 1000).
- gno mainnet is `chain_id: gnoland-1` with public RPC `https://rpc.gno.land` (put at least two endpoints if you have them — head detection and commit fetches both prefer the healthy one and fall back).
- gno has **no on-chain slashing module**, so there is no slashing window to query. The optional `gno_signed_blocks_window` (default 10000) defines a **LOCAL sliding signing-health window** (last N blocks) that drives the dashboard uptime % and the `percentage_missed` alert — it is a health metric, **not** a slashing window. At ~3.5s blocks 10000 ≈ 9.7 hours; use a smaller value (e.g. 1000 ≈ 1h) if you want the uptime % to react faster. `consecutive_missed` remains the **primary** gno alert.
- A validator that is resolved but **not in the active set** is not expected to sign, so it neither accrues missed counters nor fires false consecutive alerts; the dashboard shows its blocks as "No Data".
- Inactive detection: gno has no jail, so the "jailed" alert fires when the validator **leaves the active set** (bonded → unbonded, debounced over two health ticks). Last processed height and block time are persisted, so a restart resumes where it stopped and does not mask a stalled chain.

### espresso notes

- HotShot has **no per-validator block signature** and the public query service publishes only per-epoch **participation rates** (`/v1/node/participation/proposal/{epoch}`) — no counts, no leader schedule, no DRB. The validator's own Status API is the authoritative source: `consensus_view_duration_as_leader_count` is the number of leader slots assigned to the node and `consensus_number_of_timeouts_as_leader` the ones it failed to propose, so `proposals = assigned - missed`. Verified against public validators: the counter ratio matches the delegation program's published weekly miss rate.
- **`espresso_node_url` must be your own validator's Status API** (`ESPRESSO_NODE_API_PORT`) — not another operator's node, not the public query service. Both leader-duty counters are **node-local**: they describe the proposals of the node answering them, so pointing this at a foreign node would silently monitor that operator's duties. If `espresso_staking_key` is set and the node reports a different BLS key, tenderduty refuses duty accounting and logs `🛑 ... reports ..., not the configured espresso_staking_key ... point espresso_node_url at YOUR validator`. The endpoint serves plain HTTP — put it behind an HTTPS reverse proxy or allow only the monitoring host through the firewall. The BLS staking key, operator address and node name are discovered from `/v1/status/keys` and the `consensus_node` / `consensus_node_identity_general` metrics.
- `nodes[]` are **query-service** endpoints, not the validator. Every configured endpoint is asked for the chain head each poll and the highest height wins, so one stale endpoint cannot freeze monitoring or mask a stalled chain (same model as gnoland). The public mainnet query service is `https://query.main.net.espresso.network`.
- The dashboard grid is per **block** (green = a block this validator proposed, blue = a decided block while it was voting) and flows at block rate like the cosmos/gno grids; every missed leader duty is inserted as an orange cell in that stream. The counters do not identify the exact block, so a successful duty paints the newest signed cell green (within the ~15s sample window). Uptime % and `missed / window` use the last `espresso_duty_window` **assigned leader duties** (not blocks: at a small stake share 10k blocks contain only ~tens of duties) with real counts, persisted across restarts. Below them the row shows the current epoch tally — `epoch N · M missed / A assigned` — the same numbers the delegation-program bots report.
- Extra alarms: `validator node ... is not responding` (own Status API down longer than `node_down_alert_minutes`) and `... has not voted ... in N views` (`current_view - last_voted_view` exceeds `espresso_vote_lag_views` while the chain keeps moving — the node lost consensus connectivity).
- Counter drops mean the node restarted (Prometheus counters are process-lifetime); the baseline is re-seeded instead of reporting a bogus delta. Inactive detection is stake-table membership, debounced over two health ticks (Espresso has no jail).
- Sampling: chain head every 5s, the node's `/status/metrics` every 15s (it is ~75KB plain) and the stake table every 2 minutes (45KB, membership only changes at epoch boundaries), so a missed duty alerts within ~15s. `percentage_missed` is only evaluated once the duty window holds at least 100 duties (or the configured `espresso_duty_window` if smaller); `consecutive_missed: 1` is the immediate per-miss alert, and "not voting" works from the first served metrics snapshot even before the first assigned duty.

---

## Flags

```
-f           config file (default config.yml, or ENV CONFIG)
-cc          directory for extra chain configs (default chains.d)
-state       state file (default .tenderduty-state.json)
-example-config   print a sample config.yml and exit
-encrypt / -decrypt   config encryption (see -password, ENV PASSWORD)
```

---

## Credits

- **[blockpane/tenderduty](https://github.com/blockpane/tenderduty)** — the original Cosmos validator monitor this project is forked from.
- **[aviaone/gnoduty](https://github.com/aviaone/gnoduty)** — the gnoland/TM2 monitoring approach the gno provider is ported from.

## License

See [LICENSE](LICENSE).
