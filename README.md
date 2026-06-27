# enchanted-tenderduty

A validator monitoring and alerting service with a web dashboard and a
Prometheus exporter. It watches Cosmos-SDK / Evmos chains **and** gnoland
(gno.land / Tendermint 2) chains from a single binary, and alerts on missed
blocks, stalled chains, jailed/inactive validators, and dead RPC nodes.

> Forked from [blockpane/tenderduty](https://github.com/blockpane/tenderduty).
> The gnoland (gno.land / TM2) provider is ported from the
> [GnoDuty](https://github.com/aviaone/gnoduty) reference and hardened with
> canonical address normalization and a dual operator/signing address model.

## Features

- **Dual-provider monitoring** in one binary:
  - `cosmos` (default) — Cosmos-SDK / Evmos chains via websocket + staking/slashing queries.
  - `gnoland` — gno.land / TM2 chains via HTTP polling (`/block`, `/validators`, `vm/qeval`), for public nodes that expose no websocket.
- **Alerts** to Discord, Telegram, Slack, PagerDuty, and **Pushover** (emergency priority that breaks through Do Not Disturb).
- **Web dashboard** — every chain (cosmos *and* gnoland, testnet *and* mainnet) shows up the same way: bonded/jailed status, missed-block grid, height, active alerts, with mainnet/testnet tabs driven by a per-chain `network` field.
- **Prometheus exporter** — gauges prefixed `tenderduty_*`.
- **State persistence** — missed-block counters and alarm state survive restarts.
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
go build -o "$(go env GOPATH)/bin/tenderduty" .   # installs the binary to $HOME/go/bin/tenderduty
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
go build -o "$(go env GOPATH)/bin/tenderduty" .
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
```

See `example-config.yml` for the full set of fields (all five sinks with
per-chain overrides, healthcheck, etc.).

### Per-chain fields

| Field | Description |
|---|---|
| `chain_id` | Chain ID; must match what the RPC `/status` reports. |
| `network` | `mainnet` or `testnet` — drives the dashboard's tabs. Optional; empty = appears only in "all". |
| `type` | `cosmos` (default) or `gnoland`. |
| `valoper_address` | Validator **operator** address. Cosmos: `...valoper1...`. Gno: the operator `g1...` shown on gnoweb (the signing/consensus address is auto-derived). |
| `gno_valopers_realm` | (gnoland only) Realm path for moniker/signing lookup. Default `gno.land/r/gnops/valopers`. |
| `alerts` | Alert thresholds + per-sink overrides (see `example-config.yml`). |
| `nodes[]` | RPC endpoints; `alert_if_down` toggles node-down alerts. |

### gnoland notes

- gno.land has **two addresses per validator**: an operator address (the valopers realm key, shown on gnoweb) and a signing/consensus address (used by `/validators` and `/block`). Put the **operator** address in `valoper_address`; the signing address is resolved automatically from the realm.
- Public gno.land nodes generally **don't support websocket subscriptions**, so the gno provider **HTTP-polls** `/block` every 5s and reconstructs signing status from `last_commit.precommits`.
- gno has **no on-chain slashing module**, so `missed`/`window` are tracked locally (and persisted across restart). `consecutive_missed` is the **primary** gno alert; `percentage_missed` is best-effort.
- A validator that is resolved but **not in the active set** is not expected to sign, so it neither accrues missed counters nor fires false consecutive alerts; the dashboard shows its blocks as "No Data".
- Jailed is detected when the validator leaves the active set.

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
