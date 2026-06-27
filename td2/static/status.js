// Node Monitor - Stable Updates (No Reordering)

let currentTab = 'all';
let allStatus = [];
const blocks = new Map();

function initApp() {
    showSkeleton();
    setupEventListeners();
    loadState();
    connect();
    if (typeof lucide !== 'undefined') lucide.createIcons();
}

// Loading skeleton rows, shown until the first real data arrives.
function showSkeleton() {
    const tbody = document.getElementById("statusTable");
    if (!tbody || tbody.querySelector('tr.td-skeleton-row')) return;
    let html = '';
    for (let i = 0; i < 4; i++) {
        html += '<tr class="td-skeleton-row">'
            + '<td></td>'
            + '<td><span class="td-skeleton" style="width:55%"></span></td>'
            + '<td></td>'
            + '<td><span class="td-skeleton" style="width:40%"></span></td>'
            + '<td><span class="td-skeleton" style="width:50%"></span></td>'
            + '<td></td>'
            + '</tr>';
    }
    tbody.innerHTML = html;
}

function clearSkeleton() {
    const tbody = document.getElementById("statusTable");
    if (!tbody) return;
    tbody.querySelectorAll('tr.td-skeleton-row').forEach(r => r.remove());
}

function setupEventListeners() {
    document.querySelectorAll('.td-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const t = tab.getAttribute('data-tab');
            if (t) switchTab(t);
        });
    });
}

async function loadState() {
    try {
        const res = await fetch("state", { cache: "no-store" });
        if (!res.ok) throw new Error("Net error");
        const data = await res.json();
        if (data && data.Status) {
            allStatus = data.Status;
            updateTabCounts();
            refreshView();
        }
    } catch (e) { console.error(e); }
}

function groupByNetwork(chains) {
    const mainnet = [], testnet = [];
    if (!Array.isArray(chains)) return { mainnet: [], testnet: [], all: [] };

    // Classification is driven by the explicit `network` config field
    // (mainnet|testnet), not by name suffix or chain_id heuristics. A chain
    // with no network set appears only in the "all" tab.
    chains.forEach(c => {
        const net = (c.network || '').toLowerCase();
        if (net === 'mainnet') mainnet.push(c);
        else if (net === 'testnet') testnet.push(c);
    });
    return { mainnet, testnet, all: chains };
}

function updateTabCounts() {
    const g = groupByNetwork(allStatus);
    const set = (id, n) => { const el = document.getElementById(id); if (el) el.textContent = n; };
    set('count-all', g.all.length);
    set('count-mainnet', g.mainnet.length);
    set('count-testnet', g.testnet.length);
}

function switchTab(tab) {
    if (currentTab === tab) return;
    currentTab = tab;

    document.querySelectorAll('.td-tab').forEach(t => {
        t.classList.toggle('active', t.getAttribute('data-tab') === tab);
    });

    document.getElementById("statusTable").innerHTML = '';
    refreshView();
}

function getFilteredStatus() {
    const g = groupByNetwork(allStatus);
    return currentTab === 'mainnet' ? g.mainnet : (currentTab === 'testnet' ? g.testnet : g.all);
}

function refreshView() {
    if (allStatus.length > 0) {
        const filtered = getFilteredStatus();
        updateTableStable(filtered);
        if (typeof drawSeries === 'function') drawSeries({ Status: filtered });
    }
}

// STABLE UPDATE: Never reorder rows, only update content
function updateTableStable(chains) {
    const tbody = document.getElementById("statusTable");
    if (!tbody) return;
    clearSkeleton();

    // Build a map of chain data by unique name for quick lookup.
    // NOTE: keyed by name, NOT chain_id — multiple validators can share a
    // chain_id (e.g. two gno validators on test-13) and must not collide.
    const chainMap = new Map();
    chains.forEach(c => chainMap.set(c.name, c));

    // Get existing row IDs
    const existingRowIds = new Set();
    Array.from(tbody.children).forEach(row => {
        const id = row.getAttribute('data-chain-id');
        if (id) existingRowIds.add(id);
    });

    // Handle empty state
    if (chains.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:40px;color:var(--text-muted);">No chains found.</td></tr>';
        return;
    }

    // Update existing rows and add new ones
    chains.forEach(chain => {
        const chainId = chain.name;
        let row = tbody.querySelector(`tr[data-chain-id="${CSS.escape(chainId)}"]`);

        // Prepare data
        const heightStr = chain.height ? chain.height.toLocaleString() : '...';
        const isHeightChanged = blocks.get(chainId) !== chain.height;
        blocks.set(chainId, chain.height);

        let statusClass = 'info', statusText = 'Inactive', statusIcon = 'circle-dashed';
        if (chain.tombstoned) { statusClass = 'danger'; statusText = 'Tombstoned'; statusIcon = 'skull'; }
        else if (chain.jailed) { statusClass = 'warning'; statusText = 'Jailed'; statusIcon = 'lock'; }
        else if (chain.bonded) { statusClass = 'success'; statusText = 'Active'; statusIcon = 'circle-check'; }

        let uptimePct = 'N/A', uptimeColor = '', barWidth = 0, barColor = 'var(--accent-success)';
        if (chain.window > 0) {
            const pct = 100 - (chain.missed / chain.window * 100);
            uptimePct = pct.toFixed(2) + '%';
            barWidth = Math.max(0, Math.min(100, pct));
            if (pct < 98) { uptimeColor = 'color: var(--accent-warning);'; barColor = 'var(--accent-warning)'; }
            if (pct < 90) { uptimeColor = 'color: var(--accent-danger);'; barColor = 'var(--accent-danger)'; }
        }

        const hasAlert = chain.active_alerts > 0 || chain.last_error;
        const alertTitle = chain.last_error || 'Active alert';
        const alertHtml = hasAlert ? `<i data-lucide="triangle-alert" title="${escapeHtml(alertTitle)}"></i>` : '';

        const healthy = chain.healthy_nodes || 0;
        const total = chain.nodes || 0;
        let dotsHtml = '';
        for (let n = 0; n < total; n++) dotsHtml += `<span class="td-node-dot ${n < healthy ? 'active' : 'down'}"></span>`;

        if (!row) {
            // CREATE NEW ROW (only happens once per chain)
            row = document.createElement('tr');
            row.setAttribute('data-chain-id', chainId);
            row.innerHTML = `
                <td class="cell-alert" data-alert="${hasAlert ? '1' : '0'}">${alertHtml}</td>
                <td>
                    <div class="td-chain-name">${escapeHtml(chain.name)}</div>
                    <div class="td-chain-id">${escapeHtml(chain.chain_id)}</div>
                </td>
                <td style="text-align:center;"><span class="td-badge td-badge-${statusClass}" data-status="${statusIcon}"><i data-lucide="${statusIcon}"></i>${statusText}</span></td>
                <td><span class="td-height">${heightStr}</span></td>
                <td>
                    <div class="td-uptime">
                        <span class="td-uptime-percent" style="${uptimeColor}">${uptimePct}</span>
                        <span class="td-uptime-ratio">${chain.missed} / ${chain.window}</span>
                        <span class="td-uptime-bar"><span class="td-uptime-bar-fill" style="width:${barWidth}%;background:${barColor};"></span></span>
                    </div>
                </td>
                <td><div class="td-nodes"><div class="td-node-dots">${dotsHtml}</div> ${healthy}/${total}</div></td>
            `;
            row.classList.toggle('has-alert', hasAlert);
            tbody.appendChild(row);
        } else {
            // UPDATE EXISTING ROW - Only change text content, never move!
            row.classList.toggle('has-alert', hasAlert);

            // Alert (compare by flag — innerHTML becomes <svg> after lucide render)
            const alertCell = row.querySelector('.cell-alert');
            const alertFlag = hasAlert ? '1' : '0';
            if (alertCell && alertCell.dataset.alert !== alertFlag) {
                alertCell.dataset.alert = alertFlag;
                alertCell.innerHTML = alertHtml;
            }

            // Height - with subtle highlight
            const heightEl = row.querySelector('.td-height');
            if (heightEl && heightEl.textContent !== heightStr) {
                heightEl.textContent = heightStr;
                heightEl.classList.add('updating');
                setTimeout(() => heightEl.classList.remove('updating'), 400);
            }

            // Status (compare by icon key, rebuild innerHTML so lucide re-renders)
            const badge = row.querySelector('.td-badge');
            if (badge && badge.dataset.status !== statusIcon) {
                badge.dataset.status = statusIcon;
                badge.className = `td-badge td-badge-${statusClass}`;
                badge.innerHTML = `<i data-lucide="${statusIcon}"></i>${statusText}`;
            }

            // Uptime percent
            const upPct = row.querySelector('.td-uptime-percent');
            if (upPct && upPct.textContent !== uptimePct) {
                upPct.textContent = uptimePct;
                upPct.style.cssText = uptimeColor;
            }

            // Uptime ratio
            const upRatio = row.querySelector('.td-uptime-ratio');
            const ratioText = `${chain.missed} / ${chain.window}`;
            if (upRatio && upRatio.textContent !== ratioText) upRatio.textContent = ratioText;

            // Uptime bar
            const barFill = row.querySelector('.td-uptime-bar-fill');
            if (barFill) {
                barFill.style.width = barWidth + '%';
                barFill.style.background = barColor;
            }

            // Nodes
            const nodesDiv = row.querySelector('.td-nodes');
            const newNodesHtml = `<div class="td-node-dots">${dotsHtml}</div> ${healthy}/${total}`;
            if (nodesDiv && nodesDiv.innerHTML !== newNodesHtml) nodesDiv.innerHTML = newNodesHtml;

            // NO REORDERING - row stays where it was created!
        }
    });

    // Remove rows for chains that no longer exist
    Array.from(tbody.querySelectorAll('tr[data-chain-id]')).forEach(row => {
        const id = row.getAttribute('data-chain-id');
        if (!chainMap.has(id)) row.remove();
    });

    if (typeof lucide !== 'undefined') lucide.createIcons();
}

function escapeHtml(str) {
    if (!str) return '';
    return String(str).replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' })[m]);
}

function connect() {
    const s = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
    s.onmessage = e => {
        try {
            const m = JSON.parse(e.data);
            if (m.msgType === "update" && document.visibilityState !== "hidden") {
                allStatus = m.Status || [];
                updateTabCounts();
                refreshView();
            }
        } catch (x) { }
    };
    s.onclose = () => setTimeout(connect, 3000);
}