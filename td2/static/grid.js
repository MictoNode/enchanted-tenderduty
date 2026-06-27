// Tenderduty Grid Renderer - Final Polish (Aligned + Nice)

const h = 24;
const w = 8;
let gridH = h;
let gridW = w;
const maxCanvasWidth = 4096;

// Theme colors
const colors = {
    text: "#94a3b8",
    rowEven: "rgba(30, 41, 59, 0.4)",
    rowOdd: "rgba(15, 23, 42, 0.4)",
    gridLine: "rgba(255, 255, 255, 0.05)"
};

function fix_dpi(id) {
    const canvas = document.getElementById(id);
    if (!canvas) return;

    const dpi = window.devicePixelRatio || 1;
    gridH = h * dpi;
    gridW = w * dpi;

    const rect = canvas.getBoundingClientRect();
    let targetWidth = rect.width * dpi;
    if (targetWidth > maxCanvasWidth) targetWidth = maxCanvasWidth;

    canvas.width = targetWidth;
    canvas.height = rect.height * dpi;
    return dpi;
}

function drawSeries(multiStates) {
    const canvas = document.getElementById("canvas");
    if (!canvas || !multiStates.Status || multiStates.Status.length === 0) return;

    const rowHeightPx = h + 12;
    const totalHeight = (rowHeightPx * multiStates.Status.length) + 20;
    canvas.style.height = totalHeight + 'px';

    const scale = fix_dpi("canvas");
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const width = canvas.width;
    const height = canvas.height;

    ctx.clearRect(0, 0, width, height);

    const labelWidth = 120 * scale;
    const blockStart = labelWidth + (20 * scale);
    const boxH = (gridH - 6 * scale); // Smaller blocks for cleaner look
    const boxW = (gridW - 2 * scale);
    const rowH = (gridH + 12 * scale);

    for (var j = 0; j < multiStates.Status.length; j++) {
        var rowY = (j * rowH) + (10 * scale);

        // Row Background
        ctx.fillStyle = j % 2 === 0 ? colors.rowEven : colors.rowOdd;
        ctx.fillRect(0, rowY, width, rowH);

        // Chain Name
        ctx.font = '600 ' + (13 * scale) + 'px Inter, system-ui, sans-serif';
        ctx.fillStyle = colors.text;
        ctx.textBaseline = 'middle';
        ctx.fillText(multiStates.Status[j].name, 12 * scale, rowY + rowH / 2);

        // Separator
        ctx.beginPath();
        ctx.moveTo(labelWidth, rowY + rowH / 2 - 12 * scale);
        ctx.lineTo(labelWidth, rowY + rowH / 2 + 12 * scale);
        ctx.strokeStyle = colors.gridLine;
        ctx.lineWidth = 1 * scale;
        ctx.stroke();

        // Blocks
        const blocks = multiStates.Status[j].blocks || [];
        const maxBlocks = Math.floor((width - blockStart) / (boxW + 2 * scale));
        const visibleBlocks = blocks.slice(0, maxBlocks);

        for (var i = 0; i < visibleBlocks.length; i++) {
            var x = blockStart + (i * (boxW + 2 * scale));
            var y = rowY + (rowH - boxH) / 2;

            var blockType = visibleBlocks[i];
            var fillColor;
            var isMissed = false;
            var glow = false;

            switch (blockType) {
                case 4: // Proposed (Green, bright)
                    fillColor = '#10b981';
                    glow = true;
                    break;
                case 3: // Signed (Subtle Blue)
                    fillColor = 'rgba(59, 130, 246, 0.25)';
                    break;
                case 2: // Precommit (Blue)
                    fillColor = '#3b82f6';
                    break;
                case 1: // Prevote (Cyan — distinct from precommit blue for color-blind access)
                    fillColor = '#22d3ee';
                    break;
                case 0: // Missed (Orange/Amber)
                    fillColor = '#f59e0b';
                    isMissed = true;
                    glow = true;
                    break;
                default:
                    fillColor = 'rgba(148, 163, 184, 0.1)';
            }

            ctx.fillStyle = fillColor;

            // Draw Block (Rounded)
            const radius = 2 * scale;
            ctx.beginPath();
            if (ctx.roundRect) ctx.roundRect(x, y, boxW, boxH, radius);
            else ctx.rect(x, y, boxW, boxH);
            ctx.fill();

            // Glow Effect for important blocks
            if (glow) {
                ctx.shadowColor = fillColor;
                ctx.shadowBlur = 4 * scale;
                ctx.fill();
                ctx.shadowBlur = 0; // reset
            }

            // Missed Indicator (small dash inside)
            if (isMissed) {
                ctx.fillStyle = 'rgba(255,255,255,0.6)';
                ctx.fillRect(x + boxW / 2 - 1 * scale, y + 4 * scale, 2 * scale, boxH - 8 * scale);
            }
        }
    }
}

var resizeTimer;
window.addEventListener('resize', function () {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function () {
        if (typeof allStatus !== 'undefined' && allStatus.length > 0) {
            var filtered = typeof getFilteredStatus === 'function' ? getFilteredStatus() : allStatus;
            drawSeries({ Status: filtered });
        }
    }, 200);
});