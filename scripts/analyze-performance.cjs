const db = require("better-sqlite3")("./data/sentiment_arb.db");

console.log("═══════════════════════════════════════════════════════════════");
console.log("        正式交易时段分析 (悉尼22:30后)");
console.log("        注: 数据库时间已是悉尼本地时间");
console.log("═══════════════════════════════════════════════════════════════");

const allTrades = db.prepare(`
    SELECT symbol, pnl_percent, entry_time, exit_time, exit_type, current_pnl
    FROM positions
    ORDER BY entry_time DESC
`).all();

// 数据库时间是悉尼本地时间，筛选22:30后
const nightTrades = allTrades.filter(t => {
    if (!t.entry_time) return false;
    const timeStr = t.entry_time.substring(11, 16);
    const parts = timeStr.split(":");
    const hour = parseInt(parts[0]);
    const min = parseInt(parts[1]);
    return (hour > 22) || (hour === 22 && min >= 30);
});

console.log("\n📊 正式交易时段(22:30+)统计");
console.log("─────────────────────────────────────────");
console.log("正式交易入场:", nightTrades.length);

if (nightTrades.length > 0) {
    const closed = nightTrades.filter(t => t.exit_time);
    const open = nightTrades.filter(t => !t.exit_time);
    const wins = closed.filter(t => (t.pnl_percent || 0) > 0);
    const totalPnl = closed.reduce((sum, t) => sum + (t.pnl_percent || 0), 0);

    console.log("已平仓:", closed.length);
    console.log("持仓中:", open.length);
    console.log("胜/负:", wins.length + "/" + (closed.length - wins.length));
    console.log("胜率:", closed.length > 0 ? ((wins.length / closed.length) * 100).toFixed(1) + "%" : "N/A");
    console.log("累计PnL:", (totalPnl >= 0 ? "+" : "") + totalPnl.toFixed(1) + "%");

    console.log("\n明细:");
    nightTrades.forEach(t => {
        const pnl = t.exit_time ? (t.pnl_percent || 0) : (t.current_pnl || 0);
        const entryTime = t.entry_time ? t.entry_time.substring(5, 16).replace("T", " ") : "";
        const exitTime = t.exit_time ? t.exit_time.substring(11, 16) : "持仓中";

        let duration = "";
        if (t.exit_time && t.entry_time) {
            const mins = Math.round((new Date(t.exit_time) - new Date(t.entry_time)) / 60000);
            duration = mins + "min";
        }

        console.log((t.symbol || "?").padEnd(14),
            "| 入:", entryTime,
            "| 出:", exitTime.padEnd(6),
            "| " + (duration || "-").padEnd(7),
            "| PnL:" + (pnl >= 0 ? "+" : "") + pnl.toFixed(1) + "%",
            "|", t.exit_time ? (t.exit_type || "-") : "🔵");
    });
} else {
    console.log("暂无22:30后的正式交易记录");
}

// 白天交易
const dayTrades = allTrades.filter(t => {
    if (!t.entry_time) return false;
    const timeStr = t.entry_time.substring(11, 16);
    const parts = timeStr.split(":");
    const hour = parseInt(parts[0]);
    const min = parseInt(parts[1]);
    return !((hour > 22) || (hour === 22 && min >= 30));
});

console.log("\n📊 非正式时段(<22:30)统计");
console.log("─────────────────────────────────────────");
console.log("白天入场:", dayTrades.length);
const dayClosed = dayTrades.filter(t => t.exit_time);
const dayWins = dayClosed.filter(t => (t.pnl_percent || 0) > 0);
const dayPnl = dayClosed.reduce((sum, t) => sum + (t.pnl_percent || 0), 0);
console.log("已平仓:", dayClosed.length);
console.log("胜率:", dayClosed.length > 0 ? ((dayWins.length / dayClosed.length) * 100).toFixed(1) + "%" : "N/A");
console.log("累计PnL:", (dayPnl >= 0 ? "+" : "") + dayPnl.toFixed(1) + "%");

// 当前持仓
console.log("\n💼 当前所有持仓");
console.log("─────────────────────────────────────────");
const openPositions = allTrades.filter(t => !t.exit_time);
openPositions.forEach(p => {
    const pnl = p.current_pnl || p.pnl_percent || 0;
    console.log((p.symbol || "?").padEnd(14),
        "| 入场:", (p.entry_time || "").substring(5, 16),
        "| PnL:" + (pnl >= 0 ? "+" : "") + pnl.toFixed(1) + "%");
});

db.close();
