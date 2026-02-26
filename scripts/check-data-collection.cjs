const db = require("better-sqlite3")("./data/sentiment_arb.db");

console.log("═══════════════════════════════════════════════════════════════");
console.log("        数据收集状态检查");
console.log("═══════════════════════════════════════════════════════════════");

// 1. rejected_signals 表
console.log("\n📊 rejected_signals (被拒绝的信号)");
console.log("─────────────────────────────────────────");
try {
    const rejectedCount = db.prepare("SELECT COUNT(*) as c FROM rejected_signals").get();
    console.log("总记录数:", rejectedCount.c);

    const recentRejected = db.prepare("SELECT created_at, token_ca, gate_type, rejection_reason FROM rejected_signals ORDER BY created_at DESC LIMIT 3").all();
    if (recentRejected.length > 0) {
        console.log("最近记录:");
        recentRejected.forEach(r => {
            console.log("  ", (r.created_at || "").substring(11,19), "|", (r.token_ca || "").slice(0,8), "|", r.gate_type, "|", (r.rejection_reason || "").slice(0,30));
        });
    }
} catch(e) {
    console.log("❌ 错误:", e.message);
}

// 2. passed_signals 表
console.log("\n📊 passed_signals (通过的信号)");
console.log("─────────────────────────────────────────");
try {
    const passedCount = db.prepare("SELECT COUNT(*) as c FROM passed_signals").get();
    console.log("总记录数:", passedCount.c);

    const recentPassed = db.prepare("SELECT created_at, token_ca, decision FROM passed_signals ORDER BY created_at DESC LIMIT 3").all();
    if (recentPassed.length > 0) {
        console.log("最近记录:");
        recentPassed.forEach(r => {
            console.log("  ", (r.created_at || "").substring(11,19), "|", (r.token_ca || "").slice(0,8), "|", r.decision);
        });
    }
} catch(e) {
    console.log("❌ 错误:", e.message);
}

// 3. watch_signals 表
console.log("\n📊 watch_signals (观察中的信号)");
console.log("─────────────────────────────────────────");
try {
    const watchCount = db.prepare("SELECT COUNT(*) as c FROM watch_signals").get();
    console.log("总记录数:", watchCount.c);
} catch(e) {
    console.log("❌ 错误:", e.message);
}

// 4. positions 表关键字段检查
console.log("\n📊 positions 表字段完整性");
console.log("─────────────────────────────────────────");
try {
    const sample = db.prepare("SELECT * FROM positions ORDER BY id DESC LIMIT 1").get();
    if (sample) {
        const keyFields = [
            "entry_time", "exit_time", "entry_price", "exit_price",
            "pnl_percent", "exit_type", "signal_source", "hunter_type",
            "intention_tier", "tier_strategy", "alpha_tier"
        ];
        keyFields.forEach(f => {
            const hasValue = sample[f] != null;
            console.log("  ", hasValue ? "✅" : "❌", f.padEnd(18), ":", hasValue ? "有数据" : "空");
        });
    }
} catch(e) {
    console.log("❌ 错误:", e.message);
}

// 5. telegram_signals 表
console.log("\n📊 telegram_signals (TG信号)");
console.log("─────────────────────────────────────────");
try {
    const tgCount = db.prepare("SELECT COUNT(*) as c FROM telegram_signals").get();
    const tgToday = db.prepare("SELECT COUNT(*) as c FROM telegram_signals WHERE created_at >= date('now')").get();
    console.log("总记录数:", tgCount.c);
    console.log("今日记录:", tgToday.c);
} catch(e) {
    console.log("❌ 错误:", e.message);
}

// 6. hunter_signals 表
console.log("\n📊 hunter_signals (猎人信号)");
console.log("─────────────────────────────────────────");
try {
    const hunterCount = db.prepare("SELECT COUNT(*) as c FROM hunter_signals").get();
    console.log("总记录数:", hunterCount.c);
} catch(e) {
    console.log("❌ 表不存在或错误");
}

// 7. signal_snapshot_recorder 检查
console.log("\n📊 数据收集模块状态");
console.log("─────────────────────────────────────────");
try {
    // 检查最近1小时的数据
    const recentReject = db.prepare("SELECT COUNT(*) as c FROM rejected_signals WHERE created_at >= datetime('now', '-1 hour')").get();
    const recentPass = db.prepare("SELECT COUNT(*) as c FROM passed_signals WHERE created_at >= datetime('now', '-1 hour')").get();
    console.log("最近1小时:");
    console.log("  rejected_signals:", recentReject.c);
    console.log("  passed_signals:", recentPass.c);

    if (recentReject.c === 0 && recentPass.c === 0) {
        console.log("  ⚠️ 警告: 最近1小时没有新数据，可能recorder未正常工作");
    } else {
        console.log("  ✅ 数据收集正常");
    }
} catch(e) {
    console.log("❌ 错误:", e.message);
}

db.close();
