#!/usr/bin/env node
/**
 * AUSUB 验证脚本
 *
 * 从 Cloudflare Worker 拉取 pending 数据，本地重新回测验证，
 * 结果一致则 approve，不一致则 reject。
 *
 * 用法:
 *   node scripts/verify.js --api https://your-worker.workers.dev --token your_admin_token
 *
 * 可选:
 *   --auto      自动循环验证（每60秒检查一次）
 */

const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");
const {
  barsFromCsvText, ASSET_KEYS,
  runPortfolioBacktest, portfolioScore, runBacktest, score,
} = require("../backend/engine");

// --- CLI args ---
const args = process.argv.slice(2);
function getArg(name, defaultVal) {
  const idx = args.indexOf(`--${name}`);
  return idx >= 0 && args[idx + 1] ? args[idx + 1] : defaultVal;
}
const API_URL = getArg("api", "https://ausub-api.juryory.workers.dev").replace(/\/$/, "");
const ADMIN_TOKEN = getArg("token", "");
const AUTO_MODE = args.includes("--auto");
const TOLERANCE = 0.01; // 1% tolerance for floating point differences

if (!API_URL) {
  console.error("用法: node scripts/verify.js --api https://ausub-api.juryory.workers.dev --token TOKEN");
  process.exit(1);
}

// --- Load data ---
const dataDir = path.resolve(__dirname, "..", "data");
const assetBarsMap = {};
const csvFiles = {
  au9999: path.resolve(dataDir, "au9999_history.csv"),
  csi300: path.resolve(dataDir, "csi300_history.csv"),
  sp500: path.resolve(dataDir, "sp500_history.csv"),
};
for (const [key, filePath] of Object.entries(csvFiles)) {
  try {
    assetBarsMap[key] = barsFromCsvText(fs.readFileSync(filePath, "utf8"));
    console.log(`[Verify] loaded ${key}: ${assetBarsMap[key].length} bars`);
  } catch (err) {
    console.error(`[Verify] ${key} not found: ${err.message}`);
    process.exit(1);
  }
}

function yearFromTradeDate(d) { return Number(String(d).slice(0, 4)); }

function barsForRange(assetKey, sy, ey) {
  return (assetBarsMap[assetKey] || []).filter((b) => {
    const y = yearFromTradeDate(b.tradeDate);
    return y >= sy && y <= ey;
  });
}

function barsMapForRange(sy, ey) {
  const map = {};
  for (const k of ASSET_KEYS) map[k] = barsForRange(k, sy, ey);
  return map;
}

// --- HTTP helpers ---
function fetchJson(url, method = "GET", body = null, token = null) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === "https:" ? https : http;
    const headers = { "Content-Type": "application/json" };
    if (token) headers["Authorization"] = `Bearer ${token}`;
    const bodyStr = body ? JSON.stringify(body) : null;
    if (bodyStr) headers["Content-Length"] = Buffer.byteLength(bodyStr);

    const req = mod.request({
      hostname: parsed.hostname, port: parsed.port,
      path: parsed.pathname + parsed.search,
      method, headers,
    }, (res) => {
      let chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
        catch { resolve(null); }
      });
    });
    req.on("error", reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

// --- Verify a range entry ---
function verifyRangeEntry(entry) {
  const seg = entry.rangeKey.split("-");
  const sy = Number(seg[0]), ey = Number(seg[1]);
  if (!Number.isInteger(sy) || !Number.isInteger(ey)) return { ok: false, reason: "bad rangeKey" };

  const barsMap = barsMapForRange(sy, ey);
  const portfolioCfg = { weights: entry.weights, configs: entry.configs };

  try {
    const res = runPortfolioBacktest(barsMap, portfolioCfg);
    const s = portfolioScore(res, 0.08);
    const scoreDiff = Math.abs(s - entry.score);
    const rateDiff = Math.abs(res.returnRate - entry.returnRate);

    if (scoreDiff > Math.abs(entry.score) * TOLERANCE + 0.5) {
      return { ok: false, reason: `score mismatch: expected ${entry.score}, got ${s.toFixed(4)}` };
    }
    if (rateDiff > Math.abs(entry.returnRate) * TOLERANCE + 0.01) {
      return { ok: false, reason: `returnRate mismatch: expected ${entry.returnRate.toFixed(4)}, got ${res.returnRate.toFixed(4)}` };
    }
    return { ok: true, verifiedScore: s };
  } catch (err) {
    return { ok: false, reason: `backtest error: ${err.message}` };
  }
}

// --- Verify a robust entry ---
function verifyRobustEntry(entry) {
  // For robust entries, we can't fully re-verify because the random windows differ.
  // But we can check: run the same configs on the full data range and see if it's reasonable.
  if (!entry.configs || !entry.weights) return { ok: false, reason: "missing configs/weights" };
  if (entry.robustScore > 500 || entry.robustScore < -1000) return { ok: false, reason: "score out of reasonable range" };

  // Spot-check: run on full range
  const allYears = Object.values(assetBarsMap).flatMap((bars) => bars.map((b) => yearFromTradeDate(b.tradeDate)));
  const minY = Math.max(...ASSET_KEYS.map((k) => Math.min(...(assetBarsMap[k] || []).map((b) => yearFromTradeDate(b.tradeDate)))));
  const maxY = Math.min(...ASSET_KEYS.map((k) => Math.max(...(assetBarsMap[k] || []).map((b) => yearFromTradeDate(b.tradeDate)))));

  try {
    const barsMap = barsMapForRange(minY, maxY);
    const res = runPortfolioBacktest(barsMap, { weights: entry.weights, configs: entry.configs });
    // Just check it doesn't crash and returns reasonable values
    if (res.returnRate < -1 || res.returnRate > 100) {
      return { ok: false, reason: `unreasonable returnRate on full range: ${res.returnRate}` };
    }
    return { ok: true };
  } catch (err) {
    return { ok: false, reason: `backtest error: ${err.message}` };
  }
}

// --- Main verify loop ---
async function verifyAll() {
  console.log("[Verify] fetching pending data...");
  const data = await fetchJson(`${API_URL}/api/pending`, "GET", null, ADMIN_TOKEN);
  if (!data || !data.pending || !data.pending.length) {
    console.log("[Verify] no pending data");
    return;
  }

  console.log(`[Verify] ${data.pending.length} pending submissions`);

  for (const submission of data.pending) {
    const { key, entries } = submission;
    const isRange = key.startsWith("pending:range:");
    const isRobust = key.startsWith("pending:robust:");
    let allOk = true;
    let failReason = "";

    console.log(`[Verify] checking ${key} (${entries.length} entries)...`);

    for (const entry of entries) {
      let result;
      if (isRange) {
        result = verifyRangeEntry(entry);
      } else if (isRobust) {
        result = verifyRobustEntry(entry);
      } else {
        result = { ok: false, reason: "unknown type" };
      }

      if (!result.ok) {
        allOk = false;
        failReason = result.reason;
        break;
      }
    }

    const action = allOk ? "approve" : "reject";
    console.log(`[Verify] ${key}: ${action}${failReason ? ` (${failReason})` : ""}`);

    try {
      const resp = await fetchJson(`${API_URL}/api/verify`, "POST", { pendingKey: key, action }, ADMIN_TOKEN);
      console.log(`[Verify] ${action} response:`, resp.ok ? "success" : (resp.error || "failed"));
    } catch (err) {
      console.error(`[Verify] ${action} failed: ${err.message}`);
    }
  }
}

// --- Entry point ---
(async () => {
  if (AUTO_MODE) {
    console.log("[Verify] auto mode: checking every 60s");
    await verifyAll();
    setInterval(verifyAll, 60000);
  } else {
    await verifyAll();
    console.log("[Verify] done");
  }
})();
