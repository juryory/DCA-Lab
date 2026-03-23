#!/usr/bin/env node
/**
 * DCALab 验证脚本
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
const { execFile } = require("child_process");
const os = require("os");
const path = require("path");
const http = require("http");
const https = require("https");
const {
  barsFromCsvText, ASSET_KEYS,
  runPortfolioBacktest, portfolioScore, runBacktest, score,
} = require("../backend/engine");

function loadDotEnv() {
  const envPath = path.resolve(process.cwd(), ".env");
  if (!fs.existsSync(envPath)) return;
  const text = fs.readFileSync(envPath, "utf8");
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const idx = line.indexOf("=");
    if (idx < 0) continue;
    const key = line.slice(0, idx).trim();
    if (!key || process.env[key] !== undefined) continue;
    let value = line.slice(idx + 1).trim();
    if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

loadDotEnv();

// --- CLI args ---
const args = process.argv.slice(2);
function getArg(name, defaultVal) {
  const idx = args.indexOf(`--${name}`);
  return idx >= 0 && args[idx + 1] ? args[idx + 1] : defaultVal;
}
const API_URL = getArg("api", process.env.CLOUDFLARE_API_URL || "https://dcalab.juryory.workers.dev").replace(/\/$/, "");
const ADMIN_TOKEN = getArg("token", process.env.CLOUDFLARE_ADMIN_TOKEN || process.env.ADMIN_TOKEN || "");
const AUTO_MODE = args.includes("--auto");
const TOLERANCE = 0.01; // 1% tolerance for floating point differences

if (!API_URL) {
  console.error("用法: node scripts/verify.js --api https://dcalab.juryory.workers.dev --token TOKEN");
  process.exit(1);
}

if (!ADMIN_TOKEN) {
  console.error("Missing Cloudflare admin token. Set CLOUDFLARE_ADMIN_TOKEN in .env or pass --token.");
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

function parseRangeKey(rangeKey) {
  const seg = String(rangeKey || "").split("-");
  const sy = Number(seg[0]);
  const ey = Number(seg[1]);
  if (!Number.isInteger(sy) || !Number.isInteger(ey)) return null;
  return sy <= ey ? { sy, ey } : { sy: ey, ey: sy };
}

function robustScoreFromScores(scores) {
  const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
  const min = Math.min(...scores);
  const max = Math.max(...scores);
  const stdDev = Math.sqrt(scores.reduce((s, v) => s + (v - avg) ** 2, 0) / scores.length);
  return {
    robustScore: Number((avg * 0.5 + min * 0.3 - stdDev * 0.2).toFixed(4)),
    avgScore: Number(avg.toFixed(4)),
    minScore: Number(min.toFixed(4)),
    maxScore: Number(max.toFixed(4)),
    stdDev: Number(stdDev.toFixed(4)),
  };
}

// --- HTTP helpers ---
function isRetryableNetworkError(err) {
  if (!err) return false;
  if (err.code === "ETIMEDOUT" || err.code === "ECONNRESET" || err.code === "ECONNREFUSED") return true;
  if (Array.isArray(err.errors) && err.errors.some(isRetryableNetworkError)) return true;
  return /timeout|proxy|connect/i.test(String(err.message || ""));
}

function fetchJsonViaPowerShell(url, method = "GET", body = null, token = null) {
  return new Promise((resolve, reject) => {
    const responseFile = path.join(os.tmpdir(), `dcalab-verify-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.json`);
    const env = {
      ...process.env,
      REQ_URL: url,
      REQ_METHOD: method,
      REQ_BODY: body ? JSON.stringify(body) : "",
      REQ_TOKEN: token || "",
      RESP_FILE: responseFile,
    };
    const script = [
      "$ProgressPreference='SilentlyContinue'",
      "$headers = @{}",
      "if ($env:REQ_TOKEN) { $headers['Authorization'] = 'Bearer ' + $env:REQ_TOKEN }",
      "if ($env:REQ_BODY) {",
      "  $resp = Invoke-RestMethod -Uri $env:REQ_URL -Method $env:REQ_METHOD -Headers $headers -ContentType 'application/json' -Body $env:REQ_BODY",
      "} else {",
      "  $resp = Invoke-RestMethod -Uri $env:REQ_URL -Method $env:REQ_METHOD -Headers $headers",
      "}",
      "$json = $resp | ConvertTo-Json -Depth 100 -Compress",
      "[System.IO.File]::WriteAllText($env:RESP_FILE, $json, [System.Text.Encoding]::UTF8)",
    ].join("; ");
    execFile("powershell.exe", ["-NoProfile", "-Command", script], {
      env,
      windowsHide: true,
      maxBuffer: 16 * 1024 * 1024,
    }, (error, stdout, stderr) => {
      if (error) {
        try { if (fs.existsSync(responseFile)) fs.unlinkSync(responseFile); } catch (_) {}
        reject(new Error((stderr || error.message || "").trim() || "PowerShell request failed"));
        return;
      }
      let text = "";
      try {
        text = fs.existsSync(responseFile) ? fs.readFileSync(responseFile, "utf8").trim() : String(stdout || "").trim();
      } catch (readErr) {
        reject(readErr);
        return;
      } finally {
        try { if (fs.existsSync(responseFile)) fs.unlinkSync(responseFile); } catch (_) {}
      }
      if (!text) {
        resolve(null);
        return;
      }
      try {
        resolve(JSON.parse(text));
      } catch {
        resolve(null);
      }
    });
  });
}

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
  }).catch((err) => {
    if (process.platform !== "win32" || !isRetryableNetworkError(err)) throw err;
    console.warn(`[Verify] native HTTP failed, retrying via PowerShell: ${err.code || err.message}`);
    return fetchJsonViaPowerShell(url, method, body, token);
  });
}

// --- Verify a range entry ---
function verifyRangeEntry(entry) {
  const parsedRange = parseRangeKey(entry.rangeKey);
  if (!parsedRange) return { ok: false, reason: "bad rangeKey" };
  const { sy, ey } = parsedRange;

  const barsMap = barsMapForRange(sy, ey);
  const portfolioCfg = { weights: entry.weights, configs: entry.configs };
  const penaltyWeight = Number.isFinite(entry.penaltyWeight) ? entry.penaltyWeight : 0.08;

  try {
    const res = runPortfolioBacktest(barsMap, portfolioCfg);
    const s = portfolioScore(res, penaltyWeight);
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
  if (!entry.configs || !entry.weights) return { ok: false, reason: "missing configs/weights" };
  if (!Array.isArray(entry.windows) || entry.windows.length < 2) return { ok: false, reason: "missing windows" };
  if (entry.robustScore > 500 || entry.robustScore < -1000) return { ok: false, reason: "score out of reasonable range" };
  const penaltyWeight = Number.isFinite(entry.penaltyWeight) ? entry.penaltyWeight : 0.08;
  const scores = [];

  try {
    for (const rangeKey of entry.windows) {
      const parsedRange = parseRangeKey(rangeKey);
      if (!parsedRange) return { ok: false, reason: `bad window: ${rangeKey}` };
      const barsMap = barsMapForRange(parsedRange.sy, parsedRange.ey);
      const res = runPortfolioBacktest(barsMap, { weights: entry.weights, configs: entry.configs });
      if (res.returnRate < -1 || res.returnRate > 100) {
        return { ok: false, reason: `unreasonable returnRate in window ${rangeKey}: ${res.returnRate}` };
      }
      scores.push(portfolioScore(res, penaltyWeight));
    }
    if (scores.length < 2) return { ok: false, reason: "not enough valid windows" };
    const recomputed = robustScoreFromScores(scores);
    const checks = [
      ["robustScore", recomputed.robustScore, entry.robustScore, 0.5],
      ["avgScore", recomputed.avgScore, entry.avgScore, 0.5],
      ["minScore", recomputed.minScore, entry.minScore, 0.5],
      ["maxScore", recomputed.maxScore, entry.maxScore, 0.5],
      ["stdDev", recomputed.stdDev, entry.stdDev, 0.2],
    ];
    for (const [name, actual, expected, baseTolerance] of checks) {
      if (typeof expected !== "number") continue;
      const diff = Math.abs(actual - expected);
      if (diff > Math.abs(expected) * TOLERANCE + baseTolerance) {
        return { ok: false, reason: `${name} mismatch: expected ${expected}, got ${actual}` };
      }
    }
    return { ok: true, verifiedScore: recomputed.robustScore };
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
