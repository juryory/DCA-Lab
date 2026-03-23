#!/usr/bin/env node
/**
 * AUSUB 分布式计算节点
 *
 * 独立运行，跑回测计算，定期将好结果提交到 Cloudflare Worker API。
 *
 * 用法:
 *   node compute_node.js [--api https://your-worker.workers.dev] [--interval 30]
 *
 * 参数:
 *   --api       Worker API 地址（必填）
 *   --interval  提交间隔（秒），默认 30
 *   --batch     每轮回测数量，默认 50
 *   --penalty   惩罚权重，默认 0.08
 */

const { Worker } = require("worker_threads");
const path = require("path");
const fs = require("fs");
const http = require("http");
const https = require("https");

// --- Parse CLI args ---
const args = process.argv.slice(2);
function getArg(name, defaultVal) {
  const idx = args.indexOf(`--${name}`);
  return idx >= 0 && args[idx + 1] ? args[idx + 1] : defaultVal;
}

const API_URL = getArg("api", "https://ausub-api.juryory.workers.dev").replace(/\/$/, "");
const SUBMIT_INTERVAL = Number(getArg("interval", "30")) * 1000;
const BATCH = Number(getArg("batch", "50"));
const PENALTY = Number(getArg("penalty", "0.08"));

if (!API_URL) {
  console.error("请指定 Worker API 地址: node compute_node.js --api https://ausub-api.juryory.workers.dev");
  process.exit(1);
}

// --- Load CSV data ---
const dataDir = path.resolve(__dirname, "data");
const csvFiles = {
  au9999: path.resolve(dataDir, "au9999_history.csv"),
  csi300: path.resolve(dataDir, "csi300_history.csv"),
  sp500: path.resolve(dataDir, "sp500_history.csv"),
};

const assetCsvTexts = {};
for (const [key, filePath] of Object.entries(csvFiles)) {
  try {
    assetCsvTexts[key] = fs.readFileSync(filePath, "utf8");
    console.log(`[Node] loaded ${key}: ${filePath}`);
  } catch (err) {
    console.error(`[Node] ${key} data not found: ${err.message}`);
    process.exit(1);
  }
}

// --- Result buffers ---
let rangeBuffer = [];
let robustBuffer = [];
const RANGE_BUFFER_MAX = 50;
const ROBUST_BUFFER_MAX = 50;

// --- HTTP POST helper ---
function postJson(url, data) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === "https:" ? https : http;
    const body = JSON.stringify(data);
    const req = mod.request({
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
      },
    }, (res) => {
      let chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
        catch { resolve({ raw: Buffer.concat(chunks).toString() }); }
      });
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

// --- Submit buffered results ---
async function submitResults() {
  if (rangeBuffer.length > 0) {
    const toSend = rangeBuffer.splice(0, RANGE_BUFFER_MAX);
    try {
      const resp = await postJson(`${API_URL}/api/submit/range`, { results: toSend });
      console.log(`[Node] submitted ${toSend.length} range results:`, resp.ok ? "accepted" : resp.error);
    } catch (err) {
      console.error(`[Node] range submit failed: ${err.message}`);
      rangeBuffer.unshift(...toSend); // put back
    }
  }

  if (robustBuffer.length > 0) {
    const toSend = robustBuffer.splice(0, ROBUST_BUFFER_MAX);
    try {
      const resp = await postJson(`${API_URL}/api/submit/robust`, { results: toSend });
      console.log(`[Node] submitted ${toSend.length} robust results:`, resp.ok ? "accepted" : resp.error);
    } catch (err) {
      console.error(`[Node] robust submit failed: ${err.message}`);
      robustBuffer.unshift(...toSend);
    }
  }
}

// --- Start workers ---
function startWorker(workerFile, label, onResults) {
  const worker = new Worker(path.resolve(__dirname, "backend", workerFile), {
    workerData: { assetCsvTexts, batch: BATCH, penalty: PENALTY, windowCount: 10 },
  });

  worker.on("message", (msg) => {
    if (msg.ready) {
      console.log(`[Node] ${label} worker ready (${msg.minYear}-${msg.maxYear}, ${msg.assetCount} assets)`);
      worker.postMessage({ cmd: "start", batch: BATCH, penalty: PENALTY, windowCount: 10 });
      return;
    }
    if (msg.results) {
      onResults(msg.results);
    }
  });

  worker.on("error", (err) => {
    console.error(`[Node] ${label} worker error: ${err.message}`);
  });

  worker.on("exit", (code) => {
    console.warn(`[Node] ${label} worker exited (code ${code}), restarting in 3s...`);
    setTimeout(() => startWorker(workerFile, label, onResults), 3000);
  });

  return worker;
}

// Keep only top results in buffer (no point submitting bad ones)
function addToRangeBuffer(results) {
  for (const r of results) {
    rangeBuffer.push(r);
  }
  rangeBuffer.sort((a, b) => b.score - a.score);
  if (rangeBuffer.length > RANGE_BUFFER_MAX) rangeBuffer.length = RANGE_BUFFER_MAX;
}

function addToRobustBuffer(results) {
  for (const r of results) {
    robustBuffer.push(r);
  }
  robustBuffer.sort((a, b) => b.robustScore - a.robustScore);
  if (robustBuffer.length > ROBUST_BUFFER_MAX) robustBuffer.length = ROBUST_BUFFER_MAX;
}

// --- Main ---
console.log(`[Node] AUSUB compute node starting`);
console.log(`[Node] API: ${API_URL}`);
console.log(`[Node] Submit interval: ${SUBMIT_INTERVAL / 1000}s, batch: ${BATCH}, penalty: ${PENALTY}`);

startWorker("worker_range.js", "Range", addToRangeBuffer);
startWorker("worker_robust.js", "Robust", addToRobustBuffer);

// Periodic submit
setInterval(submitResults, SUBMIT_INTERVAL);

// Stats
let lastRangeCount = 0, lastRobustCount = 0;
setInterval(() => {
  const rc = rangeBuffer.length, rbc = robustBuffer.length;
  console.log(`[Node] buffer: range=${rc} robust=${rbc} | best range score: ${rc ? rangeBuffer[0].score.toFixed(2) : "-"} | best robust score: ${rbc ? robustBuffer[0].robustScore.toFixed(2) : "-"}`);
}, 10000);

console.log("[Node] workers started, computing...");
