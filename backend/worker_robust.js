const { parentPort, workerData } = require("worker_threads");
const {
  barsFromCsvText, ASSET_KEYS,
  buildRandomPortfolioConfig, runPortfolioBacktest, portfolioScore,
  breedPortfolioConfig,
} = require("./engine");

// Load all asset data
const assetBars = {};
const assetCsvTexts = workerData.assetCsvTexts || {};
for (const key of ASSET_KEYS) {
  if (assetCsvTexts[key]) {
    assetBars[key] = barsFromCsvText(assetCsvTexts[key]);
  }
}

let running = false;
let batch = workerData.batch || 200;
let penalty = workerData.penalty || 0.01;
let windowCount = workerData.windowCount || 15;

parentPort.on("message", (msg) => {
  if (msg.cmd === "start") {
    batch = msg.batch || batch;
    penalty = msg.penalty || penalty;
    windowCount = msg.windowCount || windowCount;
    if (!running) { running = true; loop(); }
  } else if (msg.cmd === "stop") {
    running = false;
  }
});

function yearFromTradeDate(d) { return Number(String(d).slice(0, 4)); }

// Compute intersection year range
const assetYearRanges = {};
const loadedKeys = [];
for (const key of ASSET_KEYS) {
  const bars = assetBars[key];
  if (!bars || !bars.length) continue;
  const years = bars.map((b) => yearFromTradeDate(b.tradeDate));
  assetYearRanges[key] = { min: Math.min(...years), max: Math.max(...years) };
  loadedKeys.push(key);
}
const minYear = loadedKeys.length ? Math.max(...loadedKeys.map((k) => assetYearRanges[k].min)) : 2008;
const maxYear = loadedKeys.length ? Math.min(...loadedKeys.map((k) => assetYearRanges[k].max)) : 2026;

// Range cache per asset
const rangeCaches = {};
for (const key of ASSET_KEYS) rangeCaches[key] = new Map();

function barsForRange(assetKey, sy, ey) {
  const cache = rangeCaches[assetKey];
  const cacheKey = `${sy}-${ey}`;
  if (cache.has(cacheKey)) return cache.get(cacheKey);
  const bars = assetBars[assetKey] || [];
  const sub = bars.filter((b) => {
    const y = yearFromTradeDate(b.tradeDate);
    return y >= sy && y <= ey;
  });
  if (cache.size > 80) { cache.delete(cache.keys().next().value); }
  cache.set(cacheKey, sub);
  return sub;
}

function barsMapForRange(sy, ey) {
  const map = {};
  for (const key of ASSET_KEYS) {
    map[key] = barsForRange(key, sy, ey);
  }
  return map;
}

function buildWindows(windowYears) {
  const span = maxYear - minYear + 1;
  if (span < windowYears) return [];
  const windows = [];
  const seen = new Set();
  let tries = 0;
  while (windows.length < windowCount && tries < windowCount * 5) {
    tries++;
    const startMax = maxYear - windowYears + 1;
    if (startMax < minYear) break;
    const sy = minYear + Math.floor(Math.random() * (startMax - minYear + 1));
    const ey = sy + windowYears - 1;
    const key = `${sy}-${ey}`;
    if (seen.has(key)) continue;
    seen.add(key);
    windows.push([sy, ey]);
  }
  return windows;
}

function evaluateRobust(portfolioCfg, windows) {
  const scores = [];
  const details = [];
  for (const [sy, ey] of windows) {
    const barsMap = barsMapForRange(sy, ey);
    const hasEnough = ASSET_KEYS.every((k) => (barsMap[k] || []).length >= 120);
    if (!hasEnough) continue;
    try {
      const res = runPortfolioBacktest(barsMap, portfolioCfg);
      const s = portfolioScore(res, penalty);
      scores.push(s);
      details.push({
        range: `${sy}-${ey}`, score: s, returnRate: res.returnRate,
        totalInvested: res.totalInvested, finalValue: res.finalValue,
        tradeCount: res.tradeCount, assetDetails: res.assetDetails,
      });
    } catch (_) { /* skip */ }
  }
  if (scores.length < 2) return null;
  const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
  const min = Math.min(...scores);
  const stdDev = Math.sqrt(scores.reduce((s, v) => s + (v - avg) ** 2, 0) / scores.length);
  const robustScore = Number((avg * 0.5 + min * 0.3 - stdDev * 0.2).toFixed(4));
  return {
    robustScore, avgScore: Number(avg.toFixed(4)), minScore: Number(min.toFixed(4)),
    maxScore: Number(Math.max(...scores).toFixed(4)), stdDev: Number(stdDev.toFixed(4)),
    windowCount: scores.length, windows: windows.map(([sy, ey]) => `${sy}-${ey}`), details,
    penaltyWeight: penalty,
    weights: portfolioCfg.weights, configs: portfolioCfg.configs,
  };
}

// Elite pool for genetic algorithm (per year-window)
const elitePools = {}; // { yr: [{robustScore, portfolioCfg}] }
for (let y = 1; y <= 10; y++) elitePools[y] = [];
const ELITE_SIZE = 20;
const RANDOM_RATIO = 0.3;

function pickElite(pool) {
  const a = pool[Math.floor(Math.random() * pool.length)];
  const b = pool[Math.floor(Math.random() * pool.length)];
  return a.robustScore >= b.robustScore ? a : b;
}

function generateConfig(yr) {
  const pool = elitePools[yr] || [];
  if (pool.length < 4 || Math.random() < RANDOM_RATIO) {
    return buildRandomPortfolioConfig();
  }
  const pA = pickElite(pool);
  const pB = pickElite(pool);
  return breedPortfolioConfig(pA.portfolioCfg, pB.portfolioCfg);
}

function addToElite(yr, robustScore, portfolioCfg) {
  const pool = elitePools[yr];
  if (!pool) return;
  pool.push({ robustScore, portfolioCfg });
  pool.sort((a, b) => b.robustScore - a.robustScore);
  if (pool.length > ELITE_SIZE) pool.length = ELITE_SIZE;
}

function loop() {
  if (!running) return;
  const results = [];
  for (let i = 0; i < batch; i++) {
    try {
      for (let yr = 1; yr <= 10; yr++) {
        const portfolioCfg = generateConfig(yr);
        const windows = buildWindows(yr);
        if (windows.length < 2) continue;
        const result = evaluateRobust(portfolioCfg, windows);
        if (result) {
          result.windowYears = yr;
          addToElite(yr, result.robustScore, portfolioCfg);
          results.push(result);
        }
      }
    } catch (_) { /* skip */ }
  }
  if (results.length) parentPort.postMessage({ results });
  setImmediate(loop);
}

parentPort.postMessage({ ready: true, minYear, maxYear, assetCount: loadedKeys.length });
