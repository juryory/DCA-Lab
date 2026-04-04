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
let batch = workerData.batch || 500;
let penalty = workerData.penalty || 0.01;

parentPort.on("message", (msg) => {
  if (msg.cmd === "start") {
    batch = msg.batch || batch;
    penalty = msg.penalty || penalty;
    if (!running) { running = true; loop(); }
  } else if (msg.cmd === "stop") {
    running = false;
  }
});

function yearFromTradeDate(d) { return Number(String(d).slice(0, 4)); }

// Compute per-asset year ranges
const assetYearRanges = {};
let globalMinYear = Infinity, globalMaxYear = -Infinity;
for (const key of ASSET_KEYS) {
  const bars = assetBars[key];
  if (!bars || !bars.length) continue;
  const years = bars.map((b) => yearFromTradeDate(b.tradeDate));
  const min = Math.min(...years), max = Math.max(...years);
  assetYearRanges[key] = { min, max };
  if (min > globalMinYear) globalMinYear = min; // intersection: take latest start
  if (max < globalMaxYear) globalMaxYear = max; // intersection: take earliest end
}
// For intersection: minYear = max of all mins, maxYear = min of all maxes
const loadedKeys = Object.keys(assetYearRanges);
if (loadedKeys.length) {
  globalMinYear = Math.max(...loadedKeys.map((k) => assetYearRanges[k].min));
  globalMaxYear = Math.min(...loadedKeys.map((k) => assetYearRanges[k].max));
}
const minYear = globalMinYear;
const maxYear = globalMaxYear;

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

// Elite pool for genetic algorithm
const elitePool = []; // {score, portfolioCfg}
const ELITE_SIZE = 20;
const RANDOM_RATIO = 0.3; // 30% pure random, 70% breed from elites

function pickElite() {
  const a = elitePool[Math.floor(Math.random() * elitePool.length)];
  const b = elitePool[Math.floor(Math.random() * elitePool.length)];
  return a.score >= b.score ? a : b;
}

function generateConfig() {
  if (elitePool.length < 4 || Math.random() < RANDOM_RATIO) {
    return buildRandomPortfolioConfig();
  }
  const pA = pickElite();
  const pB = pickElite();
  return breedPortfolioConfig(pA.portfolioCfg, pB.portfolioCfg);
}

function addToElite(score, portfolioCfg) {
  elitePool.push({ score, portfolioCfg });
  elitePool.sort((a, b) => b.score - a.score);
  if (elitePool.length > ELITE_SIZE) elitePool.length = ELITE_SIZE;
}

function loop() {
  if (!running) return;
  const results = [];
  for (let i = 0; i < batch; i++) {
    try {
      const y1 = minYear + Math.floor(Math.random() * (maxYear - minYear + 1));
      const y2 = minYear + Math.floor(Math.random() * (maxYear - minYear + 1));
      const [sy, ey] = y1 <= y2 ? [y1, y2] : [y2, y1];
      const barsMap = barsMapForRange(sy, ey);
      const hasEnough = ASSET_KEYS.every((k) => (barsMap[k] || []).length >= 120);
      if (!hasEnough) continue;

      const portfolioCfg = generateConfig();
      const res = runPortfolioBacktest(barsMap, portfolioCfg);
      const s = portfolioScore(res, penalty);
      addToElite(s, portfolioCfg);
      results.push({
        rangeKey: `${sy}-${ey}`, startYear: sy, endYear: ey,
        score: s, returnRate: res.returnRate,
        totalInvested: res.totalInvested, finalValue: res.finalValue,
        tradeCount: res.tradeCount,
        penaltyWeight: penalty,
        weights: res.weights, assetDetails: res.assetDetails,
        configs: res.configs,
      });

      // Also test full range
      if (sy !== minYear || ey !== maxYear) {
        const fullMap = barsMapForRange(minYear, maxYear);
        const fullEnough = ASSET_KEYS.every((k) => (fullMap[k] || []).length >= 120);
        if (fullEnough) {
          const res2 = runPortfolioBacktest(fullMap, portfolioCfg);
          const s2 = portfolioScore(res2, penalty);
          addToElite(s2, portfolioCfg);
          results.push({
            rangeKey: `${minYear}-${maxYear}`, startYear: minYear, endYear: maxYear,
            score: s2, returnRate: res2.returnRate,
            totalInvested: res2.totalInvested, finalValue: res2.finalValue,
            tradeCount: res2.tradeCount,
            penaltyWeight: penalty,
            weights: res2.weights, assetDetails: res2.assetDetails,
            configs: res2.configs,
          });
        }
      }
    } catch (_) { /* skip */ }
  }
  if (results.length) parentPort.postMessage({ results });
  setImmediate(loop);
}

parentPort.postMessage({ ready: true, minYear, maxYear, assetCount: loadedKeys.length });
