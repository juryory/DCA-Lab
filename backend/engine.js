function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let quoted = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"' && line[i + 1] === '"') {
      cur += '"';
      i += 1;
    } else if (ch === '"') {
      quoted = !quoted;
    } else if (ch === "," && !quoted) {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }

  out.push(cur);
  return out;
}

function parseCsv(text) {
  const lines = text.replace(/\r/g, "").split("\n").filter(Boolean);
  if (!lines.length) return [];

  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = splitCsvLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

function pick(row, names) {
  const map = {};
  Object.keys(row).forEach((k) => {
    map[String(k).toLowerCase()] = row[k];
  });

  for (const name of names) {
    const hit = map[String(name).toLowerCase()];
    if (hit !== undefined) return hit;
  }
  return null;
}

function parseFloatSafe(v) {
  const t = String(v ?? "").trim().replace(/,/g, "");
  if (!t) return null;
  const n = Number(t);
  return Number.isFinite(n) ? n : null;
}

function parseDateSafe(v) {
  const t = String(v ?? "").trim().replace(/\D/g, "");
  return t.length === 8 ? t : null;
}

function barsFromCsvText(text) {
  const map = new Map();
  const rows = parseCsv(text);

  rows.forEach((r) => {
    const tradeDate = parseDateSafe(pick(r, ["trade_date", "date"]));
    const close = parseFloatSafe(pick(r, ["close"]));
    if (!tradeDate || close === null) return;

    map.set(tradeDate, {
      tradeDate,
      close,
      open: parseFloatSafe(pick(r, ["open"])),
      high: parseFloatSafe(pick(r, ["high"])),
      low: parseFloatSafe(pick(r, ["low"])),
    });
  });

  return Array.from(map.values()).sort((a, b) => a.tradeDate.localeCompare(b.tradeDate));
}

// Index-based helpers: operate directly on array indices, zero allocations
function smaAt(arr, end, w) {
  let sum = 0;
  for (let i = end - w + 1; i <= end; i++) sum += arr[i];
  return sum / w;
}

function stdAt(arr, end, w) {
  const mean = smaAt(arr, end, w);
  let sum = 0;
  for (let i = end - w + 1; i <= end; i++) sum += (arr[i] - mean) ** 2;
  return Math.sqrt(sum / w);
}

function rsiAt(arr, end, p) {
  let sumGain = 0, sumLoss = 0;
  for (let i = end - p + 1; i <= end; i++) {
    const delta = arr[i] - arr[i - 1];
    if (delta > 0) sumGain += delta;
    else sumLoss -= delta;
  }
  const avgGain = sumGain / p;
  const avgLoss = sumLoss / p;
  if (avgLoss === 0) return 100;
  return 100 - 100 / (1 + avgGain / avgLoss);
}

function maxAt(arr, end, w) {
  let m = -Infinity;
  for (let i = end - w + 1; i <= end; i++) {
    if (arr[i] > m) m = arr[i];
  }
  return m;
}

function weekday(ymd) {
  const y = Number(ymd.slice(0, 4));
  const m = Number(ymd.slice(4, 6));
  const d = Number(ymd.slice(6, 8));
  const js = new Date(y, m - 1, d).getDay();
  return js === 0 ? 7 : js;
}

// Accepts pre-computed indicator values to avoid redundant calculations
function classify(last, ma20, ma60, rv, hi60, cfg) {
  const dd = hi60 > 0 ? (hi60 - last) / hi60 : 0;
  const v20 = (last - ma20) / ma20;
  const v60 = (last - ma60) / ma60;

  let cnt = 0;
  if (v20 <= cfg.discountToMa20) cnt++;
  if (rv <= cfg.oversoldRsi) cnt++;
  if (dd >= cfg.deepDrawdown) cnt++;
  if (v60 < 0) cnt++;
  if (cnt >= 2) return ["oversold", cfg.oversoldMultiplier];

  cnt = 0;
  if (v20 >= cfg.premiumToMa20) cnt++;
  if (rv >= cfg.hotRsi) cnt++;
  if (v60 > 0) cnt++;
  if (dd < cfg.drawdownWeak) cnt++;
  if (cnt >= 2) return ["hot", cfg.hotMultiplier];

  cnt = 0;
  if (v20 < 0) cnt++;
  if (rv >= cfg.oversoldRsi && rv <= cfg.weakRsiMax) cnt++;
  if (dd >= cfg.drawdownWeak && dd < cfg.deepDrawdown) cnt++;
  if (cnt >= 2) return ["weak", cfg.weakMultiplier];

  return ["neutral", cfg.neutralMultiplier];
}

function runBacktest(bars, cfg, { recordTrades = true } = {}) {
  const minBars = Math.max(cfg.maWindow, 60);
  if (bars.length < minBars + 2) {
    throw new Error("Not enough bar data");
  }

  let invested = 0;
  let units = 0;
  let cash = 0;
  let prevBelow = false;
  let addon = false;
  let tradeCount = 0;
  const pending = [];
  const closes = [];
  const trades = recordTrades ? [] : null;

  for (let i = 0; i < bars.length; i += 1) {
    const b = bars[i];

    if (pending.length) {
      const todays = pending.splice(0);
      for (let j = 0; j < todays.length; j++) {
        const o = todays[j];
        if (o.type === "buy") {
          const u = o.amount / b.close;
          units += u;
          invested += o.amount;
          tradeCount++;
          if (trades) trades.push({ d: b.tradeDate, p: b.close, a: o.amount, u, t: "BUY", r: o.reason });
        } else if (units > 0) {
          const su = units * o.pct;
          const sa = su * b.close;
          units -= su;
          cash += sa;
          tradeCount++;
          if (trades) trades.push({ d: b.tradeDate, p: b.close, a: sa, u: su, t: "SELL", r: o.reason });
        }
      }
    }

    closes.push(b.close);
    if (closes.length < minBars) continue;

    const ci = closes.length - 1;
    const ma = smaAt(closes, ci, cfg.maWindow);
    const ma20 = smaAt(closes, ci, 20);
    const bollUp = ma20 + 2 * stdAt(closes, ci, 20);
    const rv = rsiAt(closes, ci, 14);
    const ma60 = smaAt(closes, ci, 60);
    const hi60 = maxAt(closes, ci, 60);
    const [state, mult] = classify(b.close, ma20, ma60, rv, hi60, cfg);

    const intraday = cfg.buyMode === "intraday_break_same_close";
    const belowClose = b.close < ma;
    const belowIntra = (b.low ?? b.close) < ma;
    const below = intraday ? belowIntra : belowClose;
    const prev = i > 0 ? bars[i - 1].close : null;
    const hasNext = i + 1 < bars.length;

    const scheduled = cfg.scheduleMode === "weekly_weekday"
      ? weekday(b.tradeDate) === cfg.scheduleWeekday
      : i >= cfg.maWindow && (i - cfg.maWindow) % cfg.scheduleDays === 0;

    if (scheduled) {
      const rawAmount = cfg.dynamicEnabled ? cfg.baseAmount * mult : cfg.baseAmount;
      const amount = Math.max(100, Math.round(rawAmount / 100) * 100);
      if (intraday) {
        const u = amount / b.close;
        units += u;
        invested += amount;
        tradeCount++;
        if (trades) trades.push({ d: b.tradeDate, p: b.close, a: amount, u, t: "BUY", r: `SCHEDULED(${state})` });
      } else if (hasNext) {
        pending.push({ type: "buy", amount, reason: `SCHEDULED_NEXT(${state})` });
      }
    }

    const firstBreak = intraday ? below && !prevBelow : belowClose && !prevBelow;
    const followBreak = addon && below && prev !== null && b.close < prev;

    if (firstBreak || followBreak) {
      if (intraday) {
        const u = cfg.dipAmount / b.close;
        units += u;
        invested += cfg.dipAmount;
        tradeCount++;
        if (trades) trades.push({ d: b.tradeDate, p: b.close, a: cfg.dipAmount, u, t: "BUY", r: firstBreak ? "BREAK_ADD" : "FOLLOW_ADD" });
      } else if (hasNext) {
        pending.push({ type: "buy", amount: cfg.dipAmount, reason: firstBreak ? "BREAK_ADD_NEXT" : "FOLLOW_ADD_NEXT" });
      }
      addon = true;
    }

    if (!below) addon = false;
    prevBelow = belowClose;

    const prem = (b.close - ma20) / ma20;
    const s1 = units > 0 && prem >= cfg.sell1Premium && rv >= cfg.sell1Rsi;
    const s2 = units > 0 && prem >= cfg.sell2Premium && b.close >= bollUp * (1 + cfg.sell2BollBuffer);
    const s3 = units > 0 && prem >= cfg.sell3Premium && rv >= cfg.sell3Rsi;

    if (s1 && hasNext) pending.push({ type: "sell", pct: cfg.sell1Pct, reason: "SELL_1" });
    if (s2 && hasNext) pending.push({ type: "sell", pct: cfg.sell2Pct, reason: "SELL_2" });
    if (s3 && hasNext) pending.push({ type: "sell", pct: cfg.sell3Pct, reason: "SELL_3" });
  }

  const fp = bars.at(-1).close;
  const fv = units * fp + cash;
  return {
    config: cfg,
    totalInvested: invested,
    finalValue: fv,
    returnRate: invested > 0 ? (fv - invested) / invested : 0,
    averageCost: units > 0 ? (invested - cash) / units : 0,
    tradeCount,
    trades: trades || [],
  };
}

function sampleFrom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randFloat(min, max, precision = 4) {
  const v = min + Math.random() * (max - min);
  return Number(v.toFixed(precision));
}

function stepPrecision(step) {
  const s = String(step);
  const idx = s.indexOf(".");
  return idx >= 0 ? s.length - idx - 1 : 0;
}

function randIntStep(range, step = 1) {
  const min = Math.round(range.min);
  const max = Math.round(range.max);
  const start = Math.ceil(min / step) * step;
  const end = Math.floor(max / step) * step;
  if (start > end) return min;
  const count = Math.floor((end - start) / step);
  return start + randInt(0, count) * step;
}

function randFloatStep(range, step = 0.1) {
  const precision = stepPrecision(step);
  const start = Number((Math.ceil(range.min / step) * step).toFixed(precision));
  const end = Number((Math.floor(range.max / step) * step).toFixed(precision));
  if (start > end) return Number(range.min.toFixed(precision));
  const count = Math.floor(Number(((end - start) / step).toFixed(8)));
  return Number((start + randInt(0, count) * step).toFixed(precision));
}

const DEFAULT_RANGES = {
  baseAmount: { min: 300, max: 1000 },
  dipAmount: { min: 100, max: 200 },
  scheduleMode: ["every_n_days", "weekly_weekday"],
  scheduleDays: { min: 1, max: 30 },
  scheduleWeekday: [1, 2, 3, 4, 5],
  buyMode: ["close_confirm_next_close", "intraday_break_same_close"],
  maWindow: { min: 11, max: 60 },
  dynamicEnabled: [true, false],
  oversoldMultiplier: { min: 1.5, max: 3.0 },
  weakMultiplier: { min: 1.1, max: 1.4 },
  hotMultiplier: { min: 0.5, max: 0.9 },
  oversoldRsi: { min: 30, max: 38 },
  weakRsiMax: { min: 42, max: 48 },
  hotRsi: { min: 66, max: 72 },
  discountToMa20: { min: -0.05, max: -0.02 },
  premiumToMa20: { min: 0.04, max: 0.08 },
  drawdownWeak: { min: 0.03, max: 0.05 },
  deepDrawdown: { min: 0.06, max: 0.12 },
  sell1Premium: { min: 0.06, max: 0.14 },
  sell2Premium: { min: 0.09, max: 0.16 },
  sell3Premium: { min: 0.12, max: 0.20 },
  sell1Rsi: { min: 66, max: 74 },
  sell3Rsi: { min: 72, max: 80 },
  sell1Pct: { min: 0.08, max: 0.18 },
  sell2Pct: { min: 0.10, max: 0.20 },
  sell3Pct: { min: 0.12, max: 0.25 },
  sell2BollBuffer: { min: 0.0, max: 0.01 },
};

function normalizeRange(range, fallback) {
  const min = Number(range && range.min);
  const max = Number(range && range.max);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return { ...fallback };
  return min <= max ? { min, max } : { min: max, max: min };
}

function normalizeChoices(choices, fallback) {
  return Array.isArray(choices) && choices.length ? [...choices] : [...fallback];
}

function normalizeRanges(input = {}) {
  const out = {};
  Object.keys(DEFAULT_RANGES).forEach((k) => {
    const fallback = DEFAULT_RANGES[k];
    if (Array.isArray(fallback)) {
      out[k] = normalizeChoices(input[k], fallback);
    } else {
      out[k] = normalizeRange(input[k], fallback);
    }
  });
  return out;
}

function randIntIn(range) {
  return randInt(Math.round(range.min), Math.round(range.max));
}

function randFloatIn(range, precision = 4) {
  return randFloat(range.min, range.max, precision);
}

function randOrderedInt(prev, range, gap = 1) {
  const min = Math.max(Math.round(range.min), prev + gap);
  const max = Math.round(range.max);
  return min <= max ? randInt(min, max) : min;
}

function randOrderedFloat(prev, range, gap = 0.0001, precision = 4) {
  const min = Math.max(range.min, Number((prev + gap).toFixed(precision)));
  const max = range.max;
  return min <= max ? randFloat(min, max, precision) : Number(min.toFixed(precision));
}

function buildRandomConfig(customRanges = null) {
  const ranges = normalizeRanges(customRanges || {});
  const oversoldRsi = randIntIn(ranges.oversoldRsi);
  const weakRsiMax = randOrderedInt(oversoldRsi, ranges.weakRsiMax, 1);
  const hotRsi = randOrderedInt(weakRsiMax, ranges.hotRsi, 1);

  const sell1Premium = randFloatIn(ranges.sell1Premium, 4);
  const sell2Premium = randOrderedFloat(sell1Premium, ranges.sell2Premium, 0.001, 4);
  const sell3Premium = randOrderedFloat(sell2Premium, ranges.sell3Premium, 0.001, 4);

  const sell1Rsi = randIntIn(ranges.sell1Rsi);
  const sell3Rsi = randOrderedInt(sell1Rsi, ranges.sell3Rsi, 1);

  const sell1Pct = randFloatIn(ranges.sell1Pct, 4);
  const sell2Pct = randOrderedFloat(sell1Pct, ranges.sell2Pct, 0, 4);
  const sell3Pct = randOrderedFloat(sell2Pct, ranges.sell3Pct, 0, 4);

  return {
    baseAmount: randIntStep(ranges.baseAmount, 100),
    dipAmount: randIntStep(ranges.dipAmount, 100),
    scheduleMode: sampleFrom(ranges.scheduleMode),
    scheduleDays: randIntIn(ranges.scheduleDays),
    scheduleWeekday: sampleFrom(ranges.scheduleWeekday),
    buyMode: sampleFrom(ranges.buyMode),
    maWindow: randIntIn(ranges.maWindow),
    dynamicEnabled: sampleFrom(ranges.dynamicEnabled),
    oversoldMultiplier: randFloatStep(ranges.oversoldMultiplier, 0.1),
    weakMultiplier: randFloatStep(ranges.weakMultiplier, 0.1),
    neutralMultiplier: 1.0,
    hotMultiplier: randFloatStep(ranges.hotMultiplier, 0.1),
    oversoldRsi,
    weakRsiMax,
    hotRsi,
    discountToMa20: randFloatIn(ranges.discountToMa20, 4),
    premiumToMa20: randFloatIn(ranges.premiumToMa20, 4),
    drawdownWeak: randFloatIn(ranges.drawdownWeak, 4),
    deepDrawdown: randFloatIn(ranges.deepDrawdown, 4),
    sell1Premium,
    sell1Rsi,
    sell1Pct,
    sell2Premium,
    sell2Pct,
    sell2BollBuffer: randFloatIn(ranges.sell2BollBuffer, 4),
    sell3Premium,
    sell3Rsi,
    sell3Pct,
  };
}

function score(res, penaltyWeight) {
  return Number(((res.returnRate * 100) - (res.tradeCount * penaltyWeight)).toFixed(4));
}

// ============================================================
//  Portfolio (multi-asset) support
// ============================================================

const ASSET_KEYS = ["au9999", "csi300", "sp500"];
const ASSET_LABELS = { au9999: "黄金AU9999", csi300: "沪深300", sp500: "标普500" };

/**
 * Generate random portfolio weights that sum to 1.
 * Each weight is between 0.05 and 0.80, stepped by 0.05.
 */
function randomWeights() {
  const step = 0.05;
  const minW = 0.05;
  const maxW = 0.80;
  // generate 3 random values, then normalize
  let tries = 0;
  while (tries < 200) {
    tries++;
    const raw = ASSET_KEYS.map(() => {
      const steps = Math.floor((maxW - minW) / step) + 1;
      return minW + Math.floor(Math.random() * steps) * step;
    });
    const sum = raw.reduce((a, b) => a + b, 0);
    // normalize to 1.0, round to 0.05 steps
    const normed = raw.map((v) => Math.round((v / sum) / step) * step);
    const nSum = normed.reduce((a, b) => a + b, 0);
    // adjust last to make sum exactly 1
    normed[normed.length - 1] = Number((1 - normed.slice(0, -1).reduce((a, b) => a + b, 0)).toFixed(2));
    if (normed.every((w) => w >= minW && w <= maxW) && Math.abs(normed.reduce((a, b) => a + b, 0) - 1) < 0.001) {
      const weights = {};
      ASSET_KEYS.forEach((k, i) => { weights[k] = normed[i]; });
      return weights;
    }
  }
  // fallback: equal weight
  return { au9999: 0.35, csi300: 0.35, sp500: 0.30 };
}

/**
 * Build a random portfolio config: weights + shared strategy with per-asset amounts.
 * baseAmount/dipAmount are independent per asset; all other params are shared.
 */
function buildRandomPortfolioConfig(customRanges = null) {
  const weights = randomWeights();
  const sharedCfg = buildRandomConfig(customRanges);
  const ranges = normalizeRanges(customRanges || {});
  const configs = {};
  for (const key of ASSET_KEYS) {
    configs[key] = {
      ...sharedCfg,
      baseAmount: randIntStep(ranges.baseAmount, 100),
      dipAmount: randIntStep(ranges.dipAmount, 100),
    };
  }
  return { weights, configs };
}

/**
 * Run portfolio backtest across multiple assets.
 * @param {Object} barsMap - { au9999: bars[], csi300: bars[], sp500: bars[] }
 * @param {Object} portfolioCfg - { weights: {au9999:0.4,...}, configs: {au9999:cfg,...} }
 * @param {number} totalBase - total base investment amount per period (split by weights)
 * @returns portfolio result
 */
function runPortfolioBacktest(barsMap, portfolioCfg, totalBase = 1000) {
  const { weights, configs } = portfolioCfg;
  const results = {};
  let totalInvested = 0;
  let totalFinalValue = 0;
  let totalTrades = 0;
  const assetDetails = [];
  const actualConfigs = {};

  for (const key of ASSET_KEYS) {
    const bars = barsMap[key];
    const cfg = { ...configs[key] };
    const w = weights[key] || 0;
    // baseAmount scaled by weight; dipAmount per-asset; both rounded to 100
    cfg.baseAmount = Math.round(totalBase * w / 100) * 100;
    cfg.dipAmount = Math.round(cfg.dipAmount / 100) * 100;
    if (cfg.baseAmount < 100) cfg.baseAmount = 100;
    if (cfg.dipAmount < 100) cfg.dipAmount = 100;
    actualConfigs[key] = cfg;

    if (!bars || bars.length < Math.max(cfg.maWindow, 60) + 2) {
      assetDetails.push({ asset: key, label: ASSET_LABELS[key], weight: w, skipped: true, reason: "数据不足" });
      continue;
    }

    try {
      const res = runBacktest(bars, cfg, { recordTrades: false });
      results[key] = res;
      totalInvested += res.totalInvested;
      totalFinalValue += res.finalValue;
      totalTrades += res.tradeCount;
      assetDetails.push({
        asset: key, label: ASSET_LABELS[key], weight: w,
        totalInvested: res.totalInvested, finalValue: res.finalValue,
        returnRate: res.returnRate, tradeCount: res.tradeCount,
      });
    } catch (e) {
      assetDetails.push({ asset: key, label: ASSET_LABELS[key], weight: w, skipped: true, reason: e.message });
    }
  }

  const returnRate = totalInvested > 0 ? (totalFinalValue - totalInvested) / totalInvested : 0;

  return {
    weights,
    totalInvested,
    finalValue: totalFinalValue,
    returnRate,
    tradeCount: totalTrades,
    assetDetails,
    configs: actualConfigs,
  };
}

function portfolioScore(res, penaltyWeight) {
  return Number(((res.returnRate * 100) - (res.tradeCount * penaltyWeight)).toFixed(4));
}

// ============================================================
//  Genetic Algorithm: crossover + mutation
// ============================================================

// Numeric params that use {min,max} ranges
const NUMERIC_PARAMS = Object.keys(DEFAULT_RANGES).filter((k) => !Array.isArray(DEFAULT_RANGES[k]));
// Choice params that use arrays
const CHOICE_PARAMS = Object.keys(DEFAULT_RANGES).filter((k) => Array.isArray(DEFAULT_RANGES[k]));

/**
 * Crossover two single-asset configs: for each param, randomly pick from parent A or B.
 */
function crossoverConfig(a, b) {
  const child = {};
  for (const k of NUMERIC_PARAMS) {
    child[k] = Math.random() < 0.5 ? a[k] : b[k];
  }
  for (const k of CHOICE_PARAMS) {
    child[k] = Math.random() < 0.5 ? a[k] : b[k];
  }
  child.neutralMultiplier = 1.0;
  return child;
}

/**
 * Mutate a single-asset config: each numeric param has a chance to shift slightly.
 * mutationRate: probability each param mutates (0.15 = 15%)
 * mutationStrength: max relative shift (0.2 = ±20% of range width)
 */
function mutateConfig(cfg, mutationRate = 0.15, mutationStrength = 0.2) {
  const out = { ...cfg };
  for (const k of NUMERIC_PARAMS) {
    if (Math.random() >= mutationRate) continue;
    const range = DEFAULT_RANGES[k];
    const span = range.max - range.min;
    const delta = (Math.random() * 2 - 1) * mutationStrength * span;
    let v = out[k] + delta;
    v = Math.max(range.min, Math.min(range.max, v));
    // Round integers
    if (Number.isInteger(range.min) && Number.isInteger(range.max)) {
      v = Math.round(v);
    } else {
      v = Number(v.toFixed(4));
    }
    out[k] = v;
  }
  for (const k of CHOICE_PARAMS) {
    if (Math.random() < mutationRate) {
      out[k] = sampleFrom(DEFAULT_RANGES[k]);
    }
  }
  // Enforce 100-step rounding for amount fields
  if (out.baseAmount !== undefined) out.baseAmount = Math.round(out.baseAmount / 100) * 100;
  if (out.dipAmount !== undefined) out.dipAmount = Math.round(out.dipAmount / 100) * 100;
  // Enforce ordering constraints
  if (out.weakRsiMax <= out.oversoldRsi) out.weakRsiMax = out.oversoldRsi + 1;
  if (out.hotRsi <= out.weakRsiMax) out.hotRsi = out.weakRsiMax + 1;
  if (out.sell2Premium <= out.sell1Premium) out.sell2Premium = Number((out.sell1Premium + 0.001).toFixed(4));
  if (out.sell3Premium <= out.sell2Premium) out.sell3Premium = Number((out.sell2Premium + 0.001).toFixed(4));
  if (out.sell3Rsi <= out.sell1Rsi) out.sell3Rsi = out.sell1Rsi + 1;
  out.neutralMultiplier = 1.0;
  return out;
}

/**
 * Crossover two portfolio weights, then normalize to sum=1.
 */
function crossoverWeights(wa, wb) {
  const w = {};
  for (const k of ASSET_KEYS) {
    w[k] = Math.random() < 0.5 ? wa[k] : wb[k];
  }
  // normalize
  const sum = ASSET_KEYS.reduce((s, k) => s + (w[k] || 0), 0);
  for (const k of ASSET_KEYS) w[k] = Number(((w[k] || 0) / sum).toFixed(2));
  // fix rounding
  const diff = 1 - ASSET_KEYS.reduce((s, k) => s + w[k], 0);
  w[ASSET_KEYS[ASSET_KEYS.length - 1]] = Number((w[ASSET_KEYS[ASSET_KEYS.length - 1]] + diff).toFixed(2));
  return w;
}

/**
 * Mutate portfolio weights slightly.
 */
function mutateWeights(weights, mutationRate = 0.3, mutationStrength = 0.1) {
  const w = { ...weights };
  for (const k of ASSET_KEYS) {
    if (Math.random() < mutationRate) {
      w[k] = Math.max(0.05, Math.min(0.80, w[k] + (Math.random() * 2 - 1) * mutationStrength));
    }
  }
  const sum = ASSET_KEYS.reduce((s, k) => s + w[k], 0);
  for (const k of ASSET_KEYS) w[k] = Number((w[k] / sum).toFixed(2));
  const diff = 1 - ASSET_KEYS.reduce((s, k) => s + w[k], 0);
  w[ASSET_KEYS[ASSET_KEYS.length - 1]] = Number((w[ASSET_KEYS[ASSET_KEYS.length - 1]] + diff).toFixed(2));
  return w;
}

/**
 * Breed a new portfolio config from two parents via crossover + mutation.
 * Strategy params are shared (crossover from first asset); amounts are per-asset.
 */
function breedPortfolioConfig(parentA, parentB) {
  const weights = mutateWeights(crossoverWeights(parentA.weights, parentB.weights));
  // Shared strategy crossover from first asset's config
  const parentACfg = parentA.configs[ASSET_KEYS[0]];
  const parentBCfg = parentB.configs[ASSET_KEYS[0]];
  const sharedChild = mutateConfig(crossoverConfig(parentACfg, parentBCfg));
  const configs = {};
  for (const k of ASSET_KEYS) {
    // Per-asset amounts: crossover + mutation independently
    let ba = Math.random() < 0.5 ? parentA.configs[k].baseAmount : parentB.configs[k].baseAmount;
    let da = Math.random() < 0.5 ? parentA.configs[k].dipAmount : parentB.configs[k].dipAmount;
    if (Math.random() < 0.15) {
      const r = DEFAULT_RANGES.baseAmount;
      ba += (Math.random() * 2 - 1) * 0.2 * (r.max - r.min);
      ba = Math.max(r.min, Math.min(r.max, ba));
    }
    if (Math.random() < 0.15) {
      const r = DEFAULT_RANGES.dipAmount;
      da += (Math.random() * 2 - 1) * 0.2 * (r.max - r.min);
      da = Math.max(r.min, Math.min(r.max, da));
    }
    configs[k] = {
      ...sharedChild,
      baseAmount: Math.round(ba / 100) * 100,
      dipAmount: Math.round(da / 100) * 100,
    };
  }
  return { weights, configs };
}

module.exports = {
  DEFAULT_RANGES,
  ASSET_KEYS,
  ASSET_LABELS,
  barsFromCsvText,
  normalizeRanges,
  runBacktest,
  buildRandomConfig,
  buildRandomPortfolioConfig,
  runPortfolioBacktest,
  portfolioScore,
  breedPortfolioConfig,
  score,
};
