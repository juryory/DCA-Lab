const fs = require("fs");
const os = require("os");
const path = require("path");
const { Worker } = require("worker_threads");
const { barsFromCsvText, ASSET_KEYS } = require("./engine");

function nowBeijing() {
  return new Date().toLocaleString("sv-SE", { timeZone: "Asia/Shanghai" }).replace(" ", "T");
}

function yearFromTradeDate(yyyymmdd) {
  return Number(String(yyyymmdd).slice(0, 4));
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function envInt(name) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || raw === "") return null;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function cpuCount() {
  try {
    if (typeof os.availableParallelism === "function") return os.availableParallelism();
  } catch (_) {}
  const cpus = os.cpus();
  return Array.isArray(cpus) && cpus.length ? cpus.length : 1;
}

function tieredDefaultWorkerCount(limit = 2) {
  const cpus = cpuCount();
  let suggested = 1;
  if (cpus >= 16) suggested = 4;
  else if (cpus >= 8) suggested = 2;
  else suggested = 1;
  return Math.min(limit, suggested);
}

function configuredWorkerCount(envName, fallbackLimit = 2) {
  return envInt(envName) || tieredDefaultWorkerCount(fallbackLimit);
}

function barsToCsvText(bars) {
  const header = "trade_date,open,high,low,close";
  const lines = bars.map((b) => {
    const v = (n) => (n === null || n === undefined || Number.isNaN(n) ? "" : String(n));
    return `${b.tradeDate},${v(b.open)},${v(b.high)},${v(b.low)},${v(b.close)}`;
  });
  return [header, ...lines].join("\n");
}

// ============================================================
//  RandomRunner - range optimal search (1 dedicated worker)
// ============================================================
class RandomRunner {
  constructor() {
    this.running = false;
    this.workers = new Map();
    this.workerSeq = 0;
    this.lastError = null;
    this.attempts = 0;
    this.valid = 0;
    this.skipped = 0;
    this.batch = 100;
    this.penalty = 0.08;
    this.topLimit = 10;
    this.workerCount = configuredWorkerCount("RANGE_WORKERS", 4);

    this.dataDir = path.resolve(process.cwd(), "data");
    this.csvPath = path.resolve(this.dataDir, "au9999_history.csv");
    this.storePath = path.resolve(this.dataDir, "leaderboard_store.json");
    ensureDir(this.dataDir);

    this.bars = [];
    this.csvText = "";
    this.minYear = null;
    this.maxYear = null;
    this.rangeBarsCache = new Map();
    this._cacheOrder = [];

    // Multi-asset data
    this.assetCsvTexts = {};
    this.assetBars = {};
    this.assetYears = {};

    this.store = {
      meta: { updatedAt: nowBeijing(), csvPath: this.csvPath, minYear: null, maxYear: null },
      all: [],
      ranges: {},
    };

    this._dirty = false;
    this._lastSaveAt = 0;
    this._loadStore();
    try {
      this.loadBars(this.csvPath);
    } catch (_) {
      const fallback = path.resolve(this.dataDir, "market_history.csv");
      try {
        this.loadBars(fallback);
      } catch (err) {
        console.warn(`[Runner] CSV not found, waiting for upload: ${err.message}`);
      }
    }
    this._loadAllAssets();
  }

  _loadAllAssets() {
    const csvFiles = {
      au9999: path.resolve(this.dataDir, "au9999_history.csv"),
      csi300: path.resolve(this.dataDir, "csi300_history.csv"),
      sp500: path.resolve(this.dataDir, "sp500_history.csv"),
    };
    for (const key of ASSET_KEYS) {
      try {
        const text = fs.readFileSync(csvFiles[key], "utf8");
        const bars = barsFromCsvText(text);
        if (bars.length) {
          this.assetCsvTexts[key] = text;
          this.assetBars[key] = bars;
          const years = bars.map((b) => yearFromTradeDate(b.tradeDate));
          this.assetYears[key] = { min: Math.min(...years), max: Math.max(...years) };
          console.log(`[Runner] ${key}: ${bars.length} bars, ${this.assetYears[key].min}-${this.assetYears[key].max}`);
        }
      } catch (err) {
        console.warn(`[Runner] ${key} data not found: ${err.message}`);
      }
    }
  }

  // --- RandomRunner store ---
  _loadStore() {
    if (!fs.existsSync(this.storePath)) return;
    try {
      const raw = fs.readFileSync(this.storePath, "utf8");
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") {
        this.store = {
          meta: parsed.meta || this.store.meta,
          all: Array.isArray(parsed.all) ? parsed.all : [],
          ranges: parsed.ranges && typeof parsed.ranges === "object" ? parsed.ranges : {},
        };
      }
    } catch (err) {
      console.warn(`[Runner] load store failed: ${err.message}`);
    }
  }

  _saveStore(force = false) {
    const now = Date.now();
    if (!force && (!this._dirty || now - this._lastSaveAt < 1500)) return;
    try {
      this.store.meta.updatedAt = nowBeijing();
      this.store.meta.csvPath = this.csvPath;
      this.store.meta.minYear = this.minYear;
      this.store.meta.maxYear = this.maxYear;
      const tmp = this.storePath + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify(this.store, null, 2), "utf8");
      fs.renameSync(tmp, this.storePath);
      this._dirty = false;
      this._lastSaveAt = now;
    } catch (err) {
      console.error(`[Runner] save store failed: ${err.message}`);
    }
  }

  _resetResultStore() {
    this.store = {
      meta: { updatedAt: nowBeijing(), csvPath: this.csvPath, minYear: this.minYear, maxYear: this.maxYear },
      all: [],
      ranges: {},
    };
    this._dirty = true;
    this._saveStore(true);
  }

  loadBars(csvPath) {
    const resolved = path.resolve(process.cwd(), csvPath || this.csvPath);
    const text = fs.readFileSync(resolved, "utf8");
    const bars = barsFromCsvText(text);
    if (!bars.length) throw new Error("CSV has no valid data: " + resolved);
    this.csvPath = resolved;
    this.csvText = text;
    this.bars = bars;
    const years = bars.map((b) => yearFromTradeDate(b.tradeDate));
    this.minYear = Math.min(...years);
    this.maxYear = Math.max(...years);
    this.rangeBarsCache.clear();
    this._cacheOrder = [];
    console.log(`[Runner] loaded ${bars.length} bars, ${this.minYear}-${this.maxYear}`);
  }

  uploadCsv(filename, text) {
    if (!text || !String(text).trim()) throw new Error("CSV content is empty");
    const incoming = barsFromCsvText(text);
    if (!incoming.length) throw new Error("No parseable data in uploaded file");

    const oldMinYear = this.minYear;
    const oldMaxYear = this.maxYear;
    const existingMap = new Map(this.bars.map((b) => [b.tradeDate, b]));

    let added = 0, ignored = 0, addedMinYear = null, addedMaxYear = null;
    incoming.forEach((row) => {
      if (existingMap.has(row.tradeDate)) { ignored += 1; return; }
      existingMap.set(row.tradeDate, row);
      added += 1;
      const y = yearFromTradeDate(row.tradeDate);
      addedMinYear = addedMinYear === null ? y : Math.min(addedMinYear, y);
      addedMaxYear = addedMaxYear === null ? y : Math.max(addedMaxYear, y);
    });

    if (added === 0) {
      return { csvPath: this.csvPath, minYear: this.minYear, maxYear: this.maxYear, addedRows: 0, ignoredRows: ignored, message: "No new dates found" };
    }

    const mergedBars = Array.from(existingMap.values()).sort((a, b) => a.tradeDate.localeCompare(b.tradeDate));
    const dest = path.resolve(this.dataDir, "market_history.csv");
    fs.writeFileSync(dest, barsToCsvText(mergedBars), "utf8");

    const wasRunning = this.running;
    if (wasRunning) this.stop();
    this.loadBars(dest);
    this.attempts = 0;
    this.valid = 0;
    this.skipped = 0;
    this.lastError = null;

    if (addedMinYear !== null) {
      const nextRanges = {};
      Object.keys(this.store.ranges).forEach((k) => {
        const seg = k.split("-");
        const sy = Number(seg[0]), ey = Number(seg[1]);
        if (Number.isInteger(sy) && Number.isInteger(ey) && ey < addedMinYear) nextRanges[k] = this.store.ranges[k];
      });
      this.store.ranges = nextRanges;
      const merged = [];
      Object.values(this.store.ranges).forEach((arr) => { if (Array.isArray(arr)) merged.push(...arr); });
      merged.sort((a, b) => b.score - a.score);
      this.store.all = merged.slice(0, this.topLimit);
      this._dirty = true;
      this._saveStore(true);
    }

    if (wasRunning) this.start();
    console.log(`[Runner] CSV upload: +${added} rows, ignored ${ignored}`);
    return { csvPath: this.csvPath, minYear: this.minYear, maxYear: this.maxYear, oldMinYear, oldMaxYear, addedRows: added, ignoredRows: ignored, addedYearFrom: addedMinYear, addedYearTo: addedMaxYear, message: "Incremental update done" };
  }

  start(opts = {}) {
    if (this.running) return;
    this.batch = Number(opts.batch) > 0 ? Number(opts.batch) : this.batch;
    this.penalty = Number.isFinite(Number(opts.penalty)) ? Number(opts.penalty) : this.penalty;
    this.topLimit = Number(opts.topLimit) > 0 ? Number(opts.topLimit) : this.topLimit;
    if (opts.csvPath) this.loadBars(opts.csvPath);
    else if (!this.bars.length) this.loadBars(this.csvPath);
    if (!this.bars.length) throw new Error("No data available, please upload CSV first");
    this.running = true;
    this.lastError = null;
    this.workerCount = Number(opts.workerCount) > 0 ? Number(opts.workerCount) : this.workerCount;
    this._startWorkers();
    console.log(`[Runner] started (${this.workerCount} worker${this.workerCount > 1 ? "s" : ""}), batch=${this.batch}`);
  }

  stop() {
    this.running = false;
    this._stopWorkers();
    this._saveStore(true);
    console.log(`[Runner] stopped, attempts=${this.attempts}, valid=${this.valid}`);
  }

  _stopWorkers() {
    for (const { worker } of this.workers.values()) {
      worker.postMessage({ cmd: "stop" });
      worker.terminate();
    }
    this.workers.clear();
  }

  _startWorkers() {
    this._stopWorkers();
    for (let i = 0; i < this.workerCount; i++) this._startWorker();
  }

  _startWorker() {
    const workerId = ++this.workerSeq;
    const worker = new Worker(path.resolve(__dirname, "worker_range.js"), {
      workerData: {
        csvText: this.csvText,
        assetCsvTexts: this.assetCsvTexts,
        batch: this.batch,
        penalty: this.penalty,
      },
    });
    this.workers.set(workerId, { worker });

    worker.on("message", (msg) => {
      if (msg.ready) {
        worker.postMessage({ cmd: "start", batch: this.batch, penalty: this.penalty });
        return;
      }
      if (msg.results) {
        for (const row of msg.results) {
          this.attempts++;
          this.valid++;
          row.updatedAt = nowBeijing();
          if (!this.store.ranges[row.rangeKey]) this.store.ranges[row.rangeKey] = [];
          this._pushTop(this.store.ranges[row.rangeKey], row);
          this._pushTop(this.store.all, row);
          this._dirty = true;
        }
        this._saveStore(false);
      }
    });

    worker.on("error", (err) => {
      this.lastError = err.message;
      console.error(`[Runner] worker error: ${err.message}`);
    });

    worker.on("exit", (code) => {
      this.workers.delete(workerId);
      if (this.running) {
        console.warn(`[Runner] worker ${workerId} exited (code ${code}), restarting...`);
        setTimeout(() => { if (this.running) this._startWorker(); }, 1000);
      }
    });
  }

  _pushTop(list, entry) {
    list.push(entry);
    list.sort((a, b) => b.score - a.score);
    if (list.length > this.topLimit) list.length = this.topLimit;
  }

  // Keep for strategy_lab tasks (run on main thread)
  _barsForRange(startYear, endYear) {
    const key = `${startYear}-${endYear}`;
    if (this.rangeBarsCache.has(key)) {
      const idx = this._cacheOrder.indexOf(key);
      if (idx > -1) this._cacheOrder.splice(idx, 1);
      this._cacheOrder.push(key);
      return this.rangeBarsCache.get(key);
    }
    const subset = this.bars.filter((b) => {
      const y = yearFromTradeDate(b.tradeDate);
      return y >= startYear && y <= endYear;
    });
    while (this._cacheOrder.length >= 50) {
      const oldest = this._cacheOrder.shift();
      this.rangeBarsCache.delete(oldest);
    }
    this.rangeBarsCache.set(key, subset);
    this._cacheOrder.push(key);
    return subset;
  }

  _rangeOptions() {
    const keys = Object.keys(this.store.ranges);
    const list = keys.map((k) => {
      const arr = this.store.ranges[k] || [];
      const best = arr[0] || null;
      return { key: k, count: arr.length, bestScore: best ? best.score : null };
    });
    list.sort((a, b) => (b.bestScore ?? -Infinity) - (a.bestScore ?? -Infinity));
    return list;
  }

  leaderboard(rangeKey = "all", limit = 50) {
    const lim = Math.min(Math.max(Number(limit) || 50, 1), 200);
    const selected = rangeKey && rangeKey !== "all" ? String(rangeKey) : "all";
    const source = selected === "all" ? this.store.all : (this.store.ranges[selected] || []);
    return {
      selectedRange: selected,
      ranges: this._rangeOptions(),
      entries: source.slice(0, lim),
      meta: { csvPath: this.csvPath, minYear: this.minYear, maxYear: this.maxYear, updatedAt: this.store.meta.updatedAt },
    };
  }

  status() {
    return {
      running: this.running, attempts: this.attempts, valid: this.valid, skipped: this.skipped,
      csvPath: this.csvPath, minYear: this.minYear, maxYear: this.maxYear,
      batch: this.batch, penalty: this.penalty, topLimit: this.topLimit,
      workerCount: this.workerCount, activeWorkers: this.workers.size,
      totalRanges: Object.keys(this.store.ranges).length,
      totalAllRows: this.store.all.length,
      lastError: this.lastError, storePath: this.storePath,
      updatedAt: nowBeijing(),
    };
  }
}

// ============================================================
//  RobustSearchRunner - average optimal search (1 dedicated worker)
// ============================================================
class RobustSearchRunner {
  constructor(runner) {
    this.runner = runner;
    this.workers = new Map();
    this.workerSeq = 0;
    this.running = false;
    this.attempts = 0;
    this.valid = 0;
    this.skipped = 0;
    this.batch = 50;
    this.penalty = 0.08;
    this.topLimit = 10;
    this.windowCount = 10;
    this.workerCount = configuredWorkerCount("ROBUST_WORKERS", 4);

    this.storePath = path.resolve(runner.dataDir, "robust_store.json");
    this.topByYear = {};
    for (let y = 1; y <= 10; y++) this.topByYear[y] = [];
    this._dirty = false;
    this._lastSaveAt = 0;
    this._loadStore();
  }

  _loadStore() {
    if (!fs.existsSync(this.storePath)) return;
    try {
      const raw = fs.readFileSync(this.storePath, "utf8");
      const parsed = JSON.parse(raw);
      if (parsed && parsed.topByYear && typeof parsed.topByYear === "object") {
        for (let y = 1; y <= 10; y++) {
          if (Array.isArray(parsed.topByYear[y])) this.topByYear[y] = parsed.topByYear[y];
        }
      }
    } catch (err) {
      console.warn(`[Robust] load store failed: ${err.message}`);
    }
  }

  _saveStore(force = false) {
    const now = Date.now();
    if (!force && (!this._dirty || now - this._lastSaveAt < 2000)) return;
    try {
      const data = {
        updatedAt: nowBeijing(),
        attempts: this.attempts,
        valid: this.valid,
        topByYear: this.topByYear,
      };
      const tmp = this.storePath + ".tmp";
      fs.writeFileSync(tmp, JSON.stringify(data, null, 2), "utf8");
      fs.renameSync(tmp, this.storePath);
      this._dirty = false;
      this._lastSaveAt = now;
    } catch (err) {
      console.error(`[Robust] save store failed: ${err.message}`);
    }
  }

  start(opts = {}) {
    if (this.running) return;
    if (!this.runner.bars.length) throw new Error("No data available, please upload CSV first");
    this.batch = Number(opts.batch) > 0 ? Number(opts.batch) : this.batch;
    this.penalty = Number.isFinite(Number(opts.penalty)) ? Number(opts.penalty) : this.penalty;
    this.topLimit = Number(opts.topLimit) > 0 ? Number(opts.topLimit) : this.topLimit;
    this.windowCount = Number(opts.windowCount) > 0 ? Number(opts.windowCount) : this.windowCount;
    this.workerCount = Number(opts.workerCount) > 0 ? Number(opts.workerCount) : this.workerCount;
    this.running = true;
    this._startWorkers();
    console.log(`[Robust] started (${this.workerCount} worker${this.workerCount > 1 ? "s" : ""}), years=1-10, count=${this.windowCount}, batch=${this.batch}`);
  }

  stop() {
    this.running = false;
    this._stopWorkers();
    this._saveStore(true);
    console.log(`[Robust] stopped, attempts=${this.attempts}, valid=${this.valid}`);
  }

  _stopWorkers() {
    for (const { worker } of this.workers.values()) {
      worker.postMessage({ cmd: "stop" });
      worker.terminate();
    }
    this.workers.clear();
  }

  _startWorkers() {
    this._stopWorkers();
    for (let i = 0; i < this.workerCount; i++) this._startWorker();
  }

  _startWorker() {
    const workerId = ++this.workerSeq;
    const worker = new Worker(path.resolve(__dirname, "worker_robust.js"), {
      workerData: {
        csvText: this.runner.csvText,
        assetCsvTexts: this.runner.assetCsvTexts,
        batch: this.batch,
        penalty: this.penalty,
        windowCount: this.windowCount,
      },
    });
    this.workers.set(workerId, { worker });

    worker.on("message", (msg) => {
      if (msg.ready) {
        worker.postMessage({ cmd: "start", batch: this.batch, penalty: this.penalty, windowCount: this.windowCount });
        return;
      }
      if (msg.results) {
        for (const row of msg.results) {
          this.attempts++;
          this.valid++;
          row.updatedAt = nowBeijing();
          const yr = row.windowYears;
          if (yr >= 1 && yr <= 10) {
            const list = this.topByYear[yr];
            list.push(row);
            list.sort((a, b) => b.robustScore - a.robustScore);
            if (list.length > this.topLimit) list.length = this.topLimit;
            this._dirty = true;
          }
        }
        this._saveStore(false);
      }
    });

    worker.on("error", (err) => {
      console.error(`[Robust] worker error: ${err.message}`);
    });

    worker.on("exit", (code) => {
      this.workers.delete(workerId);
      if (this.running) {
        console.warn(`[Robust] worker ${workerId} exited (code ${code}), restarting...`);
        setTimeout(() => { if (this.running) this._startWorker(); }, 1000);
      }
    });
  }

  leaderboard(year, limit = 10) {
    const yr = Number(year);
    const lim = Math.min(Math.max(Number(limit) || 10, 1), 200);
    if (yr >= 1 && yr <= 10) {
      return {
        year: yr,
        entries: (this.topByYear[yr] || []).slice(0, lim),
        meta: {
          running: this.running, attempts: this.attempts, valid: this.valid,
          windowCount: this.windowCount,
          minYear: this.runner.minYear, maxYear: this.runner.maxYear,
          updatedAt: nowBeijing(),
        },
      };
    }
    const summary = {};
    for (let y = 1; y <= 10; y++) {
      const list = this.topByYear[y] || [];
      summary[y] = { count: list.length, best: list[0] ? list[0].robustScore : null };
    }
    return {
      summary,
      meta: {
        running: this.running, attempts: this.attempts, valid: this.valid,
        windowCount: this.windowCount,
        minYear: this.runner.minYear, maxYear: this.runner.maxYear,
        updatedAt: nowBeijing(),
      },
    };
  }

  status() {
    const topCounts = {};
    for (let y = 1; y <= 10; y++) topCounts[y] = (this.topByYear[y] || []).length;
    return {
      running: this.running, attempts: this.attempts, valid: this.valid, skipped: this.skipped,
      batch: this.batch, penalty: this.penalty, windowCount: this.windowCount,
      workerCount: this.workerCount, activeWorkers: this.workers.size,
      topCounts, updatedAt: nowBeijing(),
    };
  }
}

module.exports = { RandomRunner, RobustSearchRunner };
