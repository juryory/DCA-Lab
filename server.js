const http = require("http");
const fs = require("fs");
const https = require("https");
const os = require("os");
const path = require("path");
const { Worker } = require("worker_threads");
const { RandomRunner, RobustSearchRunner } = require("./backend/runner");

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
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  }
}

loadDotEnv();

const PORT = Number(process.env.PORT || 8787);
const LOCAL_ADMIN_TOKEN = process.env.LOCAL_ADMIN_TOKEN || process.env.ADMIN_TOKEN || "change_me";
const MAX_BODY = 5 * 1024 * 1024; // 5MB

const runner = new RandomRunner();
const robustRunner = new RobustSearchRunner(runner);

// --- Task manager for strategy_lab random search ---
const tasks = new Map();
let taskIdCounter = 0;
const taskWorkers = new Map();
const uploadIntervals = new Set([10, 20, 30, 60]);
const cloudSync = {
  enabled: false,
  apiUrl: "",
  intervalMinutes: 30,
  timer: null,
  lastRunAt: null,
  lastResult: null,
  running: false,
};

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

function taskWorkerLimit() {
  const cpus = cpuCount();
  const manual = envInt("TASK_WORKERS");
  if (manual) return manual;
  if (cpus >= 16) return 4;
  if (cpus >= 8) return 2;
  return 1;
}

function totalWorkerBudget() {
  const manual = envInt("TOTAL_WORKERS");
  if (manual) return manual;
  const cpus = cpuCount();
  if (cpus >= 16) return 8;
  if (cpus >= 8) return 4;
  return 2;
}

function activeSearchWorkers() {
  return (runner.workers ? runner.workers.size : 0) + (robustRunner.workers ? robustRunner.workers.size : 0);
}

function availableTaskSlots() {
  const remaining = Math.max(0, totalWorkerBudget() - activeSearchWorkers());
  return Math.min(taskWorkerLimit(), remaining);
}

function desiredSearchWorkers(kind, requested) {
  const explicit = Number(requested);
  if (explicit > 0) return explicit;

  const budget = totalWorkerBudget();
  const reservedForTasks = Math.min(1, taskWorkerLimit());
  const currentRange = runner.workers ? runner.workers.size : 0;
  const currentRobust = robustRunner.workers ? robustRunner.workers.size : 0;
  const otherActive = kind === "range" ? currentRobust : currentRange;
  const remaining = Math.max(1, budget - reservedForTasks - otherActive);

  if (kind === "range") {
    return Math.min(runner.workerCount || 1, remaining);
  }
  return Math.min(robustRunner.workerCount || 1, remaining);
}

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
  });
  res.end(body);
}

function text(res, status, payload, type = "text/plain; charset=utf-8") {
  res.writeHead(status, {
    "Content-Type": type,
    "Content-Length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

function postJson(targetUrl, payload) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const mod = parsed.protocol === "https:" ? https : http;
    const body = JSON.stringify(payload);
    const req = mod.request({
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname + parsed.search,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
      },
    }, (resp) => {
      const chunks = [];
      resp.on("data", (c) => chunks.push(c));
      resp.on("end", () => {
        const raw = Buffer.concat(chunks).toString("utf8");
        try {
          resolve(JSON.parse(raw));
        } catch (_) {
          resolve({ raw });
        }
      });
    });
    req.on("error", reject);
    req.write(body);
    req.end();
  });
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;

    req.on("data", (c) => {
      size += c.length;
      if (size > MAX_BODY) {
        reject(new Error("请求体过大（上限 5MB）"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });

    req.on("end", () => {
      if (!chunks.length) { resolve({}); return; }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch (err) {
        reject(new Error("JSON 格式无效"));
      }
    });

    req.on("error", reject);
  });
}

// --- Auth middleware ---
function checkAuth(req) {
  const authHeader = req.headers["authorization"] || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
  const queryToken = new URL(req.url, `http://${req.headers.host}`).searchParams.get("token") || "";
  return token === LOCAL_ADMIN_TOKEN || queryToken === LOCAL_ADMIN_TOKEN;
}

// --- Static file serving with whitelist ---
const STATIC_WHITELIST = new Set([
  "leaderboard.html",
  "range.html",
  "admin.html",
  "strategy_lab.html",
  "robust.html",
  "data/au9999_history.csv",
]);

function serveStatic(reqPath, res) {
  const root = process.cwd();
  let target = reqPath === "/" ? "leaderboard.html" : reqPath.slice(1);
  if (reqPath === "/admin") target = "admin.html";
  if (reqPath === "/lab") target = "strategy_lab.html";
  if (reqPath === "/robust") target = "robust.html";
  if (reqPath === "/range") target = "range.html";
  if (reqPath === "/au9999_history.csv") target = "data/au9999_history.csv";

  const safe = path.normalize(target).replace(/\\/g, "/");
  if (!STATIC_WHITELIST.has(safe)) return text(res, 404, "资源不存在");

  const full = path.join(root, safe);
  if (!full.startsWith(root)) return text(res, 403, "禁止访问");
  if (!fs.existsSync(full) || !fs.statSync(full).isFile()) return text(res, 404, "资源不存在");

  const ext = path.extname(full).toLowerCase();
  const type = ext === ".html" ? "text/html; charset=utf-8"
    : ext === ".csv" ? "text/csv; charset=utf-8"
    : ext === ".js" ? "text/javascript; charset=utf-8"
    : ext === ".json" ? "application/json; charset=utf-8"
    : "application/octet-stream";

  const data = fs.readFileSync(full);
  res.writeHead(200, { "Content-Type": type, "Content-Length": data.length, "Cache-Control": "no-store" });
  res.end(data);
}

// --- Task API helpers for strategy_lab ---
function createTask(startYear, endYear, batch, top, penalty) {
  const id = ++taskIdCounter;
  const label = `${startYear}-${endYear}`;
  const task = {
    id, label, startYear, endYear, batch, top, penalty,
    running: false, queued: false, attempts: 0, valid: 0, best: null,
  };
  tasks.set(id, task);
  return task;
}

function runTask(task) {
  const bars = runner._barsForRange(task.startYear, task.endYear);
  if (bars.length < 120) {
    task.running = false;
    task.queued = false;
    return;
  }
  if (task.running || task.queued) return;
  task.queued = true;
  scheduleTasks();
}

function stopTask(task) {
  task.queued = false;
  task.running = false;
  const worker = taskWorkers.get(task.id);
  if (worker) {
    worker.postMessage({ cmd: "stop" });
    worker.terminate();
    taskWorkers.delete(task.id);
  }
}

function scheduleTasks() {
  const limit = availableTaskSlots();
  while (taskWorkers.size < limit) {
    const nextTask = Array.from(tasks.values()).find((task) => task.queued && !task.running);
    if (!nextTask) break;
    startTaskWorker(nextTask);
  }
}

function startTaskWorker(task) {
  const bars = runner._barsForRange(task.startYear, task.endYear);
  if (bars.length < 120) {
    task.queued = false;
    task.running = false;
    return;
  }

  task.running = true;
  task.queued = false;

  const worker = new Worker(path.resolve(__dirname, "backend", "worker_task.js"), {
    workerData: {
      bars,
      batch: task.batch,
      top: task.top,
      penalty: task.penalty,
    },
  });

  taskWorkers.set(task.id, worker);

  worker.on("message", (msg) => {
    if (msg.ready) {
      worker.postMessage({ cmd: "start" });
      return;
    }
    if (typeof msg.attempts === "number") task.attempts = msg.attempts;
    if (typeof msg.valid === "number") task.valid = msg.valid;
    if (msg.best) task.best = msg.best;
  });

  worker.on("error", (err) => {
    console.error(`[Task ${task.id}] worker error: ${err.message}`);
  });

  worker.on("exit", () => {
    taskWorkers.delete(task.id);
    if (task.running) {
      task.running = false;
      if (task.queued) scheduleTasks();
    }
    scheduleTasks();
  });
}

function getTasksStatus() {
  const jobs = [];
  let runningJobs = 0;
  let queuedJobs = 0;
  for (const t of tasks.values()) {
    if (t.running) runningJobs++;
    if (t.queued) queuedJobs++;
    jobs.push({
      id: t.id, label: t.label, startYear: t.startYear, endYear: t.endYear,
      running: t.running, queued: t.queued, attempts: t.attempts, valid: t.valid,
      best: t.best,
    });
  }
  return {
    jobs, runningJobs, totalJobs: tasks.size,
    queuedJobs, workerLimit: availableTaskSlots(), configuredTaskWorkers: taskWorkerLimit(), activeWorkers: taskWorkers.size,
    totalWorkerBudget: totalWorkerBudget(), activeSearchWorkers: activeSearchWorkers(),
    minYear: runner.minYear, maxYear: runner.maxYear,
  };
}

function dedupeEntries(entries, keyFn) {
  const seen = new Set();
  const out = [];
  for (const entry of entries) {
    const key = keyFn(entry);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(entry);
  }
  return out;
}

function collectRangeEntries() {
  const merged = [];
  for (const entries of Object.values(runner.store.ranges || {})) {
    if (Array.isArray(entries)) merged.push(...entries);
  }
  return dedupeEntries(merged, (entry) => JSON.stringify([
    entry.rangeKey,
    entry.score,
    entry.returnRate,
    entry.tradeCount,
    entry.weights,
    entry.configs,
  ]));
}

function collectRobustEntries() {
  const merged = [];
  for (const entries of Object.values(robustRunner.topByYear || {})) {
    if (Array.isArray(entries)) merged.push(...entries);
  }
  return dedupeEntries(merged, (entry) => JSON.stringify([
    entry.windowYears,
    entry.robustScore,
    entry.avgScore,
    entry.minScore,
    entry.maxScore,
    entry.stdDev,
    entry.windows,
    entry.weights,
    entry.configs,
  ]));
}

async function uploadToCloudflare(apiUrl) {
  const cleanApiUrl = String(apiUrl || "").trim().replace(/\/$/, "");
  if (!cleanApiUrl) throw new Error("missing apiUrl");

  const rangeEntries = collectRangeEntries();
  const robustEntries = collectRobustEntries();
  const result = {
    apiUrl: cleanApiUrl,
    rangeSubmitted: 0,
    robustSubmitted: 0,
    rangeResponse: null,
    robustResponse: null,
  };

  if (rangeEntries.length) {
    result.rangeResponse = await postJson(`${cleanApiUrl}/api/submit/range`, { results: rangeEntries });
    result.rangeSubmitted = rangeEntries.length;
  }
  if (robustEntries.length) {
    result.robustResponse = await postJson(`${cleanApiUrl}/api/submit/robust`, { results: robustEntries });
    result.robustSubmitted = robustEntries.length;
  }
  if (!rangeEntries.length && !robustEntries.length) {
    result.message = "no local results to upload";
  }

  cloudSync.lastRunAt = new Date().toISOString();
  cloudSync.lastResult = result;
  return result;
}

function stopCloudSync() {
  cloudSync.enabled = false;
  if (cloudSync.timer) clearInterval(cloudSync.timer);
  cloudSync.timer = null;
}

function startCloudSync(apiUrl, intervalMinutes) {
  const minutes = Number(intervalMinutes);
  if (!uploadIntervals.has(minutes)) throw new Error("invalid interval");
  const cleanApiUrl = String(apiUrl || "").trim().replace(/\/$/, "");
  if (!cleanApiUrl) throw new Error("missing apiUrl");

  stopCloudSync();
  cloudSync.enabled = true;
  cloudSync.apiUrl = cleanApiUrl;
  cloudSync.intervalMinutes = minutes;
  cloudSync.timer = setInterval(async () => {
    if (cloudSync.running) return;
    cloudSync.running = true;
    try {
      await uploadToCloudflare(cloudSync.apiUrl);
    } catch (err) {
      cloudSync.lastRunAt = new Date().toISOString();
      cloudSync.lastResult = { error: err.message, apiUrl: cloudSync.apiUrl };
    } finally {
      cloudSync.running = false;
    }
  }, minutes * 60 * 1000);
}

function cloudSyncStatus() {
  return {
    enabled: cloudSync.enabled,
    running: cloudSync.running,
    apiUrl: cloudSync.apiUrl,
    intervalMinutes: cloudSync.intervalMinutes,
    lastRunAt: cloudSync.lastRunAt,
    lastResult: cloudSync.lastResult,
    rangeEntries: collectRangeEntries().length,
    robustEntries: collectRobustEntries().length,
    allowedIntervals: Array.from(uploadIntervals.values()),
  };
}

// --- HTTP server ---
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  try {
    // Public API: leaderboard
    if (req.method === "GET" && pathname === "/api/leaderboard") {
      const range = url.searchParams.get("range") || "all";
      const limit = Number(url.searchParams.get("limit") || 50);
      json(res, 200, runner.leaderboard(range, limit));
      return;
    }

    // Public API: task status for strategy_lab
    if (req.method === "GET" && pathname === "/api/random/tasks") {
      json(res, 200, getTasksStatus());
      return;
    }

    // Public API: start a task from strategy_lab
    if (req.method === "POST" && pathname === "/api/random/tasks/start") {
      if (!checkAuth(req)) {
        json(res, 401, { error: "未授权，请提供正确的 token" });
        return;
      }
      const body = await readJson(req);
      const startYear = Number(body.startYear) || runner.minYear;
      const endYear = Number(body.endYear) || runner.maxYear;
      const batch = Number(body.batch) || 100;
      const top = Number(body.top) || 50;
      const penalty = Number(body.penalty) || 0.08;
      const task = createTask(startYear, endYear, batch, top, penalty);
      runTask(task);
      json(res, 200, { ok: true, taskId: task.id });
      return;
    }

    // Public API: stop all tasks from strategy_lab
    if (req.method === "POST" && pathname === "/api/random/tasks/stop") {
      if (!checkAuth(req)) {
        json(res, 401, { error: "未授权，请提供正确的 token" });
        return;
      }
      for (const t of tasks.values()) stopTask(t);
      json(res, 200, { ok: true });
      return;
    }

    // --- Robust search API (public: read-only) ---
    if (req.method === "GET" && pathname === "/api/robust/leaderboard") {
      const year = url.searchParams.get("year");
      const limit = Number(url.searchParams.get("limit") || 10);
      json(res, 200, robustRunner.leaderboard(year, limit));
      return;
    }

    if (req.method === "GET" && pathname === "/api/robust/status") {
      json(res, 200, robustRunner.status());
      return;
    }

    // --- Admin APIs: require auth ---
    if (pathname.startsWith("/api/admin/")) {
      if (!checkAuth(req)) {
        json(res, 401, { error: "未授权，请提供正确的 token" });
        return;
      }

      if (req.method === "GET" && pathname === "/api/admin/status") {
        json(res, 200, { ...runner.status(), cloudSync: cloudSyncStatus() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/start") {
        const body = await readJson(req);
        runner.start({ ...(body || {}), workerCount: desiredSearchWorkers("range", body && body.workerCount) });
        json(res, 200, { ok: true, status: runner.status() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/stop") {
        runner.stop();
        json(res, 200, { ok: true, status: runner.status() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/upload-csv") {
        const body = await readJson(req);
        const filename = body && body.filename ? String(body.filename) : "uploaded_history.csv";
        const content = body && body.content ? String(body.content) : "";
        const out = runner.uploadCsv(filename, content);
        json(res, 200, { ok: true, upload: out, status: runner.status() });
        return;
      }

      if (req.method === "GET" && pathname === "/api/admin/robust/status") {
        json(res, 200, { ...robustRunner.status(), cloudSync: cloudSyncStatus() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/robust/start") {
        const body = await readJson(req);
        robustRunner.start({ ...(body || {}), workerCount: desiredSearchWorkers("robust", body && body.workerCount) });
        json(res, 200, { ok: true, status: robustRunner.status() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/robust/stop") {
        robustRunner.stop();
        json(res, 200, { ok: true, status: robustRunner.status() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/cloudflare/upload") {
        const body = await readJson(req);
        const result = await uploadToCloudflare(body && body.apiUrl);
        json(res, 200, { ok: true, result, cloudSync: cloudSyncStatus() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/cloudflare/auto-start") {
        const body = await readJson(req);
        startCloudSync(body && body.apiUrl, Number(body && body.intervalMinutes));
        json(res, 200, { ok: true, cloudSync: cloudSyncStatus() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/cloudflare/auto-stop") {
        stopCloudSync();
        json(res, 200, { ok: true, cloudSync: cloudSyncStatus() });
        return;
      }

      text(res, 404, "接口不存在");
      return;
    }

    // Static files
    if (req.method === "GET") {
      serveStatic(pathname, res);
      return;
    }

    text(res, 404, "接口不存在");
  } catch (err) {
    console.error(`[${new Date().toISOString()}] ${req.method} ${pathname} Error:`, err.message);
    json(res, 500, { error: err.message });
  }
});

server.listen(PORT, () => {
console.log(`DCALab server running on http://localhost:${PORT}`);
  console.log(`Local admin token: ${LOCAL_ADMIN_TOKEN} (set LOCAL_ADMIN_TOKEN env to change; ADMIN_TOKEN is still supported for compatibility)`);
});
