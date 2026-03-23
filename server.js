const http = require("http");
const fs = require("fs");
const path = require("path");
const { RandomRunner, RobustSearchRunner } = require("./backend/runner");

const PORT = Number(process.env.PORT || 8787);
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "change_me";
const MAX_BODY = 5 * 1024 * 1024; // 5MB

const runner = new RandomRunner();
const robustRunner = new RobustSearchRunner(runner);

// --- Task manager for strategy_lab random search ---
const tasks = new Map();
let taskIdCounter = 0;

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
  return token === ADMIN_TOKEN || queryToken === ADMIN_TOKEN;
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
    running: false, attempts: 0, valid: 0, best: null, timer: null,
    _topList: [],
  };
  tasks.set(id, task);
  return task;
}

function runTask(task) {
  if (task.running) return;
  task.running = true;

  const bars = runner._barsForRange(task.startYear, task.endYear);
  if (bars.length < 120) {
    task.running = false;
    return;
  }

  const { buildRandomConfig, runBacktest, score } = require("./backend/engine");

  function tick() {
    if (!task.running) return;
    for (let i = 0; i < task.batch; i++) {
      task.attempts++;
      try {
        const cfg = buildRandomConfig();
        const res = runBacktest(bars, cfg);
        const s = score(res, task.penalty);
        task.valid++;
        task._topList.push({ score: s, returnRate: res.returnRate, totalInvested: res.totalInvested, finalValue: res.finalValue, tradeCount: res.tradeCount, cfg });
        task._topList.sort((a, b) => b.score - a.score);
        if (task._topList.length > task.top) task._topList.length = task.top;
        task.best = task._topList[0] || null;
      } catch (_) { /* skip invalid */ }
    }
    task.timer = setTimeout(tick, 0);
  }
  tick();
}

function stopTask(task) {
  task.running = false;
  if (task.timer) { clearTimeout(task.timer); task.timer = null; }
}

function getTasksStatus() {
  const jobs = [];
  let runningJobs = 0;
  for (const t of tasks.values()) {
    if (t.running) runningJobs++;
    jobs.push({
      id: t.id, label: t.label, startYear: t.startYear, endYear: t.endYear,
      running: t.running, attempts: t.attempts, valid: t.valid,
      best: t.best,
    });
  }
  return {
    jobs, runningJobs, totalJobs: tasks.size,
    minYear: runner.minYear, maxYear: runner.maxYear,
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
        json(res, 200, runner.status());
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/start") {
        const body = await readJson(req);
        runner.start(body || {});
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
        json(res, 200, robustRunner.status());
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/robust/start") {
        const body = await readJson(req);
        robustRunner.start(body || {});
        json(res, 200, { ok: true, status: robustRunner.status() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/robust/stop") {
        robustRunner.stop();
        json(res, 200, { ok: true, status: robustRunner.status() });
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
  console.log(`AUSUB server running on http://localhost:${PORT}`);
  console.log(`Admin token: ${ADMIN_TOKEN} (set ADMIN_TOKEN env to change)`);
});
