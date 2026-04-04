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

function desiredSearchWorkers(requested) {
  const explicit = Number(requested);
  if (explicit > 0) return explicit;

  const budget = totalWorkerBudget();
  const reservedForTasks = Math.min(1, taskWorkerLimit());
  const remaining = Math.max(1, budget - reservedForTasks);
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
  "admin.html",
  "strategy_lab.html",
  "robust.html",
  "data/au9999_history.csv",
]);

function serveStatic(reqPath, res) {
  const root = process.cwd();
  let target = reqPath === "/" ? "robust.html" : reqPath.slice(1);
  if (reqPath === "/admin") target = "admin.html";
  if (reqPath === "/lab") target = "strategy_lab.html";
  if (reqPath === "/robust") target = "robust.html";
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



// --- Static page generator ---
function buildStaticHtml(dataByYear) {
  const dataJson = JSON.stringify(dataByYear);
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DCALab 稳健排行榜</title>
  <style>
    :root{--bg:#f6f1e7;--panel:#fffdf8;--line:#dfd3c0;--text:#30271b;--muted:#6f6554;--gold:#9a6818;--gold2:#c58b2a;--good:#1f6b3c;--bad:#973939;--soft:#f8f0df}
    *{box-sizing:border-box}
    body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 "Segoe UI","Microsoft YaHei",sans-serif}
    .wrap{max-width:1400px;margin:0 auto;padding:24px}
    .panel{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:16px;margin-bottom:12px}
    .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    .meta{font-size:12px;color:var(--muted)}
    .tabs{display:flex;gap:6px;margin:12px 0}
    .tab{padding:8px 16px;border:1px solid var(--line);border-radius:999px;cursor:pointer;background:#fff;font-size:13px}
    .tab.active{background:linear-gradient(135deg,var(--gold),var(--gold2));color:#fff;border-color:transparent}
    .cards{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:14px}
    .card{padding:12px;border:1px solid var(--line);border-radius:14px;background:#fff}
    .k{font-size:12px;color:var(--muted)}.v{font-size:20px;font-weight:700}
    .good{color:var(--good)}.bad{color:var(--bad)}
    .layout{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .layout>.panel{margin-bottom:0}
    .table-wrap{overflow:auto;border:1px solid var(--line);border-radius:14px;background:#fff;max-height:420px}
    .detail-col .detail{overflow:auto;max-height:420px}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th,td{border-bottom:1px solid #eee4d3;padding:8px 6px;text-align:left}
    th{position:sticky;top:0;background:#fff;color:var(--muted)}
    tbody tr{cursor:pointer}
    tbody tr.selected{background:var(--soft)}
    .detail{border:1px solid var(--line);border-radius:12px;padding:14px;background:#fff}
    .detail-empty{color:var(--muted);padding:8px 0}
    .asset-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
    .asset-card{border:1px solid var(--line);border-radius:14px;padding:12px;background:#fffdfa}
    .asset-card.full-row{grid-column:1/-1}
    .asset-head{display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:8px}
    .asset-name{font-weight:700}
    .asset-points{display:grid;gap:6px}
    .point{font-size:13px;color:var(--text)}
    .point strong{color:var(--muted);font-weight:600}
    .mini-table{width:100%;border-collapse:collapse;font-size:12px;background:#fff;border:1px solid var(--line);border-radius:12px;overflow:hidden}
    .mini-table th,.mini-table td{padding:8px 6px;border-bottom:1px solid #eee4d3;text-align:left}
    .mini-table th{background:#fcf7ef;color:var(--muted);position:static}
    .mini-wrap{overflow:auto;border-radius:12px}
    @media(max-width:1100px){.asset-grid{grid-template-columns:1fr}}
    @media(max-width:1000px){.cards{grid-template-columns:repeat(3,1fr)}.layout{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <div class="row"><h2 style="margin:0">DCALab 稳健排行榜</h2></div>
      <div class="meta" style="margin-top:6px">按滚动年份窗口表现，筛选最稳健的多资产定投策略。</div>
    </div>
    <div class="tabs" id="yearTabs"></div>
    <div class="cards">
      <div class="card"><div class="k">最佳稳健分</div><div class="v" id="vBestRobust">-</div></div>
      <div class="card"><div class="k">最佳平均分</div><div class="v" id="vBestAvg">-</div></div>
      <div class="card"><div class="k">最佳最低分</div><div class="v" id="vBestMin">-</div></div>
      <div class="card"><div class="k">结果数量</div><div class="v" id="vCount">-</div></div>
      <div class="card"><div class="k">当前窗口</div><div class="v" id="vYear">1Y</div></div>
    </div>
    <div class="layout">
      <div class="panel" style="padding:0"><div class="table-wrap">
        <table><thead><tr>
          <th>#</th><th>稳健分</th><th>平均分</th><th>最低分</th><th>最高分</th><th>标准差</th><th>窗口数</th><th>黄金</th><th>沪深300</th><th>标普500</th>
        </tr></thead><tbody id="rows"></tbody></table>
      </div></div>
      <div class="detail-col"><div class="detail">
        <div class="k" style="margin-bottom:8px">各窗口回测结果</div>
        <div id="detail" class="detail-empty">点击任意一行查看窗口回测结果。</div>
      </div></div>
    </div>
    <div class="panel">
      <div class="k" style="margin-bottom:8px">3个资产策略参数</div>
      <div id="strategyParams" class="detail-empty">点击任意一行查看详细策略参数。</div>
    </div>
  </div>
  <script>
    const ALL_DATA = ${dataJson};
    let selectedYear = 1, selectedIdx = 0, entries = [];

    function n(v,d=2){const num=Number(v);return Number.isFinite(num)?num.toFixed(d):"-"}
    function pct(v){return n((Number(v)||0)*100,0)+"%"}
    function pctScore(v){const num=Number(v);return Number.isFinite(num)?(num*100).toFixed(2)+"%":"-"}
    function money(v){const num=Number(v);return Number.isFinite(num)?num.toLocaleString("zh-CN",{minimumFractionDigits:2,maximumFractionDigits:2}):"-"}
    function esc(v){return String(v).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")}

    function formatSchedule(c){if(!c)return"-";if(c.scheduleMode==="every_n_days")return"每 "+(c.scheduleDays??"-")+" 天一次";if(c.scheduleMode==="weekly_weekday")return"每周 "+["一","二","三","四","五","六","日"][Number(c.scheduleWeekday)-1]||"-";return c.scheduleMode||"-"}
    function formatScheduleMode(m){return{every_n_days:"按固定天数定投",weekly:"按周定投",weekly_weekday:"按周几定投",monthly:"按月定投"}[m]||m||"-"}
    function formatBuyMode(m){return{close_confirm_next_close:"收盘确认，下一交易日收盘买入",intraday_break_same_close:"盘中触发，当天收盘买入"}[m]||m||"-"}
    function formatWeekday(w){const l=["一","二","三","四","五","六","日"][Number(w)-1];return l?"周"+l:"-"}

    function renderTabs(){yearTabs.innerHTML="";for(let y=1;y<=10;y++){const t=document.createElement("div");t.className="tab"+(y===selectedYear?" active":"");t.textContent=y+"Y";t.onclick=()=>{selectedYear=y;selectedIdx=0;renderTabs();render()};yearTabs.appendChild(t)}}

    function renderAmountCard(key,label,entry){const c=(entry.configs||{})[key];if(!c)return"";return'<div class="asset-card"><div class="asset-head"><div class="asset-name">'+esc(label)+'</div></div><div class="asset-points"><div class="point"><strong>基础定投</strong> '+esc(c.baseAmount)+'</div><div class="point"><strong>回撤加仓</strong> '+esc(c.dipAmount)+'</div></div></div>'}
    function renderStrategyCard(entry){const c=(entry.configs||{}).au9999||(entry.configs||{}).csi300||(entry.configs||{}).sp500;if(!c)return"";const sd=c.scheduleMode==="every_n_days",sw=c.scheduleMode==="weekly"||c.scheduleMode==="weekly_weekday";return'<div class="asset-card full-row"><div class="asset-head"><div class="asset-name">策略参数（三资产共享）</div></div><div class="asset-points" style="grid-template-columns:repeat(auto-fill,minmax(180px,1fr))"><div class="point"><strong>均线窗口</strong> '+esc(c.maWindow)+' 日</div><div class="point"><strong>定投频率</strong> '+esc(formatSchedule(c))+'</div><div class="point"><strong>频率模式</strong> '+esc(formatScheduleMode(c.scheduleMode))+'</div>'+(sd?'<div class="point"><strong>间隔天数</strong> '+esc(c.scheduleDays??"-")+'</div>':"")+(sw?'<div class="point"><strong>星期设置</strong> '+esc(formatWeekday(c.scheduleWeekday))+'</div>':'')+'<div class="point"><strong>买入方式</strong> '+esc(formatBuyMode(c.buyMode))+'</div></div></div>'}

    function render(){
      entries=ALL_DATA[selectedYear]||[];
      vYear.textContent=selectedYear+"Y";
      vCount.textContent=entries.length;
      const best=entries[0];
      vBestRobust.textContent=best?n(best.robustScore):"-";
      vBestAvg.textContent=best?n(best.avgScore):"-";
      vBestMin.textContent=best?n(best.minScore):"-";
      if(!entries.length){rows.innerHTML='<tr><td colspan="10">当前窗口暂无结果。</td></tr>';detail.innerHTML='';strategyParams.innerHTML='';return}
      if(selectedIdx>=entries.length)selectedIdx=0;
      rows.innerHTML=entries.map((e,i)=>{const w=e.weights||{};return'<tr data-i="'+i+'" class="'+(i===selectedIdx?"selected":"")+'"><td>'+(i+1)+'</td><td>'+n(e.robustScore)+'</td><td>'+n(e.avgScore)+'</td><td class="'+(Number(e.minScore)>=0?"good":"bad")+'">'+n(e.minScore)+'</td><td>'+n(e.maxScore)+'</td><td>'+n(e.stdDev)+'</td><td>'+(e.windowCount||(e.windows?e.windows.length:"-"))+'</td><td>'+pct(w.au9999||0)+'</td><td>'+pct(w.csi300||0)+'</td><td>'+pct(w.sp500||0)+'</td></tr>'}).join("");
      const sel=entries[selectedIdx];
      const details=Array.isArray(sel.details)?sel.details:[];
      detail.innerHTML=details.length?'<div class="mini-wrap"><table class="mini-table"><thead><tr><th>窗口</th><th>收益率</th><th>投入</th><th>最终市值</th><th>交易数</th></tr></thead><tbody>'+details.map(d=>'<tr><td>'+esc(d.range||"-")+'</td><td class="'+(Number(d.returnRate)>=0?"good":"bad")+'">'+pctScore(d.returnRate)+'</td><td>'+money(d.totalInvested)+'</td><td>'+money(d.finalValue)+'</td><td>'+esc(d.tradeCount??"-")+'</td></tr>').join("")+'</tbody></table></div>':'';
      strategyParams.innerHTML='<div class="asset-grid">'+renderAmountCard("au9999","黄金 AU9999",sel)+renderAmountCard("csi300","沪深300",sel)+renderAmountCard("sp500","标普500",sel)+renderStrategyCard(sel)+'</div>';
    }
    rows.onclick=(e)=>{const tr=e.target.closest("tr[data-i]");if(!tr)return;selectedIdx=Number(tr.dataset.i);render()};
    renderTabs();render();
  </script>
</body>
</html>`;
}

// --- HTTP server ---
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  try {
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
        json(res, 200, runner.status());
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
        robustRunner.start({ ...(body || {}), workerCount: desiredSearchWorkers(body && body.workerCount) });
        json(res, 200, { ok: true, status: robustRunner.status() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/robust/stop") {
        robustRunner.stop();
        json(res, 200, { ok: true, status: robustRunner.status() });
        return;
      }

      if (req.method === "POST" && pathname === "/api/admin/export-static") {
        const body = await readJson(req);
        const siteUrl = body && body.siteUrl ? String(body.siteUrl).trim().replace(/\/$/, "") : "";

        // Collect local data
        const localByYear = {};
        for (let y = 1; y <= 10; y++) {
          localByYear[y] = Array.isArray(robustRunner.topByYear[y]) ? robustRunner.topByYear[y] : [];
        }
        const localCount = Object.values(localByYear).reduce((s, a) => s + a.length, 0);

        // Fetch remote data by parsing ALL_DATA from the live index.html
        let remoteByYear = {};
        let remoteCount = 0;
        let comparison = [];
        if (siteUrl) {
          try {
            const html = await new Promise((resolve, reject) => {
              const mod = siteUrl.startsWith("https") ? https : http;
              mod.get(siteUrl, (resp) => {
                if (resp.statusCode >= 300 && resp.statusCode < 400 && resp.headers.location) {
                  mod.get(resp.headers.location, (r2) => {
                    const c = []; r2.on("data", d => c.push(d)); r2.on("end", () => resolve(Buffer.concat(c).toString("utf8")));
                  }).on("error", reject);
                  return;
                }
                const c = []; resp.on("data", d => c.push(d)); resp.on("end", () => resolve(Buffer.concat(c).toString("utf8")));
              }).on("error", reject);
            });
            const m = html.match(/const\s+ALL_DATA\s*=\s*(\{[\s\S]*?\});/);
            if (m) {
              const parsed = JSON.parse(m[1]);
              for (let y = 1; y <= 10; y++) {
                remoteByYear[y] = Array.isArray(parsed[y]) ? parsed[y] : [];
              }
              remoteCount = Object.values(remoteByYear).reduce((s, a) => s + a.length, 0);
            }
            for (let y = 1; y <= 10; y++) {
              const lb = localByYear[y] && localByYear[y][0] ? localByYear[y][0].robustScore : null;
              const rb = remoteByYear[y] && remoteByYear[y][0] ? remoteByYear[y][0].robustScore : null;
              comparison.push({
                year: y,
                localBest: lb !== null ? Number(Number(lb).toFixed(4)) : null,
                remoteBest: rb !== null ? Number(Number(rb).toFixed(4)) : null,
                localCount: (localByYear[y] || []).length,
                remoteCount: (remoteByYear[y] || []).length,
                winner: lb === null && rb === null ? "none" : lb === null ? "remote" : rb === null ? "local" : lb >= rb ? "local" : "remote",
              });
            }
          } catch (err) {
            json(res, 500, { error: `拉取线上数据失败: ${err.message}` });
            return;
          }
        }

        // Merge: keep best per year
        const mergedByYear = {};
        for (let y = 1; y <= 10; y++) {
          mergedByYear[y] = dedupeEntries(
            [...(localByYear[y] || []), ...(remoteByYear[y] || [])],
            (e) => JSON.stringify([e.robustScore, e.avgScore, e.minScore, e.maxScore, e.stdDev, e.weights, e.configs])
          ).sort((a, b) => Number(b.robustScore) - Number(a.robustScore)).slice(0, 10);
        }
        const mergedCount = Object.values(mergedByYear).reduce((s, a) => s + a.length, 0);

        // Generate static HTML
        const staticHtml = buildStaticHtml(mergedByYear);
        const outPath = path.resolve(process.cwd(), "index.html");
        fs.writeFileSync(outPath, staticHtml, "utf8");

        // Git commit & push
        const { execSync } = require("child_process");
        const cwd = process.cwd();
        let pushed = false;
        try {
          execSync("git add index.html", { cwd });
          execSync('git diff --cached --quiet index.html', { cwd });
          // No changes — skip commit
        } catch (_) {
          // There are changes to commit
          const msg = `更新排行榜 ${new Date().toISOString().slice(0, 16)}`;
          execSync(`git commit -m "${msg}"`, { cwd });
          try {
            execSync("git push", { cwd, timeout: 30000 });
            pushed = true;
          } catch (pushErr) {
            // push failed (no remote, auth, etc.)
          }
        }

        json(res, 200, {
          ok: true,
          file: outPath,
          localCount,
          remoteCount,
          mergedCount,
          comparison,
          pushed,
        });
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
