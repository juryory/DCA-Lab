/**
 * DCALab Cloudflare Worker
 * - POST /api/submit/range   提交区间最优解结果（pending）
 * - POST /api/submit/robust  提交平均最优解结果（pending）
 * - POST /api/verify         验证脚本确认结果（需 admin token）
 * - GET  /api/range           读取已验证的区间最优解
 * - GET  /api/robust          读取已验证的平均最优解
 * - GET  /api/pending         读取待验证数据（需 admin token）
 *
 * KV keys:
 *   range:{rangeKey}          已验证的区间 top10
 *   robust:{year}             已验证的平均最优解 top10
 *   pending:range:{timestamp} 待验证区间数据
 *   pending:robust:{timestamp} 待验证平均最优解数据
 *   meta:range_keys           所有区间 key 列表
 */

const TOP_LIMIT = 10;
const RATE_LIMIT_WINDOW = 60000; // 1 min
const RATE_LIMIT_MAX = 10;
const rateLimitMap = new Map();

function envFlag(value) {
  return value === true || value === "1" || value === "true" || value === "yes" || value === "on";
}

function jsonResp(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-store",
    },
  });
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
  };
}

// --- Validation helpers ---
function validateWeights(w) {
  if (!w || typeof w !== "object") return false;
  const keys = ["au9999", "csi300", "sp500"];
  for (const k of keys) {
    if (typeof w[k] !== "number" || w[k] < 0 || w[k] > 1) return false;
  }
  const sum = keys.reduce((s, k) => s + (w[k] || 0), 0);
  return Math.abs(sum - 1) < 0.05;
}

function validateConfigs(configs) {
  if (!configs || typeof configs !== "object") return false;
  for (const k of ["au9999", "csi300", "sp500"]) {
    const c = configs[k];
    if (!c || typeof c !== "object") return false;
    if (typeof c.baseAmount !== "number" || c.baseAmount < 1) return false;
    if (typeof c.maWindow !== "number" || c.maWindow < 2) return false;
  }
  return true;
}

function validateRangeEntry(entry) {
  if (!entry || typeof entry !== "object") return "invalid object";
  if (typeof entry.score !== "number") return "missing score";
  if (typeof entry.returnRate !== "number") return "missing returnRate";
  if (entry.returnRate < -1 || entry.returnRate > 100) return "returnRate out of range";
  if (typeof entry.totalInvested !== "number" || entry.totalInvested <= 0) return "bad totalInvested";
  if (typeof entry.tradeCount !== "number" || entry.tradeCount < 0) return "bad tradeCount";
  if (!entry.rangeKey || typeof entry.rangeKey !== "string") return "missing rangeKey";
  if (entry.penaltyWeight !== undefined && (typeof entry.penaltyWeight !== "number" || entry.penaltyWeight < 0)) return "bad penaltyWeight";
  if (!validateWeights(entry.weights)) return "invalid weights";
  if (!validateConfigs(entry.configs)) return "invalid configs";
  return null;
}

function validateRobustEntry(entry) {
  if (!entry || typeof entry !== "object") return "invalid object";
  if (typeof entry.robustScore !== "number") return "missing robustScore";
  if (typeof entry.avgScore !== "number") return "missing avgScore";
  if (typeof entry.windowYears !== "number" || entry.windowYears < 2 || entry.windowYears > 8) return "bad windowYears";
  if (!Array.isArray(entry.windows) || entry.windows.length < 2) return "missing windows";
  if (entry.penaltyWeight !== undefined && (typeof entry.penaltyWeight !== "number" || entry.penaltyWeight < 0)) return "bad penaltyWeight";
  if (!validateWeights(entry.weights)) return "invalid weights";
  if (!validateConfigs(entry.configs)) return "invalid configs";
  return null;
}

// --- Rate limiting ---
function checkRateLimit(ip) {
  const now = Date.now();
  const record = rateLimitMap.get(ip);
  if (!record || now - record.start > RATE_LIMIT_WINDOW) {
    rateLimitMap.set(ip, { start: now, count: 1 });
    return true;
  }
  record.count++;
  return record.count <= RATE_LIMIT_MAX;
}

// --- Auth ---
function checkAdmin(request, env) {
  const token = env.ADMIN_TOKEN || "dcalab_admin";
  const auth = request.headers.get("Authorization") || "";
  return auth === `Bearer ${token}`;
}

// --- KV helpers ---
async function getJson(env, key) {
  const raw = await env.STORE.get(key);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch { return null; }
}

async function putJson(env, key, data) {
  await env.STORE.put(key, JSON.stringify(data));
}

async function snapshotRangeStore(env) {
  const rangeKeys = await getJson(env, "meta:range_keys") || [];
  const ranges = {};
  for (const rk of rangeKeys) {
    ranges[rk] = await getJson(env, `range:${rk}`) || [];
  }
  return {
    metaRangeKeys: rangeKeys,
    global: await getJson(env, "range:__global__") || [],
    ranges,
  };
}

function htmlResp(html, status = 200) {
  return new Response(html, {
    status,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

const HOME_HTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DCALab</title>
  <style>
    :root{--bg:#f6f1e7;--panel:#fffdf8;--line:#dfd3c0;--text:#30271b;--muted:#6f6554;--gold:#9a6818}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 "Segoe UI","Microsoft YaHei",sans-serif}
    .wrap{max-width:960px;margin:0 auto;padding:36px 20px}.hero{margin-bottom:24px}.hero h1{margin:0 0 8px;font-size:32px}
    .cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}.card{display:block;text-decoration:none;color:inherit;background:var(--panel);border:1px solid var(--line);border-radius:20px;padding:28px}
    .card h2{margin:0 0 10px;color:var(--gold)}.muted{color:var(--muted)}@media(max-width:700px){.cards{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>DCALab</h1>
      <div class="muted">Multi-asset DCA leaderboard and robustness explorer.</div>
    </div>
    <div class="cards">
      <a class="card" href="/range">
        <h2>Range Leaderboard</h2>
        <div>View the best strategies for a selected historical year range.</div>
      </a>
      <a class="card" href="/robust">
        <h2>Robust Leaderboard</h2>
        <div>View the most stable strategies across rolling year windows.</div>
      </a>
    </div>
  </div>
</body>
</html>`;

const RANGE_HTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DCALab Range Leaderboard</title>
  <style>
    :root{--bg:#f6f1e7;--panel:#fffdf8;--line:#dfd3c0;--text:#30271b;--muted:#6f6554;--good:#1f6b3c;--bad:#973939;--gold:#9a6818}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 "Segoe UI","Microsoft YaHei",sans-serif}
    .wrap{max-width:1280px;margin:0 auto;padding:20px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:14px;margin-bottom:12px}
    .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}.meta{font-size:12px;color:var(--muted)}select,button{padding:8px 10px;border:1px solid var(--line);border-radius:10px;background:#fff}
    table{width:100%;border-collapse:collapse}th,td{padding:8px 6px;border-bottom:1px solid #eee4d3;text-align:left}th{color:var(--muted)}tbody tr{cursor:pointer}.selected{background:#f8f0df}
    .grid{display:grid;grid-template-columns:1.2fr 1fr;gap:12px}.detail{border:1px solid var(--line);border-radius:12px;padding:10px;background:#fff}pre{margin:0;white-space:pre-wrap;word-break:break-word;font:12px/1.45 Consolas,"Courier New",monospace}
    .good{color:var(--good)} .bad{color:var(--bad)} a{color:var(--gold);text-decoration:none}@media(max-width:980px){.grid{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <div class="row">
        <h2 style="margin:0">Range Leaderboard</h2>
        <a href="/">Home</a>
      </div>
      <div class="row" style="margin-top:8px">
        <label>Year Range</label>
        <select id="startYear"></select>
        <span>to</span>
        <select id="endYear"></select>
        <button id="applyBtn">Apply</button>
        <button id="refreshBtn">Refresh</button>
      </div>
      <div id="meta" class="meta" style="margin-top:8px">Loading...</div>
    </div>
    <div class="grid">
      <div class="panel">
        <table>
          <thead><tr><th>#</th><th>Range</th><th>Score</th><th>Return</th><th>Invested</th><th>Final</th><th>AU</th><th>CSI300</th><th>SP500</th></tr></thead>
          <tbody id="rows"><tr><td colspan="9">Loading...</td></tr></tbody>
        </table>
      </div>
      <div class="detail">
        <div style="font-weight:700;margin-bottom:6px">Selected Parameters</div>
        <pre id="detail">Click a row to inspect details.</pre>
      </div>
    </div>
  </div>
  <script>
    const state={selectedRange:"",selectedIndex:0,entries:[],ranges:[],minYear:null,maxYear:null};
    const pct=v=>\`\${(Number(v)*100).toFixed(2)}%\`;
    const money=v=>Number(v).toLocaleString("zh-CN",{minimumFractionDigits:2,maximumFractionDigits:2});
    async function api(path){const r=await fetch(path);if(!r.ok)throw new Error(\`HTTP \${r.status}\`);return r.json();}
    function deriveBounds(items){let minYear=null,maxYear=null;for(const item of items||[]){const seg=String(item.key||"").split("-");const a=Number(seg[0]),b=Number(seg[1]);if(!Number.isInteger(a)||!Number.isInteger(b))continue;minYear=minYear===null?Math.min(a,b):Math.min(minYear,a,b);maxYear=maxYear===null?Math.max(a,b):Math.max(maxYear,a,b);}return {minYear,maxYear};}
    function formatEntry(entry){if(!entry)return "No details.";const lines=[];const weights=entry.weights||{};const configs=entry.configs||{};lines.push("Weights");lines.push(\`AU9999: \${((weights.au9999||0)*100).toFixed(0)}%\`);lines.push(\`CSI300: \${((weights.csi300||0)*100).toFixed(0)}%\`);lines.push(\`SP500: \${((weights.sp500||0)*100).toFixed(0)}%\`);lines.push("");for(const key of ["au9999","csi300","sp500"]){const cfg=configs[key];if(!cfg)continue;lines.push(key.toUpperCase());lines.push(\`baseAmount: \${cfg.baseAmount}\`);lines.push(\`dipAmount: \${cfg.dipAmount}\`);lines.push(\`maWindow: \${cfg.maWindow}\`);lines.push(\`scheduleMode: \${cfg.scheduleMode}\`);lines.push(\`buyMode: \${cfg.buyMode}\`);lines.push("");}return lines.join("\\n");}
    function setYearOptions(){if(!Number.isInteger(state.minYear)||!Number.isInteger(state.maxYear))return;const years=[];for(let y=state.minYear;y<=state.maxYear;y++)years.push(y);startYear.innerHTML=years.map(y=>\`<option value="\${y}">\${y}</option>\`).join("");endYear.innerHTML=years.map(y=>\`<option value="\${y}">\${y}</option>\`).join("");const seg=String(state.selectedRange||\`\${state.minYear}-\${state.maxYear}\`).split("-");startYear.value=String(Number(seg[0])||state.minYear);endYear.value=String(Number(seg[1])||state.maxYear);}
    function render(){setYearOptions();if(!state.entries.length){rows.innerHTML='<tr><td colspan="9">No results for this range.</td></tr>';detail.textContent="No results for this range.";return;}if(state.selectedIndex>=state.entries.length)state.selectedIndex=0;rows.innerHTML=state.entries.map((entry,i)=>{const w=entry.weights||{};return \`<tr data-i="\${i}" class="\${i===state.selectedIndex?"selected":""}"><td>\${i+1}</td><td>\${entry.rangeKey}</td><td>\${Number(entry.score).toFixed(2)}</td><td class="\${entry.returnRate>=0?"good":"bad"}">\${pct(entry.returnRate)}</td><td>\${money(entry.totalInvested)}</td><td>\${money(entry.finalValue)}</td><td>\${((w.au9999||0)*100).toFixed(0)}%</td><td>\${((w.csi300||0)*100).toFixed(0)}%</td><td>\${((w.sp500||0)*100).toFixed(0)}%</td></tr>\`;}).join("");detail.textContent=formatEntry(state.entries[state.selectedIndex]);}
    async function load(){try{const summary=await api("/api/range?range=all");state.ranges=Array.isArray(summary.ranges)?summary.ranges:[];const bounds=deriveBounds(state.ranges);state.minYear=bounds.minYear;state.maxYear=bounds.maxYear;if(!state.selectedRange&&state.minYear!==null&&state.maxYear!==null)state.selectedRange=\`\${state.minYear}-\${state.maxYear}\`;if(!state.selectedRange){state.entries=[];meta.textContent="No range data.";render();return;}const detailData=await api(\`/api/range?range=\${encodeURIComponent(state.selectedRange)}\`);state.entries=Array.isArray(detailData.entries)?detailData.entries:[];meta.textContent=\`Cloud API | ranges: \${state.ranges.length} | current: \${state.selectedRange}\`;render();}catch(err){meta.textContent=\`Load failed: \${err.message}\`;}}
    applyBtn.onclick=()=>{const s=Number(startYear.value),e=Number(endYear.value);if(!Number.isInteger(s)||!Number.isInteger(e))return;state.selectedRange=s<=e?\`\${s}-\${e}\`:\`\${e}-\${s}\`;state.selectedIndex=0;load();};
    refreshBtn.onclick=()=>load();
    rows.onclick=(e)=>{const tr=e.target.closest("tr[data-i]");if(!tr)return;state.selectedIndex=Number(tr.dataset.i);render();};
    load();setInterval(load,15000);
  </script>
</body>
</html>`;

const ROBUST_HTML = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DCALab Robust Leaderboard</title>
  <style>
    :root{--bg:#f6f1e7;--panel:#fffdf8;--line:#dfd3c0;--text:#30271b;--muted:#6f6554;--gold:#9a6818;--gold2:#c58b2a;--good:#1f6b3c;--bad:#973939}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 "Segoe UI","Microsoft YaHei",sans-serif}
    .wrap{max-width:1400px;margin:0 auto;padding:24px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:16px;margin-bottom:12px}
    .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.meta{font-size:12px;color:var(--muted)}.tabs{display:flex;gap:6px;margin:12px 0}
    .tab{padding:8px 16px;border:1px solid var(--line);border-radius:999px;cursor:pointer;background:#fff;font-size:13px}.tab.active{background:linear-gradient(135deg,var(--gold),var(--gold2));color:#fff;border-color:transparent}
    .cards{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:14px}.card{padding:12px;border:1px solid var(--line);border-radius:14px;background:#fff}.k{font-size:12px;color:var(--muted)}.v{font-size:20px;font-weight:700}
    .layout{display:grid;grid-template-columns:1fr 1fr;gap:12px}.table-wrap{max-height:500px;overflow:auto;border:1px solid var(--line);border-radius:14px;background:#fff}table{width:100%;border-collapse:collapse;font-size:13px}th,td{border-bottom:1px solid #eee4d3;padding:8px 6px;text-align:left}th{position:sticky;top:0;background:#fff;color:var(--muted)}tbody tr{cursor:pointer}.selected{background:#f8f0df}.detail{border:1px solid var(--line);border-radius:12px;padding:12px;background:#fff}pre{margin:0;white-space:pre-wrap;word-break:break-word;font:12px/1.5 Consolas,"Courier New",monospace}a{color:var(--gold);text-decoration:none}@media(max-width:1000px){.cards{grid-template-columns:repeat(3,1fr)}.layout{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel"><div class="row"><h2 style="margin:0">Robust Leaderboard</h2><a href="/">Home</a></div><div class="meta" style="margin-top:6px">Stable multi-asset strategies ranked by rolling window performance.</div></div>
    <div class="panel"><div id="statusText" class="meta">Loading...</div></div>
    <div class="tabs" id="yearTabs"></div>
    <div class="cards">
      <div class="card"><div class="k">Best Robust</div><div class="v" id="vBestRobust">-</div></div>
      <div class="card"><div class="k">Best Avg</div><div class="v" id="vBestAvg">-</div></div>
      <div class="card"><div class="k">Best Min</div><div class="v" id="vBestMin">-</div></div>
      <div class="card"><div class="k">Entries</div><div class="v" id="vAttempts">-</div></div>
      <div class="card"><div class="k">Valid</div><div class="v" id="vValid">-</div></div>
    </div>
    <div class="layout">
      <div class="panel" style="padding:0"><div class="table-wrap"><table><thead><tr><th>#</th><th>Robust</th><th>Avg</th><th>Min</th><th>Max</th><th>StdDev</th><th>Windows</th><th>AU</th><th>CSI300</th><th>SP500</th></tr></thead><tbody id="rows"><tr><td colspan="10">Loading...</td></tr></tbody></table></div></div>
      <div><div class="detail"><div class="k" style="margin-bottom:6px">Selected Strategy</div><pre id="detail">Click a row to view details.</pre></div></div>
    </div>
  </div>
  <script>
    let selectedYear=3,selectedIdx=0,entries=[];
    async function api(path){const r=await fetch(path);if(!r.ok)throw new Error(\`HTTP \${r.status}\`);return r.json();}
    function renderTabs(){yearTabs.innerHTML="";for(let y=2;y<=8;y++){const tab=document.createElement("div");tab.className="tab"+(y===selectedYear?" active":"");tab.textContent=\`\${y}Y\`;tab.onclick=()=>{selectedYear=y;selectedIdx=0;renderTabs();refresh();};yearTabs.appendChild(tab);}}
    function formatEntry(entry){if(!entry)return "No details.";const lines=[];const weights=entry.weights||{};const configs=entry.configs||{};lines.push(\`Robust: \${entry.robustScore}\`);lines.push(\`Avg: \${entry.avgScore}\`);lines.push(\`Min: \${entry.minScore}\`);lines.push(\`Max: \${entry.maxScore}\`);lines.push(\`StdDev: \${entry.stdDev}\`);lines.push(\`Window Years: \${entry.windowYears||selectedYear}\`);lines.push(\`Window Count: \${entry.windowCount||(entry.windows?entry.windows.length:"-")}\`);lines.push("");lines.push("Weights");lines.push(\`AU9999: \${((weights.au9999||0)*100).toFixed(0)}%\`);lines.push(\`CSI300: \${((weights.csi300||0)*100).toFixed(0)}%\`);lines.push(\`SP500: \${((weights.sp500||0)*100).toFixed(0)}%\`);lines.push("");for(const key of ["au9999","csi300","sp500"]){const cfg=configs[key];if(!cfg)continue;lines.push(key.toUpperCase());lines.push(\`baseAmount: \${cfg.baseAmount}\`);lines.push(\`dipAmount: \${cfg.dipAmount}\`);lines.push(\`maWindow: \${cfg.maWindow}\`);lines.push(\`buyMode: \${cfg.buyMode}\`);lines.push("");}if(Array.isArray(entry.windows)&&entry.windows.length){lines.push("Windows");lines.push(entry.windows.join(", "));}return lines.join("\\n");}
    function render(list,count){entries=list||[];vAttempts.textContent=count??entries.length;vValid.textContent=count??entries.length;const best=entries[0];vBestRobust.textContent=best?Number(best.robustScore).toFixed(2):"-";vBestAvg.textContent=best?Number(best.avgScore).toFixed(2):"-";vBestMin.textContent=best?Number(best.minScore).toFixed(2):"-";statusText.textContent=\`Cloud API | year window: \${selectedYear}Y | entries: \${count??entries.length}\`;if(!entries.length){rows.innerHTML='<tr><td colspan="10">No results for this year window.</td></tr>';detail.textContent="No results for this year window.";return;}if(selectedIdx>=entries.length)selectedIdx=0;rows.innerHTML=entries.map((entry,i)=>{const w=entry.weights||{};return \`<tr data-i="\${i}" class="\${i===selectedIdx?"selected":""}"><td>\${i+1}</td><td>\${Number(entry.robustScore).toFixed(2)}</td><td>\${Number(entry.avgScore).toFixed(2)}</td><td>\${Number(entry.minScore).toFixed(2)}</td><td>\${Number(entry.maxScore).toFixed(2)}</td><td>\${Number(entry.stdDev).toFixed(2)}</td><td>\${entry.windowCount||(entry.windows?entry.windows.length:"-")}</td><td>\${((w.au9999||0)*100).toFixed(0)}%</td><td>\${((w.csi300||0)*100).toFixed(0)}%</td><td>\${((w.sp500||0)*100).toFixed(0)}%</td></tr>\`;}).join("");detail.textContent=formatEntry(entries[selectedIdx]);}
    rows.onclick=(e)=>{const tr=e.target.closest("tr[data-i]");if(!tr)return;selectedIdx=Number(tr.dataset.i);render(entries,entries.length);};
    async function refresh(){try{const summary=await api("/api/robust");const detailData=await api(\`/api/robust?year=\${selectedYear}\`);const count=(summary.summary&&summary.summary[selectedYear]&&summary.summary[selectedYear].count)||0;render(Array.isArray(detailData.entries)?detailData.entries:[],count);}catch(err){statusText.textContent=\`Refresh failed: \${err.message}\`;}}
    renderTabs();refresh();setInterval(refresh,15000);
  </script>
</body>
</html>`;

async function snapshotRobustStore(env) {
  const robustSnap = {};
  for (let y = 2; y <= 8; y++) {
    robustSnap[y] = await getJson(env, `robust:${y}`) || [];
  }
  return robustSnap;
}

// --- Merge logic ---
function mergeTopRange(existing, newEntries) {
  const list = [...(existing || [])];
  for (const e of newEntries) {
    // Deduplicate by checking if same configs already exist (by score + rangeKey)
    list.push(e);
  }
  list.sort((a, b) => b.score - a.score);
  if (list.length > TOP_LIMIT) list.length = TOP_LIMIT;
  return list;
}

function mergeTopRobust(existing, newEntries) {
  const list = [...(existing || [])];
  for (const e of newEntries) {
    list.push(e);
  }
  list.sort((a, b) => b.robustScore - a.robustScore);
  if (list.length > TOP_LIMIT) list.length = TOP_LIMIT;
  return list;
}

async function mergeApprovedRangeEntries(env, entries) {
  const byRange = {};
  for (const e of entries) {
    if (!byRange[e.rangeKey]) byRange[e.rangeKey] = [];
    byRange[e.rangeKey].push(e);
  }

  const rangeKeys = new Set(await getJson(env, "meta:range_keys") || []);
  for (const [rk, rangeEntries] of Object.entries(byRange)) {
    const existing = await getJson(env, `range:${rk}`) || [];
    const merged = mergeTopRange(existing, rangeEntries);
    await putJson(env, `range:${rk}`, merged);
    rangeKeys.add(rk);
  }
  await putJson(env, "meta:range_keys", [...rangeKeys]);

  const globalExisting = await getJson(env, "range:__global__") || [];
  const globalMerged = mergeTopRange(globalExisting, entries);
  await putJson(env, "range:__global__", globalMerged);
}

async function mergeApprovedRobustEntries(env, entries) {
  const byYear = {};
  for (const e of entries) {
    const yr = e.windowYears;
    if (!byYear[yr]) byYear[yr] = [];
    byYear[yr].push(e);
  }
  for (const [yr, yearEntries] of Object.entries(byYear)) {
    const existing = await getJson(env, `robust:${yr}`) || [];
    const merged = mergeTopRobust(existing, yearEntries);
    await putJson(env, `robust:${yr}`, merged);
  }
}

async function createBackupForPendingType(env, pendingKey) {
  const backupTs = Date.now();
  if (pendingKey.startsWith("pending:range:")) {
    await putJson(env, `backup:range:${backupTs}`, { ts: backupTs, ...(await snapshotRangeStore(env)) });
  } else if (pendingKey.startsWith("pending:robust:")) {
    await putJson(env, `backup:robust:${backupTs}`, { ts: backupTs, data: await snapshotRobustStore(env) });
  }
}

async function approvePendingData(env, pendingKey, pendingData, options = {}) {
  const { backup = true, deletePending = true } = options;
  if (backup) {
    await createBackupForPendingType(env, pendingKey);
  }

  if (pendingKey.startsWith("pending:range:")) {
    await mergeApprovedRangeEntries(env, pendingData.entries);
  } else if (pendingKey.startsWith("pending:robust:")) {
    await mergeApprovedRobustEntries(env, pendingData.entries);
  } else {
    throw new Error("unknown pending type");
  }

  if (deletePending) {
    await env.STORE.delete(pendingKey);
  }
}

// --- Handlers ---
async function handleSubmitRange(request, env) {
  const ip = request.headers.get("CF-Connecting-IP") || "unknown";
  if (!checkRateLimit(ip)) return jsonResp({ error: "rate limited" }, 429);

  let body;
  try { body = await request.json(); } catch { return jsonResp({ error: "invalid JSON" }, 400); }

  const entries = Array.isArray(body) ? body : (body.results || [body]);
  if (!entries.length) return jsonResp({ error: "no entries" }, 400);

  const valid = [];
  for (const e of entries) {
    const err = validateRangeEntry(e);
    if (err) continue;
    valid.push(e);
  }
  if (!valid.length) return jsonResp({ error: "no valid entries" }, 400);

  if (envFlag(env.DIRECT_SUBMIT)) {
    await mergeApprovedRangeEntries(env, valid);
    return jsonResp({ ok: true, accepted: valid.length, pending: false, direct: true });
  }

  // Store as pending
  const pendingKey = `pending:range:${Date.now()}:${ip.replace(/[:.]/g, "_")}`;
  await putJson(env, pendingKey, { submittedAt: new Date().toISOString(), ip, entries: valid });

  return jsonResp({ ok: true, accepted: valid.length, pending: true });
}

async function handleSubmitRobust(request, env) {
  const ip = request.headers.get("CF-Connecting-IP") || "unknown";
  if (!checkRateLimit(ip)) return jsonResp({ error: "rate limited" }, 429);

  let body;
  try { body = await request.json(); } catch { return jsonResp({ error: "invalid JSON" }, 400); }

  const entries = Array.isArray(body) ? body : (body.results || [body]);
  if (!entries.length) return jsonResp({ error: "no entries" }, 400);

  const valid = [];
  for (const e of entries) {
    const err = validateRobustEntry(e);
    if (err) continue;
    valid.push(e);
  }
  if (!valid.length) return jsonResp({ error: "no valid entries" }, 400);

  if (envFlag(env.DIRECT_SUBMIT)) {
    await mergeApprovedRobustEntries(env, valid);
    return jsonResp({ ok: true, accepted: valid.length, pending: false, direct: true });
  }

  const pendingKey = `pending:robust:${Date.now()}:${ip.replace(/[:.]/g, "_")}`;
  await putJson(env, pendingKey, { submittedAt: new Date().toISOString(), ip, entries: valid });

  return jsonResp({ ok: true, accepted: valid.length, pending: true });
}

// --- Read verified data ---
async function handleGetRange(request, env) {
  const url = new URL(request.url);
  const rangeKey = url.searchParams.get("range") || "all";

  if (rangeKey === "all") {
    // Return summary of all ranges
    const keysData = await getJson(env, "meta:range_keys") || [];
    const summary = [];
    for (const rk of keysData) {
      const list = await getJson(env, `range:${rk}`) || [];
      summary.push({ key: rk, count: list.length, bestScore: list[0] ? list[0].score : null });
    }
    summary.sort((a, b) => (b.bestScore ?? -Infinity) - (a.bestScore ?? -Infinity));

    // Also return global top 10
    const globalTop = await getJson(env, "range:__global__") || [];
    return jsonResp({ ranges: summary, entries: globalTop });
  }

  const list = await getJson(env, `range:${rangeKey}`) || [];
  return jsonResp({ range: rangeKey, entries: list });
}

async function handleGetRobust(request, env) {
  const url = new URL(request.url);
  const year = url.searchParams.get("year");

  if (year) {
    const yr = Number(year);
    if (yr < 2 || yr > 8) return jsonResp({ error: "year must be 2-8" }, 400);
    const list = await getJson(env, `robust:${yr}`) || [];
    return jsonResp({ year: yr, entries: list });
  }

  // Summary
  const summary = {};
  for (let y = 2; y <= 8; y++) {
    const list = await getJson(env, `robust:${y}`) || [];
    summary[y] = { count: list.length, best: list[0] ? list[0].robustScore : null };
  }
  return jsonResp({ summary });
}

// --- Pending data (admin only) ---
async function handleGetPending(request, env) {
  if (!checkAdmin(request, env)) return jsonResp({ error: "unauthorized" }, 401);

  const allKeys = await env.STORE.list({ prefix: "pending:" });
  const pending = [];
  for (const key of allKeys.keys) {
    const data = await getJson(env, key.name);
    if (data) pending.push({ key: key.name, ...data });
  }
  return jsonResp({ pending, count: pending.length });
}

// --- Verify (admin confirms pending data) ---
async function handleVerify(request, env) {
  if (!checkAdmin(request, env)) return jsonResp({ error: "unauthorized" }, 401);

  let body;
  try { body = await request.json(); } catch { return jsonResp({ error: "invalid JSON" }, 400); }

  const { pendingKey, action } = body; // action: "approve" or "reject"
  if (!pendingKey) return jsonResp({ error: "missing pendingKey" }, 400);

  if (action === "reject") {
    await env.STORE.delete(pendingKey);
    return jsonResp({ ok: true, action: "rejected" });
  }

  // Approve: merge into verified store
  const pendingData = await getJson(env, pendingKey);
  if (!pendingData || !pendingData.entries) {
    return jsonResp({ error: "pending data not found" }, 404);
  }

  // Backup current data before merge (防止污染数据挤掉好数据后无法恢复)
  const backupTs = Date.now();
  if (pendingKey.startsWith("pending:range:")) {
    await putJson(env, `backup:range:${backupTs}`, { ts: backupTs, ...(await snapshotRangeStore(env)) });
  } else if (pendingKey.startsWith("pending:robust:")) {
    const robustSnap = {};
    for (let y = 2; y <= 8; y++) {
      robustSnap[y] = await getJson(env, `robust:${y}`) || [];
    }
    await putJson(env, `backup:robust:${backupTs}`, { ts: backupTs, data: robustSnap });
  }

  if (pendingKey.startsWith("pending:range:")) {
    // Group entries by rangeKey
    const byRange = {};
    for (const e of pendingData.entries) {
      if (!byRange[e.rangeKey]) byRange[e.rangeKey] = [];
      byRange[e.rangeKey].push(e);
    }

    // Merge each range
    const rangeKeys = new Set(await getJson(env, "meta:range_keys") || []);
    for (const [rk, entries] of Object.entries(byRange)) {
      const existing = await getJson(env, `range:${rk}`) || [];
      const merged = mergeTopRange(existing, entries);
      await putJson(env, `range:${rk}`, merged);
      rangeKeys.add(rk);
    }
    await putJson(env, "meta:range_keys", [...rangeKeys]);

    // Update global top
    const globalExisting = await getJson(env, "range:__global__") || [];
    const globalMerged = mergeTopRange(globalExisting, pendingData.entries);
    await putJson(env, "range:__global__", globalMerged);

  } else if (pendingKey.startsWith("pending:robust:")) {
    // Group by windowYears
    const byYear = {};
    for (const e of pendingData.entries) {
      const yr = e.windowYears;
      if (!byYear[yr]) byYear[yr] = [];
      byYear[yr].push(e);
    }
    for (const [yr, entries] of Object.entries(byYear)) {
      const existing = await getJson(env, `robust:${yr}`) || [];
      const merged = mergeTopRobust(existing, entries);
      await putJson(env, `robust:${yr}`, merged);
    }
  }

  // Delete pending
  await env.STORE.delete(pendingKey);
  return jsonResp({ ok: true, action: "approved", entriesCount: pendingData.entries.length });
}

async function handleVerifyV2(request, env) {
  if (!checkAdmin(request, env)) return jsonResp({ error: "unauthorized" }, 401);

  let body;
  try { body = await request.json(); } catch { return jsonResp({ error: "invalid JSON" }, 400); }

  const { pendingKey, action } = body;
  if (!pendingKey) return jsonResp({ error: "missing pendingKey" }, 400);

  if (action === "reject") {
    await env.STORE.delete(pendingKey);
    return jsonResp({ ok: true, action: "rejected" });
  }

  const pendingData = await getJson(env, pendingKey);
  if (!pendingData || !Array.isArray(pendingData.entries)) {
    return jsonResp({ error: "pending data not found" }, 404);
  }

  await approvePendingData(env, pendingKey, pendingData, { backup: true, deletePending: true });
  return jsonResp({ ok: true, action: "approved", entriesCount: pendingData.entries.length });
}

async function handleApproveAllPending(request, env) {
  if (!checkAdmin(request, env)) return jsonResp({ error: "unauthorized" }, 401);

  const list = await env.STORE.list({ prefix: "pending:" });
  const keys = (list.keys || []).map((k) => k.name);
  if (!keys.length) return jsonResp({ ok: true, approved: 0, entries: 0 });

  let hasRange = false;
  let hasRobust = false;
  for (const key of keys) {
    if (key.startsWith("pending:range:")) hasRange = true;
    if (key.startsWith("pending:robust:")) hasRobust = true;
  }

  const backupTs = Date.now();
  if (hasRange) {
    await putJson(env, `backup:range:${backupTs}`, { ts: backupTs, ...(await snapshotRangeStore(env)) });
  }
  if (hasRobust) {
    await putJson(env, `backup:robust:${backupTs}`, { ts: backupTs, data: await snapshotRobustStore(env) });
  }

  let approved = 0;
  let entryCount = 0;
  for (const key of keys) {
    const pendingData = await getJson(env, key);
    if (!pendingData || !Array.isArray(pendingData.entries)) continue;
    await approvePendingData(env, key, pendingData, { backup: false, deletePending: true });
    approved++;
    entryCount += pendingData.entries.length;
  }

  return jsonResp({ ok: true, approved, entries: entryCount, directSubmit: envFlag(env.DIRECT_SUBMIT) });
}

// --- Rollback: restore from backup ---
async function handleRollback(request, env) {
  if (!checkAdmin(request, env)) return jsonResp({ error: "unauthorized" }, 401);

  let body;
  try { body = await request.json(); } catch { return jsonResp({ error: "invalid JSON" }, 400); }

  const { backupKey } = body;
  if (!backupKey) return jsonResp({ error: "missing backupKey" }, 400);

  const backup = await getJson(env, backupKey);
  if (!backup) return jsonResp({ error: "backup not found" }, 404);

  if (backupKey.startsWith("backup:range:")) {
    const existing = await env.STORE.list({ prefix: "range:" });
    for (const key of existing.keys || []) {
      await env.STORE.delete(key.name);
    }
    await putJson(env, "range:__global__", Array.isArray(backup.global) ? backup.global : []);
    await putJson(env, "meta:range_keys", Array.isArray(backup.metaRangeKeys) ? backup.metaRangeKeys : []);
    for (const [rk, entries] of Object.entries(backup.ranges || {})) {
      await putJson(env, `range:${rk}`, Array.isArray(entries) ? entries : []);
    }
    return jsonResp({ ok: true, restored: "range", entries: (backup.global || []).length, ranges: Object.keys(backup.ranges || {}).length });
  }

  if (backupKey.startsWith("backup:robust:")) {
    for (let y = 2; y <= 8; y++) {
      await env.STORE.delete(`robust:${y}`);
    }
    for (const [yr, entries] of Object.entries(backup.data || {})) {
      await putJson(env, `robust:${yr}`, entries);
    }
    return jsonResp({ ok: true, restored: "robust" });
  }

  return jsonResp({ error: "unknown backup type" }, 400);
}

// --- List backups ---
async function handleListBackups(request, env) {
  if (!checkAdmin(request, env)) return jsonResp({ error: "unauthorized" }, 401);

  const list = await env.STORE.list({ prefix: "backup:" });
  const keys = (list.keys || []).map((k) => k.name);
  return jsonResp({ backups: keys });
}

// --- Main fetch handler ---
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    try {
      if (request.method === "GET" && (path === "/" || path === "/index.html")) {
        return htmlResp(HOME_HTML);
      }
      if (request.method === "GET" && (path === "/range" || path === "/range.html")) {
        return htmlResp(RANGE_HTML);
      }
      if (request.method === "GET" && (path === "/robust" || path === "/robust.html")) {
        return htmlResp(ROBUST_HTML);
      }

      // Submit endpoints
      if (request.method === "POST" && path === "/api/submit/range") {
        return await handleSubmitRange(request, env);
      }
      if (request.method === "POST" && path === "/api/submit/robust") {
        return await handleSubmitRobust(request, env);
      }

      // Verify endpoint (admin)
      if (request.method === "POST" && path === "/api/verify") {
        return await handleVerifyV2(request, env);
      }

      // Read endpoints
      if (request.method === "GET" && path === "/api/range") {
        return await handleGetRange(request, env);
      }
      if (request.method === "GET" && path === "/api/robust") {
        return await handleGetRobust(request, env);
      }

      // Pending (admin)
      if (request.method === "GET" && path === "/api/pending") {
        return await handleGetPending(request, env);
      }
      if (request.method === "POST" && path === "/api/pending/approve-all") {
        return await handleApproveAllPending(request, env);
      }

      // Rollback (admin) — 恢复到备份数据
      if (request.method === "POST" && path === "/api/rollback") {
        return await handleRollback(request, env);
      }

      // List backups (admin)
      if (request.method === "GET" && path === "/api/backups") {
        return await handleListBackups(request, env);
      }

      return jsonResp({ error: "not found" }, 404);
    } catch (err) {
      return jsonResp({ error: err.message || "internal error" }, 500);
    }
  },
};
