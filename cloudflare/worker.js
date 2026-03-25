/**
 * DCALab Cloudflare Worker
 * - GET  /api/range           读取已验证的区间最优解
 * - GET  /api/robust          读取已验证的平均最优解
 * - POST /api/admin/robust/replace  直接覆盖稳健排行榜（需 admin token）
 *
 * KV keys:
 *   range:{rangeKey}          已验证的区间 top10
 *   robust:{year}             已验证的平均最优解 top10
 *   meta:range_keys           所有区间 key 列表
 */

const TOP_LIMIT = 10;
const RATE_LIMIT_WINDOW = 60000; // 1 min
const RATE_LIMIT_MAX = 10;
const rateLimitMap = new Map();

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
  if (typeof entry.windowYears !== "number" || entry.windowYears < 1 || entry.windowYears > 10) return "bad windowYears";
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
      <div class="muted">多资产定投排行榜与稳健性探索平台。</div>
    </div>
    <div class="cards">
      <a class="card" href="/range">
        <h2>区间排行榜</h2>
        <div>查看指定历史年份区间内表现最好的定投策略。</div>
      </a>
      <a class="card" href="/robust">
        <h2>稳健排行榜</h2>
        <div>查看不同滚动年份窗口下最稳健的策略表现。</div>
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
  <title>DCALab 区间排行榜</title>
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
        <h2 style="margin:0">区间排行榜</h2>
        <a href="/">首页</a>
      </div>
      <div class="row" style="margin-top:8px">
        <label>年份区间</label>
        <select id="startYear"></select>
        <span>至</span>
        <select id="endYear"></select>
        <button id="applyBtn">应用</button>
        <button id="refreshBtn">刷新</button>
      </div>
      <div id="meta" class="meta" style="margin-top:8px">加载中...</div>
    </div>
    <div class="grid">
      <div class="panel">
        <table>
          <thead><tr><th>#</th><th>区间</th><th>评分</th><th>收益率</th><th>投入</th><th>最终市值</th><th>黄金</th><th>沪深300</th><th>标普500</th></tr></thead>
          <tbody id="rows"><tr><td colspan="9">加载中...</td></tr></tbody>
        </table>
      </div>
      <div class="detail">
        <div style="font-weight:700;margin-bottom:6px">选中参数</div>
        <pre id="detail">点击任意一行查看详情。</pre>
      </div>
    </div>
  </div>
  <script>
    const state={selectedRange:"",selectedIndex:0,entries:[],ranges:[],minYear:null,maxYear:null};
    const pct=v=>\`\${(Number(v)*100).toFixed(2)}%\`;
    const money=v=>Number(v).toLocaleString("zh-CN",{minimumFractionDigits:2,maximumFractionDigits:2});
    async function api(path){const r=await fetch(path);if(!r.ok)throw new Error(\`HTTP \${r.status}\`);return r.json();}
    function deriveBounds(items){let minYear=null,maxYear=null;for(const item of items||[]){const seg=String(item.key||"").split("-");const a=Number(seg[0]),b=Number(seg[1]);if(!Number.isInteger(a)||!Number.isInteger(b))continue;minYear=minYear===null?Math.min(a,b):Math.min(minYear,a,b);maxYear=maxYear===null?Math.max(a,b):Math.max(maxYear,a,b);}return {minYear,maxYear};}
    function formatEntry(entry){if(!entry)return "暂无详情。";const lines=[];const weights=entry.weights||{};const configs=entry.configs||{};lines.push("权重");lines.push(\`黄金AU9999: \${((weights.au9999||0)*100).toFixed(0)}%\`);lines.push(\`沪深300: \${((weights.csi300||0)*100).toFixed(0)}%\`);lines.push(\`标普500: \${((weights.sp500||0)*100).toFixed(0)}%\`);lines.push("");for(const key of ["au9999","csi300","sp500"]){const cfg=configs[key];if(!cfg)continue;lines.push(key.toUpperCase());lines.push(\`baseAmount: \${cfg.baseAmount}\`);lines.push(\`dipAmount: \${cfg.dipAmount}\`);lines.push(\`maWindow: \${cfg.maWindow}\`);lines.push(\`scheduleMode: \${cfg.scheduleMode}\`);lines.push(\`buyMode: \${cfg.buyMode}\`);lines.push("");}return lines.join("\\n");}
    function setYearOptions(){if(!Number.isInteger(state.minYear)||!Number.isInteger(state.maxYear))return;const years=[];for(let y=state.minYear;y<=state.maxYear;y++)years.push(y);startYear.innerHTML=years.map(y=>\`<option value="\${y}">\${y}</option>\`).join("");endYear.innerHTML=years.map(y=>\`<option value="\${y}">\${y}</option>\`).join("");const seg=String(state.selectedRange||\`\${state.minYear}-\${state.maxYear}\`).split("-");startYear.value=String(Number(seg[0])||state.minYear);endYear.value=String(Number(seg[1])||state.maxYear);}
    function render(){setYearOptions();if(!state.entries.length){rows.innerHTML='<tr><td colspan="9">当前区间暂无结果。</td></tr>';detail.textContent="当前区间暂无结果。";return;}if(state.selectedIndex>=state.entries.length)state.selectedIndex=0;rows.innerHTML=state.entries.map((entry,i)=>{const w=entry.weights||{};return \`<tr data-i="\${i}" class="\${i===state.selectedIndex?"selected":""}"><td>\${i+1}</td><td>\${entry.rangeKey}</td><td>\${Number(entry.score).toFixed(2)}</td><td class="\${entry.returnRate>=0?"good":"bad"}">\${pct(entry.returnRate)}</td><td>\${money(entry.totalInvested)}</td><td>\${money(entry.finalValue)}</td><td>\${((w.au9999||0)*100).toFixed(0)}%</td><td>\${((w.csi300||0)*100).toFixed(0)}%</td><td>\${((w.sp500||0)*100).toFixed(0)}%</td></tr>\`;}).join("");detail.textContent=formatEntry(state.entries[state.selectedIndex]);}
    async function load(){try{const summary=await api("/api/range?range=all");state.ranges=Array.isArray(summary.ranges)?summary.ranges:[];const bounds=deriveBounds(state.ranges);state.minYear=bounds.minYear;state.maxYear=bounds.maxYear;if(!state.selectedRange&&state.minYear!==null&&state.maxYear!==null)state.selectedRange=\`\${state.minYear}-\${state.maxYear}\`;if(!state.selectedRange){state.entries=[];meta.textContent="暂无区间数据。";render();return;}const detailData=await api(\`/api/range?range=\${encodeURIComponent(state.selectedRange)}\`);state.entries=Array.isArray(detailData.entries)?detailData.entries:[];meta.textContent=\`云端 API | 区间数量: \${state.ranges.length} | 当前区间: \${state.selectedRange}\`;render();}catch(err){meta.textContent=\`加载失败: \${err.message}\`;}}
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
  <title>DCALab 稳健排行榜</title>
  <style>
    :root{--bg:#f6f1e7;--panel:#fffdf8;--line:#dfd3c0;--text:#30271b;--muted:#6f6554;--gold:#9a6818;--gold2:#c58b2a;--good:#1f6b3c;--bad:#973939;--soft:#f8f0df;--soft2:#f3e7d2}
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
    .k{font-size:12px;color:var(--muted)}
    .v{font-size:20px;font-weight:700}
    .good{color:var(--good)}
    .bad{color:var(--bad)}
    .layout{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .table-wrap{max-height:500px;overflow:auto;border:1px solid var(--line);border-radius:14px;background:#fff}
    table{width:100%;border-collapse:collapse;font-size:13px}
    th,td{border-bottom:1px solid #eee4d3;padding:8px 6px;text-align:left}
    th{position:sticky;top:0;background:#fff;color:var(--muted)}
    tbody tr{cursor:pointer}
    tbody tr.selected{background:var(--soft)}
    .detail{border:1px solid var(--line);border-radius:12px;padding:14px;background:#fff}
    .detail-empty{color:var(--muted);padding:8px 0}
    .section{margin-top:14px}
    .section-title{font-size:13px;font-weight:700;margin:0 0 8px}
    .asset-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
    .asset-card{border:1px solid var(--line);border-radius:14px;padding:12px;background:#fffdfa}
    .asset-head{display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:8px}
    .asset-name{font-weight:700}
    .asset-points{display:grid;gap:6px}
    .point{font-size:13px;color:var(--text)}
    .point strong{color:var(--muted);font-weight:600}
    .point code{font:12px/1.4 Consolas,"Courier New",monospace;background:#f8f0df;padding:1px 5px;border-radius:999px}
    .mini-table{width:100%;border-collapse:collapse;font-size:12px;background:#fff;border:1px solid var(--line);border-radius:12px;overflow:hidden}
    .mini-table th,.mini-table td{padding:8px 6px;border-bottom:1px solid #eee4d3;text-align:left}
    .mini-table th{background:#fcf7ef;color:var(--muted);position:static}
    .mini-wrap{overflow:auto;border-radius:12px}
    a{color:var(--gold);text-decoration:none}
    @media(max-width:1100px){.asset-grid{grid-template-columns:1fr}}
    @media(max-width:1000px){.cards{grid-template-columns:repeat(3,1fr)}.layout{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <div class="row">
        <h2 style="margin:0">稳健排行榜</h2>
      </div>
      <div class="meta" style="margin-top:6px">按滚动年份窗口表现，筛选最稳健的多资产定投策略。</div>
    </div>

    <div class="panel">
      <div id="statusText" class="meta">加载中...</div>
    </div>

    <div class="tabs" id="yearTabs"></div>

    <div class="cards">
      <div class="card"><div class="k">最佳稳健分</div><div class="v" id="vBestRobust">-</div></div>
      <div class="card"><div class="k">最佳平均分</div><div class="v" id="vBestAvg">-</div></div>
      <div class="card"><div class="k">最佳最低分</div><div class="v" id="vBestMin">-</div></div>
      <div class="card"><div class="k">结果数量</div><div class="v" id="vAttempts">-</div></div>
      <div class="card"><div class="k">有效数量</div><div class="v" id="vValid">-</div></div>
    </div>

    <div class="layout">
      <div class="panel" style="padding:0">
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th>#</th><th>稳健分</th><th>平均分</th><th>最低分</th><th>最高分</th><th>标准差</th><th>窗口数</th><th>黄金</th><th>沪深300</th><th>标普500</th>
            </tr></thead>
            <tbody id="rows"><tr><td colspan="10">加载中...</td></tr></tbody>
          </table>
        </div>
      </div>
      <div>
        <div class="detail">
          <div class="k" style="margin-bottom:8px">各窗口回测结果</div>
          <div id="detail" class="detail-empty">点击任意一行查看窗口回测结果。</div>
        </div>
      </div>
    </div>

    <div class="panel">
      <div class="k" style="margin-bottom:8px">3个资产策略参数</div>
      <div id="strategyParams" class="detail-empty">点击任意一行查看详细策略参数。</div>
    </div>
  </div>

  <script>
    let selectedYear = 1;
    let selectedIdx = 0;
    let entries = [];
    let lastMeta = {};
    const cloudMode = location.hostname === "dcalab.juryory.com";

    async function callApi(path, method, body) {
      const r = await fetch(path, {
        method: method || "GET",
        headers: { "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : undefined,
      });
      if (!r.ok) throw new Error(\`HTTP \${r.status}\`);
      return r.json();
    }

    function renderTabs() {
      yearTabs.innerHTML = "";
      for (let y = 1; y <= 10; y++) {
        const tab = document.createElement("div");
        tab.className = "tab" + (y === selectedYear ? " active" : "");
        tab.textContent = \`\${y}Y\`;
        tab.onclick = () => {
          selectedYear = y;
          selectedIdx = 0;
          renderTabs();
          refresh();
        };
        yearTabs.appendChild(tab);
      }
    }

    function n(v, digits = 2) {
      const num = Number(v);
      return Number.isFinite(num) ? num.toFixed(digits) : "-";
    }

    function pct(v) {
      return \`\${n((Number(v) || 0) * 100, 0)}%\`;
    }

    function formatSchedule(cfg) {
      if (!cfg) return "-";
      if (cfg.scheduleMode === "every_n_days") return \`每 \${cfg.scheduleDays ?? "-"} 天一次\`;
      if (cfg.scheduleMode === "weekly_weekday") return \`每周 \${["一", "二", "三", "四", "五", "六", "日"][Number(cfg.scheduleWeekday) - 1] || "-"}\`;
      if (cfg.scheduleMode === "weekly") return \`每周 \${["一","二","三","四","五","六","日"][Number(cfg.scheduleWeekday) - 1] || "-"}\`;
      if (cfg.scheduleMode === "monthly") return "按月定投";
      return cfg.scheduleMode || "-";
    }

    function formatScheduleMode(mode) {
      const map = {
        every_n_days: "按固定天数定投",
        weekly: "按周定投",
        weekly_weekday: "按周几定投",
        monthly: "按月定投",
      };
      return map[mode] || mode || "-";
    }

    function formatBuyMode(mode) {
      const map = {
        close_confirm_next_close: "收盘确认，下一交易日收盘买入",
        intraday_break_same_close: "盘中触发，当天收盘买入",
        close_break_same_close: "收盘触发，当天收盘买入",
        close_break_next_close: "收盘触发，下一交易日收盘买入",
      };
      return map[mode] || mode || "-";
    }

    function formatWeekday(weekday) {
      const label = ["一", "二", "三", "四", "五", "六", "日"][Number(weekday) - 1];
      return label ? \`周\${label}\` : "-";
    }

    function escapeHtml(value) {
      return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function renderAssetCard(key, label, entry) {
      const cfg = (entry.configs || {})[key];
      if (!cfg) return "";
      const showScheduleDays = cfg.scheduleMode === "every_n_days";
      const showWeekday = cfg.scheduleMode === "weekly" || cfg.scheduleMode === "weekly_weekday";
      return \`<div class="asset-card">
        <div class="asset-head">
          <div class="asset-name">\${escapeHtml(label)}</div>
        </div>
        <div class="asset-points">
          <div class="point"><strong>基础定投</strong> \${escapeHtml(cfg.baseAmount)}</div>
          <div class="point"><strong>回撤加仓</strong> \${escapeHtml(cfg.dipAmount)}</div>
          <div class="point"><strong>均线窗口</strong> \${escapeHtml(cfg.maWindow)} 日</div>
          <div class="point"><strong>定投频率</strong> \${escapeHtml(formatSchedule(cfg))}</div>
          <div class="point"><strong>频率模式</strong> \${escapeHtml(formatScheduleMode(cfg.scheduleMode))}</div>
          \${showScheduleDays ? \`<div class="point"><strong>间隔天数</strong> \${escapeHtml(cfg.scheduleDays ?? "-")}</div>\` : ""}
          \${showWeekday ? \`<div class="point"><strong>星期设置</strong> \${escapeHtml(formatWeekday(cfg.scheduleWeekday))}</div>\` : ""}
          <div class="point"><strong>买入方式</strong> \${escapeHtml(formatBuyMode(cfg.buyMode))}</div>
        </div>
      </div>\`;
    }

    function money(v) {
      const num = Number(v);
      return Number.isFinite(num) ? num.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "-";
    }

    function pctScore(v) {
      const num = Number(v);
      return Number.isFinite(num) ? \`\${(num * 100).toFixed(2)}%\` : "-";
    }

    function renderWindowRows(entry) {
      const details = Array.isArray(entry.details) ? entry.details : [];
      if (!details.length) {
        return '<div class="detail-empty">当前没有窗口明细。</div>';
      }
      return \`<div class="mini-wrap">
        <table class="mini-table">
          <thead>
            <tr><th>窗口</th><th>收益率</th><th>投入</th><th>最终市值</th><th>交易数</th></tr>
          </thead>
          <tbody>
            \${details.map((item) => \`<tr>
              <td>\${escapeHtml(item.range || "-")}</td>
              <td class="\${Number(item.returnRate) >= 0 ? "good" : "bad"}">\${pctScore(item.returnRate)}</td>
              <td>\${money(item.totalInvested)}</td>
              <td>\${money(item.finalValue)}</td>
              <td>\${escapeHtml(item.tradeCount ?? "-")}</td>
            </tr>\`).join("")}
          </tbody>
        </table>
      </div>\`;
    }

    function renderParamsHtml(entry) {
      if (!entry) return '<div class="detail-empty">当前暂无策略参数。</div>';
      return \`<div class="asset-grid">
        \${renderAssetCard("au9999", "黄金 AU9999", entry)}
        \${renderAssetCard("csi300", "沪深300", entry)}
        \${renderAssetCard("sp500", "标普500", entry)}
      </div>\`;
    }

    function render(data) {
      entries = data.entries || [];
      const meta = data.meta || {};
      lastMeta = meta;
      vAttempts.textContent = meta.attempts ?? "-";
      vValid.textContent = meta.valid ?? entries.length;
      statusText.textContent = cloudMode
        ? \`云端 API | 当前窗口: \${selectedYear} 年 | 结果数: \${entries.length}\`
        : \`\${meta.running ? "运行中" : "已停止"} | 窗口数: \${meta.windowCount || "-"} | 数据年份: \${meta.minYear || "-"}-\${meta.maxYear || "-"} | 当前窗口: \${selectedYear} 年\`;
      const best = entries[0];
      vBestRobust.textContent = best ? n(best.robustScore) : "-";
      vBestAvg.textContent = best ? n(best.avgScore) : "-";
      vBestMin.textContent = best ? n(best.minScore) : "-";
      if (!entries.length) {
        rows.innerHTML = '<tr><td colspan="10">当前窗口暂无结果。</td></tr>';
        detail.innerHTML = '<div class="detail-empty">当前窗口暂无结果。</div>';
        strategyParams.innerHTML = '<div class="detail-empty">当前窗口暂无策略参数。</div>';
        return;
      }
      if (selectedIdx >= entries.length) selectedIdx = 0;
      rows.innerHTML = entries.map((entry, i) => {
        const w = entry.weights || {};
        return \`<tr data-i="\${i}" class="\${i === selectedIdx ? "selected" : ""}">
          <td>\${i + 1}</td>
          <td>\${n(entry.robustScore)}</td>
          <td>\${n(entry.avgScore)}</td>
          <td class="\${Number(entry.minScore) >= 0 ? "good" : "bad"}">\${n(entry.minScore)}</td>
          <td>\${n(entry.maxScore)}</td>
          <td>\${n(entry.stdDev)}</td>
          <td>\${entry.windowCount || (entry.windows ? entry.windows.length : "-")}</td>
          <td>\${pct(w.au9999 || 0)}</td>
          <td>\${pct(w.csi300 || 0)}</td>
          <td>\${pct(w.sp500 || 0)}</td>
        </tr>\`;
      }).join("");
      detail.innerHTML = renderWindowRows(entries[selectedIdx]);
      strategyParams.innerHTML = renderParamsHtml(entries[selectedIdx]);
    }

    rows.onclick = (e) => {
      const tr = e.target.closest("tr[data-i]");
      if (!tr) return;
      selectedIdx = Number(tr.dataset.i);
      render({ entries, meta: lastMeta });
    };

    async function refreshCloud() {
      const summary = await callApi("/api/robust");
      const detailData = await callApi(\`/api/robust?year=\${selectedYear}\`);
      const count = (summary.summary && summary.summary[selectedYear] && summary.summary[selectedYear].count) || 0;
      render({
        entries: Array.isArray(detailData.entries) ? detailData.entries : [],
        meta: { attempts: "-", valid: count, running: false, windowCount: "-" },
      });
      statusText.textContent = \`云端 API | 当前窗口: \${selectedYear} 年 | 结果数: \${count}\`;
    }

    async function refreshLocal() {
      const data = await callApi(\`/api/robust/leaderboard?year=\${selectedYear}&limit=10\`);
      render(data);
    }

    async function refresh() {
      try {
        if (cloudMode) return await refreshCloud();
        return await refreshLocal();
      } catch (err) {
        statusText.textContent = \`刷新失败: \${err.message}\`;
      }
    }

    renderTabs();
    refresh();
    setInterval(refresh, cloudMode ? 15000 : 5000);
  </script>
</body>
</html>
`;

async function snapshotRobustStore(env) {
  const robustSnap = {};
  for (let y = 1; y <= 10; y++) {
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

async function replaceRobustEntries(env, topByYear) {
  const backupTs = Date.now();
  await putJson(env, `backup:robust:${backupTs}`, { ts: backupTs, data: await snapshotRobustStore(env) });

  for (let y = 1; y <= 10; y++) {
    const incoming = Array.isArray(topByYear && topByYear[y]) ? topByYear[y] : [];
    const valid = [];
    for (const entry of incoming) {
      const err = validateRobustEntry(entry);
      if (err) continue;
      valid.push(entry);
    }
    valid.sort((a, b) => Number(b.robustScore) - Number(a.robustScore));
    await putJson(env, `robust:${y}`, valid.slice(0, TOP_LIMIT));
  }

}

// --- Read data ---
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
    if (yr < 1 || yr > 10) return jsonResp({ error: "year must be 1-10" }, 400);
    const list = await getJson(env, `robust:${yr}`) || [];
    return jsonResp({ year: yr, entries: list });
  }

  // Summary
  const summary = {};
  for (let y = 1; y <= 10; y++) {
    const list = await getJson(env, `robust:${y}`) || [];
    summary[y] = { count: list.length, best: list[0] ? list[0].robustScore : null };
  }
  return jsonResp({ summary });
}

async function handleReplaceRobust(request, env) {
  if (!checkAdmin(request, env)) return jsonResp({ error: "unauthorized" }, 401);

  let body;
  try { body = await request.json(); } catch { return jsonResp({ error: "invalid JSON" }, 400); }

  const topByYear = body && body.topByYear;
  if (!topByYear || typeof topByYear !== "object") {
    return jsonResp({ error: "missing topByYear" }, 400);
  }

  await replaceRobustEntries(env, topByYear);

  const counts = {};
  let total = 0;
  for (let y = 1; y <= 10; y++) {
    const list = await getJson(env, `robust:${y}`) || [];
    counts[y] = list.length;
    total += list.length;
  }
  return jsonResp({ ok: true, replaced: total, counts });
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
    for (let y = 1; y <= 10; y++) {
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
        return htmlResp(ROBUST_HTML);
      }
      if (request.method === "GET" && (path === "/range" || path === "/range.html")) {
        return htmlResp(RANGE_HTML);
      }
      if (request.method === "GET" && (path === "/robust" || path === "/robust.html")) {
        return htmlResp(ROBUST_HTML);
      }

      // Read endpoints
      if (request.method === "GET" && path === "/api/range") {
        return await handleGetRange(request, env);
      }
      if (request.method === "GET" && path === "/api/robust") {
        return await handleGetRobust(request, env);
      }
      if (request.method === "POST" && path === "/api/admin/robust/replace") {
        return await handleReplaceRobust(request, env);
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
