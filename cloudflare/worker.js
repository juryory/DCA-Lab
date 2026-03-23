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
