#!/usr/bin/env node
/**
 * 备份 Cloudflare KV 中的已验证数据到本地 JSON 文件。
 * 用法: node scripts/backup_kv.js [--api URL] [--token TOKEN]
 */

const https = require("https");
const http = require("http");
const fs = require("fs");
const path = require("path");

const args = process.argv.slice(2);
function getArg(name, def) {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : def;
}

const API = getArg("api", "https://dcalab.juryory.com").replace(/\/$/, "");
const TOKEN = getArg("token", "");
const BACKUP_DIR = path.resolve(__dirname, "..", "data", "backups");

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const mod = parsed.protocol === "https:" ? https : http;
    mod.get({ hostname: parsed.hostname, port: parsed.port, path: parsed.pathname + parsed.search,
      headers: { "Authorization": `Bearer ${TOKEN}` },
    }, (res) => {
      let d = "";
      res.on("data", (c) => d += c);
      res.on("end", () => { try { resolve(JSON.parse(d)); } catch { resolve(null); } });
    }).on("error", reject);
  });
}

(async () => {
  if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

  // Backup range data
  const rangeData = await fetchJson(`${API}/api/range?range=all`);
  // Backup robust data (all years)
  const robustData = {};
  for (let y = 1; y <= 10; y++) {
    robustData[y] = await fetchJson(`${API}/api/robust?year=${y}`);
  }

  const backup = { timestamp: ts, range: rangeData, robust: robustData };
  const outFile = path.join(BACKUP_DIR, `backup_${ts}.json`);
  fs.writeFileSync(outFile, JSON.stringify(backup, null, 2), "utf8");
  console.log(`[Backup] saved to ${outFile}`);
})();
