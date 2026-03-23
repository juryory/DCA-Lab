const { parentPort, workerData } = require("worker_threads");
const { buildRandomConfig, runBacktest, score } = require("./engine");

const bars = Array.isArray(workerData.bars) ? workerData.bars : [];
const batch = Number(workerData.batch) > 0 ? Number(workerData.batch) : 100;
const top = Number(workerData.top) > 0 ? Number(workerData.top) : 50;
const penalty = Number.isFinite(Number(workerData.penalty)) ? Number(workerData.penalty) : 0.08;

let running = false;
let timer = null;
let attempts = 0;
let valid = 0;
const topList = [];

function pushTop(entry) {
  topList.push(entry);
  topList.sort((a, b) => b.score - a.score);
  if (topList.length > top) topList.length = top;
}

function emitUpdate() {
  parentPort.postMessage({
    attempts,
    valid,
    best: topList[0] || null,
  });
}

function stopLoop() {
  running = false;
  if (timer) {
    clearTimeout(timer);
    timer = null;
  }
}

function loop() {
  if (!running) return;
  for (let i = 0; i < batch; i++) {
    attempts++;
    try {
      const cfg = buildRandomConfig();
      const res = runBacktest(bars, cfg);
      const s = score(res, penalty);
      valid++;
      pushTop({
        score: s,
        returnRate: res.returnRate,
        totalInvested: res.totalInvested,
        finalValue: res.finalValue,
        tradeCount: res.tradeCount,
        cfg,
      });
    } catch (_) {}
  }
  emitUpdate();
  timer = setTimeout(loop, 0);
}

parentPort.on("message", (msg) => {
  if (msg && msg.cmd === "start" && !running) {
    running = true;
    loop();
    return;
  }
  if (msg && msg.cmd === "stop") {
    stopLoop();
    return;
  }
  if (msg && msg.cmd === "snapshot") {
    emitUpdate();
  }
});

parentPort.postMessage({ ready: true });
