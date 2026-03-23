# DCA Lab — 多资产定投策略回测实验室

基于 AU9999 黄金、沪深300、标普500 三种资产的定投策略随机搜索与回测系统。通过遗传算法优化投资组合权重和策略参数，寻找历史最优定投方案。

## 功能

- **区间最优搜索** — 在指定年份区间内搜索最高得分的投资组合
- **鲁棒性搜索** — 在多个随机时间窗口上评估策略稳定性
- **分布式计算** — 多台机器并行计算，结果汇总到 Cloudflare Worker
- **遗传算法** — 精英池 + 交叉变异，加速收敛
- **策略实验室** — 浏览器端单资产快速回测

## 快速开始

### 1. 安装依赖

```bash
npm install
```

### 2. 准备数据

将以下 CSV 文件放入项目目录：

- `data/au9999_history.csv` — AU9999 黄金历史数据
- `data/csi300_history.csv` — 沪深300历史数据
- `data/sp500_history.csv` — 标普500历史数据

CSV 格式：`tradeDate,open,close`（日期格式 YYYYMMDD）

可使用 `scripts/fetch_early_data.py` 拉取数据。

### 3. 一键启动

Linux / macOS:
```bash
./start.sh
```

Windows:
```
start.bat
```

或手动启动:
```bash
ADMIN_TOKEN=your_secret npm start
```

访问 `http://localhost:8787`：
- `/` — 排行榜
- `/range` — 区间最优结果
- `/robust` — 鲁棒性结果
- `/admin` — 管理后台（需要 token）
- `/lab` — 策略实验室

## 分布式计算

### 部署 Cloudflare Worker

1. 编辑 `cloudflare/wrangler.toml`，填入你的 KV namespace ID 和 admin token
2. 部署：

```bash
cd cloudflare
npx wrangler deploy
```

### 运行计算节点

```bash
node compute_node.js --api https://your-worker.workers.dev
```

任何人都可以运行计算节点贡献算力。

### 验证提交

```bash
node scripts/verify.js --api https://your-worker.workers.dev --token YOUR_ADMIN_TOKEN --auto
```

### 备份数据

```bash
node scripts/backup_kv.js --api https://your-worker.workers.dev --token YOUR_ADMIN_TOKEN
```

## 策略参数

详见 [STRATEGY_GUIDE.md](STRATEGY_GUIDE.md)。

## 架构

```
server.js              — HTTP 服务 + 管理 API
backend/
  engine.js            — 回测引擎 + 遗传算法
  runner.js            — 搜索任务管理（加载数据、启动 worker）
  worker_range.js      — 区间最优搜索 worker
  worker_robust.js     — 鲁棒性搜索 worker
cloudflare/
  worker.js            — Cloudflare Worker API（提交/验证/查询）
compute_node.js        — 分布式计算节点
scripts/
  verify.js            — 提交验证脚本
  backup_kv.js         — KV 数据备份
  fetch_early_data.py  — 历史数据拉取
```

## License

MIT
