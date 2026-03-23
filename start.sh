#!/bin/bash
# AUSUB 一键启动脚本 (Linux/macOS)

echo "=== AUSUB 多资产定投策略回测实验室 ==="

# 检查 Node.js
if ! command -v node &> /dev/null; then
  echo "[错误] 未找到 Node.js，请先安装: https://nodejs.org/"
  exit 1
fi

echo "[信息] Node.js $(node -v)"

# 设置 admin token
if [ -z "$ADMIN_TOKEN" ]; then
  read -p "请输入管理员 token (直接回车使用默认值 'change_me'): " token
  export ADMIN_TOKEN="${token:-change_me}"
fi

# 检查数据文件
missing=0
for f in data/au9999_history.csv data/csi300_history.csv data/sp500_history.csv; do
  if [ ! -f "$f" ]; then
    echo "[警告] 缺少数据文件: $f"
    missing=1
  fi
done

if [ $missing -eq 1 ]; then
  echo "[提示] 可使用 python scripts/fetch_early_data.py 拉取历史数据"
  echo "[提示] 或手动将 CSV 文件放到对应位置 (格式: tradeDate,open,close)"
fi

echo ""
echo "[启动] 服务运行在 http://localhost:8787"
echo "[提示] 管理后台: http://localhost:8787/admin"
echo "[提示] 按 Ctrl+C 停止服务"
echo ""

node server.js
