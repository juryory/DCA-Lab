#!/bin/bash

echo "=== DCALab DCA Strategy Lab ==="

if ! command -v node >/dev/null 2>&1; then
  echo "[ERROR] Node.js not found. Please install it from https://nodejs.org/"
  exit 1
fi

echo "[INFO] Node.js $(node -v)"

if [ -z "$LOCAL_ADMIN_TOKEN" ]; then
  read -r -p "Enter local admin token (press Enter to use default 'change_me'): " token
  export LOCAL_ADMIN_TOKEN="${token:-change_me}"
fi

missing=0
for f in data/au9999_history.csv data/csi300_history.csv data/sp500_history.csv; do
  if [ ! -f "$f" ]; then
    echo "[WARN] Missing data file: $f"
    missing=1
  fi
done

if [ "$missing" -eq 1 ]; then
  echo "[TIP] You can fetch history with: python scripts/fetch_early_data.py"
  echo "[TIP] Or place CSV files under the data folder."
fi

echo ""
echo "[START] Server: http://localhost:8787"
echo "[TIP] Admin:  http://localhost:8787/admin"
echo "[TIP] Press Ctrl+C to stop"
echo ""

node server.js
