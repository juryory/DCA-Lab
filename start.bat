@echo off
setlocal EnableDelayedExpansion

echo === AUSUB DCA Strategy Lab ===

where node >nul 2>&1
if %errorlevel% neq 0 (
  echo [ERROR] Node.js not found. Please install it from https://nodejs.org/
  pause
  exit /b 1
)

for /f "tokens=*" %%v in ('node -v') do echo [INFO] Node.js %%v

if "%LOCAL_ADMIN_TOKEN%"=="" (
  set /p "LOCAL_ADMIN_TOKEN=Enter local admin token (press Enter to use default 'change_me'): "
  if "!LOCAL_ADMIN_TOKEN!"=="" set "LOCAL_ADMIN_TOKEN=change_me"
)

set "missing=0"
if not exist data\au9999_history.csv (
  echo [WARN] Missing data file: data\au9999_history.csv
  set "missing=1"
)
if not exist data\csi300_history.csv (
  echo [WARN] Missing data file: data\csi300_history.csv
  set "missing=1"
)
if not exist data\sp500_history.csv (
  echo [WARN] Missing data file: data\sp500_history.csv
  set "missing=1"
)

if "!missing!"=="1" (
  echo [TIP] You can fetch history with: python scripts\fetch_early_data.py
  echo [TIP] Or place CSV files under the data folder.
)

echo.
echo [START] Server: http://localhost:8787
echo [TIP] Admin:  http://localhost:8787/admin
echo [TIP] Press Ctrl+C to stop
echo.

node server.js
pause
