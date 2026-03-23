@echo off
chcp 65001 >nul 2>&1
echo === AUSUB 多资产定投策略回测实验室 ===

:: 检查 Node.js
where node >nul 2>&1
if %errorlevel% neq 0 (
  echo [错误] 未找到 Node.js，请先安装: https://nodejs.org/
  pause
  exit /b 1
)

for /f "tokens=*" %%v in ('node -v') do echo [信息] Node.js %%v

:: 设置 admin token
if "%ADMIN_TOKEN%"=="" (
  set /p ADMIN_TOKEN="请输入管理员 token (直接回车使用默认值 'change_me'): "
  if "%ADMIN_TOKEN%"=="" set ADMIN_TOKEN=change_me
)

:: 检查数据文件
set missing=0
if not exist data\au9999_history.csv (
  echo [警告] 缺少数据文件: data\au9999_history.csv
  set missing=1
)
if not exist data\csi300_history.csv (
  echo [警告] 缺少数据文件: data\csi300_history.csv
  set missing=1
)
if not exist data\sp500_history.csv (
  echo [警告] 缺少数据文件: data\sp500_history.csv
  set missing=1
)

if %missing%==1 (
  echo [提示] 可使用 python scripts\fetch_early_data.py 拉取历史数据
  echo [提示] 或手动将 CSV 文件放到对应位置 (格式: tradeDate,open,close)
)

echo.
echo [启动] 服务运行在 http://localhost:8787
echo [提示] 管理后台: http://localhost:8787/admin
echo [提示] 按 Ctrl+C 停止服务
echo.

node server.js
pause
