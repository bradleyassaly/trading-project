@echo off
REM Sets up Windows Task Scheduler tasks for automated trading platform jobs.
REM Run this script as Administrator.

cd /d "%~dp0\.."
set PROJECT_ROOT=%cd%
set PYTHON=%PROJECT_ROOT%\.venv\Scripts\python.exe

echo Setting up Trading Platform automation tasks...
echo Project root: %PROJECT_ROOT%
echo Python: %PYTHON%
echo.

REM 1. Monitor — runs at login (auto-restart loop)
schtasks /create /tn "TradingPlatform_Monitor" /tr "\"%PROJECT_ROOT%\scripts\run_monitor.bat\"" /sc onlogon /rl highest /f
if %ERRORLEVEL%==0 (echo [OK] TradingPlatform_Monitor created) else (echo [FAIL] TradingPlatform_Monitor)

REM 2. Orderflow — runs at login (auto-restart loop)
schtasks /create /tn "TradingPlatform_Orderflow" /tr "\"%PROJECT_ROOT%\scripts\run_orderflow.bat\"" /sc onlogon /rl highest /f
if %ERRORLEVEL%==0 (echo [OK] TradingPlatform_Orderflow created) else (echo [FAIL] TradingPlatform_Orderflow)

REM 3. Daily Refresh — runs daily at 6:00am (output tee'd to log by script)
schtasks /create /tn "TradingPlatform_DailyRefresh" /tr "\"%PROJECT_ROOT%\scripts\run_daily_refresh.bat\"" /sc daily /st 06:00 /rl highest /f
if %ERRORLEVEL%==0 (echo [OK] TradingPlatform_DailyRefresh created) else (echo [FAIL] TradingPlatform_DailyRefresh)

REM 4. Resolution Check — runs daily at 9:00am
schtasks /create /tn "TradingPlatform_ResolutionCheck" /tr "\"%PYTHON%\" \"%PROJECT_ROOT%\scripts\daily_resolution_check.py\" >> \"%PROJECT_ROOT%\logs\resolution_check.log\" 2>&1" /sc daily /st 09:00 /rl highest /f
if %ERRORLEVEL%==0 (echo [OK] TradingPlatform_ResolutionCheck created) else (echo [FAIL] TradingPlatform_ResolutionCheck)

REM 5. Wallet Profiles — runs daily at 7:00am
schtasks /create /tn "TradingPlatform_WalletProfiles" /tr "\"%PYTHON%\" -m trading_platform.cli data polymarket goldsky-wallet-profiles >> \"%PROJECT_ROOT%\logs\wallet_profiles.log\" 2>&1" /sc daily /st 07:00 /rl highest /f
if %ERRORLEVEL%==0 (echo [OK] TradingPlatform_WalletProfiles created) else (echo [FAIL] TradingPlatform_WalletProfiles)

echo.
echo Done. Use 'schtasks /query /tn TradingPlatform_*' to verify.
echo To remove all: schtasks /delete /tn TradingPlatform_Monitor /f (repeat for each)
pause
