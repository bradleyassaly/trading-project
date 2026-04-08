# Polymarket trading platform — local launcher (PowerShell, no Docker).
#
# Starts api / frontend / scheduler / watchdog as PowerShell background
# jobs. Use this when Docker isn't available or for quick local dev.
# For 24/7 production use docker compose up -d instead.
#
# Usage:
#   .\scripts\run_all.ps1
#   .\scripts\status.ps1
#   .\scripts\stop_all.ps1
#
# Each background job runs from $projectRoot. Crashed jobs are
# restarted automatically every 30 seconds.

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path "$PSScriptRoot\..").Path
Write-Host "Project root: $projectRoot"

$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Error "venv python not found at $venvPython"
    exit 1
}

$jobs = @{
    api = {
        param($root, $py)
        Set-Location $root
        & $py -m uvicorn trading_platform.api.main:app --host 0.0.0.0 --port 8001
    }
    frontend = {
        param($root, $py)
        Set-Location (Join-Path $root "src\trading_platform\frontend")
        & npm run dev -- --host 0.0.0.0
    }
    scheduler = {
        param($root, $py)
        Set-Location $root
        & $py scripts\task_scheduler.py
    }
    watchdog = {
        param($root, $py)
        Set-Location $root
        $env:WATCHDOG_API_URL = "http://localhost:8001"
        & $py scripts\health_watchdog.py
    }
}

function Start-AllJobs {
    foreach ($name in $jobs.Keys) {
        Write-Host "[start] $name"
        Start-Job -Name $name -ScriptBlock $jobs[$name] `
                  -ArgumentList $projectRoot, $venvPython | Out-Null
    }
}

function Restart-CrashedJobs {
    $bad = Get-Job | Where-Object { $_.State -in @('Failed','Completed','Stopped') }
    foreach ($job in $bad) {
        $name = $job.Name
        Write-Host "[$(Get-Date -Format 'HH:mm:ss')] restarting $name (state=$($job.State))"
        Receive-Job $job -ErrorAction SilentlyContinue | Out-Null
        Remove-Job $job -Force
        if ($jobs.ContainsKey($name)) {
            Start-Job -Name $name -ScriptBlock $jobs[$name] `
                      -ArgumentList $projectRoot, $venvPython | Out-Null
        }
    }
}

Start-AllJobs
Write-Host ""
Write-Host "All jobs started. Tail status with: .\scripts\status.ps1"
Write-Host "Stop everything with: .\scripts\stop_all.ps1"
Write-Host ""
Write-Host "Monitoring jobs (Ctrl-C to stop monitoring; jobs keep running)..."

while ($true) {
    Restart-CrashedJobs
    Start-Sleep -Seconds 30
}
