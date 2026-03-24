$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$dateStamp = Get-Date -Format "yyyy-MM-dd"
$logDir = Join-Path $repoRoot "artifacts\operating_baseline_daily\logs"
$summaryDir = Join-Path $repoRoot "artifacts\operating_baseline_daily"
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
$logPath = Join-Path $logDir "$dateStamp.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null
New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python executable not found at $pythonExe"
    exit 1
}

$arguments = @(
    "-m",
    "trading_platform.system.operating_baseline_daily",
    "--config",
    "configs/orchestration_operating_baseline.yaml",
    "--summary-dir",
    "artifacts/operating_baseline_daily",
    "--log-path",
    $logPath
)

& $pythonExe @arguments 2>&1 | Tee-Object -FilePath $logPath
exit $LASTEXITCODE
