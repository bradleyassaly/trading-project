param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$dateStamp = Get-Date -Format "yyyy-MM-dd"
$logDir = Join-Path $repoRoot "artifacts\operating_baseline_daily\logs"
$summaryDir = Join-Path $repoRoot "artifacts\operating_baseline_daily"
$pythonExe = Join-Path $repoRoot ".venv\Scripts\python.exe"
$logPath = Join-Path $logDir "$dateStamp.log"
$activateScript = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"
$stdoutPath = Join-Path $logDir "$dateStamp.stdout.tmp"
$stderrPath = Join-Path $logDir "$dateStamp.stderr.tmp"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null
New-Item -ItemType Directory -Force -Path $summaryDir | Out-Null

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python executable not found at $pythonExe"
    exit 1
}

if (Test-Path $activateScript) {
    . $activateScript
}

$arguments = @(
    "-m",
    "trading_platform.system.operating_baseline_daily",
    "--config",
    "configs/orchestration_operating_baseline.yaml",
    "--summary-dir",
    "artifacts/operating_baseline_daily",
    "--alerts-config",
    "configs/alerts.yaml",
    "--log-path",
    $logPath
)

if ($ExtraArgs.Count -gt 0) {
    $arguments += $ExtraArgs
}

$process = Start-Process -FilePath $pythonExe `
    -ArgumentList $arguments `
    -WorkingDirectory $repoRoot `
    -NoNewWindow `
    -Wait `
    -PassThru `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath

if (Test-Path $stdoutPath) {
    Get-Content $stdoutPath | Tee-Object -FilePath $logPath -Append
    Remove-Item $stdoutPath -Force
}

if (Test-Path $stderrPath) {
    Get-Content $stderrPath | Tee-Object -FilePath $logPath -Append
    Remove-Item $stderrPath -Force
}

exit $process.ExitCode
