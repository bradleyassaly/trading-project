param(
    [string]$StartDate = "2025-01-03",
    [string]$EndDate = "2025-02-14"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $RepoRoot ".venv\\Scripts\\python.exe"

$runs = @(
    @{ Name = "ev_baseline"; Config = "configs/experiments/ev_baseline.yaml"; Output = "artifacts/daily_replay/ev_baseline" },
    @{ Name = "ev_hard"; Config = "configs/experiments/ev_hard.yaml"; Output = "artifacts/daily_replay/ev_hard" },
    @{ Name = "ev_soft5"; Config = "configs/experiments/ev_soft5.yaml"; Output = "artifacts/daily_replay/ev_soft5" },
    @{ Name = "ev_soft10"; Config = "configs/experiments/ev_soft10.yaml"; Output = "artifacts/daily_replay/ev_soft10" }
)

foreach ($run in $runs) {
    Write-Host "Running $($run.Name) ..."
    & $Python -m trading_platform.cli ops pipeline replay-daily `
        --config (Join-Path $RepoRoot $run.Config) `
        --start-date $StartDate `
        --end-date $EndDate `
        --output-dir (Join-Path $RepoRoot $run.Output)
}
