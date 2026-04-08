# Show background-job status + hit health endpoints.
$ErrorActionPreference = "SilentlyContinue"

Write-Host "─── PowerShell jobs ───" -ForegroundColor Cyan
Get-Job | Format-Table Name, State, HasMoreData -AutoSize

Write-Host "─── API health ───" -ForegroundColor Cyan
try {
    $r = Invoke-RestMethod -Uri "http://localhost:8001/api/system/status" -TimeoutSec 5
    Write-Host "API: HEALTHY" -ForegroundColor Green
    if ($r.overall_health) { Write-Host "  overall_health: $($r.overall_health)" }
} catch {
    Write-Host "API: DOWN ($_)" -ForegroundColor Red
}

Write-Host "─── Scheduler ───" -ForegroundColor Cyan
$state = "data\scheduler\state.json"
if (Test-Path $state) {
    $data = Get-Content $state | ConvertFrom-Json
    foreach ($t in $data.tasks) {
        if (-not $t.enabled) { continue }
        $status = switch ($t.last_status) {
            "ok"     { "OK" }
            "failed" { "FAILED" }
            default  { "pending" }
        }
        $color = switch ($t.last_status) {
            "ok"     { "Green" }
            "failed" { "Red" }
            default  { "Yellow" }
        }
        Write-Host ("  {0,-25} {1}" -f $t.name, $status) -ForegroundColor $color
    }
} else {
    Write-Host "  state file missing — scheduler not yet run"
}

Write-Host "─── Frontend ───" -ForegroundColor Cyan
try {
    $r = Invoke-WebRequest -Uri "http://localhost:5173" -TimeoutSec 3 -UseBasicParsing
    Write-Host "Frontend: UP (HTTP $($r.StatusCode))" -ForegroundColor Green
} catch {
    Write-Host "Frontend: DOWN" -ForegroundColor Red
}
