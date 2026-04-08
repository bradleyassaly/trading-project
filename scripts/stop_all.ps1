# Stop all background jobs started by run_all.ps1.
$ErrorActionPreference = "SilentlyContinue"
Get-Job | Stop-Job
Get-Job | Remove-Job -Force
Write-Host "All scheduler / watchdog / api / frontend jobs stopped."
