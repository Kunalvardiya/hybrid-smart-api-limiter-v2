# Run one traffic scenario headless against LOCAL gateway, then verify the outcome in Redis.
#
#   .\load\run_scenario.ps1 -Scenario flash_crowd
#   .\load\run_scenario.ps1 -Scenario multi_identity -RunId demo2
#   .\load\run_scenario.ps1 -Scenario normal -RedisUrl redis://127.0.0.1:6390/0   # no-Docker dev stack
#
# -RunId must be NEW for every run (baselines and policy state persist in Redis per identity).
# Without -RedisUrl the observer uses REDIS_* from the environment or .env (the Docker stack).
param(
    [Parameter(Mandatory = $true)][string]$Scenario,
    [string]$RunId = (Get-Date -Format "MMdd-HHmmss"),
    [string]$TargetHost = "http://127.0.0.1:8000",
    [string]$RedisUrl = ""
)
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$py = Join-Path $root ".venv\Scripts\python.exe"

$info = & $py -m load.info $Scenario
$users, $seconds = $info -split " "
$env:SCN_RUN_ID = $RunId
Write-Host "Scenario $Scenario  run id $RunId  users $users  duration ${seconds}s  target $TargetHost"

& $py -m locust -f "load/scenarios/$Scenario.py" --host $TargetHost --headless -u $users -r $users `
    -t "${seconds}s" --csv "load/results/$Scenario-$RunId" --only-summary
if ($LASTEXITCODE -ne 0) { Write-Host "Locust exited with code $LASTEXITCODE"; exit $LASTEXITCODE }

Start-Sleep -Seconds 6     # let the worker finish the last telemetry batch
$obs = @("-m", "load.observe", "--scenario", $Scenario, "--run-id", $RunId)
if ($RedisUrl) { $obs += @("--redis-url", $RedisUrl) }
& $py @obs
exit $LASTEXITCODE

