# Runs the order book + trades recorder headless in the background.
#
#   $env:CONFIG_PASSWORD = "<your hummingbot password>"
#   .\run_recorder.ps1                                   # start (uses conf/scripts/conf_orderbook_trade_recorder.yml)
#   .\run_recorder.ps1 -Config conf_recorder_wazirx.yml  # start with another config
#   .\run_recorder.ps1 -Stop                             # stop
#
# Each run auto-restarts if hummingbot exits unexpectedly. Console output goes to logs\recorder_<config>.out.log
param(
    [string]$Config = "conf_orderbook_trade_recorder.yml",
    [switch]$Stop
)

$root = $PSScriptRoot
$name = [IO.Path]::GetFileNameWithoutExtension($Config)
$pidFile = Join-Path $root "logs\recorder_$name.pid"

if ($Stop) {
    if (Test-Path $pidFile) {
        $procId = Get-Content $pidFile
        # Kill the supervisor loop and the hummingbot child it spawned
        Get-CimInstance Win32_Process -Filter "ParentProcessId=$procId" | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
        Remove-Item $pidFile
        Write-Host "Stopped recorder $name (pid $procId)"
    } else {
        Write-Host "No running recorder for $name"
    }
    return
}

if (-not $env:CONFIG_PASSWORD) {
    Write-Error "Set `$env:CONFIG_PASSWORD to your hummingbot password first."
    return
}
if ((Test-Path $pidFile) -and (Get-Process -Id (Get-Content $pidFile) -ErrorAction SilentlyContinue)) {
    Write-Host "Recorder $name already running (pid $(Get-Content $pidFile))"
    return
}

New-Item -ItemType Directory -Force (Join-Path $root "logs") | Out-Null
$python = Join-Path $root ".venv\Scripts\python.exe"
$outLog = Join-Path $root "logs\recorder_$name.out.log"

# Supervisor loop: restart hummingbot after 10s if it ever exits (network drop, crash, etc.)
$loop = @"
Set-Location '$root'
`$env:HEADLESS_MQTT = 'false'  # recorder needs no remote control; avoids errors when no MQTT broker is running
while (`$true) {
    & '$python' bin\hummingbot_quickstart.py --headless -f orderbook_trade_recorder.py -c '$Config' *>> '$outLog'
    Add-Content '$outLog' "[`$(Get-Date -Format o)] hummingbot exited with code `$LASTEXITCODE, restarting in 10s"
    Start-Sleep 10
}
"@

$proc = Start-Process powershell.exe -WindowStyle Hidden -PassThru -ArgumentList "-NoProfile", "-Command", $loop
$proc.Id | Set-Content $pidFile
Write-Host "Started recorder $name (pid $($proc.Id)). Log: $outLog"
Write-Host "Data: $root\data\market_data\"
