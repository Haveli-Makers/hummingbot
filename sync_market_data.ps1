# Pulls recorded order book + trades files from the recorder server into data\market_data\.
# Only downloads files that are new or have grown since the last sync (finished days are copied once).
#
#   .\sync_market_data.ps1 -Server ubuntu@<server ip> -Key $HOME\.ssh\oracle_recorder.key
#
# Or set $env:RECORDER_SERVER / $env:RECORDER_KEY once and just run .\sync_market_data.ps1
# Setup guide: docs\RECORDER_SERVER_SETUP.md
param(
    [string]$Server = $env:RECORDER_SERVER,
    [string]$Key = $env:RECORDER_KEY,
    [string]$RemoteDir = "~/hummingbot/data/market_data"
)

if (-not $Server) {
    Write-Error "Pass -Server ubuntu@<ip> (or set `$env:RECORDER_SERVER)."
    return
}
$sshArgs = @()
if ($Key) { $sshArgs += @("-i", $Key) }

$localDir = Join-Path $PSScriptRoot "data\market_data"

# "<relative path> <size>" for every recorded file on the server
$listing = ssh @sshArgs $Server "cd $RemoteDir && find . -type f -name '*.jsonl.gz' -printf '%P %s\n'"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Could not list files on $Server"
    return
}

$downloaded = 0
foreach ($line in $listing) {
    $relPath, $size = $line -split " ", 2
    $localPath = Join-Path $localDir ($relPath -replace "/", "\")
    if ((Test-Path $localPath) -and ((Get-Item $localPath).Length -eq [long]$size)) {
        continue
    }
    New-Item -ItemType Directory -Force (Split-Path $localPath) | Out-Null
    scp @sshArgs -q "${Server}:$RemoteDir/$relPath" $localPath
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  $relPath ($([math]::Round($size / 1MB, 1)) MB)"
        $downloaded++
    } else {
        Write-Warning "Failed to download $relPath"
    }
}
Write-Host "Synced $downloaded file(s) into $localDir"
