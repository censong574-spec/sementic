# Download temporal-server Linux bundle locally, then deploy.
# Next: .\scripts\deploy\deploy.ps1 temporal
# Or:   python scripts/deploy/deploy.py temporal
param(
    [string]$Version = "1.27.2"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Artifacts = Join-Path $ScriptDir "artifacts"
$Name = "temporal_${Version}_linux_amd64.tar.gz"
$Out = Join-Path $Artifacts $Name
$Url = "https://github.com/temporalio/temporal/releases/download/v${Version}/${Name}"

New-Item -ItemType Directory -Force -Path $Artifacts | Out-Null
if (Test-Path $Out) {
    Write-Host "Already exists: $Out"
    exit 0
}

Write-Host "Downloading $Url ..."
curl.exe -fL --http1.1 --retry 3 --retry-delay 2 $Url -o $Out
Write-Host "Saved: $Out ($((Get-Item $Out).Length) bytes)"
Write-Host "Deploy: .\scripts\deploy\deploy.ps1 temporal"
