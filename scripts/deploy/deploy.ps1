param(
    [Parameter(Position = 0)]
    [ValidateSet("full", "code", "config", "restart", "status", "logs", "messages", "redis", "diagnose", "temporal", "temporal-bundle", "temporal-status", "temporal-logs", "cleanup")]
    [string]$Command = "full",

    [ValidateSet("gateway", "worker", "")]
    [string]$Only = "",

    [int]$Lines = 30,

    [string]$Since = "2 hours ago",

    [string]$Grep = "",

    [int]$HistoryLimit = 10
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
$EnvFile = Join-Path $ScriptDir "deploy.env"

if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
        $parts = $_ -split '=', 2
        if ($parts.Count -eq 2) {
            [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
        }
    }
}

$argsList = @($Command)
if ($Only) {
    $argsList += @("--only", $Only)
}
if ($Command -in @("logs", "messages", "diagnose", "temporal-logs")) {
    $argsList += @("--lines", $Lines.ToString())
}
if ($Command -in @("messages", "diagnose")) {
    $argsList += @("--since", $Since)
}
if ($Command -in @("redis", "diagnose") -and $Grep) {
    $argsList += @("--grep", $Grep)
}
if ($Command -eq "redis") {
    $argsList += @("--history-limit", $HistoryLimit.ToString())
}

Push-Location $RepoRoot
try {
    python (Join-Path $ScriptDir "deploy.py") @argsList
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
    Pop-Location
}
