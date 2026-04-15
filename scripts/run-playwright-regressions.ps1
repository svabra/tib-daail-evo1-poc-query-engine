param(
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [int]$TimeoutMs = 30000,
    [switch]$Headed,
    [switch]$All
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$manifestPath = Join-Path $PSScriptRoot "playwright-smokes.json"

if (-not (Test-Path $python)) {
    throw "Python virtual environment not found at $python"
}

if (-not (Test-Path $manifestPath)) {
    throw "Playwright smoke manifest not found at $manifestPath"
}

$manifest = Get-Content $manifestPath -Raw | ConvertFrom-Json
$smokeScripts = @(
    $manifest.smokes |
        Where-Object { $All -or $_.enabledByDefault } |
        ForEach-Object { $_.script }
)

if (-not $smokeScripts.Count) {
    throw "The Playwright smoke manifest did not select any smoke scripts."
}

$scriptArgs = @(
    "--base-url", $BaseUrl,
    "--timeout-ms", $TimeoutMs
)

if ($Headed) {
    $scriptArgs += "--headed"
}

Write-Host "Running Playwright regressions against $BaseUrl" -ForegroundColor Cyan
Write-Host ("Scope: " + ($(if ($All) { "all" } else { "default" }))) -ForegroundColor Cyan

foreach ($smokeScript in $smokeScripts) {
    $scriptPath = Join-Path $PSScriptRoot $smokeScript
    if (-not (Test-Path $scriptPath)) {
        throw "Smoke script not found: $scriptPath"
    }

    Write-Host "`n==> $smokeScript" -ForegroundColor Yellow
    & $python $scriptPath @scriptArgs
    if ($LASTEXITCODE -ne 0) {
        throw "$smokeScript failed with exit code $LASTEXITCODE"
    }
}

Write-Host "`nPlaywright regressions passed." -ForegroundColor Green