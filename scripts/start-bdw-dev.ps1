param(
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"

function Get-ListeningProcessIds {
    param(
        [int]$LocalPort
    )

    $connections = Get-NetTCPConnection -State Listen -LocalPort $LocalPort -ErrorAction SilentlyContinue
    if ($connections) {
        return $connections | Select-Object -ExpandProperty OwningProcess -Unique
    }

    $matches = netstat -ano | Select-String ":$LocalPort"
    $processIds = @()
    foreach ($match in $matches) {
        $parts = ($match.ToString() -split "\s+") | Where-Object { $_ }
        if ($parts.Length -ge 5 -and $parts[3] -eq "LISTENING") {
            $processIds += $parts[-1]
        }
    }
    return $processIds | Select-Object -Unique
}

function Resolve-PortConflicts {
    param(
        [int]$LocalPort
    )

    $containerName = "bit-data-workbench"
    $dockerContainerId = docker ps --filter "name=^/${containerName}$" --format "{{.ID}}"
    if ($dockerContainerId) {
        Write-Host "Stopping Docker container '$containerName' to free port $LocalPort..."
        docker stop $containerName | Out-Null
    }

    $processIds = Get-ListeningProcessIds -LocalPort $LocalPort
    foreach ($processId in $processIds) {
        if (-not $processId) {
            continue
        }

        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if (-not $process) {
            continue
        }

        Write-Host "Stopping process '$($process.ProcessName)' (PID $processId) on port $LocalPort..."
        Stop-Process -Id $processId -Force
    }
}

if (-not (Test-Path $python)) {
    throw "Virtual environment Python not found at '$python'. Create .venv first."
}

$imageVersion = (Get-Content -Raw (Join-Path $repoRoot "VERSION")).Trim()
$localDir = Join-Path $repoRoot ".local\bdw"
$workspaceDir = Join-Path $repoRoot "workspace"
$extensionDir = Join-Path $localDir "duckdb\extensions"
$dbPath = Join-Path $workspaceDir "bit-data-workbench.dev.duckdb"

New-Item -ItemType Directory -Force -Path $localDir | Out-Null
New-Item -ItemType Directory -Force -Path $workspaceDir | Out-Null
New-Item -ItemType Directory -Force -Path $extensionDir | Out-Null

# The application itself has no "local mode" branch. This launcher makes the
# run local by injecting localhost endpoints and inline development credentials.
$env:IMAGE_VERSION = $imageVersion
$env:PORT = "$Port"
$env:MAX_RESULT_ROWS = "200"
$env:DUCKDB_DATABASE = $dbPath
$env:DUCKDB_EXTENSION_DIRECTORY = $extensionDir
$env:S3_ENDPOINT = "localhost:9000"
$env:S3_BUCKET = "vat-smoke-test"
$env:S3_ACCESS_KEY_ID = "minioadmin"
$env:S3_SECRET_ACCESS_KEY = "minioadmin"
$env:S3_URL_STYLE = "path"
$env:S3_USE_SSL = "false"
$env:S3_VERIFY_SSL = "false"
$env:S3_STARTUP_VIEW_SCHEMA = "s3"
$env:S3_STARTUP_VIEWS = "vat_smoke=csv:s3://vat-smoke-test/startup/vat_smoke.csv"
$env:PG_HOST = "localhost"
$env:PG_PORT = "5432"
$env:PG_USER = "evo1"
$env:PG_PASSWORD = "evo1"
$env:PG_OLTP_DATABASE = "evo1_oltp"
$env:PG_OLAP_DATABASE = "evo1_olap"

Resolve-PortConflicts -LocalPort $Port

Write-Host "Starting DAAIFL Data Workbench dev server with auto-reload..."
Write-Host "URL: http://localhost:$Port"
Write-Host "DuckDB database: $dbPath"
Write-Host "DuckDB extension directory: $extensionDir"
Write-Host "Requires local dependencies on localhost:9000 and localhost:5432."

& $python -m uvicorn bit_data_workbench.main:app `
    --app-dir (Join-Path $repoRoot "bdw") `
    --host 127.0.0.1 `
    --port $Port `
    --reload `
    --reload-dir (Join-Path $repoRoot "bdw")
