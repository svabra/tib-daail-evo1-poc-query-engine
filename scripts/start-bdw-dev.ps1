param(
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$logConfigPath = Join-Path $repoRoot "bdw\logging.json"
$logsRoot = Join-Path $repoRoot "logs"
$bdwLogDir = Join-Path $logsRoot "bdw"
$requirementsPath = Join-Path $repoRoot "bdw\requirements.txt"

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

    function Stop-OrphanedPythonChildren {
        param(
            [int]$ParentProcessId
        )

        $orphanChildren = Get-CimInstance Win32_Process | Where-Object {
            $_.ParentProcessId -eq $ParentProcessId -and $_.Name -like "python*"
        }

        foreach ($child in $orphanChildren) {
            Write-Host "Stopping orphaned Python child '$($child.Name)' (PID $($child.ProcessId)) for stale listener PID $ParentProcessId..."
            Stop-Process -Id $child.ProcessId -Force -ErrorAction SilentlyContinue
        }
    }

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
            Stop-OrphanedPythonChildren -ParentProcessId $processId
            continue
        }

        Write-Host "Stopping process '$($process.ProcessName)' (PID $processId) on port $LocalPort..."
        Stop-Process -Id $processId -Force
    }
}

function Ensure-VirtualEnvironment {
    param(
        [string]$RepoRoot,
        [string]$PythonPath,
        [string]$RequirementsPath
    )

    if (Test-Path $PythonPath) {
        return $PythonPath
    }

    Write-Host "Creating .venv at '$RepoRoot\\.venv'..."
    $venvRoot = Split-Path -Parent $PythonPath

    $pythonLauncher = Get-Command python -ErrorAction SilentlyContinue
    if (-not $pythonLauncher) {
        $pythonLauncher = Get-Command py -ErrorAction SilentlyContinue
        if (-not $pythonLauncher) {
            throw "Python was not found in PATH. Install Python and retry."
        }

        & $pythonLauncher.Source -3 -m venv $venvRoot
    } else {
        & $pythonLauncher.Source -m venv $venvRoot
    }

    if (-not (Test-Path $PythonPath)) {
        throw "Failed to create .venv at '$venvRoot'."
    }

    if (-not (Test-Path $RequirementsPath)) {
        throw "Requirements file not found at '$RequirementsPath'."
    }

    Write-Host "Installing dependencies into .venv from '$RequirementsPath'..."
    & $PythonPath -m pip install -r $RequirementsPath
    if ($LASTEXITCODE -ne 0) {
        throw "Dependency install failed. Ensure network access and requirements are valid."
    }

    return $PythonPath
}

$python = Ensure-VirtualEnvironment -RepoRoot $repoRoot -PythonPath $python -RequirementsPath $requirementsPath

& (Join-Path $PSScriptRoot "cleanup-logs.ps1")

$imageVersion = (Get-Content -Raw (Join-Path $repoRoot "VERSION")).Trim()
$localDir = Join-Path $repoRoot ".local\bdw"
$workspaceDir = Join-Path $repoRoot "workspace"
$extensionDir = Join-Path $localDir "duckdb\extensions"
$dbPath = Join-Path $workspaceDir "bit-data-workbench.dev.duckdb"

New-Item -ItemType Directory -Force -Path $logsRoot | Out-Null
New-Item -ItemType Directory -Force -Path $bdwLogDir | Out-Null
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
$env:S3_REGION = "us-east-1"
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
Write-Host "Application log: $(Join-Path $bdwLogDir 'server.log')"
Write-Host "Requires local dependencies on localhost:9000 and localhost:5432."

Push-Location $repoRoot
try {
    & $python -m uvicorn bit_data_workbench.main:app `
        --app-dir (Join-Path $repoRoot "bdw") `
        --host 127.0.0.1 `
        --port $Port `
        --reload `
        --reload-dir (Join-Path $repoRoot "bdw") `
        --log-config $logConfigPath
}
finally {
    Pop-Location
}
