param()

$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$repoRoot = Split-Path -Parent $PSScriptRoot

function Wait-DockerEngine {
    param(
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        cmd /c "docker info >nul 2>&1"
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Docker is not ready. Start Docker Desktop and try again."
}

function Get-ListeningProcessIds {
    param(
        [int]$LocalPort
    )

    $connections = Get-NetTCPConnection -State Listen -LocalPort $LocalPort -ErrorAction SilentlyContinue
    if ($connections) {
        return $connections | Select-Object -ExpandProperty OwningProcess -Unique
    }

    return @()
}

function Start-DockerDesktopIfNeeded {
    cmd /c "docker info >nul 2>&1"
    if ($LASTEXITCODE -eq 0) {
        return
    }

    $dockerDesktop = Join-Path $env:ProgramFiles "Docker\\Docker\\Docker Desktop.exe"
    if (-not (Test-Path $dockerDesktop)) {
        throw "Docker Desktop is not running and was not found at '$dockerDesktop'."
    }

    Write-Host "Starting Docker Desktop for local dependencies..."
    Start-Process -FilePath $dockerDesktop | Out-Null
    Wait-DockerEngine
}

function Stop-ComposeAppContainer {
    Write-Host "Stopping the Docker app container so F5 can own http://127.0.0.1:8000 ..."
    cmd /c "docker compose stop bit-data-workbench >nul 2>&1"
    $containerId = docker ps --filter "name=^/bit-data-workbench$" --format "{{.ID}}"
    if ($containerId) {
        docker stop bit-data-workbench | Out-Null
    }
}

function Stop-StaleWorkbenchProcesses {
    $repoRootNormalized = $repoRoot.ToLowerInvariant()
    $staleProcesses = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -like "python*" -and
        $_.CommandLine -and
        $_.CommandLine.ToLowerInvariant().Contains($repoRootNormalized) -and
        $_.CommandLine.ToLowerInvariant().Contains("bit_data_workbench.main:app")
    }

    foreach ($process in $staleProcesses) {
        Write-Host "Stopping stale workbench process '$($process.Name)' (PID $($process.ProcessId)) ..."
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Stop-LocalWorkbenchListener {
    param(
        [int]$LocalPort = 8000
    )

    $repoRootNormalized = $repoRoot.ToLowerInvariant()
    foreach ($processId in Get-ListeningProcessIds -LocalPort $LocalPort) {
        if (-not $processId) {
            continue
        }

        $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
        if (-not $process) {
            continue
        }

        $commandLine = ""
        try {
            $commandLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $processId").CommandLine
        } catch {
            $commandLine = ""
        }

        $commandLineNormalized = ($commandLine | Out-String).Trim().ToLowerInvariant()
        $isWorkbenchProcess = (
            $process.ProcessName -match "^python" -and (
                $commandLineNormalized.Contains("bit_data_workbench.main:app") -or
                $commandLineNormalized.Contains($repoRootNormalized)
            )
        )

        if ($isWorkbenchProcess) {
            Write-Host "Stopping stale local workbench process '$($process.ProcessName)' (PID $processId) on port $LocalPort ..."
            Stop-Process -Id $processId -Force
            continue
        }

        throw "Port $LocalPort is already in use by '$($process.ProcessName)' (PID $processId). Stop that process before using F5."
    }
}

Start-DockerDesktopIfNeeded
Stop-ComposeAppContainer
Stop-StaleWorkbenchProcesses
Stop-LocalWorkbenchListener

Write-Host "Starting local dependency services for F5 ..."
docker compose up -d minio minio-init minio-seed postgres pgadmin
