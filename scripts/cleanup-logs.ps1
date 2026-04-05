param()

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$logsRoot = Join-Path $repoRoot "logs"

$contextRoots = @{
    "bdw" = Join-Path $logsRoot "bdw"
    "playwright" = Join-Path $logsRoot "playwright"
    "schema-panel" = Join-Path $logsRoot "schema-panel"
    "misc" = Join-Path $logsRoot "misc"
}

foreach ($path in $contextRoots.Values) {
    New-Item -ItemType Directory -Force -Path $path | Out-Null
}

function Get-LogContext {
    param(
        [string]$Name
    )

    if ($Name -like "playwright-server-*.log") {
        return "playwright"
    }

    if ($Name -like "schema-panel-*.log") {
        return "schema-panel"
    }

    if (
        $Name -like "bdw-*.log" -or
        $Name -like "server*.log" -or
        $Name -like "devserver*.log"
    ) {
        return "bdw"
    }

    return "misc"
}

function Get-RelativeDirectory {
    param(
        [string]$FullPath
    )

    $relative = $FullPath.Substring($repoRoot.Length).TrimStart('\', '/')
    if ([string]::IsNullOrWhiteSpace($relative)) {
        return $null
    }
    return $relative
}

function Get-TargetFilePath {
    param(
        [System.IO.FileInfo]$File,
        [string]$Context
    )

    $targetDir = $contextRoots[$Context]
    $relativeDirectory = Get-RelativeDirectory -FullPath $File.DirectoryName
    if ($relativeDirectory) {
        $targetDir = Join-Path $targetDir $relativeDirectory
    }

    New-Item -ItemType Directory -Force -Path $targetDir | Out-Null

    $targetPath = Join-Path $targetDir $File.Name
    if (-not (Test-Path -LiteralPath $targetPath)) {
        return $targetPath
    }

    $stem = [System.IO.Path]::GetFileNameWithoutExtension($File.Name)
    $extension = $File.Extension
    $index = 1
    do {
        $targetPath = Join-Path $targetDir ("{0}-{1}{2}" -f $stem, $index, $extension)
        $index += 1
    } while (Test-Path -LiteralPath $targetPath)

    return $targetPath
}

$candidates = @()
$candidates += Get-ChildItem -LiteralPath $repoRoot -File -Filter *.log -Force

foreach ($relativeRoot in @(".local", "workspace")) {
    $sourceRoot = Join-Path $repoRoot $relativeRoot
    if (Test-Path -LiteralPath $sourceRoot) {
        $candidates += Get-ChildItem -LiteralPath $sourceRoot -File -Filter *.log -Recurse -Force
    }
}

foreach ($file in $candidates) {
    if ($file.FullName.StartsWith($logsRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        continue
    }

    $context = Get-LogContext -Name $file.Name
    $targetPath = Get-TargetFilePath -File $file -Context $context
    Move-Item -LiteralPath $file.FullName -Destination $targetPath
}
