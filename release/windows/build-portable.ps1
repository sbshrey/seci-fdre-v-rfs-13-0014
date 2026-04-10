param(
    [string]$Python = ".\.venv\Scripts\python.exe",
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $repoRoot

& $Python -m pip install -e ".[dev,windows]"

if (-not $SkipTests) {
    & $Python -m pytest tests/test_web_app.py tests/test_desktop.py -q
}

& $Python -m PyInstaller --noconfirm --clean .\release\windows\seci-fdre-v-desktop.spec

$distRoot = Join-Path $repoRoot "dist"
$portableDir = Join-Path $distRoot "SECI-FDRE-V"
$zipPath = Join-Path $distRoot "SECI-FDRE-V-windows-portable.zip"

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

Compress-Archive -Path (Join-Path $portableDir "*") -DestinationPath $zipPath -Force
Write-Host "Portable bundle created at $zipPath"
