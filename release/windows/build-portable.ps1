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
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller failed with exit code $LASTEXITCODE"
}

$distRoot = Join-Path $repoRoot "dist"
$portableDir = Join-Path $distRoot "SECI-FDRE-V"
$zipPath = Join-Path $distRoot "SECI-FDRE-V-windows-portable.zip"

if (-not (Test-Path -LiteralPath $portableDir)) {
    throw "PyInstaller did not produce $portableDir"
}

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

$archiveItems = @(Get-ChildItem -LiteralPath $portableDir -Force)
if ($archiveItems.Count -eq 0) {
    throw "Portable directory is empty: $portableDir"
}
Compress-Archive -Path $archiveItems.FullName -DestinationPath $zipPath -Force
Write-Host "Portable bundle created at $zipPath"
