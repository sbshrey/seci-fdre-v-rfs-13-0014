param(
    [string]$RepoRoot = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if (-not $RepoRoot) {
    $RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
}

function Invoke-RobocopyEmptyPurge {
    param([string]$TargetPath)
    if (-not (Test-Path -LiteralPath $TargetPath)) {
        return
    }
    $empty = Join-Path ([System.IO.Path]::GetTempPath()) ("seci-fdre-v-empty-" + [Guid]::NewGuid().ToString("n"))
    try {
        New-Item -ItemType Directory -Path $empty -Force | Out-Null
        & robocopy.exe $empty $TargetPath /MIR /R:2 /W:2 /NFL /NDL /NJH /NJS | Out-Null
        $code = $LASTEXITCODE
        if ($code -ge 8) {
            throw "robocopy failed with exit code $code while purging $TargetPath"
        }
    } finally {
        if (Test-Path -LiteralPath $empty) {
            Remove-Item -LiteralPath $empty -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

function Remove-BuildTree {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    try {
        Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction Stop
        return
    } catch {
        Write-Warning ('Normal delete failed for ' + $Path + ' - trying robocopy empty-folder purge.')
        Write-Warning @'
If this still fails: quit SECI-FDRE-V if it was launched from dist, close Explorer windows on this folder, and close shells whose cwd is inside dist\ or build\.
'@
    }
    Invoke-RobocopyEmptyPurge -TargetPath $Path
    if (Test-Path -LiteralPath $Path) {
        Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $Path) {
        throw "Could not remove $Path. Unlock files above, then run this script again."
    }
}

$dist = Join-Path $RepoRoot "dist"
$build = Join-Path $RepoRoot "build"
Write-Host "Removing $dist"
Remove-BuildTree -Path $dist
Write-Host "Removing $build"
Remove-BuildTree -Path $build
Write-Host "Done."
