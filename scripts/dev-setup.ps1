param(
  [switch]$Recreate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$venv = Join-Path $root ".venv"
$python = Join-Path $venv "Scripts\python.exe"
$storageRoot = Join-Path $root "runtime\local"

if ($Recreate -and (Test-Path -LiteralPath $venv)) {
  Remove-Item -LiteralPath $venv -Recurse -Force
}

if (-not (Test-Path -LiteralPath $venv)) {
  python -m venv $venv
}

if (-not (Test-Path -LiteralPath $python)) {
  throw "virtual environment python not found: $python"
}

New-Item -ItemType Directory -Force -Path (Join-Path $storageRoot "data") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $storageRoot "uploads") | Out-Null

& $python -m pip install --upgrade pip
& $python -m pip install -r (Join-Path $root "requirements-dev.txt")

Write-Host "Dev environment ready." -ForegroundColor Green
Write-Host "venv: $venv"
Write-Host "storage: $storageRoot"
