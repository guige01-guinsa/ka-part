Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$storageRoot = Join-Path $root "runtime\test"

if (-not (Test-Path -LiteralPath $venvPython)) {
  throw "virtual environment is missing. Run .\scripts\dev-setup.ps1 first."
}

New-Item -ItemType Directory -Force -Path (Join-Path $storageRoot "data") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $storageRoot "uploads") | Out-Null

$env:KA_STORAGE_ROOT = $storageRoot
$env:ALLOW_INSECURE_DEFAULTS = "1"
$env:KA_HSTS_ENABLED = "0"

& $venvPython -m compileall app
& $venvPython -m ruff check app tests
& $venvPython -m pytest -q tests\test_engine_routes.py
