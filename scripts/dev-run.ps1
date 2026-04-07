param(
  [int]$Port = 8000,
  [switch]$NoReload,
  [switch]$SeedDemo
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$storageRoot = Join-Path $root "runtime\local"

if (-not (Test-Path -LiteralPath $venvPython)) {
  throw "virtual environment is missing. Run .\scripts\dev-setup.ps1 first."
}

New-Item -ItemType Directory -Force -Path (Join-Path $storageRoot "data") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $storageRoot "uploads") | Out-Null

$env:ALLOW_INSECURE_DEFAULTS = "1"
$env:KA_HSTS_ENABLED = "0"
$env:KA_STORAGE_ROOT = $storageRoot

if ($SeedDemo) {
  $env:KA_BOOTSTRAP_ADMIN_LOGIN = "devadmin"
  $env:KA_BOOTSTRAP_ADMIN_NAME = "로컬 관리자"
  $env:KA_BOOTSTRAP_ADMIN_PASSWORD = "DevPassword123!"
  $env:KA_BOOTSTRAP_TENANT_ID = "demo_apt"
  $env:KA_BOOTSTRAP_TENANT_NAME = "로컬 테스트 아파트"
  $env:KA_BOOTSTRAP_TENANT_SITE_CODE = "DEMO0001"
  $env:KA_BOOTSTRAP_TENANT_SITE_NAME = "로컬 테스트 아파트"
  $env:KA_BOOTSTRAP_TENANT_API_KEY = "sk-ka-dev-local-demo-key"
  $env:KA_BOOTSTRAP_MANAGER_LOGIN = "devmanager"
  $env:KA_BOOTSTRAP_MANAGER_NAME = "로컬 운영담당"
  $env:KA_BOOTSTRAP_MANAGER_PASSWORD = "DevPassword123!"
  $env:KA_BOOTSTRAP_DESK_LOGIN = "devdesk"
  $env:KA_BOOTSTRAP_DESK_NAME = "로컬 접수담당"
  $env:KA_BOOTSTRAP_DESK_PASSWORD = "DevPassword123!"
}

$args = @("-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "$Port")
if (-not $NoReload) {
  $args += "--reload"
}

& $venvPython @args
