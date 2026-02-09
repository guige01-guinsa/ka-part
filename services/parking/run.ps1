param(
    [string]$ListenHost = "0.0.0.0",
    [int]$Port = 8011,
    [string]$EnvFile = ".env.production"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Set-EnvFromFile {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        return
    }
    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            return
        }
        $kv = $line.Split("=", 2)
        if ($kv.Count -eq 2) {
            [Environment]::SetEnvironmentVariable($kv[0].Trim(), $kv[1].Trim(), "Process")
        }
    }
}

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
& $python -m pip install --disable-pip-version-check -r requirements.txt

Set-EnvFromFile -Path (Join-Path $PSScriptRoot $EnvFile)

if (-not $env:PARKING_DB_PATH) {
    $env:PARKING_DB_PATH = ".\app\data\parking.db"
}
if (-not $env:PARKING_UPLOAD_DIR) {
    $env:PARKING_UPLOAD_DIR = ".\app\uploads"
}
if (-not $env:PARKING_API_KEY) {
    $env:PARKING_API_KEY = "change-me"
}
if (-not $env:PARKING_SECRET_KEY) {
    $env:PARKING_SECRET_KEY = "change-this-secret"
}
if (-not $env:PARKING_ROOT_PATH) {
    $env:PARKING_ROOT_PATH = "/parking"
}
if (-not $env:PARKING_CONTEXT_SECRET) {
    $env:PARKING_CONTEXT_SECRET = $env:PARKING_SECRET_KEY
}
if (-not $env:PARKING_LOCAL_LOGIN_ENABLED) {
    $env:PARKING_LOCAL_LOGIN_ENABLED = "1"
}

& $python -m uvicorn app.main:app --host $ListenHost --port $Port --proxy-headers --forwarded-allow-ips "127.0.0.1"
