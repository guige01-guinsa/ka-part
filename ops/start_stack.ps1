param(
    [string]$ProjectRoot = "",
    [string]$CaddyConfig = "ops/Caddyfile.local"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

function Resolve-ProjectRoot([string]$inputRoot) {
    if ($inputRoot) {
        return (Resolve-Path $inputRoot).Path
    }
    return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Wait-Http {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 30
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $resp = Invoke-WebRequest -Method Get -Uri $Url -UseBasicParsing -TimeoutSec 3
            if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
                return
            }
        } catch {
        }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    throw "Timeout waiting for $Url"
}

function New-RandomBase64 {
    param([int]$Bytes = 32)
    $buffer = New-Object byte[] $Bytes
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($buffer)
    return [Convert]::ToBase64String($buffer)
}

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

$ProjectRoot = Resolve-ProjectRoot $ProjectRoot
$pidDir = Join-Path $ProjectRoot "ops\pids"
$logDir = Join-Path $ProjectRoot "ops\logs"
New-Item -ItemType Directory -Force -Path $pidDir | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$parkingRoot = Join-Path $ProjectRoot "services\parking"
$parkingEnv = Join-Path $parkingRoot ".env.production"
if (-not (Test-Path $parkingEnv)) {
    @(
        "PARKING_SECRET_KEY=$(New-RandomBase64 -Bytes 48)"
        "PARKING_API_KEY=$(New-RandomBase64 -Bytes 32)"
        "PARKING_SESSION_MAX_AGE=43200"
        "PARKING_DB_PATH=./app/data/parking.db"
        "PARKING_UPLOAD_DIR=./app/uploads"
        "PARKING_ROOT_PATH=/parking"
        "PARKING_LOCAL_LOGIN_ENABLED=0"
        "PARKING_CONTEXT_SECRET=$(New-RandomBase64 -Bytes 48)"
    ) | Set-Content -Path $parkingEnv -Encoding UTF8
}
Set-EnvFromFile -Path $parkingEnv
if (-not $env:ENABLE_PARKING_EMBED) { $env:ENABLE_PARKING_EMBED = "1" }
if (-not $env:PARKING_ROOT_PATH) { $env:PARKING_ROOT_PATH = "/parking" }
if (-not $env:PARKING_DB_PATH) { $env:PARKING_DB_PATH = (Join-Path $parkingRoot "app\data\parking.db") }
elseif (-not [System.IO.Path]::IsPathRooted($env:PARKING_DB_PATH)) { $env:PARKING_DB_PATH = (Join-Path $parkingRoot $env:PARKING_DB_PATH) }
if (-not $env:PARKING_UPLOAD_DIR) { $env:PARKING_UPLOAD_DIR = (Join-Path $parkingRoot "app\uploads") }
elseif (-not [System.IO.Path]::IsPathRooted($env:PARKING_UPLOAD_DIR)) { $env:PARKING_UPLOAD_DIR = (Join-Path $parkingRoot $env:PARKING_UPLOAD_DIR) }
if (-not $env:PARKING_LOCAL_LOGIN_ENABLED) { $env:PARKING_LOCAL_LOGIN_ENABLED = "0" }
if (-not $env:PARKING_API_KEY) { $env:PARKING_API_KEY = "change-me" }
if (-not $env:PARKING_SECRET_KEY) { $env:PARKING_SECRET_KEY = "change-this-secret" }
if (-not $env:PARKING_CONTEXT_SECRET) { $env:PARKING_CONTEXT_SECRET = $env:PARKING_SECRET_KEY }

$mainVenvPy = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $mainVenvPy)) {
    python -m venv (Join-Path $ProjectRoot ".venv")
}
& $mainVenvPy -m pip install --disable-pip-version-check -r (Join-Path $ProjectRoot "requirements.txt")

$mainOut = Join-Path $logDir "main.stdout.log"
$mainErr = Join-Path $logDir "main.stderr.log"
$mainProc = Start-Process -FilePath $mainVenvPy `
    -ArgumentList "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000" `
    -WorkingDirectory $ProjectRoot `
    -RedirectStandardOutput $mainOut `
    -RedirectStandardError $mainErr `
    -PassThru
Set-Content -Path (Join-Path $pidDir "main.pid") -Value $mainProc.Id
Wait-Http -Url "http://127.0.0.1:8000/pwa/" -TimeoutSeconds 60

$caddyDir = Join-Path $ProjectRoot "tools\caddy"
$caddyExe = Join-Path $caddyDir "caddy.exe"
if (-not (Test-Path $caddyExe)) {
    New-Item -ItemType Directory -Force -Path $caddyDir | Out-Null
    Invoke-WebRequest -Uri "https://caddyserver.com/api/download?os=windows&arch=amd64" -OutFile $caddyExe
}

$caddyConfigPath = Join-Path $ProjectRoot $CaddyConfig
if (-not (Test-Path $caddyConfigPath)) {
    throw "Caddy config not found: $caddyConfigPath"
}

$caddyOut = Join-Path $logDir "caddy.stdout.log"
$caddyErr = Join-Path $logDir "caddy.stderr.log"
$caddyProc = Start-Process -FilePath $caddyExe `
    -ArgumentList "run", "--config", $caddyConfigPath `
    -WorkingDirectory $ProjectRoot `
    -RedirectStandardOutput $caddyOut `
    -RedirectStandardError $caddyErr `
    -PassThru
Set-Content -Path (Join-Path $pidDir "caddy.pid") -Value $caddyProc.Id
Wait-Http -Url "http://127.0.0.1:8080/pwa/" -TimeoutSeconds 60
Wait-Http -Url "http://127.0.0.1:8080/parking/health" -TimeoutSeconds 60

Write-Host "Stack started."
Write-Host "Main app:  http://127.0.0.1:8080/pwa/"
Write-Host "Parking:   http://127.0.0.1:8080/parking/admin2"
