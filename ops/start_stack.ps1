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

$ProjectRoot = Resolve-ProjectRoot $ProjectRoot
$pidDir = Join-Path $ProjectRoot "ops\pids"
$logDir = Join-Path $ProjectRoot "ops\logs"
New-Item -ItemType Directory -Force -Path $pidDir | Out-Null
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

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
    ) | Set-Content -Path $parkingEnv -Encoding UTF8
}

$pwsh = (Get-Command pwsh.exe -ErrorAction Stop).Source
$parkingOut = Join-Path $logDir "parking.stdout.log"
$parkingErr = Join-Path $logDir "parking.stderr.log"
$parkingProc = Start-Process -FilePath $pwsh `
    -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $parkingRoot "run.ps1"), "-ListenHost", "127.0.0.1", "-Port", "8011", "-EnvFile", ".env.production" `
    -WorkingDirectory $parkingRoot `
    -RedirectStandardOutput $parkingOut `
    -RedirectStandardError $parkingErr `
    -PassThru
Set-Content -Path (Join-Path $pidDir "parking.pid") -Value $parkingProc.Id
Wait-Http -Url "http://127.0.0.1:8011/health" -TimeoutSeconds 90

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
Write-Host "Parking:   http://127.0.0.1:8080/parking/login"
