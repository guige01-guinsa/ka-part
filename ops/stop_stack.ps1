param(
    [string]$ProjectRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Continue"

function Resolve-ProjectRoot([string]$inputRoot) {
    if ($inputRoot) {
        return (Resolve-Path $inputRoot).Path
    }
    return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Stop-ByPidFile {
    param([string]$PidFilePath)
    if (-not (Test-Path $PidFilePath)) {
        return
    }
    $pidValue = Get-Content $PidFilePath | Select-Object -First 1
    if ($pidValue) {
        Stop-Process -Id ([int]$pidValue) -Force -ErrorAction SilentlyContinue
    }
    Remove-Item $PidFilePath -Force -ErrorAction SilentlyContinue
}

$ProjectRoot = Resolve-ProjectRoot $ProjectRoot
$pidDir = Join-Path $ProjectRoot "ops\pids"
$caddyExe = Join-Path $ProjectRoot "tools\caddy\caddy.exe"
$mainVenvPy = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$parkingVenvPy = Join-Path $ProjectRoot "services\parking\.venv\Scripts\python.exe"

if ((Test-Path $caddyExe) -and (Get-Process caddy -ErrorAction SilentlyContinue)) {
    & $caddyExe stop | Out-Null
}

Stop-ByPidFile -PidFilePath (Join-Path $pidDir "caddy.pid")
Stop-ByPidFile -PidFilePath (Join-Path $pidDir "parking.pid")
Stop-ByPidFile -PidFilePath (Join-Path $pidDir "main.pid")

# Fallback for orphan processes launched from this project.
Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.ExecutablePath -and $_.ExecutablePath -ieq $mainVenvPy -and $_.CommandLine -like "*uvicorn app.main:app*" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.ExecutablePath -and $_.ExecutablePath -ieq $parkingVenvPy -and $_.CommandLine -like "*uvicorn app.main:app*" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

Get-CimInstance Win32_Process -Filter "Name='pwsh.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -and $_.CommandLine -like "*services\\parking\\run.ps1*" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

Get-Process caddy -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "Stack stopped."
