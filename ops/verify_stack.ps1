param(
    [string]$ProjectRoot = "",
    [string]$BaseUrl = "http://127.0.0.1:8080"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-ProjectRoot([string]$inputRoot) {
    if ($inputRoot) {
        return (Resolve-Path $inputRoot).Path
    }
    return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Assert-Equal {
    param(
        [Parameter(Mandatory = $true)]$Expected,
        [Parameter(Mandatory = $true)]$Actual,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if ($Expected -ne $Actual) {
        throw "$Message (expected=$Expected, actual=$Actual)"
    }
}

$ProjectRoot = Resolve-ProjectRoot $ProjectRoot
$base = $BaseUrl.TrimEnd("/")
$envPath = Join-Path $ProjectRoot "services\parking\.env.production"
if (-not (Test-Path $envPath)) {
    throw "Parking env not found: $envPath"
}
$apiLine = Get-Content $envPath | Where-Object { $_ -like "PARKING_API_KEY=*" } | Select-Object -First 1
if (-not $apiLine) {
    throw "PARKING_API_KEY not found in $envPath"
}
$apiKey = $apiLine.Split("=", 2)[1]

$main = Invoke-WebRequest -UseBasicParsing -Method Get -Uri "$base/pwa/"
Assert-Equal -Expected 200 -Actual $main.StatusCode -Message "main app is unavailable"

$health = Invoke-RestMethod -Method Get -Uri "$base/parking/health"
Assert-Equal -Expected $true -Actual $health.ok -Message "parking health failed"

$plate = Invoke-RestMethod -Method Get -Uri "$base/parking/api/plates/check?plate=12가3456" -Headers @{ "X-API-Key" = $apiKey }
Assert-Equal -Expected "OK" -Actual $plate.verdict -Message "parking plate check failed"

$login = Invoke-WebRequest -Method Get -Uri "$base/parking/login" -UseBasicParsing
if ($login.Content -notmatch "action='/parking/login'") {
    throw "parking login form action is not proxy-safe"
}

Write-Host "Stack verification passed for $base"
