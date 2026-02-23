param(
    [string]$BaseUrl = "http://127.0.0.1:8011",
    [string]$ApiKey = "",
    [string]$SiteCode = "",
    [string]$EnvFile = ".env.production"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-EnvValueFromFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Name
    )
    if (-not (Test-Path $Path)) {
        return ""
    }
    foreach ($rawLine in Get-Content $Path) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }
        $kv = $line.Split("=", 2)
        if ($kv.Count -ne 2) {
            continue
        }
        if ($kv[0].Trim() -ceq $Name) {
            return $kv[1].Trim()
        }
    }
    return ""
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

$resolvedApiKey = ($ApiKey | ForEach-Object { ($_ -as [string]).Trim() })
if (-not $resolvedApiKey) {
    $resolvedApiKey = ($env:PARKING_API_KEY | ForEach-Object { ($_ -as [string]).Trim() })
}

$envFilePath = $EnvFile
if ($envFilePath -and -not [System.IO.Path]::IsPathRooted($envFilePath)) {
    $envFilePath = Join-Path $PSScriptRoot $envFilePath
}
if ((-not $resolvedApiKey) -or $resolvedApiKey -eq "change-me") {
    $fromFile = Get-EnvValueFromFile -Path $envFilePath -Name "PARKING_API_KEY"
    if ($fromFile) {
        $resolvedApiKey = $fromFile
    }
}

if (-not $resolvedApiKey -or $resolvedApiKey -in @("change-me", "replace-with-long-random-api-key")) {
    throw "PARKING_API_KEY is required. Pass -ApiKey or set PARKING_API_KEY (or set it in $envFilePath)."
}

$resolvedSiteCode = ($SiteCode | ForEach-Object { ($_ -as [string]).Trim().ToUpperInvariant() })
if (-not $resolvedSiteCode) {
    $resolvedSiteCode = ($env:PARKING_DEFAULT_SITE_CODE | ForEach-Object { ($_ -as [string]).Trim().ToUpperInvariant() })
}
if (-not $resolvedSiteCode) {
    $fromFileSite = Get-EnvValueFromFile -Path $envFilePath -Name "PARKING_DEFAULT_SITE_CODE"
    $resolvedSiteCode = ($fromFileSite | ForEach-Object { ($_ -as [string]).Trim().ToUpperInvariant() })
}
if (-not $resolvedSiteCode) {
    $resolvedSiteCode = "COMMON"
}

$health = Invoke-RestMethod -Uri "$BaseUrl/health" -Method Get
Assert-Equal -Expected $true -Actual $health.ok -Message "health check failed"

$headers = @{ "X-API-Key" = $resolvedApiKey; "X-Site-Code" = $resolvedSiteCode }

$plateOkValue = "12$([char]0xAC00)3456"       # 12가3456
$plateBlockedValue = "34$([char]0xB098)5678"  # 34나5678
$plateUnknownValue = "99$([char]0xD5C8)9999"  # 99허9999

$plateOk = Invoke-RestMethod -Uri "$BaseUrl/api/plates/check?plate=$([uri]::EscapeDataString($plateOkValue))" -Method Get -Headers $headers
Assert-Equal -Expected "OK" -Actual $plateOk.verdict -Message "registered plate verdict mismatch"

$plateBlocked = Invoke-RestMethod -Uri "$BaseUrl/api/plates/check?plate=$([uri]::EscapeDataString($plateBlockedValue))" -Method Get -Headers $headers
Assert-Equal -Expected "BLOCKED" -Actual $plateBlocked.verdict -Message "blocked plate verdict mismatch"

$plateUnknown = Invoke-RestMethod -Uri "$BaseUrl/api/plates/check?plate=$([uri]::EscapeDataString($plateUnknownValue))" -Method Get -Headers $headers
Assert-Equal -Expected "UNREGISTERED" -Actual $plateUnknown.verdict -Message "unknown plate verdict mismatch"

Write-Host "Smoke test passed for $BaseUrl ($resolvedSiteCode)"
