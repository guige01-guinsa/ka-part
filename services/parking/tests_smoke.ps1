param(
    [string]$BaseUrl = "http://127.0.0.1:8011",
    [string]$ApiKey = "change-me"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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

$health = Invoke-RestMethod -Uri "$BaseUrl/health" -Method Get
Assert-Equal -Expected $true -Actual $health.ok -Message "health check failed"

$plateOk = Invoke-RestMethod -Uri "$BaseUrl/api/plates/check?plate=12가3456" -Method Get -Headers @{ "X-API-Key" = $ApiKey }
Assert-Equal -Expected "OK" -Actual $plateOk.verdict -Message "registered plate verdict mismatch"

$plateBlocked = Invoke-RestMethod -Uri "$BaseUrl/api/plates/check?plate=34나5678" -Method Get -Headers @{ "X-API-Key" = $ApiKey }
Assert-Equal -Expected "BLOCKED" -Actual $plateBlocked.verdict -Message "blocked plate verdict mismatch"

$plateUnknown = Invoke-RestMethod -Uri "$BaseUrl/api/plates/check?plate=99허9999" -Method Get -Headers @{ "X-API-Key" = $ApiKey }
Assert-Equal -Expected "UNREGISTERED" -Actual $plateUnknown.verdict -Message "unknown plate verdict mismatch"

Write-Host "Smoke test passed for $BaseUrl"
