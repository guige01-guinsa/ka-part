# Trigger a manual deploy on Render using API key + service ID.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1 `
#     -ServiceId srv-xxxxxxxxxxxx `
#     -ApiKey rnr_xxxxxxxxxxxx
#
# Or use env vars:
#   $env:RENDER_SERVICE_ID="srv-xxxxxxxxxxxx"
#   $env:RENDER_API_KEY="rnr_xxxxxxxxxxxx"
#   powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1

param(
  [string]$ServiceId = $env:RENDER_SERVICE_ID,
  [string]$ApiKey = $env:RENDER_API_KEY
)

if (-not $ServiceId) {
  throw "Missing ServiceId. Pass -ServiceId or set RENDER_SERVICE_ID."
}

if (-not $ApiKey) {
  throw "Missing ApiKey. Pass -ApiKey or set RENDER_API_KEY."
}

$uri = "https://api.render.com/v1/services/$ServiceId/deploys"
$headers = @{
  "Authorization" = "Bearer $ApiKey"
  "Accept"        = "application/json"
  "Content-Type"  = "application/json"
}

try {
  $resp = Invoke-RestMethod -Method Post -Uri $uri -Headers $headers -Body "{}"
  Write-Host "Render deploy triggered." -ForegroundColor Green
  if ($resp.id) { Write-Host "Deploy ID: $($resp.id)" }
  if ($resp.status) { Write-Host "Status: $($resp.status)" }
  if ($resp.createdAt) { Write-Host "Created: $($resp.createdAt)" }
}
catch {
  Write-Error "Failed to trigger deploy. $($_.Exception.Message)"
  throw
}
