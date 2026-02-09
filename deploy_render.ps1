# Trigger manual deploy on Render.
# Supports two modes:
# 1) Deploy Hook URL
# 2) API key + service id
#
# Examples:
#   powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1 `
#     -HookUrl "https://api.render.com/deploy/srv-xxx?key=yyy"
#
#   powershell -ExecutionPolicy Bypass -File .\deploy_render.ps1 `
#     -ServiceId "srv-xxx" -ApiKey "rnr_xxx" -Wait

param(
  [string]$HookUrl = $env:RENDER_DEPLOY_HOOK_URL,
  [string]$ServiceId = $env:RENDER_SERVICE_ID,
  [string]$ApiKey = $env:RENDER_API_KEY,
  [switch]$Wait,
  [int]$TimeoutSec = 420,
  [int]$PollSec = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-RenderHeaders([string]$Token) {
  return @{
    "Authorization" = "Bearer $Token"
    "Accept"        = "application/json"
    "Content-Type"  = "application/json"
  }
}

function Trigger-WithHook([string]$Url) {
  if (-not $Url) { return $null }
  try {
    $resp = Invoke-RestMethod -Method Post -Uri $Url
    Write-Host "Render deploy hook triggered." -ForegroundColor Green
    return $resp
  }
  catch {
    $statusCode = ""
    try { $statusCode = [int]$_.Exception.Response.StatusCode } catch {}
    $body = ""
    try {
      $stream = $_.Exception.Response.GetResponseStream()
      if ($stream) {
        $reader = New-Object System.IO.StreamReader($stream)
        $body = $reader.ReadToEnd()
      }
    }
    catch {}
    throw "Deploy hook failed. status=$statusCode body=$body"
  }
}

function Trigger-WithApi([string]$Svc, [string]$Token) {
  if (-not $Svc) { throw "Missing ServiceId. Pass -ServiceId or set RENDER_SERVICE_ID." }
  if (-not $Token) { throw "Missing ApiKey. Pass -ApiKey or set RENDER_API_KEY." }
  $uri = "https://api.render.com/v1/services/$Svc/deploys"
  $headers = Get-RenderHeaders $Token
  $resp = Invoke-RestMethod -Method Post -Uri $uri -Headers $headers -Body "{}"
  Write-Host "Render deploy API triggered." -ForegroundColor Green
  return $resp
}

function Wait-Deploy([string]$Svc, [string]$Token, [string]$DeployId, [int]$Timeout, [int]$Poll) {
  if (-not $Svc -or -not $Token -or -not $DeployId) {
    Write-Warning "Skip wait: ServiceId/ApiKey/DeployId is missing."
    return
  }
  $uri = "https://api.render.com/v1/services/$Svc/deploys/$DeployId"
  $headers = Get-RenderHeaders $Token
  $deadline = (Get-Date).AddSeconds($Timeout)
  while ((Get-Date) -lt $deadline) {
    $state = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers
    $status = [string]($state.status)
    $updated = [string]($state.updatedAt)
    Write-Host ("status={0} updated={1}" -f $status, $updated)
    if ($status -in @("live", "deployed", "succeeded")) {
      Write-Host "Deploy is live." -ForegroundColor Green
      return
    }
    if ($status -in @("build_failed", "failed", "canceled")) {
      throw "Deploy ended with status '$status'."
    }
    Start-Sleep -Seconds $Poll
  }
  throw "Timed out waiting for deploy status. timeoutSec=$Timeout"
}

$triggerResp = $null
$deployId = ""

if ($HookUrl) {
  $triggerResp = Trigger-WithHook $HookUrl
  if ($triggerResp) {
    $props = @{}
    foreach ($p in $triggerResp.PSObject.Properties) {
      $props[$p.Name] = $p.Value
    }
    if ($props.ContainsKey("deploy") -and $props["deploy"] -and $props["deploy"].PSObject.Properties["id"]) {
      $deployId = [string]$props["deploy"].id
    }
    elseif ($props.ContainsKey("id") -and $props["id"]) {
      $deployId = [string]$props["id"]
    }
  }
}
else {
  $triggerResp = Trigger-WithApi $ServiceId $ApiKey
  if ($triggerResp -and $triggerResp.PSObject.Properties["id"]) {
    $deployId = [string]$triggerResp.id
  }
}

if ($deployId) { Write-Host "Deploy ID: $deployId" }
if ($triggerResp -and $triggerResp.PSObject.Properties["status"]) { Write-Host "Status: $($triggerResp.status)" }
if ($triggerResp -and $triggerResp.PSObject.Properties["createdAt"]) { Write-Host "Created: $($triggerResp.createdAt)" }

if ($Wait) {
  Wait-Deploy -Svc $ServiceId -Token $ApiKey -DeployId $deployId -Timeout $TimeoutSec -Poll $PollSec
}
