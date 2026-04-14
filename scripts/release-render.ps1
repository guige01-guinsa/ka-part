Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

param(
  [string]$TargetBranch = "main",
  [string]$RenderServiceName = "ka-part",
  [switch]$SkipTests,
  [switch]$WithLint,
  [switch]$SkipDeploy,
  [int]$TimeoutSec = 900,
  [int]$PollSec = 10
)

function Normalize-GitUrl([string]$Url) {
  $value = [string]($Url ?? "")
  if ($value.EndsWith(".git")) {
    return $value.Substring(0, $value.Length - 4)
  }
  return $value
}

$root = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $root ".venv\Scripts\python.exe"
$deployScript = Join-Path $root "deploy_render.ps1"

if (-not (Test-Path -LiteralPath $venvPython)) {
  throw "virtual environment is missing. Run .\scripts\dev-setup.ps1 first."
}
if (-not (Test-Path -LiteralPath $deployScript)) {
  throw "deploy_render.ps1 is missing."
}

Push-Location $root
try {
  $dirty = @(& git status --porcelain)
  if ($dirty.Count -gt 0) {
    throw "working tree is dirty. Commit or stash changes before release."
  }

  $originUrl = Normalize-GitUrl((& git remote get-url origin).Trim())
  if (-not $originUrl) {
    throw "origin remote is not configured."
  }

  & git fetch origin $TargetBranch
  $aheadBehind = ((& git rev-list --left-right --count "HEAD...origin/$TargetBranch").Trim() -split "\s+")
  $ahead = if ($aheadBehind.Length -ge 1) { [int]$aheadBehind[0] } else { 0 }
  $behind = if ($aheadBehind.Length -ge 2) { [int]$aheadBehind[1] } else { 0 }

  if ($behind -gt 0) {
    throw "local HEAD is behind origin/$TargetBranch by $behind commit(s). Rebase or merge before release."
  }

  if (-not $SkipTests) {
    & $venvPython -m compileall app
    if ($WithLint) {
      & $venvPython -m ruff check app tests
    }
    & $venvPython -m pytest -q tests\test_engine_routes.py
  }

  if ($ahead -gt 0) {
    & git push origin "HEAD:$TargetBranch"
  }
  else {
    Write-Host "origin/$TargetBranch is already at HEAD." -ForegroundColor Cyan
  }

  if (-not $SkipDeploy) {
    & powershell -ExecutionPolicy Bypass -File $deployScript `
      -ExpectedServiceName $RenderServiceName `
      -ExpectedRepo $originUrl `
      -Wait `
      -TimeoutSec $TimeoutSec `
      -PollSec $PollSec
  }
}
finally {
  Pop-Location
}
