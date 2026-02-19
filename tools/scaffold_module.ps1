param(
    [Parameter(Mandatory = $true)]
    [string]$ModuleKey,
    [Parameter(Mandatory = $false)]
    [string]$DisplayName = "",
    [Parameter(Mandatory = $false)]
    [string]$ApiPrefix = "",
    [Parameter(Mandatory = $false)]
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($ModuleKey -notmatch '^[a-z][a-z0-9_]{1,40}$') {
    throw "ModuleKey must match ^[a-z][a-z0-9_]{1,40}$"
}

if ([string]::IsNullOrWhiteSpace($DisplayName)) {
    $DisplayName = $ModuleKey
}

if ([string]::IsNullOrWhiteSpace($ApiPrefix)) {
    $ApiPrefix = "/api/$ModuleKey"
}

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$routeFile = Join-Path $root "app/routes/$ModuleKey.py"
$htmlFile = Join-Path $root "static/pwa/$ModuleKey.html"
$cssFile = Join-Path $root "static/pwa/$ModuleKey.css"
$jsFile = Join-Path $root "static/pwa/$ModuleKey.js"

$targets = @($routeFile, $htmlFile, $cssFile, $jsFile)
if (-not $Force) {
    foreach ($path in $targets) {
        if (Test-Path $path) {
            throw "File already exists: $path (use -Force to overwrite)"
        }
    }
}

$routeTemplate = @"
from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("$ApiPrefix/bootstrap")
def ${ModuleKey}_bootstrap(_request: Request) -> Dict[str, Any]:
    return {
        "ok": True,
        "module_key": "$ModuleKey",
        "message": "$DisplayName bootstrap ready",
    }
"@

$htmlTemplate = @"
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <title>$DisplayName</title>
  <link rel="stylesheet" href="/pwa/$ModuleKey.css?v=1" />
</head>
<body>
  <header class="top">
    <h1>$DisplayName</h1>
    <a class="btn" href="/pwa/">메인으로</a>
  </header>
  <main class="main">
    <section class="card">
      <p id="msg">로딩 중...</p>
    </section>
  </main>

  <script src="/pwa/auth.js?v=20260218a"></script>
  <script src="/pwa/module_base.js?v=20260219a"></script>
  <script src="/pwa/$ModuleKey.js?v=1"></script>
</body>
</html>
"@

$cssTemplate = @"
body {
  margin: 0;
  font-family: sans-serif;
  background: #f5f7fb;
  color: #1f2937;
}

.top {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
}

.main {
  padding: 16px;
}

.card {
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 10px;
  padding: 16px;
}

.btn {
  display: inline-block;
  padding: 8px 12px;
  border: 1px solid #d1d5db;
  border-radius: 8px;
  text-decoration: none;
  color: #111827;
  background: #ffffff;
}
"@

$jsTemplate = @"
(() => {
  "use strict";

  async function init() {
    if (!window.KAAuth) throw new Error("auth.js가 로드되지 않았습니다.");
    await window.KAAuth.requireAuth();
    const ctx = window.KAModuleBase
      ? await window.KAModuleBase.bootstrap("$ModuleKey", { defaultLimit: 100, maxLimit: 500 })
      : null;

    const qs = ctx ? ctx.withSite("$ApiPrefix/bootstrap") : "$ApiPrefix/bootstrap";
    const data = await window.KAAuth.requestJson(qs);
    const el = document.getElementById("msg");
    if (el) {
      const who = ctx && ctx.user ? String(ctx.user.name || "-") : "-";
      el.textContent = "$DisplayName 준비 완료 / user=" + who + " / api=" + (data && data.message ? data.message : "ok");
    }
  }

  init().catch((err) => {
    const msg = err && err.message ? err.message : String(err);
    alert("$DisplayName 초기화 오류: " + msg);
  });
})();
"@

New-Item -ItemType Directory -Force -Path (Split-Path $routeFile) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $htmlFile) | Out-Null

Set-Content -Path $routeFile -Value $routeTemplate -Encoding UTF8
Set-Content -Path $htmlFile -Value $htmlTemplate -Encoding UTF8
Set-Content -Path $cssFile -Value $cssTemplate -Encoding UTF8
Set-Content -Path $jsFile -Value $jsTemplate -Encoding UTF8

Write-Host "Generated:"
foreach ($path in $targets) {
    Write-Host " - $path"
}
Write-Host ""
Write-Host "Next:"
Write-Host "1) include router in app/main.py"
Write-Host "2) add module contract row in module_contracts table"
Write-Host "3) add module menu button/link in pwa app"
