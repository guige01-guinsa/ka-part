from __future__ import annotations

import html
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

BASE_DIR = Path(__file__).resolve().parent.parent
APP_RELEASE_ID = "2026-04-19-imagesave-1"
PWA_ASSET_VERSION = "20260419d"
AUTH_ASSET_VERSION = "20260407a"
PWA_MANIFEST_VERSION = "14"
STARTED_AT_UTC = datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _collapse(value: Any) -> str:
    return " ".join(str(value or "").replace("\u0000", " ").split()).strip()


def _read_git_head_commit() -> str:
    git_dir = BASE_DIR / ".git"
    if not git_dir.exists():
        return ""
    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return ""
    head_value = _collapse(head_path.read_text(encoding="utf-8", errors="ignore"))
    if not head_value:
        return ""
    if not head_value.startswith("ref:"):
        return head_value
    ref_name = _collapse(head_value.split(":", 1)[1])
    if not ref_name:
        return ""
    ref_path = git_dir / ref_name
    if ref_path.exists():
        return _collapse(ref_path.read_text(encoding="utf-8", errors="ignore"))
    packed_refs = git_dir / "packed-refs"
    if packed_refs.exists():
        for line in packed_refs.read_text(encoding="utf-8", errors="ignore").splitlines():
            raw = _collapse(line)
            if not raw or raw.startswith("#") or raw.startswith("^"):
                continue
            parts = raw.split(" ", 1)
            if len(parts) == 2 and _collapse(parts[1]) == ref_name:
                return _collapse(parts[0])
    return ""


def _git_commit() -> str:
    for name in ("RENDER_GIT_COMMIT", "GIT_COMMIT", "COMMIT_SHA", "RENDER_GIT_COMMIT_SHA"):
        value = _collapse(os.getenv(name) or "")
        if value:
            return value
    return _read_git_head_commit()


def build_info_payload() -> Dict[str, Any]:
    commit = _git_commit()
    return {
        "service": "ka-part-complaint-engine",
        "app_version": "4.0.0",
        "release_id": APP_RELEASE_ID,
        "started_at_utc": STARTED_AT_UTC,
        "git_commit": commit,
        "git_commit_short": commit[:7] if commit else "",
        "static_assets": {
            "pwa_asset_version": PWA_ASSET_VERSION,
            "auth_asset_version": AUTH_ASSET_VERSION,
            "manifest_version": PWA_MANIFEST_VERSION,
            "index_html": f"/pwa/index.html",
            "portal_js": f"/pwa/portal.js?v={PWA_ASSET_VERSION}",
            "portal_css": f"/pwa/portal.css?v={PWA_ASSET_VERSION}",
            "auth_js": f"/pwa/auth.js?v={AUTH_ASSET_VERSION}",
            "manifest": f"/pwa/manifest.webmanifest?v={PWA_MANIFEST_VERSION}",
        },
        "frontend_expectations": {
            "admin_learning_marker": "adminLearningTableBody",
            "admin_learning_api": "/api/admin/work_report_learning",
            "build_info_api": "/api/build_info",
            "build_info_page": "/diag/build",
        },
    }


def build_info_html() -> str:
    payload = build_info_payload()
    pretty_json = json.dumps(payload, ensure_ascii=False, indent=2)
    escaped_json = html.escape(pretty_json)
    escaped_release = html.escape(str(payload.get("release_id") or "-"))
    escaped_commit = html.escape(str(payload.get("git_commit_short") or payload.get("git_commit") or "-"))
    escaped_asset_version = html.escape(str(((payload.get("static_assets") or {}).get("pwa_asset_version")) or "-"))
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>KA-PART Build Diagnostics</title>
  <style>
    body {{
      margin: 0;
      padding: 32px 18px 48px;
      background: linear-gradient(180deg, #f6f1e8 0%, #fffdfa 100%);
      color: #1b2a28;
      font-family: "Segoe UI", "Noto Sans KR", sans-serif;
    }}
    .wrap {{
      max-width: 920px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }}
    .card {{
      padding: 20px 22px;
      border-radius: 20px;
      background: #fff;
      box-shadow: 0 16px 36px rgba(29, 42, 40, 0.08);
      border: 1px solid rgba(19, 57, 54, 0.08);
    }}
    h1, h2, p {{
      margin: 0;
    }}
    h1 {{
      font-size: 28px;
      line-height: 1.1;
    }}
    h2 {{
      font-size: 16px;
      margin-bottom: 10px;
    }}
    .meta {{
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-top: 16px;
    }}
    .metric {{
      padding: 14px 16px;
      border-radius: 16px;
      background: rgba(13, 106, 103, 0.06);
    }}
    .metric strong {{
      display: block;
      font-size: 12px;
      color: #5e716d;
      margin-bottom: 6px;
    }}
    .metric span {{
      font-size: 22px;
      font-weight: 700;
    }}
    code, pre {{
      font-family: Consolas, "SFMono-Regular", monospace;
    }}
    pre {{
      padding: 16px;
      overflow: auto;
      border-radius: 16px;
      background: #f3f6f5;
      border: 1px solid rgba(19, 57, 54, 0.08);
      font-size: 13px;
      line-height: 1.55;
    }}
    ul {{
      margin: 0;
      padding-left: 18px;
    }}
    li + li {{
      margin-top: 8px;
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="card">
      <h1>빌드 진단</h1>
      <p style="margin-top:10px;">이 페이지는 서버가 현재 어떤 빌드와 정적 자산 버전을 사용 중이라고 판단하는지 바로 보여줍니다.</p>
      <div class="meta">
        <div class="metric"><strong>Release</strong><span>{escaped_release}</span></div>
        <div class="metric"><strong>Commit</strong><span>{escaped_commit}</span></div>
        <div class="metric"><strong>PWA Asset</strong><span>{escaped_asset_version}</span></div>
      </div>
    </section>
    <section class="card">
      <h2>확인 방법</h2>
      <ul>
        <li><code>/api/build_info</code> JSON과 이 페이지의 값이 같아야 합니다.</li>
        <li><code>/pwa/index.html</code>에서 <code>portal.js?v={escaped_asset_version}</code>가 보여야 합니다.</li>
        <li>브라우저가 다른 버전을 보여주면 앞단 캐시 또는 로컬 캐시를 의심하면 됩니다.</li>
      </ul>
    </section>
    <section class="card">
      <h2>Raw JSON</h2>
      <pre>{escaped_json}</pre>
    </section>
  </main>
</body>
</html>"""
