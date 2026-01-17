# blueprints/tool_search/main.py
import os
import sqlite3
from datetime import datetime
from flask import Blueprint, request, jsonify, render_template_string, current_app

tool_search_bp = Blueprint("tool_search", __name__)

# --- DB (sqlite3, 별도 파일) ---
def _db_path():
    # instance 폴더 아래에 저장 (Render에서도 disk 붙이면 유지)
    os.makedirs(current_app.instance_path, exist_ok=True)
    return os.path.join(current_app.instance_path, "tool_search.db")

def _conn():
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con

def _init_db():
    with _conn() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS ts_item (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            content TEXT,
            tags TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_ts_item_title ON ts_item(title)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ts_item_tags ON ts_item(tags)")

# --- UI (검색 + 카드뷰) ---
TS_UI = """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>tool-search</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body{background:#f7f8fb}
    .card-title{margin-bottom:.25rem}
    .muted{color:#6c757d}
    .chip{display:inline-block;padding:.15rem .5rem;border-radius:999px;background:#eef1f6;margin-right:.25rem;font-size:.8rem}
  </style>
</head>
<body>
<nav class="navbar navbar-dark bg-dark">
  <div class="container-fluid">
    <a class="navbar-brand" href="/">🏢 ka-part</a>
    <div class="d-flex gap-2">
      <a class="btn btn-outline-light btn-sm" href="/">홈</a>
      <a class="btn btn-light btn-sm" href="/ts/">tool-search</a>
    </div>
  </div>
</nav>

<div class="container py-3">
  <div class="d-flex justify-content-between align-items-center mb-2">
    <h5 class="m-0">tool-search (카드뷰)</h5>
    <button class="btn btn-primary btn-sm" onclick="openNew()">+ 새 등록</button>
  </div>

  <div class="row g-2 mb-3">
    <div class="col-12 col-md-8">
      <input id="q" class="form-control" placeholder="검색 (제목/내용/태그)" onkeydown="if(event.key==='Enter') load()">
    </div>
    <div class="col-6 col-md-2">
      <button class="btn btn-outline-secondary w-100" onclick="load()">검색</button>
    </div>
    <div class="col-6 col-md-2">
      <button class="btn btn-outline-danger w-100" onclick="clearQ()">초기화</button>
    </div>
  </div>

  <div id="cards" class="row g-2"></div>

  <!-- modal -->
  <div class="modal fade" id="m" tabindex="-1">
    <div class="modal-dialog modal-lg">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title" id="mt">새 등록</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
        </div>
        <div class="modal-body">
          <input type="hidden" id="id">
          <div class="mb-2">
            <label class="form-label">제목</label>
            <input id="title" class="form-control" placeholder="예: 서버 장애 대응 절차">
          </div>
          <div class="mb-2">
            <label class="form-label">태그(쉼표)</label>
            <input id="tags" class="form-control" placeholder="예: 전기,서버,민원">
          </div>
          <div class="mb-2">
            <label class="form-label">내용</label>
            <textarea id="content" class="form-control" rows="8" placeholder="상세 내용을 기록"></textarea>
          </div>
          <div class="small muted">저장하면 /ts 에서 바로 검색됩니다.</div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-outline-danger me-auto" id="delBtn" onclick="delItem()" style="display:none">삭제</button>
          <button class="btn btn-secondary" data-bs-dismiss="modal">닫기</button>
          <button class="btn btn-primary" onclick="save()">저장</button>
        </div>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
<script>
let modal;

function esc(s){ return (s||"").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;"); }

function chips(tags){
  if(!tags) return "";
  return tags.split(",").map(t=>t.trim()).filter(Boolean).slice(0,8)
    .map(t=>`<span class="chip">${esc(t)}</span>`).join("");
}

function openNew(){
  document.getElementById("mt").innerText = "새 등록";
  document.getElementById("id").value = "";
  document.getElementById("title").value = "";
  document.getElementById("tags").value = "";
  document.getElementById("content").value = "";
  document.getElementById("delBtn").style.display = "none";
  modal.show();
}

function openEdit(it){
  document.getElementById("mt").innerText = "수정";
  document.getElementById("id").value = it.id;
  document.getElementById("title").value = it.title || "";
  document.getElementById("tags").value = it.tags || "";
  document.getElementById("content").value = it.content || "";
  document.getElementById("delBtn").style.display = "inline-block";
  modal.show();
}

async function load(){
  const q = document.getElementById("q").value.trim();
  const res = await fetch(`/ts/api/items?q=${encodeURIComponent(q)}`);
  const data = await res.json();
  const el = document.getElementById("cards");
  if(!data.items.length){
    el.innerHTML = `<div class="text-muted p-3">결과 없음</div>`;
    return;
  }
  el.innerHTML = data.items.map(it => `
    <div class="col-12">
      <div class="card shadow-sm">
        <div class="card-body">
          <div class="d-flex justify-content-between">
            <div>
              <div class="card-title fw-semibold">${esc(it.title)}</div>
              <div class="mb-2">${chips(it.tags)}</div>
              <div class="small muted">${esc((it.content||"").slice(0,180))}${(it.content||"").length>180?"…":""}</div>
              <div class="small muted mt-2">updated: ${esc(it.updated_at)}</div>
            </div>
            <div class="text-end">
              <button class="btn btn-outline-primary btn-sm" onclick='openEdit(${JSON.stringify(it).replaceAll("'","&#39;")})'>열기</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  `).join("");
}

function clearQ(){ document.getElementById("q").value=""; load(); }

async function save(){
  const id = document.getElementById("id").value;
  const payload = {
    title: document.getElementById("title").value.trim(),
    tags: document.getElementById("tags").value.trim(),
    content: document.getElementById("content").value
  };
  if(!payload.title){ alert("제목은 필수입니다."); return; }

  const url = id ? `/ts/api/items/${id}` : `/ts/api/items`;
  const method = id ? "PUT" : "POST";

  const res = await fetch(url, {
    method,
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  if(!data.ok){ alert(data.error || "저장 실패"); return; }
  modal.hide();
  load();
}

async function delItem(){
  const id = document.getElementById("id").value;
  if(!id) return;
  if(!confirm("삭제하시겠습니까?")) return;
  const res = await fetch(`/ts/api/items/${id}`, {method:"DELETE"});
  const data = await res.json();
  if(!data.ok){ alert(data.error || "삭제 실패"); return; }
  modal.hide();
  load();
}

window.addEventListener("DOMContentLoaded", () => {
  modal = new bootstrap.Modal(document.getElementById("m"));
  load();
});
</script>
</body>
</html>
"""

@tool_search_bp.before_app_request
def _ts_bootstrap():
    # 최초 1회 테이블 보장 (요청 들어올 때 자동)
    # 과도 호출 방지용으로 flag 사용
    if not getattr(current_app, "_ts_inited", False):
        _init_db()
        current_app._ts_inited = True

@tool_search_bp.get("/")
def ts_home():
    return render_template_string(TS_UI)

# --- API ---
@tool_search_bp.get("/api/items")
def api_list_items():
    q = (request.args.get("q") or "").strip()
    like = f"%{q}%"
    with _conn() as con:
        if q:
            rows = con.execute(
                """SELECT * FROM ts_item
                   WHERE title LIKE ? OR content LIKE ? OR tags LIKE ?
                   ORDER BY id DESC LIMIT 200""",
                (like, like, like)
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT * FROM ts_item ORDER BY id DESC LIMIT 200"
            ).fetchall()

    items = [dict(r) for r in rows]
    return jsonify(ok=True, items=items)

@tool_search_bp.post("/api/items")
def api_create_item():
    data = request.get_json(force=True) or {}
    title = (data.get("title") or "").strip()
    if not title:
        return jsonify(ok=False, error="title required"), 400
    tags = (data.get("tags") or "").strip()
    content = (data.get("content") or "")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO ts_item(title, content, tags, created_at, updated_at) VALUES(?,?,?,?,?)",
            (title, content, tags, now, now)
        )
        new_id = cur.lastrowid
    return jsonify(ok=True, id=new_id)

@tool_search_bp.put("/api/items/<int:item_id>")
def api_update_item(item_id: int):
    data = request.get_json(force=True) or {}
    title = (data.get("title") or "").strip()
    if not title:
        return jsonify(ok=False, error="title required"), 400
    tags = (data.get("tags") or "").strip()
    content = (data.get("content") or "")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as con:
        con.execute(
            "UPDATE ts_item SET title=?, content=?, tags=?, updated_at=? WHERE id=?",
            (title, content, tags, now, item_id)
        )
    return jsonify(ok=True)

@tool_search_bp.delete("/api/items/<int:item_id>")
def api_delete_item(item_id: int):
    with _conn() as con:
        con.execute("DELETE FROM ts_item WHERE id=?", (item_id,))
    return jsonify(ok=True)
