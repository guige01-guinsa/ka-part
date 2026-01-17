from fastapi.responses import HTMLResponse, RedirectResponse, Response
import os, uuid, html

from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from PIL import Image

from db import init_db, get_conn

BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

app = FastAPI()
init_db()

# 업로드 이미지 브라우저에서 바로 보이게
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")

# --------- 유틸 ---------
def save_upload(file: UploadFile) -> Path:
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
        ext = ".jpg"
    fname = f"{uuid.uuid4().hex}{ext}"
    path = UPLOAD_DIR / fname
    with open(path, "wb") as f:
        f.write(file.file.read())
    return path

def calc_ahash(img_path: Path) -> str:
    """
    aHash (average hash)
    - 외부 패키지 0
    - Android/Pydroid3 안정
    """
    img = Image.open(img_path).convert("L").resize((8, 8))
    pixels = list(img.getdata())
    avg = sum(pixels) / len(pixels)

    bits = 0
    for p in pixels:
        bits = (bits << 1) | (1 if p >= avg else 0)

    return f"{bits:016x}"

def hamming_hex(a: str, b: str) -> int:
    return (int(a, 16) ^ int(b, 16)).bit_count()

def esc(s: str) -> str:
    return html.escape(s or "")

def layout(body: str) -> str:
    template = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>공구 이미지 검색</title>
<style>
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 16px; }
  .wrap { max-width: 820px; margin: 0 auto; }
  .top { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  h1 { font-size: 20px; margin: 0; }
  .muted { color:#666; font-size: 12px; }
  .box { border: 1px solid #ddd; border-radius: 14px; padding: 14px; margin-top: 12px; background:#fff; }
  label { display:block; font-size: 12px; color:#444; margin-top: 10px; }
  input, select, button, textarea { width:100%; font-size: 16px; padding: 10px; border-radius: 12px; border:1px solid #ccc; box-sizing:border-box; }
  button { border: none; padding: 12px; font-weight: 700; }
  .btn { background: #111; color: #fff; }
  .btn2 { background: #f2f2f2; color:#111; }
  .row { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  .cards { display:grid; grid-template-columns: 1fr; gap: 10px; margin-top: 12px; }
  .card { border:1px solid #e5e5e5; border-radius: 16px; padding: 12px; display:grid; grid-template-columns: 110px 1fr; gap: 12px; align-items: start; }
  .thumb { width:110px; height:110px; border-radius: 14px; object-fit: cover; border:1px solid #eee; background:#fafafa; }
  .title { font-size: 16px; font-weight: 800; margin: 0 0 6px 0; }
  .meta { font-size: 13px; color:#444; margin: 2px 0; }
  .score { font-size: 12px; color:#666; margin-top: 6px; }
  .actions { display:flex; gap:8px; margin-top: 10px; }
  .actions form { flex:1; }
  .small { font-size: 12px; color:#666; margin-top: 10px; line-height:1.4; }
  .hr { height:1px; background:#eee; margin: 12px 0; }

  @media print {
    .noprint { display:none !important; }
    body { margin: 0; }
    .box { border: none; }
    .card { break-inside: avoid; }
  }
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <h1>공구 이미지 검색</h1>
    <div class="muted">Android · FastAPI · aHash</div>
  </div>

  __BODY__

</div>
</body>
</html>
"""
    return template.replace("__BODY__", body)

# --------- 화면 ---------
@app.get("/", response_class=HTMLResponse)
def home():
    conn = get_conn()
    tools = conn.execute("SELECT id, name, location, status FROM tools ORDER BY id DESC LIMIT 30").fetchall()
    conn.close()

    tool_rows = "".join(
        f"<option value='{t['id']}'>{esc(t['name'])} (#{t['id']}, {esc(t['location'])}, {esc(t['status'])})</option>"
        for t in tools
    )

    body = f"""
    <div class="box">
      <div class="muted">사진 한 장이 공구의 이력서가 됩니다. 등록(기준 사진) → 검색(현장 사진) → 확정(데이터 누적).</div>
    </div>

    <div class="box">
      <h2 style="margin:0 0 10px 0; font-size:16px;">1) 공구 등록</h2>
      <form action="/tools" method="post" enctype="multipart/form-data">
        <label>공구명</label>
        <input name="name" placeholder="예) 절연드라이버_1000V" required />

        <label>용도</label>
        <input name="purpose" placeholder="예) 분전반 단자 체결용" required />

        <label>보관위치</label>
        <input name="location" value="전기실" required />

        <label>상태</label>
        <select name="status">
          <option>정상</option>
          <option>수리중</option>
          <option>예비</option>
          <option>분실</option>
        </select>

        <label>기준 사진(카메라)</label>
        <input type="file" name="file" accept="image/*" capture="environment" required />
        <div class="small">※ 카메라가 바로 뜨게 하려면 <b>capture="environment"</b>가 핵심입니다.</div>

        <div class="hr"></div>
        <button class="btn" type="submit">등록</button>
      </form>
    </div>

    <div class="box">
      <h2 style="margin:0 0 10px 0; font-size:16px;">2) 이미지로 검색</h2>
      <form action="/search" method="post" enctype="multipart/form-data">
        <label>현장 사진(카메라)</label>
        <input type="file" name="file" accept="image/*" capture="environment" required />

        <div class="row">
          <div>
            <label>TopK</label>
            <input name="topk" value="5" />
          </div>
          <div>
            <label>검색 범위</label>
            <select name="scope">
              <option value="all">전체</option>
              <option value="location">같은 위치 우선</option>
            </select>
          </div>
        </div>

        <label>위치(선택)</label>
        <input name="location" placeholder="예) 전기실 / 기계실 / 창고A" />

        <div class="hr"></div>
        <button class="btn" type="submit">검색</button>
      </form>
    </div>

    <div class="box">
      <h2 style="margin:0 0 10px 0; font-size:16px;">3) 반출/반납 기록(선택)</h2>
      <form action="/events" method="post">
        <label>대상 공구</label>
        <select name="tool_id" required>
          {tool_rows if tool_rows else "<option value=''>먼저 공구를 등록하세요</option>"}
        </select>

        <div class="row">
          <div>
            <label>구분</label>
            <select name="event_type">
              <option>반출</option>
              <option>반납</option>
              <option>점검</option>
            </select>
          </div>
          <div>
            <label>담당</label>
            <input name="person" placeholder="예) 시설기사 홍길동" />
          </div>
        </div>

        <label>비고</label>
        <input name="note" placeholder="예) 27층 민원 조치" />

        <div class="hr"></div>
        <button class="btn2" type="submit">기록 저장</button>
      </form>
      <div class="small">공구는 ‘어디 있나’로 끝나지 않습니다. <b>누가, 언제, 왜</b>까지 남기면 분실률이 꺾입니다.</div>
    </div>
    """
    return HTMLResponse(layout(body))

# --------- 데이터 처리 ---------
@app.post("/tools")
def create_tool(
    name: str = Form(...),
    purpose: str = Form(...),
    location: str = Form(...),
    status: str = Form("정상"),
    file: UploadFile = File(...)
):
    img_path = save_upload(file)
    ah = calc_ahash(img_path)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tools(name, purpose, location, status) VALUES (?, ?, ?, ?)",
        (name, purpose, location, status)
    )
    tool_id = cur.lastrowid
    cur.execute(
        "INSERT INTO tool_images(tool_id, image_path, ahash) VALUES (?, ?, ?)",
        (tool_id, str(img_path), ah)
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url="/", status_code=303)

@app.post("/search", response_class=HTMLResponse)
def search(file: UploadFile = File(...), topk: str = Form("5"), scope: str = Form("all"), location: str = Form("")):
    # 쿼리 이미지 저장
    q_path = save_upload(file)
    q_hash = calc_ahash(q_path)

    conn = get_conn()
    # 검색 범위 옵션(같은 위치 우선)
    if scope == "location" and location.strip():
        rows = conn.execute("""
            SELECT ti.id as tool_image_id, ti.tool_id, ti.image_path, ti.ahash,
                   t.name, t.purpose, t.location, t.status
            FROM tool_images ti
            JOIN tools t ON t.id = ti.tool_id
            WHERE t.location = ?
        """, (location.strip(),)).fetchall()
        # 해당 위치에 데이터가 너무 적으면 전체도 섞자(현장 타협)
        if len(rows) < 8:
            rows = conn.execute("""
                SELECT ti.id as tool_image_id, ti.tool_id, ti.image_path, ti.ahash,
                       t.name, t.purpose, t.location, t.status
                FROM tool_images ti
                JOIN tools t ON t.id = ti.tool_id
            """).fetchall()
    else:
        rows = conn.execute("""
            SELECT ti.id as tool_image_id, ti.tool_id, ti.image_path, ti.ahash,
                   t.name, t.purpose, t.location, t.status
            FROM tool_images ti
            JOIN tools t ON t.id = ti.tool_id
        """).fetchall()
    conn.close()

    try:
        k = max(1, min(20, int(topk)))
    except:
        k = 5

    scored = []
    for r in rows:
        dist = hamming_hex(q_hash, r["ahash"])  # 낮을수록 유사
        scored.append({
            "tool_id": r["tool_id"],
            "name": r["name"],
            "purpose": r["purpose"],
            "location": r["location"],
            "status": r["status"],
            "ref_image": os.path.basename(r["image_path"]),
            "hamming": dist
        })

    scored.sort(key=lambda x: x["hamming"])
    hits = scored[:k]

    cards = ""
    for h in hits:
        cards += f"""
        <div class="card">
          <img class="thumb" src="/uploads/{esc(h['ref_image'])}" alt="ref"/>
          <div>
            <div class="title">{esc(h['name'])} <span class="muted">#{h['tool_id']}</span></div>
            <div class="meta">용도: {esc(h['purpose'])}</div>
            <div class="meta">위치: {esc(h['location'])} · 상태: {esc(h['status'])}</div>
            <div class="score">유사도(해밍거리): <b>{h['hamming']}</b> (낮을수록 유사)</div>

            <div class="actions">
              <form action="/feedback" method="post">
                <input type="hidden" name="tool_id" value="{h['tool_id']}"/>
                <input type="hidden" name="query_image" value="{esc(q_path.name)}"/>
                <button class="btn" type="submit">✅ 이 공구가 맞음</button>
              </form>

              <form action="/events" method="post">
                <input type="hidden" name="tool_id" value="{h['tool_id']}"/>
                <input type="hidden" name="event_type" value="반출"/>
                <input type="hidden" name="person" value=""/>
                <input type="hidden" name="note" value="검색 화면에서 반출 기록"/>
                <button class="btn2" type="submit">📦 반출 기록</button>
              </form>
            </div>
          </div>
        </div>
        """

    body = f"""
    <div class="box">
      <h2 style="margin:0 0 10px 0; font-size:16px;">검색 결과</h2>
      <div class="muted">쿼리 이미지</div>
      <img class="thumb" src="/uploads/{esc(q_path.name)}" style="width:140px;height:140px;" alt="query"/>
      <div class="small">결과가 맞다면 <b>“이 공구가 맞음”</b>을 누르세요. 그 사진이 해당 공구의 데이터로 편입되어 다음 검색이 더 단단해집니다.</div>
      <div class="cards">{cards if cards else "<div class='muted'>등록된 공구 이미지가 없습니다.</div>"}</div>
      <div class="hr"></div>
      <a href="/" style="text-decoration:none;"><button class="btn2" type="button">← 홈으로</button></a>
    </div>
    """
    return HTMLResponse(layout(body))

@app.post("/feedback")
def feedback(tool_id: int = Form(...), query_image: str = Form(...)):
    # 사용자가 "맞다"라고 확정한 순간, 쿼리 이미지를 그 공구의 이미지로 편입
    q_path = UPLOAD_DIR / query_image
    if not q_path.exists():
        return RedirectResponse(url="/", status_code=303)

    ah = calc_ahash(q_path)
    conn = get_conn()
    conn.execute(
        "INSERT INTO tool_images(tool_id, image_path, ahash) VALUES (?, ?, ?)",
        (tool_id, str(q_path), ah)
    )
    # 동시에 이력도 남겨두면 나중에 감사/추적이 쉬움
    conn.execute(
        "INSERT INTO tool_events(tool_id, event_type, person, note) VALUES (?, '점검', '', '이미지 확정(학습 데이터 편입)')",
        (tool_id,)
    )
    conn.commit()
    conn.close()

    return RedirectResponse(url="/", status_code=303)

@app.post("/events")
def add_event(
    tool_id: int = Form(...),
    event_type: str = Form(...),
    person: str = Form(""),
    note: str = Form("")
):
    conn = get_conn()
    conn.execute(
        "INSERT INTO tool_events(tool_id, event_type, person, note) VALUES (?, ?, ?, ?)",
        (tool_id, event_type, person, note)
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url="/", status_code=303)
	
# -----------------------------
# 재고 대시보드 / 리스트 / 출력 / CSV
# -----------------------------

@app.get("/dashboard",response_class=HTMLResponse)

def dashboard():
    conn = get_conn()

    total = conn.execute("SELECT COUNT(*) AS c FROM tools").fetchone()["c"]
    by_status = conn.execute("""
        SELECT status, COUNT(*) AS c
        FROM tools
        GROUP BY status
        ORDER BY c DESC
    """).fetchall()

    by_location = conn.execute("""
        SELECT location, COUNT(*) AS c
        FROM tools
        GROUP BY location
        ORDER BY c DESC, location ASC
    """).fetchall()

    loc_status = conn.execute("""
        SELECT location, status, COUNT(*) AS c
        FROM tools
        GROUP BY location, status
        ORDER BY location ASC, c DESC
    """).fetchall()

    conn.close()

    loc_map = {}
    for r in loc_status:
        loc = r["location"]
        loc_map.setdefault(loc, {})
        loc_map[loc][r["status"]] = r["c"]

    status_badges = ""
    for s in by_status:
        status_badges += f"<div class='box' style='margin-top:10px;'><b>{esc(s['status'])}</b> : {s['c']}개</div>"

    cards = ""
    for loc in by_location:
        l = loc["location"]
        c = loc["c"]
        parts = loc_map.get(l, {})
        mini = " · ".join([f"{esc(k)} {v}" for k, v in parts.items()]) if parts else "상태 데이터 없음"
        cards += f"""
        <div class="box">
          <div style="display:flex; justify-content:space-between; gap:10px; align-items:center;">
            <div>
              <div style="font-weight:900; font-size:16px;">{esc(l)}</div>
              <div class="muted" style="margin-top:4px;">{mini}</div>
            </div>
            <div style="text-align:right;">
              <div style="font-weight:900; font-size:18px;">{c}개</div>
              <div class="muted"><a href="/tools/list?location={esc(l)}">리스트 보기 →</a></div>
            </div>
          </div>
        </div>
        """

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <a href="/" style="text-decoration:none;"><button class="btn2" type="button">← 홈</button></a>
        <a href="/tools/list" style="text-decoration:none;"><button class="btn2" type="button">전체 리스트</button></a>
        <a href="/tools/print" style="text-decoration:none;"><button class="btn" type="button">🖨️ 전체 출력</button></a>
        <a href="/tools.csv" style="text-decoration:none;"><button class="btn2" type="button">CSV 다운로드</button></a>
      </div>
      <div class="small">대시보드는 “현황”입니다. 분실·수리중은 여기서 먼저 드러납니다.</div>
    </div>

    <div class="box">
      <div style="font-size:16px; font-weight:900;">전체 재고: {total}개</div>
      <div class="muted" style="margin-top:6px;">상태별 현황</div>
      {status_badges if status_badges else "<div class='muted'>데이터 없음</div>"}
    </div>

    <div class="box">
      <div style="font-size:16px; font-weight:900;">위치별 재고</div>
      <div class="muted" style="margin-top:6px;">위치를 눌러 리스트로 들어가세요.</div>
    </div>
    {cards if cards else "<div class='box muted'>등록된 공구가 없습니다.</div>"}
    """
    return HTMLResponse(layout(body))


@app.get("/tools/list", response_class=HTMLResponse)
def tools_list(location: str = "", status: str = "", q: str = ""):
    conn = get_conn()

    where = []
    params = []

    if location.strip():
        where.append("location = ?")
        params.append(location.strip())
    if status.strip():
        where.append("status = ?")
        params.append(status.strip())
    if q.strip():
        where.append("(name LIKE ? OR purpose LIKE ?)")
        params.extend([f"%{q.strip()}%", f"%{q.strip()}%"])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = conn.execute(f"""
        SELECT id, name, purpose, location, status, created_at
        FROM tools
        {where_sql}
        ORDER BY location ASC, id DESC
    """, params).fetchall()

    locs = conn.execute("SELECT DISTINCT location FROM tools ORDER BY location ASC").fetchall()
    conn.close()

    loc_options = "<option value=''>전체</option>" + "".join(
        f"<option value='{esc(r['location'])}' {'selected' if r['location']==location else ''}>{esc(r['location'])}</option>"
        for r in locs
    )

    status_list = ["", "정상", "수리중", "예비", "분실"]
    status_options = "".join(
        f"<option value='{esc(s)}' {'selected' if s==status else ''}>{esc(s) if s else '전체'}</option>"
        for s in status_list
    )

    items = ""
    for r in rows:
        items += f"""
        <div class="box">
          <div style="display:flex; justify-content:space-between; gap:10px; align-items:flex-start;">
            <div>
              <div style="font-weight:900; font-size:16px;">{esc(r['name'])} <span class="muted">#{r['id']}</span></div>
              <div class="meta">용도: {esc(r['purpose'])}</div>
              <div class="meta">위치: {esc(r['location'])} · 상태: <b>{esc(r['status'])}</b></div>
              <div class="muted">등록: {esc(r['created_at'])}</div>
            </div>
            <div class="noprint" style="min-width:130px;">
              <form action="/events" method="post">
                <input type="hidden" name="tool_id" value="{r['id']}"/>
                <input type="hidden" name="event_type" value="반출"/>
                <input type="hidden" name="person" value=""/>
                <input type="hidden" name="note" value="리스트에서 반출"/>
                <button class="btn2" type="submit">📦 반출</button>
              </form>
              <div style="height:8px;"></div>
              <form action="/events" method="post">
                <input type="hidden" name="tool_id" value="{r['id']}"/>
                <input type="hidden" name="event_type" value="반납"/>
                <input type="hidden" name="person" value=""/>
                <input type="hidden" name="note" value="리스트에서 반납"/>
                <button class="btn2" type="submit">↩️ 반납</button>
              </form>
            </div>
          </div>
        </div>
        """

    from urllib.parse import urlencode
    qs = urlencode({"location": location, "status": status, "q": q})
    print_url = "/tools/print" + (f"?{qs}" if qs else "")

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <a href="/dashboard" style="text-decoration:none;"><button class="btn2" type="button">← 대시보드</button></a>
        <a href="{print_url}" style="text-decoration:none;"><button class="btn" type="button">🖨️ 이 조건으로 출력</button></a>
        <a href="/tools.csv" style="text-decoration:none;"><button class="btn2" type="button">CSV</button></a>
      </div>

      <form method="get" action="/tools/list" style="margin-top:10px;">
        <div class="row">
          <div>
            <label>위치</label>
            <select name="location">{loc_options}</select>
          </div>
          <div>
            <label>상태</label>
            <select name="status">{status_options}</select>
          </div>
        </div>
        <label>검색어(공구명/용도)</label>
        <input name="q" value="{esc(q)}" placeholder="예) 절연 / 임팩 / 배관" />
        <div class="hr"></div>
        <button class="btn" type="submit">필터 적용</button>
      </form>
    </div>

    <div class="box">
      <div style="font-weight:900; font-size:16px;">리스트 ({len(rows)}건)</div>
      <div class="muted">필터: {esc(location) or "전체"} · {esc(status) or "전체"} · {esc(q) or "없음"}</div>
    </div>

    {items if items else "<div class='box muted'>조건에 해당하는 공구가 없습니다.</div>"}
    """
    return HTMLResponse(layout(body))


@app.get("/tools/print", response_class=HTMLResponse)
def tools_print(location: str = "", status: str = "", q: str = ""):
    conn = get_conn()

    where = []
    params = []

    if location.strip():
        where.append("location = ?")
        params.append(location.strip())
    if status.strip():
        where.append("status = ?")
        params.append(status.strip())
    if q.strip():
        where.append("(name LIKE ? OR purpose LIKE ?)")
        params.extend([f"%{q.strip()}%", f"%{q.strip()}%"])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = conn.execute(f"""
        SELECT id, name, purpose, location, status
        FROM tools
        {where_sql}
        ORDER BY location ASC, id ASC
    """, params).fetchall()

    conn.close()

    lines = ""
    for r in rows:
        lines += f"""
        <div class="box">
          <div style="font-weight:900;">{esc(r['location'])} · {esc(r['name'])} <span class="muted">#{r['id']}</span></div>
          <div class="meta">용도: {esc(r['purpose'])}</div>
          <div class="meta">상태: <b>{esc(r['status'])}</b></div>
        </div>
        """

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button class="btn" onclick="window.print()">🖨️ 인쇄</button>
        <a href="/tools/list?location={esc(location)}&status={esc(status)}&q={esc(q)}" style="text-decoration:none;">
          <button class="btn2" type="button">← 리스트로</button>
        </a>
      </div>
      <div class="small">출력은 보고용이 아니라 통제용입니다.</div>
    </div>

    <div class="box">
      <div style="font-weight:900; font-size:16px;">재고 출력 ({len(rows)}건)</div>
      <div class="muted">조건: {esc(location) or "전체"} · {esc(status) or "전체"} · {esc(q) or "없음"}</div>
    </div>

    {lines if lines else "<div class='box muted'>출력할 항목이 없습니다.</div>"}
    """
    return HTMLResponse(layout(body))


@app.get("/tools.csv")
def tools_csv():
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, name, purpose, location, status, created_at
        FROM tools
        ORDER BY location ASC, id ASC
    """).fetchall()
    conn.close()

    out = ["\ufeffid,name,purpose,location,status,created_at"]
    for r in rows:
        def q(s):  # CSV escape
            s = (s or "")
            return '"' + s.replace('"', '""') + '"'
        out.append(",".join([
            str(r["id"]),
            q(r["name"]),
            q(r["purpose"]),
            q(r["location"]),
            q(r["status"]),
            q(r["created_at"]),
        ]))

    data = "\n".join(out).encode("utf-8")
    return Response(content=data, media_type="text/csv; charset=utf-8")