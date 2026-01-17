import os, uuid, html, json
from pathlib import Path
from urllib.parse import urlencode, quote

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from PIL import Image

from db import init_db, get_conn

from typing import Optional

BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

app = FastAPI()
init_db()

app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")

# =========================================================
# ✅ 분류 트리(대/중/소) - 여기만 편집하면 현장에 맞게 확장됩니다.
# =========================================================
CATEGORY_TREE = {
    "전기": {
        "측정/시험": ["클램프미터", "절연저항계", "멀티미터", "검전기"],
        "배선/단자": ["드라이버", "압착기", "스트리퍼", "단자/슬리브"],
        "조명": ["램프교체", "안정기", "스위치/콘센트"],
    },
    "기계": {
        "배관": ["몽키", "파이프렌치", "테프론", "컷터"],
        "펌프": ["베어링툴", "그리스건", "정렬공구"],
        "공구": ["임팩", "드릴", "해머드릴"],
    },
    "소방": {
        "수신기/감지": ["감지기테스터", "회로시험기"],
        "스프링클러": ["헤드교체", "밸브조작"],
        "소화기/가스": ["압력게이지", "충전장비"],
    },
    "건축": {
        "마감": ["헤라", "실리콘건", "커터"],
        "철물": ["망치", "수평계", "줄자"],
    },
    "기타": {
        "공용": ["사다리", "연장선", "작업등"],
    }
}

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

def _cat_options(level_list, selected: str, allow_empty=True, empty_label="전체"):
    ops = []
    if allow_empty:
        ops.append(f"<option value='' {'selected' if selected=='' else ''}>{esc(empty_label)}</option>")
    for v in level_list:
        ops.append(f"<option value='{esc(v)}' {'selected' if v==selected else ''}>{esc(v)}</option>")
    return "".join(ops)

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
  button { border: none; padding: 12px; font-weight: 700; cursor: pointer; }
  .btn { background: #111; color: #fff; }
  .btn2 { background: #f2f2f2; color:#111; }
  .row { display:grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  * { box-sizing: border-box; }
  input, select, textarea, button { max-width: 100%; }
  .row > div { min-width: 0; }
  .row input, .row select, .row textarea { min-width: 0; width: 100%; }

  @media (max-width: 520px) {
    .wrap { max-width: 100%; }
    .row { grid-template-columns: 1fr; }
    .actions { flex-direction: column; }
    .card { grid-template-columns: 1fr; }
    .thumb { width: 100%; height: auto; }
  }

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
  @page { size: A4; margin: 10mm; }
  .print-title { font-size: 14pt; font-weight: 900; margin: 0 0 6mm 0; }
  .print-sub { font-size: 10pt; color: #333; margin: 0 0 4mm 0; }
  .print-table { width:100%; border-collapse: collapse; font-size: 11pt; }
  .print-table th, .print-table td { border:1px solid #333; padding:4px 6px; vertical-align: top; }
  .print-table th { background:#f2f2f2; }
</style>

<script>
  // 배포용 공통 유틸: 인쇄 버튼이 "반응"하게 만드는 핵심
  function __printSuggest(pdfUrl, csvUrl) {
    try {
      // 1) PDF를 새 탭으로 열기(보고/공유/출력용)
      // 팝업차단이 걸리면 그냥 인쇄로 fallback
      var w = window.open(pdfUrl, "_blank");
      if (!w) {
        // 팝업이 막히면 바로 브라우저 인쇄
        window.print();
        return;
      }

      // 2) 사용자가 원하면 CSV도 함께 내려받게 유도(운영/엑셀용)
      // 너무 시끄럽지 않게 confirm 한 번만
      setTimeout(function () {
        if (confirm("CSV도 함께 다운로드할까요? (엑셀/백업용)")) {
          window.open(csvUrl, "_blank");
        }
      }, 250);
    } catch (e) {
      window.print();
    }
  }
</script>

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

# =========================================================
# ✅ 분류 드롭다운(대/중/소) - JS로 연동
# =========================================================
def category_select_block(prefix: str, sel_l="", sel_m="", sel_s="", allow_empty=True, empty_label="전체"):
    # prefix: "reg" 또는 "s" (등록/검색 구분용)
    cat_json = json.dumps(CATEGORY_TREE, ensure_ascii=False)

    l_list = list(CATEGORY_TREE.keys())
    # 초기 중/소 목록
    m_list = list(CATEGORY_TREE.get(sel_l, {}).keys()) if sel_l in CATEGORY_TREE else []
    s_list = CATEGORY_TREE.get(sel_l, {}).get(sel_m, []) if sel_l and sel_m else []

    l_ops = _cat_options(l_list, sel_l, allow_empty=allow_empty, empty_label=empty_label)
    m_ops = _cat_options(m_list, sel_m, allow_empty=allow_empty, empty_label=empty_label)
    s_ops = _cat_options(s_list, sel_s, allow_empty=allow_empty, empty_label=empty_label)

    # 등록폼은 “선택 필수”로 쓰고 싶으면 allow_empty=False로 호출하면 됩니다.
    block = f"""
    <div class="row">
      <div>
        <label>분류(대)</label>
        <select id="{prefix}_cat_l" name="cat_l">{l_ops}</select>
      </div>
      <div>
        <label>분류(중)</label>
        <select id="{prefix}_cat_m" name="cat_m">{m_ops}</select>
      </div>
    </div>
    <div class="row">
      <div>
        <label>분류(소)</label>
        <select id="{prefix}_cat_s" name="cat_s">{s_ops}</select>
      </div>
      <div>
        <label>&nbsp;</label>
        <div class="muted" style="padding:10px 0 0 0;">대→중→소 순으로 자동 갱신됩니다.</div>
      </div>
    </div>

<script>
(function(){{
  const TREE = {cat_json};

  const elL = document.getElementById("{prefix}_cat_l");
  const elM = document.getElementById("{prefix}_cat_m");
  const elS = document.getElementById("{prefix}_cat_s");

  function setOptions(select, items, selected, allowEmpty, emptyLabel){{
    let html = "";
    if(allowEmpty){{
      html += `<option value="">${{emptyLabel}}</option>`;
    }}
    for(const v of items){{
      const sel = (v === selected) ? "selected" : "";
      html += `<option value="${{v}}" ${{sel}}>${{v}}</option>`;
    }}
    select.innerHTML = html;
  }}

  function refreshM(preserve=false){{
    const l = elL.value || "";
    const mKeys = l && TREE[l] ? Object.keys(TREE[l]) : [];
    const currentM = preserve ? (elM.value || "") : "";
    setOptions(elM, mKeys, currentM, {str(allow_empty).lower()}, "{empty_label}");
    refreshS(preserve);
  }}

  function refreshS(preserve=false){{
    const l = elL.value || "";
    const m = elM.value || "";
    const sList = (l && m && TREE[l] && TREE[l][m]) ? TREE[l][m] : [];
    const currentS = preserve ? (elS.value || "") : "";
    setOptions(elS, sList, currentS, {str(allow_empty).lower()}, "{empty_label}");
  }}

  elL.addEventListener("change", function(){{ refreshM(false); }});
  elM.addEventListener("change", function(){{ refreshS(false); }});

  // 초기 1회 정리(현재 선택값이 있을 때도 유지)
  refreshM(true);
}})();
</script>
    """
    return block

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

    # 등록: 분류는 “필수”로 하고 싶으면 allow_empty=False로 바꾸세요.
    reg_cat = category_select_block("reg", sel_l="", sel_m="", sel_s="", allow_empty=True, empty_label="선택")

    # 검색: 전체 허용
    search_cat = category_select_block("s", sel_l="", sel_m="", sel_s="", allow_empty=True, empty_label="전체")

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <a href="/dashboard" style="text-decoration:none;"><button class="btn2" type="button">📊 재고 대시보드</button></a>
        <a href="/tools/list" style="text-decoration:none;"><button class="btn2" type="button">📋 전체 리스트</button></a>
        <a href="/print/a4" style="text-decoration:none;"><button class="btn" type="button">🖨️ A4 전체 출력</button></a>
      </div>
    </div>

    <div class="box">
      <div class="muted">사진 한 장이 공구의 이력서가 됩니다. 등록 → 검색 → 확정(데이터 누적).</div>
    </div>

    <div class="box">
      <h2 style="margin:0 0 10px 0; font-size:16px;">1) 공구 등록</h2>
      <form action="/tools" method="post" enctype="multipart/form-data">
        <label>공구명</label>
        <input name="name" placeholder="예) 절연드라이버_1000V" required />

        <label>용도</label>
        <input name="purpose" placeholder="예) 분전반 단자 체결용" required />

        {reg_cat}

        <div class="row">
          <div>
            <label>보유수량</label>
            <input name="qty" type="number" min="1" value="1" required />
          </div>
          <div>
            <label>구입금액(원)</label>
            <input name="purchase_amount" type="number" min="0" value="0" />
          </div>
        </div>

        <label>보관위치</label>
        <input name="location" value="전기실" required />

        <label>상태</label>
        <select name="status">
          <option>정상</option>
          <option>고장(수리)</option>
          <option>폐기</option>
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
      <h2 style="margin:0 0 10px 0; font-size:16px;">2) 사진 + 조건 검색(통합)</h2>

      <form action="/search" method="post" enctype="multipart/form-data">
        <label>현장 사진(선택)</label>
        <input type="file" name="file" accept="image/*" capture="environment" />

        <div class="row">
          <div>
            <label>TopK</label>
            <input name="topk" value="5" />
          </div>
          <div>
            <label>모드</label>
            <select name="mode">
              <option value="strict">strict (조건 필터)</option>
              <option value="soft">soft (조건 가점)</option>
            </select>
          </div>
        </div>

        <div class="row">
          <div>
            <label>공구명(키워드)</label>
            <input name="name" placeholder="예) 절연드라이버 / 임팩 / 니퍼" />
          </div>
          <div>
            <label>보관위치</label>
            <input name="location" placeholder="예) 전기실 / 기계실 / 창고A" />
          </div>
        </div>

        {search_cat}

        <div class="row">
          <div>
            <label>상태</label>
            <select name="status">
              <option value="">전체</option>
              <option value="정상">정상</option>
              <option value="고장(수리)">고장(수리)</option>
              <option value="폐기">폐기</option>
              <option value="분실">분실</option>
            </select>
          </div>
          <div>
            <label>최소 수량(선택)</label>
            <input name="min_qty" type="number" min="0" placeholder="예) 1" />
          </div>
        </div>

        <div class="row">
          <div>
            <label>최대 구입금액(원, 선택)</label>
            <input name="max_amt" type="number" min="0" placeholder="예) 50000" />
          </div>
          <div>
            <label>&nbsp;</label>
            <div class="muted" style="padding:10px 0 0 0;">사진 없이도 검색됩니다.</div>
          </div>
        </div>

        <div class="hr"></div>
        <button class="btn" type="submit">검색</button>
      </form>

      <div class="small">
        strict: 조건 교집합만 / soft: 조건은 가점으로만 반영합니다.
      </div>
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
    file: UploadFile = File(...),
    qty: int = Form(1),
    purchase_amount: int = Form(0),

    # ✅ 분류
    cat_l: str = Form(""),
    cat_m: str = Form(""),
    cat_s: str = Form(""),
):
    img_path = save_upload(file)
    ah = calc_ahash(img_path)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO tools(name, purpose, location, status, qty, purchase_amount, cat_l, cat_m, cat_s)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (name, purpose, location, status, qty, purchase_amount, cat_l.strip(), cat_m.strip(), cat_s.strip())
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
def search(
    file: Optional[UploadFile] = File(None),
    topk: str = Form("5"),
    location: str = Form(""),
    name: str = Form(""),
    status: str = Form(""),
    min_qty: str = Form(""),
    max_amt: str = Form(""),
    mode: str = Form("strict"),

    # ✅ 분류(검색)
    cat_l: str = Form(""),
    cat_m: str = Form(""),
    cat_s: str = Form(""),
):
    name_s = (name or "").strip()
    loc_s = (location or "").strip()
    status_s = (status or "").strip()

    cat_l_s = (cat_l or "").strip()
    cat_m_s = (cat_m or "").strip()
    cat_s_s = (cat_s or "").strip()

    try:
        k = max(1, min(20, int(topk)))
    except:
        k = 5

    try:
        min_qty_i = int(min_qty) if str(min_qty).strip() != "" else None
    except:
        min_qty_i = None

    try:
        max_amt_i = int(max_amt) if str(max_amt).strip() != "" else None
    except:
        max_amt_i = None

    def match_filters(r) -> bool:
        if name_s and name_s.lower() not in (r["name"] or "").lower():
            return False
        if loc_s and (r["location"] or "") != loc_s:
            return False
        if status_s and (r["status"] or "") != status_s:
            return False

        # ✅ 분류 필터
        if cat_l_s and (r["cat_l"] or "") != cat_l_s:
            return False
        if cat_m_s and (r["cat_m"] or "") != cat_m_s:
            return False
        if cat_s_s and (r["cat_s"] or "") != cat_s_s:
            return False

        if min_qty_i is not None and int(r["qty"] or 0) < min_qty_i:
            return False
        if max_amt_i is not None and int(r["purchase_amount"] or 0) > max_amt_i:
            return False
        return True

    def soft_bonus(r) -> int:
        bonus = 0
        if name_s and name_s.lower() in (r["name"] or "").lower():
            bonus += 2
        if loc_s and (r["location"] or "") == loc_s:
            bonus += 2
        if status_s and (r["status"] or "") == status_s:
            bonus += 2

        # ✅ 분류 가점(soft 모드)
        if cat_l_s and (r["cat_l"] or "") == cat_l_s:
            bonus += 2
        if cat_m_s and (r["cat_m"] or "") == cat_m_s:
            bonus += 2
        if cat_s_s and (r["cat_s"] or "") == cat_s_s:
            bonus += 2

        if min_qty_i is not None and int(r["qty"] or 0) >= min_qty_i:
            bonus += 1
        if max_amt_i is not None and int(r["purchase_amount"] or 0) <= max_amt_i:
            bonus += 1
        return bonus

    # ---------- 사진 존재 여부 ----------
    has_image = False
    q_path = None
    q_hash = None

    if file is not None and getattr(file, "filename", None):
        if str(file.filename).strip() != "":
            has_image = True
            q_path = save_upload(file)
            q_hash = calc_ahash(q_path)

    # =========================================================
    # 1) 사진이 없으면: 조건만 검색 (tools)
    # =========================================================
    if not has_image:
        conn = get_conn()
        rows = conn.execute("""
    SELECT
      t.id, t.name, t.purpose, t.location, t.status, t.qty, t.purchase_amount,
      t.cat_l, t.cat_m, t.cat_s, t.created_at,
      (
        SELECT ti.image_path
        FROM tool_images ti
        WHERE ti.tool_id = t.id
        ORDER BY ti.id DESC
        LIMIT 1
      ) AS ref_image_path
    FROM tools t
    ORDER BY t.id DESC
    LIMIT 800
""").fetchall()
        conn.close()

        filtered = [r for r in rows if match_filters(r)]
        filtered = filtered[:250]

        items = ""
        for r in filtered:
            cat_str = " / ".join([x for x in [r["cat_l"], r["cat_m"], r["cat_s"]] if (x or "").strip() != ""]) or "-"

            # ✅ ref_image_path -> 파일명 추출
            ref_path = (r["ref_image_path"] or "").strip()
            ref_file = os.path.basename(ref_path) if ref_path else ""

            # ✅ 썸네일(없으면 no img)
            if ref_file:
                thumb_html = f"""
                <a href="/tools/edit/{r['id']}" style="text-decoration:none;">
                  <img class="thumb" src="/uploads/{esc(ref_file)}" alt="ref"/>
                </a>
                """
            else:
                thumb_html = f"""
                <a href="/tools/edit/{r['id']}" style="text-decoration:none;">
                  <div class="thumb" style="display:flex;align-items:center;justify-content:center;color:#999;">no img</div>
                </a>
                """

            items += f"""
            <div class="box">
              <div style="display:flex; justify-content:space-between; gap:10px; align-items:flex-start;">
                <div style="display:flex; gap:12px; align-items:flex-start;">
                  {thumb_html}
                  <div>
                    <div style="font-weight:900; font-size:16px;">{esc(r['name'])} <span class="muted">#{r['id']}</span></div>
                    <div class="meta">분류: <b>{esc(cat_str)}</b></div>
                    <div class="meta">용도: {esc(r['purpose'])}</div>
                    <div class="meta">위치: {esc(r['location'])} · 상태: <b>{esc(r['status'])}</b></div>
                    <div class="meta">수량: <b>{r['qty']}</b> · 구입금액: <b>{int(r['purchase_amount'] or 0):,}원</b></div>
                    <div class="muted">등록: {esc(r['created_at'])}</div>
                  </div>
                </div>

                <div class="noprint" style="min-width:130px;">
                  <form action="/events" method="post">
                    <input type="hidden" name="tool_id" value="{r['id']}"/>
                    <input type="hidden" name="event_type" value="반출"/>
                    <input type="hidden" name="person" value=""/>
                    <input type="hidden" name="note" value="조건검색에서 반출"/>
                    <button class="btn2" type="submit">📦 반출</button>
                  </form>
                </div>
              </div>
            </div>
            """

        cond = (
            f"공구명={esc(name_s) or '전체'} · 위치={esc(loc_s) or '전체'} · 상태={esc(status_s) or '전체'} · "
            f"분류={esc(cat_l_s) or '전체'}/{esc(cat_m_s) or '전체'}/{esc(cat_s_s) or '전체'} · "
            f"최소수량={esc(str(min_qty_i)) if min_qty_i is not None else '없음'} · "
            f"최대금액={esc(str(max_amt_i)) if max_amt_i is not None else '없음'}"
        )

        body = f"""
        <div class="box noprint">
          <a href="/" style="text-decoration:none;"><button class="btn2" type="button">← 홈</button></a>
          <div class="small">조건만 검색 · {cond}</div>
        </div>

        <div class="box">
          <div style="font-weight:900; font-size:16px;">조건 검색 결과 ({len(filtered)}건)</div>
          <div class="muted">사진이 없으므로 쿼리 이미지 영역을 숨겼습니다.</div>
        </div>

        {items if items else "<div class='box muted'>조건에 해당하는 공구가 없습니다.</div>"}
        """
        return HTMLResponse(layout(body))

    # =========================================================
    # 2) 사진이 있으면: 사진 + 조건 (tool_images JOIN)
    # =========================================================
    if q_hash is None:
        return RedirectResponse(url="/", status_code=303)

    conn = get_conn()
    rows = conn.execute("""
        SELECT ti.tool_id, ti.image_path, ti.ahash,
               t.name, t.purpose, t.location, t.status, t.qty, t.purchase_amount,
               t.cat_l, t.cat_m, t.cat_s
        FROM tool_images ti
        JOIN tools t ON t.id = ti.tool_id
    """).fetchall()
    conn.close()

    scored = []
    for r in rows:
        dist = hamming_hex(q_hash, r["ahash"])

        if mode == "strict":
            if not match_filters(r):
                continue
            adj = dist
        else:
            adj = dist - soft_bonus(r)

        scored.append({
            "tool_id": r["tool_id"],
            "name": r["name"],
            "purpose": r["purpose"],
            "location": r["location"],
            "status": r["status"],
            "qty": r["qty"],
            "purchase_amount": r["purchase_amount"],
            "cat_l": r["cat_l"],
            "cat_m": r["cat_m"],
            "cat_s": r["cat_s"],
            "ref_image": os.path.basename(r["image_path"]),
            "hamming": dist,
            "adj": adj,
        })

    scored.sort(key=lambda x: (x["adj"], x["hamming"]))
    hits = scored[:k]

    cards = ""
    for h in hits:
        cat_str = " / ".join([x for x in [h["cat_l"], h["cat_m"], h["cat_s"]] if (x or "").strip() != ""]) or "-"
        cards += f"""
        <div class="card">
          <img class="thumb" src="/uploads/{esc(h['ref_image'])}" alt="ref"/>
          <div>
            <div class="title">{esc(h['name'])} <span class="muted">#{h['tool_id']}</span></div>
            <div class="meta">분류: <b>{esc(cat_str)}</b></div>
            <div class="meta">용도: {esc(h['purpose'])}</div>
            <div class="meta">위치: {esc(h['location'])} · 상태: {esc(h['status'])}</div>
            <div class="meta">수량: <b>{h['qty']}</b> · 구입금액: <b>{int(h['purchase_amount'] or 0):,}원</b></div>
            <div class="score">유사도: <b>{h['hamming']}</b> · 보정: <b>{h['adj']}</b></div>

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
                <input type="hidden" name="note" value="혼합검색 화면에서 반출 기록"/>
                <button class="btn2" type="submit">📦 반출 기록</button>
              </form>
            </div>
          </div>
        </div>
        """

    cond = (
        f"공구명={esc(name_s) or '전체'} · 위치={esc(loc_s) or '전체'} · 상태={esc(status_s) or '전체'} · "
        f"분류={esc(cat_l_s) or '전체'}/{esc(cat_m_s) or '전체'}/{esc(cat_s_s) or '전체'} · "
        f"최소수량={esc(str(min_qty_i)) if min_qty_i is not None else '없음'} · "
        f"최대금액={esc(str(max_amt_i)) if max_amt_i is not None else '없음'} · 모드={esc(mode)}"
    )

    body = f"""
    <div class="box">
      <h2 style="margin:0 0 10px 0; font-size:16px;">혼합 검색 결과(사진 + 조건)</h2>
      <div class="muted">조건: {cond}</div>

      <div class="hr"></div>
      <div class="muted">쿼리 이미지</div>
      <img class="thumb" src="/uploads/{esc(q_path.name)}" style="width:140px;height:140px;" alt="query"/>

      <div class="cards">{cards if cards else "<div class='muted'>후보가 없습니다. (mode를 soft로 바꾸거나 조건을 완화하세요)</div>"}</div>

      <div class="hr"></div>
      <a href="/" style="text-decoration:none;"><button class="btn2" type="button">← 홈으로</button></a>
    </div>
    """
    return HTMLResponse(layout(body))

@app.post("/feedback")
def feedback(tool_id: int = Form(...), query_image: str = Form(...)):
    q_path = UPLOAD_DIR / query_image
    if not q_path.exists():
        return RedirectResponse(url="/", status_code=303)

    ah = calc_ahash(q_path)
    conn = get_conn()
    conn.execute(
        "INSERT INTO tool_images(tool_id, image_path, ahash) VALUES (?, ?, ?)",
        (tool_id, str(q_path), ah)
    )
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
# 대시보드 / 리스트 / 출력 / CSV  (분류 컬럼 반영)
# -----------------------------

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    conn = get_conn()

    totals = conn.execute("""
        SELECT COUNT(*) AS items,
               COALESCE(SUM(qty),0) AS qty,
               COALESCE(SUM(purchase_amount),0) AS amt
        FROM tools
    """).fetchone()

    by_status = conn.execute("""
        SELECT status, COUNT(*) AS items, COALESCE(SUM(qty),0) AS qty, COALESCE(SUM(purchase_amount),0) AS amt
        FROM tools
        GROUP BY status
        ORDER BY qty DESC, items DESC
    """).fetchall()

    by_location = conn.execute("""
        SELECT location, COUNT(*) AS items, COALESCE(SUM(qty),0) AS qty, COALESCE(SUM(purchase_amount),0) AS amt
        FROM tools
        GROUP BY location
        ORDER BY qty DESC, location ASC
    """).fetchall()

    # ✅ 분류(대) 현황 한 번 보여주면, “체계”가 생깁니다.
    by_cat_l = conn.execute("""
        SELECT cat_l, COUNT(*) AS items, COALESCE(SUM(qty),0) AS qty
        FROM tools
        GROUP BY cat_l
        ORDER BY qty DESC, items DESC
    """).fetchall()
# ✅ 미분류(대/중/소 모두 공란) 카운트
    unc = conn.execute("""
        SELECT COUNT(*) AS cnt
        FROM tools
        WHERE TRIM(cat_l)='' AND TRIM(cat_m)='' AND TRIM(cat_s)=''
    """).fetchone()
    unc_cnt = int(unc["cnt"] or 0)
    conn.close()

    status_badges = ""
    for s in by_status:
        status_badges += (
            "<div class='box' style='margin-top:10px;'>"
            f"<b>{esc(s['status'])}</b> : 품목 {s['items']} · 수량 {s['qty']} · 금액 {int(s['amt']):,}원"
            "</div>"
        )

    cat_badges = ""
    for c in by_cat_l:
        label = (c["cat_l"] or "").strip() or "(미분류)"
        cat_badges += (
            "<div class='box' style='margin-top:10px;'>"
            f"<b>{esc(label)}</b> : 품목 {c['items']} · 수량 {c['qty']}"
            "</div>"
        )

    cards = ""
    warn_html = ""
    if unc_cnt > 0:
        warn_html = f"""
        <div class="box" style="border:2px solid #111;">
          <div style="font-weight:900;">⚠️ 미분류 {unc_cnt}건</div>
          <div class="muted" style="margin-top:6px;">대/중/소가 모두 비어 있습니다. 출력/검색 누락이 생깁니다. 오늘 정리하세요.</div>
          <div class="muted" style="margin-top:6px;">
            <a href="/tools/list?unclassified=1">미분류만 보기 →</a>
            &nbsp;|&nbsp;
            <a href="/print/a4/category" style="text-decoration:none;"><button class="btn2" type="button">🗂️ 분류별 A4 출력</button></a>
          </div>
        </div>
        """
    for loc in by_location:
        l = loc["location"]
        cards += f"""
        <div class="box">
          <div style="display:flex; justify-content:space-between; gap:10px; align-items:center;">
            <div>
              <div style="font-weight:900; font-size:16px;">{esc(l)}</div>
              <div class="muted" style="margin-top:4px;">품목 {loc['items']} · 수량 {loc['qty']} · {int(loc['amt'] or 0):,}원</div>
            </div>
            <div style="text-align:right;">
              <div class="muted">
                <a href="/tools/list?{urlencode({'location': l})}">리스트 보기 →</a>
                &nbsp;|&nbsp;
                <a href="/print/a4/location/{quote(l)}">A4 출력 →</a>
              </div>
            </div>
          </div>
        </div>
        """

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <a href="/" style="text-decoration:none;"><button class="btn2" type="button">← 홈</button></a>
        <a href="/tools/list" style="text-decoration:none;"><button class="btn2" type="button">전체 리스트</button></a>
        <a href="/print/a4" style="text-decoration:none;"><button class="btn" type="button">🖨️ A4 전체 출력</button></a>
        <a href="/tools.csv" style="text-decoration:none;"><button class="btn2" type="button">CSV</button></a>
      </div>
    </div>
    {warn_html}
    <div class="box">
      <div style="font-size:16px; font-weight:900;">
        전체: 품목 {totals['items']} · 수량 {totals['qty']} · 금액 {int(totals['amt']):,}원
      </div>
      <div class="muted" style="margin-top:6px;">상태별</div>
      {status_badges if status_badges else "<div class='muted'>데이터 없음</div>"}
    </div>

    <div class="box">
      <div style="font-size:16px; font-weight:900;">분류(대) 현황</div>
      <div class="muted" style="margin-top:6px;">미분류가 많으면 운영이 흔들립니다. 초기에 잡아두는 게 이깁니다.</div>
      {cat_badges if cat_badges else "<div class='muted'>데이터 없음</div>"}
    </div>

    <div class="box">
      <div style="font-size:16px; font-weight:900;">위치별 재고</div>
      <div class="muted" style="margin-top:6px;">리스트/출력으로 내려가 관리하세요.</div>
    </div>
    {cards if cards else "<div class='box muted'>등록된 공구가 없습니다.</div>"}
    """
    return HTMLResponse(layout(body))


@app.get("/tools/list", response_class=HTMLResponse)
def tools_list(
    location: str = "", status: str = "", q: str = "",
    cat_l: str = "", cat_m: str = "", cat_s: str = "",
    unclassified: str = ""
):
    conn = get_conn()

    where = []
    params = []

    if location.strip():
        where.append("t.location = ?")
        params.append(location.strip())
    if status.strip():
        where.append("t.status = ?")
        params.append(status.strip())
    if q.strip():
        where.append("(t.name LIKE ? OR t.purpose LIKE ?)")
        params.extend([f"%{q.strip()}%", f"%{q.strip()}%"])

    # ✅ 분류 필터
    if cat_l.strip():
        where.append("t.cat_l = ?")
        params.append(cat_l.strip())
    if cat_m.strip():
        where.append("t.cat_m = ?")
        params.append(cat_m.strip())
    if cat_s.strip():
        where.append("t.cat_s = ?")
        params.append(cat_s.strip())

    # ✅ 미분류만 보기
    if str(unclassified).strip() == "1":
        where.append("TRIM(t.cat_l)='' AND TRIM(t.cat_m)='' AND TRIM(t.cat_s)=''")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = conn.execute(f"""
        SELECT
          t.id, t.name, t.purpose, t.location, t.status, t.qty, t.purchase_amount,
          t.cat_l, t.cat_m, t.cat_s, t.created_at,
          (
            SELECT ti.image_path
            FROM tool_images ti
            WHERE ti.tool_id = t.id
            ORDER BY ti.id DESC
            LIMIT 1
          ) AS ref_image_path
        FROM tools t
        {where_sql}
        ORDER BY t.location ASC, t.id DESC
    """, params).fetchall()

    locs = conn.execute("SELECT DISTINCT location FROM tools ORDER BY location ASC").fetchall()
    conn.close()

    loc_options = "<option value=''>전체</option>" + "".join(
        f"<option value='{esc(r['location'])}' {'selected' if r['location']==location else ''}>{esc(r['location'])}</option>"
        for r in locs
    )

    status_list = ["", "정상", "고장(수리)", "폐기", "분실"]
    status_options = "".join(
        f"<option value='{esc(s)}' {'selected' if s==status else ''}>{esc(s) if s else '전체'}</option>"
        for s in status_list
    )

    # 리스트 페이지에서도 분류 드롭다운 제공(필터 UX 고정)
    list_cat = category_select_block("list", sel_l=cat_l, sel_m=cat_m, sel_s=cat_s, allow_empty=True, empty_label="전체")

    # ✅ 리스트 아이템 렌더링(썸네일 + 수정 링크)
    items = ""
    for r in rows:
        cat_str = " / ".join([x for x in [r["cat_l"], r["cat_m"], r["cat_s"]] if (x or "").strip() != ""]) or "-"

        ref_path = (r["ref_image_path"] or "").strip()
        ref_file = os.path.basename(ref_path) if ref_path else ""

        if ref_file:
            thumb_html = f"""
            <a href="/tools/edit/{r['id']}" style="text-decoration:none;">
              <img class="thumb" src="/uploads/{esc(ref_file)}" alt="ref"/>
            </a>
            """
        else:
            thumb_html = f"""
            <a href="/tools/edit/{r['id']}" style="text-decoration:none;">
              <div class="thumb" style="display:flex;align-items:center;justify-content:center;color:#999;">no img</div>
            </a>
            """

        items += f"""
        <div class="box">
          <div style="display:flex; justify-content:space-between; gap:10px; align-items:flex-start;">
            <div style="display:flex; gap:12px; align-items:flex-start;">
              {thumb_html}
              <div>
                <div style="font-weight:900; font-size:16px;">{esc(r['name'])} <span class="muted">#{r['id']}</span></div>
                <div class="meta">분류: <b>{esc(cat_str)}</b></div>
                <div class="meta">용도: {esc(r['purpose'])}</div>
                <div class="meta">위치: {esc(r['location'])} · 상태: <b>{esc(r['status'])}</b></div>
                <div class="muted">등록: {esc(r['created_at'])}</div>
                <div class="meta">수량: <b>{r['qty']}</b> · 구입금액: <b>{int(r['purchase_amount'] or 0):,}원</b></div>
              </div>
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

              <div style="height:8px;"></div>

              <a href="/tools/edit/{r['id']}" style="text-decoration:none;">
                <button class="btn2" type="button">✏️ 수정</button>
              </a>

              <div style="height:8px;"></div>

              <form action="/tools/delete/{r['id']}" method="post" onsubmit="return confirm('정말 삭제할까요? (이미지/이력 포함)');">
                <button class="btn2" type="submit">🗑️ 삭제</button>
              </form>
            </div>
          </div>
        </div>
        """

    qs = urlencode({
        "location": location, "status": status, "q": q,
        "cat_l": cat_l, "cat_m": cat_m, "cat_s": cat_s,
        "unclassified": unclassified
    })
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

        {list_cat}

        <label>검색어(공구명/용도)</label>
        <input name="q" value="{esc(q)}" placeholder="예) 절연 / 임팩 / 배관" />

        <input type="hidden" name="unclassified" value="{esc(unclassified)}"/>

        <div class="hr"></div>
        <button class="btn" type="submit">필터 적용</button>
      </form>
    </div>

    <div class="box">
      <div style="font-weight:900; font-size:16px;">리스트 ({len(rows)}건)</div>
      <div class="muted">
        필터: {esc(location) or "전체"} · {esc(status) or "전체"} ·
        분류 {esc(cat_l) or "전체"}/{esc(cat_m) or "전체"}/{esc(cat_s) or "전체"} ·
        검색어 {esc(q) or "없음"} ·
        {("미분류만" if str(unclassified)=="1" else "전체")}
      </div>
    </div>

    {items if items else "<div class='box muted'>조건에 해당하는 공구가 없습니다.</div>"}
    """
    return HTMLResponse(layout(body))


@app.get("/tools/print", response_class=HTMLResponse)
def tools_print(location: str = "", status: str = "", q: str = "", cat_l: str = "", cat_m: str = "", cat_s: str = ""):
    conn = get_conn()

    where = []
    params = []

    if location.strip():
        where.append("location = ?"); params.append(location.strip())
    if status.strip():
        where.append("status = ?"); params.append(status.strip())
    if q.strip():
        where.append("(name LIKE ? OR purpose LIKE ?)")
        params.extend([f"%{q.strip()}%", f"%{q.strip()}%"])

    if cat_l.strip():
        where.append("cat_l = ?"); params.append(cat_l.strip())
    if cat_m.strip():
        where.append("cat_m = ?"); params.append(cat_m.strip())
    if cat_s.strip():
        where.append("cat_s = ?"); params.append(cat_s.strip())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = conn.execute(f"""
        SELECT id, name, purpose, location, status, cat_l, cat_m, cat_s
        FROM tools
        {where_sql}
        ORDER BY location ASC, id ASC
    """, params).fetchall()

    conn.close()

    lines = ""
    for r in rows:
        cat_str = " / ".join([x for x in [r["cat_l"], r["cat_m"], r["cat_s"]] if (x or "").strip() != ""]) or "-"
        lines += f"""
        <div class="box">
          <div style="font-weight:900;">{esc(r['location'])} · {esc(r['name'])} <span class="muted">#{r['id']}</span></div>
          <div class="meta">분류: <b>{esc(cat_str)}</b></div>
          <div class="meta">용도: {esc(r['purpose'])}</div>
          <div class="meta">상태: <b>{esc(r['status'])}</b></div>
        </div>
        """

    back_qs = urlencode({"location": location, "status": status, "q": q, "cat_l": cat_l, "cat_m": cat_m, "cat_s": cat_s})
# tools_print() 안에서 back_qs 만들어둔 것 그대로 활용 가능
    pdf_url = "/tools.pdf" + (f"?{back_qs}" if back_qs else "")
    csv_url = "/tools.csv" + (f"?{back_qs}" if back_qs else "")

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button class="btn"
  onclick="__printSuggest('{pdf_url}', '{csv_url}')">🖨️ 인쇄</button>
        <a href="/tools/list?{back_qs}" style="text-decoration:none;">
          <button class="btn2" type="button">← 리스트로</button>
        </a>
      </div>
      <div class="small">출력은 보고용이 아니라 통제용입니다.</div>
    </div>

    <div class="box">
      <div style="font-weight:900; font-size:16px;">재고 출력 ({len(rows)}건)</div>
      <div class="muted">조건: {esc(location) or "전체"} · {esc(status) or "전체"} · 분류 {esc(cat_l) or "전체"}/{esc(cat_m) or "전체"}/{esc(cat_s) or "전체"} · {esc(q) or "없음"}</div>
    </div>

    {lines if lines else "<div class='box muted'>출력할 항목이 없습니다.</div>"}
    """
    return HTMLResponse(layout(body))


@app.get("/tools.csv")
def tools_csv(
    location: str = "", status: str = "", q: str = "",
    cat_l: str = "", cat_m: str = "", cat_s: str = "",
    unclassified: str = ""
):
    rows = fetch_tools_for_export(location, status, q, cat_l, cat_m, cat_s, unclassified)

    out = ["\ufeffid,name,purpose,location,status,qty,purchase_amount,cat_l,cat_m,cat_s,created_at"]

    def qcsv(s):
        s = (s or "")
        return '"' + s.replace('"', '""') + '"'

    for r in rows:
        out.append(",".join([
            str(r["id"]),
            qcsv(r["name"]),
            qcsv(r["purpose"]),
            qcsv(r["location"]),
            qcsv(r["status"]),
            str(r["qty"]),
            str(r["purchase_amount"]),
            qcsv(r["cat_l"]),
            qcsv(r["cat_m"]),
            qcsv(r["cat_s"]),
            qcsv(r["created_at"]),
        ]))

    data = "\n".join(out).encode("utf-8")
    return Response(content=data, media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": "attachment; filename=tools.csv"})


@app.get("/print/a4", response_class=HTMLResponse)
def print_a4_all():
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, name, purpose, location, status, qty, purchase_amount, cat_l, cat_m, cat_s
        FROM tools
        ORDER BY location ASC, name ASC, id ASC
    """).fetchall()
    conn.close()

    pdf_url = "/tools.pdf"
    csv_url = "/tools.csv"

    trs = ""
    for r in rows:
        cat_str = " / ".join([x for x in [r["cat_l"], r["cat_m"], r["cat_s"]] if (x or "").strip() != ""]) or "-"
        trs += f"""
        <tr>
          <td style="width:10mm;">{r['id']}</td>
          <td>
            <b>{esc(r['name'])}</b><br/>
            <span class="muted">{esc(r['purpose'])}</span><br/>
            <span class="muted">분류: {esc(cat_str)}</span>
          </td>
          <td style="width:28mm;">{esc(r['location'])}</td>
          <td style="width:20mm;">{esc(r['status'])}</td>
          <td style="width:18mm; text-align:right;">{r['qty']}</td>
          <td style="width:28mm; text-align:right;">{int(r['purchase_amount'] or 0):,}</td>
        </tr>
        """

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button class="btn" onclick="__printSuggest('{pdf_url}', '{csv_url}')">🖨️ 인쇄</button>
        <a href="/tools/list" style="text-decoration:none;"><button class="btn2" type="button">전체 리스트</button></a>
      </div>
      <div class="small">종이에 찍히는 순간, 재고는 ‘말’이 아니라 ‘근거’가 됩니다.</div>
    </div>

    <div class="print-title">공구 보유현황(전체)</div>
    <div class="print-sub">총 {len(rows)}건 · 정렬: 위치 → 공구명</div>

    <table class="print-table">
      <thead>
        <tr>
          <th style="width:10mm;">ID</th>
          <th>공구명 / 용도 / 분류</th>
          <th style="width:28mm;">위치</th>
          <th style="width:20mm;">상태</th>
          <th style="width:18mm;">수량</th>
          <th style="width:28mm;">구입금액</th>
        </tr>
      </thead>
      <tbody>
        {trs if trs else "<tr><td colspan='6'>데이터 없음</td></tr>"}
      </tbody>
    </table>
    """
    return HTMLResponse(layout(body))

@app.get("/print/a4/location/{loc}", response_class=HTMLResponse)
def print_a4_location(loc: str):
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, name, purpose, location, status, qty, purchase_amount, cat_l, cat_m, cat_s
        FROM tools
        WHERE location = ?
        ORDER BY name ASC, id ASC
    """, (loc,)).fetchall()
    conn.close()

    # PDF/CSV 다운로드 URL(해당 위치 필터 적용)
    back_qs = urlencode({"location": loc})
    pdf_url = "/tools.pdf" + (f"?{back_qs}" if back_qs else "")
    csv_url = "/tools.csv" + (f"?{back_qs}" if back_qs else "")

    trs = ""
    for r in rows:
        cat_str = " / ".join([x for x in [r["cat_l"], r["cat_m"], r["cat_s"]] if (x or "").strip() != ""]) or "-"
        trs += f"""
        <tr>
          <td style="width:10mm;">{r['id']}</td>
          <td>
            <b>{esc(r['name'])}</b><br/>
            <span class="muted">{esc(r['purpose'])}</span><br/>
            <span class="muted">분류: {esc(cat_str)}</span>
          </td>
          <td style="width:22mm;">{esc(r['status'])}</td>
          <td style="width:18mm; text-align:right;">{r['qty']}</td>
          <td style="width:28mm; text-align:right;">{int(r['purchase_amount'] or 0):,}</td>
        </tr>
        """

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button class="btn" onclick="__printSuggest('{pdf_url}', '{csv_url}')">🖨️ 인쇄</button>
        <a href="/dashboard" style="text-decoration:none;"><button class="btn2" type="button">← 대시보드</button></a>
        <a href="/tools/list?{urlencode({'location': loc})}" style="text-decoration:none;"><button class="btn2" type="button">이 위치 리스트</button></a>
      </div>
      <div class="small">위치별 출력은 점검의 체크리스트가 됩니다.</div>
    </div>

    <div class="print-title">공구 보유현황(위치별)</div>
    <div class="print-sub">위치: <b>{esc(loc)}</b> · 총 {len(rows)}건 · 정렬: 공구명</div>

    <table class="print-table">
      <thead>
        <tr>
          <th style="width:10mm;">ID</th>
          <th>공구명 / 용도 / 분류</th>
          <th style="width:22mm;">상태</th>
          <th style="width:18mm;">수량</th>
          <th style="width:28mm;">구입금액</th>
        </tr>
      </thead>
      <tbody>
        {trs if trs else "<tr><td colspan='5'>데이터 없음</td></tr>"}
      </tbody>
    </table>
    """
    return HTMLResponse(layout(body))
@app.get("/print/a4/category", response_class=HTMLResponse)
def print_a4_category_all():
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, name, purpose, location, status, qty, purchase_amount,
               cat_l, cat_m, cat_s
        FROM tools
        ORDER BY
          CASE WHEN TRIM(cat_l)='' AND TRIM(cat_m)='' AND TRIM(cat_s)='' THEN 1 ELSE 0 END,
          cat_l ASC, cat_m ASC, cat_s ASC,
          location ASC, name ASC, id ASC
    """).fetchall()
    conn.close()

    def cat_key(r):
        l = (r["cat_l"] or "").strip()
        m = (r["cat_m"] or "").strip()
        s = (r["cat_s"] or "").strip()

        # ✅ 완전 미분류
        if l == "" and m == "" and s == "":
            return "미분류"

        # ✅ 단계까지만 묶기 (대 / 대중 / 대중소)
        if l != "" and m == "" and s == "":
            return l
        if l != "" and m != "" and s == "":
            return f"{l} / {m}"
        if l != "" and m != "" and s != "":
            return f"{l} / {m} / {s}"

        # ✅ 예외 데이터(중만 있음, 소만 있음 등)도 운영상 묶어두기
        # (원하시면 여기서 '미분류'로 강제해도 됩니다)
        parts = [x for x in [l, m, s] if x]
        return " / ".join(parts) if parts else "미분류"
        

    groups = {}
    for r in rows:
        k = cat_key(r)
        groups.setdefault(k, []).append(r)

    sections = ""
    for k, items in groups.items():
        trs = ""
        for r in items:
            trs += f"""
            <tr>
              <td style="width:10mm;">{r['id']}</td>
              <td><b>{esc(r['name'])}</b><br/><span class="muted">{esc(r['purpose'])}</span></td>
              <td style="width:22mm;">{esc(r['location'])}</td>
              <td style="width:18mm;">{esc(r['status'])}</td>
              <td style="width:14mm; text-align:right;">{r['qty']}</td>
              <td style="width:24mm; text-align:right;">{int(r['purchase_amount'] or 0):,}</td>
            </tr>
            """

        sections += f"""
        <div class="print-title">{esc(k)}</div>
        <div class="print-sub">총 {len(items)}건</div>
        <table class="print-table">
          <thead>
            <tr>
              <th style="width:10mm;">ID</th>
              <th>공구명 / 용도</th>
              <th style="width:22mm;">위치</th>
              <th style="width:18mm;">상태</th>
              <th style="width:14mm;">수량</th>
              <th style="width:24mm;">구입금액</th>
            </tr>
          </thead>
          <tbody>
            {trs}
          </tbody>
        </table>
        <div style="height:8mm;"></div>
        """

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button class="btn" onclick="window.print()">🖨️ 인쇄</button>
        <a href="/dashboard" style="text-decoration:none;"><button class="btn2" type="button">← 대시보드</button></a>
        <a href="/print/a4" style="text-decoration:none;"><button class="btn2" type="button">전체 A4</button></a>
      </div>
      <div class="small">분류별 출력은 ‘정리’가 아니라 ‘통제’입니다.</div>
    </div>

    {sections if sections else "<div class='box muted'>데이터 없음</div>"}
    """
    return HTMLResponse(layout(body))

@app.get("/print/a4/unclassified", response_class=HTMLResponse)
def print_a4_unclassified():
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, name, purpose, location, status, qty, purchase_amount
        FROM tools
        WHERE TRIM(cat_l)='' AND TRIM(cat_m)='' AND TRIM(cat_s)=''
        ORDER BY location ASC, name ASC, id ASC
    """).fetchall()
    conn.close()

    trs = ""
    for r in rows:
        trs += f"""
        <tr>
          <td style="width:10mm;">{r['id']}</td>
          <td><b>{esc(r['name'])}</b><br/><span class="muted">{esc(r['purpose'])}</span></td>
          <td style="width:28mm;">{esc(r['location'])}</td>
          <td style="width:20mm;">{esc(r['status'])}</td>
          <td style="width:18mm; text-align:right;">{r['qty']}</td>
          <td style="width:28mm; text-align:right;">{int(r['purchase_amount'] or 0):,}</td>
        </tr>
        """

    body = f"""
    <div class="box noprint">
      <div style="display:flex; gap:10px; flex-wrap:wrap;">
        <button class="btn" onclick="window.print()">🖨️ 인쇄</button>
        <a href="/dashboard" style="text-decoration:none;"><button class="btn2" type="button">← 대시보드</button></a>
        <a href="/tools/list?unclassified=1" style="text-decoration:none;"><button class="btn2" type="button">미분류 리스트</button></a>
      </div>
      <div class="small">미분류는 방치하면 분실로 자랍니다. 오늘 끊어냅시다.</div>
    </div>

    <div class="print-title">미분류 공구 목록</div>
    <div class="print-sub">총 {len(rows)}건</div>

    <table class="print-table">
      <thead>
        <tr>
          <th style="width:10mm;">ID</th>
          <th>공구명 / 용도</th>
          <th style="width:28mm;">위치</th>
          <th style="width:20mm;">상태</th>
          <th style="width:18mm;">수량</th>
          <th style="width:28mm;">구입금액</th>
        </tr>
      </thead>
      <tbody>
        {trs if trs else "<tr><td colspan='6'>미분류 없음</td></tr>"}
      </tbody>
    </table>
    """
    return HTMLResponse(layout(body))
    
@app.get("/tools/edit/{tool_id}", response_class=HTMLResponse)
def tool_edit_page(tool_id: int):
    conn = get_conn()
    t = conn.execute("""
        SELECT id, name, purpose, location, status, qty, purchase_amount, cat_l, cat_m, cat_s
        FROM tools
        WHERE id = ?
    """, (tool_id,)).fetchone()
    conn.close()

    if not t:
        return HTMLResponse(layout("<div class='box'>존재하지 않는 공구입니다.</div>"))

    # 분류 드롭다운: 현재 값으로 preselect
    edit_cat = category_select_block(
        "edit",
        sel_l=(t["cat_l"] or "").strip(),
        sel_m=(t["cat_m"] or "").strip(),
        sel_s=(t["cat_s"] or "").strip(),
        allow_empty=True,
        empty_label="선택"
    )

    body = f"""
    <div class="box noprint">
      <a href="/tools/list" style="text-decoration:none;"><button class="btn2" type="button">← 리스트</button></a>
    </div>

    <div class="box">
      <div style="font-weight:900; font-size:16px;">✏️ 공구 수정 #{t['id']}</div>

      <form action="/tools/update/{t['id']}" method="post">
        <label>공구명</label>
        <input name="name" value="{esc(t['name'])}" required />

        <label>용도</label>
        <input name="purpose" value="{esc(t['purpose'])}" required />

        {edit_cat}

        <div class="row">
          <div>
            <label>보유수량</label>
            <input name="qty" type="number" min="0" value="{int(t['qty'] or 0)}" required />
          </div>
          <div>
            <label>구입금액(원)</label>
            <input name="purchase_amount" type="number" min="0" value="{int(t['purchase_amount'] or 0)}" />
          </div>
        </div>

        <label>보관위치</label>
        <input name="location" value="{esc(t['location'])}" required />

        <label>상태</label>
        <select name="status">
          {''.join([f"<option {'selected' if t['status']==s else ''}>{s}</option>" for s in ['정상','고장(수리)','폐기','분실']])}
        </select>

        <div class="hr"></div>
        <button class="btn" type="submit">저장</button>
      </form>

      <div class="hr"></div>
      <form action="/tools/delete/{t['id']}" method="post" onsubmit="return confirm('정말 삭제할까요? (이미지/이력 포함)');">
        <button class="btn2" type="submit">🗑️ 이 공구 삭제</button>
      </form>
    </div>
    """
    return HTMLResponse(layout(body))
    
@app.post("/tools/update/{tool_id}")
def tool_update(
    tool_id: int,
    name: str = Form(...),
    purpose: str = Form(...),
    location: str = Form(...),
    status: str = Form("정상"),
    qty: int = Form(0),
    purchase_amount: int = Form(0),
    cat_l: str = Form(""),
    cat_m: str = Form(""),
    cat_s: str = Form(""),
):
    conn = get_conn()
    conn.execute("""
        UPDATE tools
        SET name=?, purpose=?, location=?, status=?, qty=?, purchase_amount=?,
            cat_l=?, cat_m=?, cat_s=?
        WHERE id=?
    """, (
        name.strip(), purpose.strip(), location.strip(), status.strip(),
        int(qty), int(purchase_amount),
        cat_l.strip(), cat_m.strip(), cat_s.strip(),
        tool_id
    ))
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/tools/edit/{tool_id}", status_code=303)
    
@app.post("/tools/delete/{tool_id}")
def tool_delete(tool_id: int):
    conn = get_conn()

    # 연결 이미지 경로 수집(파일도 삭제)
    imgs = conn.execute("SELECT image_path FROM tool_images WHERE tool_id=?", (tool_id,)).fetchall()

    # DB 삭제(자식 → 부모 순)
    conn.execute("DELETE FROM tool_images WHERE tool_id=?", (tool_id,))
    conn.execute("DELETE FROM tool_events WHERE tool_id=?", (tool_id,))
    conn.execute("DELETE FROM tools WHERE id=?", (tool_id,))
    conn.commit()
    conn.close()

    # 파일 삭제는 DB 커밋 후
    for r in imgs:
        p = Path(r["image_path"])
        try:
            if p.exists() and p.is_file():
                p.unlink()
        except:
            pass

    return RedirectResponse(url="/tools/list", status_code=303)

from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ---- PDF 폰트 등록(한글) ----
FONT_NAME = "Helvetica"  # 폰트 없을 때 대비
FONT_PATH = BASE_DIR / "fonts" / "NanumGothic.ttf"

if FONT_PATH.exists():
    try:
        pdfmetrics.registerFont(TTFont("NanumGothic", str(FONT_PATH)))
        FONT_NAME = "NanumGothic"
    except Exception:
        pass  # 폰트 등록 실패 시 Helvetica로 진행(한글 깨질 수 있음)

def fetch_tools_for_export(
    location: str = "", status: str = "", q: str = "",
    cat_l: str = "", cat_m: str = "", cat_s: str = "",
    unclassified: str = ""
):
    conn = get_conn()
    where = []
    params = []

    if location.strip():
        where.append("location = ?"); params.append(location.strip())
    if status.strip():
        where.append("status = ?"); params.append(status.strip())
    if q.strip():
        where.append("(name LIKE ? OR purpose LIKE ?)")
        params.extend([f"%{q.strip()}%", f"%{q.strip()}%"])

    if cat_l.strip():
        where.append("cat_l = ?"); params.append(cat_l.strip())
    if cat_m.strip():
        where.append("cat_m = ?"); params.append(cat_m.strip())
    if cat_s.strip():
        where.append("cat_s = ?"); params.append(cat_s.strip())

    if str(unclassified).strip() == "1":
        where.append("TRIM(cat_l)='' AND TRIM(cat_m)='' AND TRIM(cat_s)=''")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = conn.execute(f"""
        SELECT id, name, purpose, location, status, qty, purchase_amount, cat_l, cat_m, cat_s, created_at
        FROM tools
        {where_sql}
        ORDER BY location ASC, name ASC, id ASC
    """, params).fetchall()
    conn.close()
    return rows

def build_tools_pdf(rows, title: str, subtitle: str) -> bytes:
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=24, rightMargin=24, topMargin=24, bottomMargin=24
    )

    styles = getSampleStyleSheet()
    h = ParagraphStyle(
        "h",
        parent=styles["Heading1"],
        fontName=FONT_NAME,
        fontSize=16,
        leading=18,
        spaceAfter=8
    )
    sub = ParagraphStyle(
        "sub",
        parent=styles["Normal"],
        fontName=FONT_NAME,
        fontSize=10,
        leading=12,
        textColor=colors.black,
        spaceAfter=10
    )
    normal = ParagraphStyle(
        "normal",
        parent=styles["Normal"],
        fontName=FONT_NAME,
        fontSize=9,
        leading=11
    )

    story = []
    story.append(Paragraph(title, h))
    story.append(Paragraph(subtitle, sub))
    story.append(Spacer(1, 6))

    data = [[
        "ID", "공구명/용도/분류", "위치", "상태", "수량", "금액(원)"
    ]]

    for r in rows:
        cat_str = " / ".join([x for x in [r["cat_l"], r["cat_m"], r["cat_s"]] if (x or "").strip()]) or "-"
        name_block = (
            f"<b>{html.escape(r['name'] or '')}</b><br/>"
            f"{html.escape(r['purpose'] or '')}<br/>"
            f"<font color='#555555'>분류: {html.escape(cat_str)}</font>"
        )
        data.append([
            str(r["id"]),
            Paragraph(name_block, normal),
            r["location"] or "",
            r["status"] or "",
            str(int(r["qty"] or 0)),
            f"{int(r['purchase_amount'] or 0):,}"
        ])

    table = Table(
        data,
        colWidths=[28, 260, 70, 50, 40, 60],
        repeatRows=1
    )
    table.setStyle(TableStyle([
        ("FONT", (0,0), (-1,-1), FONT_NAME),
        ("FONTSIZE", (0,0), (-1,0), 10),
        ("BACKGROUND", (0,0), (-1,0), colors.whitesmoke),
        ("GRID", (0,0), (-1,-1), 0.5, colors.black),
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("ALIGN", (0,0), (0,-1), "CENTER"),
        ("ALIGN", (-2,1), (-1,-1), "RIGHT"),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.Color(0.98,0.98,0.98)]),
        ("BOTTOMPADDING", (0,0), (-1,0), 6),
        ("TOPPADDING", (0,0), (-1,0), 6),
        ("TOPPADDING", (0,1), (-1,-1), 4),
        ("BOTTOMPADDING", (0,1), (-1,-1), 4),
    ]))

    story.append(table)
    doc.build(story)
    return buf.getvalue()

@app.get("/tools.pdf")
def tools_pdf(
    location: str = "", status: str = "", q: str = "",
    cat_l: str = "", cat_m: str = "", cat_s: str = "",
    unclassified: str = ""
):
    rows = fetch_tools_for_export(location, status, q, cat_l, cat_m, cat_s, unclassified)

    # 제목/부제(필터 표시)
    filt = f"위치={location or '전체'} · 상태={status or '전체'} · 검색어={q or '없음'} · " \
           f"분류={cat_l or '전체'}/{cat_m or '전체'}/{cat_s or '전체'} · " \
           f"{'미분류만' if str(unclassified).strip()=='1' else '전체'} · 총 {len(rows)}건"

    pdf_bytes = build_tools_pdf(rows, "공구 보유현황", filt)
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=tools.pdf"}
    )