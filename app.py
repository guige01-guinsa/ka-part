# app.py — Android(Pydroid3) 호환 All-in-One 아파트 관리 클라이언트
# 기능: 수변전 일지(전력/급수/열량/유량) + 민원/고장 + 업무파일 + 설정
# 보조: 누락 컬럼 자동 마이그레이션, CSV 내보내기, 월별 집계, 일괄 재계산, 음성 입력
# 주의: 같은 폰의 크롬에서 http://127.0.0.1:8000/ 로 접속

import os, math, json, mimetypes, uuid
import requests  # ← 카카오 REST 호출
from datetime import datetime, date, time
from typing import Optional, List

from flask import (
    Flask, request, jsonify, redirect, url_for,
    render_template_string, flash, send_file
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, text

# ───────────────────────────────────────────────────────────────────
# 1) Flask & SQLite 초기 설정 (instance 폴더를 DB/업로드 저장소로 사용)
#    - Pydroid3에서 쓰기 권한 보장
# ───────────────────────────────────────────────────────────────────
app = Flask(__name__, instance_relative_config=True)
os.makedirs(app.instance_path, exist_ok=True)
UPLOAD_DIR = os.path.join(app.instance_path, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

db_path = os.path.join(app.instance_path, "apartment.db")
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + db_path
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
# 안드로이드 환경에서 같은 스레드 체크로 생기는 경고 방지
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"connect_args": {"check_same_thread": False}}
app.config["SECRET_KEY"] = "replace_me_for_forms"

db = SQLAlchemy(app)

# ───────────────────────────────────────────────────────────────────
# 2) 유틸 함수들 (파서/반올림/전력 계산)
# ───────────────────────────────────────────────────────────────────
def parse_date(s):
    """'YYYY-MM-DD' → date, 빈 값은 None"""
    if not s: return None
    if isinstance(s, date): return s
    return datetime.strptime(s, "%Y-%m-%d").date()

def parse_time(s):
    """'HH:MM' → time, 빈 값은 None"""
    if not s: return None
    if isinstance(s, time): return s
    return datetime.strptime(s, "%H:%M").time()

def parse_float(s):
    """문자열 → float, 빈 값/잘못된 값은 None"""
    if s in (None, "", "null"): return None
    try: return float(str(s).replace(",", ""))
    except Exception: return None

def r3(x):
    """소수점 3자리 반올림"""
    if x is None: return None
    try: return round(float(x), 3)
    except Exception: return None

def calc_kw(voltage, current, pf, *, is_kv=False):
    """
    3상 유효전력 kW = √3 * V * I * PF / 1000
    - 고압측: 전압이 kV 단위 → is_kv=True 로 받아 V로 변환 후 계산
    """
    if voltage is None or current is None or pf in (None, "", "null"):
        return None
    try:
        v = float(voltage) * (1000 if is_kv else 1)
        i = float(current)
        pf = float(pf)
        kw = math.sqrt(3) * v * i * pf / 1000.0
        return r3(kw)
    except Exception:
        return None

# 기본 점검자(드롭다운)
OPERATORS = ["이상석", "이창희", "신충기"]
DEFAULT_OPERATOR = OPERATORS[0]

# ───────────────────────────────────────────────────────────────────
# 3) 모델 정의
#    - Settings: 사용량 보정 계수/요금/카카오 전송 설정
#    - SubstationLog: 수변전/설비 일지 (누적/일사용량 포함)
#    - WorkFile: 업무파일 저장소
#    - Complaint: 민원/고장 접수
# ───────────────────────────────────────────────────────────────────
class Settings(db.Model):
    __tablename__ = "settings"
    id = db.Column(db.Integer, primary_key=True)
    public_base_url = db.Column(db.String)  # 업로드/첨부의 공개 URL prefix(선택)

    # 사용량 보정 계수(일 사용량 계산시 곱함)
    hv_factor = db.Column(db.Float)       # 고압 사용량(요청: 1800 배)
    ind_factor = db.Column(db.Float)      # 산업용 사용량(요청: 30 배)
    street_factor = db.Column(db.Float)   # 가로등 사용량(기본 1 배)

    # 급수/열량/유량 보정(선택)
    water_factor = db.Column(db.Float, default=1.0)
    heat_factor  = db.Column(db.Float, default=1.0)
    flow_factor  = db.Column(db.Float, default=1.0)

    # 고지/배분(옵션)
    tariff_per_kwh = db.Column(db.Float, default=0.0)
    base_charge    = db.Column(db.Float, default=0.0)
    allocation_method = db.Column(db.String, default="equal")

    # 카카오 전송 설정(옵션)
    kakao_rest_key = db.Column(db.String)
    kakao_access_token = db.Column(db.String)
    kakao_friend_uuid  = db.Column(db.String)

    @staticmethod
    def get():
        """ID=1의 설정 레코드를 항상 보장(없으면 생성)"""
        row = Settings.query.get(1)
        if not row:
            row = Settings(
                id=1,
                hv_factor=1800.0, ind_factor=30.0, street_factor=1.0,
                water_factor=1.0, heat_factor=1.0, flow_factor=1.0,
                tariff_per_kwh=0.0, base_charge=0.0, allocation_method="equal",
                public_base_url=None,
            )
            db.session.add(row); db.session.commit()
        return row

class SubstationLog(db.Model):
    """
    수변전/설비 일지
    - 전력: 고압 수전, 저압 3회선, 누적/일사용량
    - 설비: 급수/열량/유량 누적/일사용량, 각종 온도
    """
    __tablename__ = "substation_log"
    id = db.Column(db.Integer, primary_key=True)

    # 공통
    log_date = db.Column(db.Date, nullable=False)   # 일지 날짜
    log_time = db.Column(db.Time)                   # 일지 시각
    operator = db.Column(db.String(32), default=DEFAULT_OPERATOR)  # 점검자

    # 고압 수전측 (전압 kV 입력 → 자동 kW 계산)
    incomer_voltage = db.Column(db.Float)    # kV
    incomer_curr    = db.Column(db.Float)    # A
    vcb_p_factor    = db.Column(db.Float)    # 역률
    electric_energy = db.Column(db.Float)    # kW(자동계산)

    # 저압측 (V 입력 → 자동 kW 계산)
    lv1_v = db.Column(db.Float); lv1_a = db.Column(db.Float); lv1_kw = db.Column(db.Float)
    lv2_v = db.Column(db.Float); lv2_a = db.Column(db.Float); lv2_kw = db.Column(db.Float)
    lv3_v = db.Column(db.Float); lv3_a = db.Column(db.Float); lv3_kw = db.Column(db.Float)
    power_factor = db.Column(db.Float)       # 저압 공통 역률

    # 전력 누적/일사용량
    hv_acc_kwh   = db.Column(db.Float)   # 누적 고압 유효전력
    ind_acc_kwh  = db.Column(db.Float)   # 누적 산업용 유효전력
    str_acc_kwh  = db.Column(db.Float)   # 누적 가로등 유효전력
    hv_use_kwh   = db.Column(db.Float)   # 일 사용량(보정 반영) ← (오늘-전일)*계수
    ind_use_kwh  = db.Column(db.Float)
    str_use_kwh  = db.Column(db.Float)

    # 급수/열량/유량 (누적/일사용)
    acc_water = db.Column(db.Float); day_water = db.Column(db.Float)
    acc_heat  = db.Column(db.Float); day_heat  = db.Column(db.Float)
    acc_flow  = db.Column(db.Float); day_flow  = db.Column(db.Float)

    # 온도(설비)
    hst = db.Column(db.Float); hrt = db.Column(db.Float)
    lst = db.Column(db.Float); lrt = db.Column(db.Float)
    dhws = db.Column(db.Float); dhwr = db.Column(db.Float)

    # 기타
    air_temp = db.Column(db.Float); winding_temp = db.Column(db.Float)
    event = db.Column(db.String(160), default="")   # 특이사항
    remarks = db.Column(db.Text, default="")        # 비고
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # API/CSV/폼에 쓰기 좋게 dict 변환(반올림)
    def to_dict(self):
        f = lambda x: None if x is None else r3(x)
        return {
            "id": self.id,
            "log_date": self.log_date.isoformat(),
            "log_time": self.log_time.strftime("%H:%M") if self.log_time else None,
            "operator": self.operator,
            "incomer_voltage": f(self.incomer_voltage), "incomer_curr": f(self.incomer_curr), "vcb_p_factor": f(self.vcb_p_factor),
            "electric_energy": f(self.electric_energy),
            "lv1_v": f(self.lv1_v), "lv1_a": f(self.lv1_a), "lv1_kw": f(self.lv1_kw),
            "lv2_v": f(self.lv2_v), "lv2_a": f(self.lv2_a), "lv2_kw": f(self.lv2_kw),
            "lv3_v": f(self.lv3_v), "lv3_a": f(self.lv3_a), "lv3_kw": f(self.lv3_kw),
            "power_factor": f(self.power_factor),
            "hv_acc_kwh": f(self.hv_acc_kwh), "hv_use_kwh": f(self.hv_use_kwh),
            "ind_acc_kwh": f(self.ind_acc_kwh), "ind_use_kwh": f(self.ind_use_kwh),
            "str_acc_kwh": f(self.str_acc_kwh), "str_use_kwh": f(self.str_use_kwh),
            "acc_water": f(self.acc_water), "day_water": f(self.day_water),
            "acc_heat":  f(self.acc_heat),  "day_heat":  f(self.day_heat),
            "acc_flow":  f(self.acc_flow),  "day_flow":  f(self.day_flow),
            "hst": f(self.hst), "hrt": f(self.hrt), "lst": f(self.lst), "lrt": f(self.lrt),
            "dhws": f(self.dhws), "dhwr": f(self.dhwr),
            "air_temp": f(self.air_temp), "winding_temp": f(self.winding_temp),
            "event": self.event, "remarks": self.remarks,
        }

class WorkFile(db.Model):
    """업무 파일 보관/분류/전송(메타 정보)"""
    __tablename__ = "work_file"
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    title = db.Column(db.String(140)); description = db.Column(db.Text)
    category = db.Column(db.String(40)); tags = db.Column(db.String(140))
    filename = db.Column(db.String(300)); ext = db.Column(db.String(15))
    size = db.Column(db.Integer); uploader = db.Column(db.String(40))

class Complaint(db.Model):
    """민원/고장 접수 (간이 자동 분류 + 미디어 첨부)"""
    __tablename__ = "complaint"
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    name = db.Column(db.String(40)); unit = db.Column(db.String(40))
    phone = db.Column(db.String(40)); channel = db.Column(db.String(20))
    text = db.Column(db.Text)
    category = db.Column(db.String(40)); confidence = db.Column(db.Float)
    tags = db.Column(db.String(140))
    status = db.Column(db.String(20), default="접수")
    priority = db.Column(db.String(20), default="보통")
    assigned_to = db.Column(db.String(20), default="전기과장")
    media_filename = db.Column(db.String(300)); media_type = db.Column(db.String(20))

# ───────────────────────────────────────────────────────────────────
# 4) DB 자동 마이그레이션 (기존 DB에 누락 컬럼이 있어도 안전하게 보강)
# ───────────────────────────────────────────────────────────────────
def table_exists(conn, name):
    names = {r[0] for r in conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))}
    return name in names

def get_cols(conn, table):
    return {row[1] for row in conn.execute(text(f"PRAGMA table_info({table})"))}

def ensure_column(conn, table, name, ddl):
    """테이블에 컬럼이 없으면 ALTER TABLE ADD COLUMN 실행"""
    if not table_exists(conn, table): return
    if name not in get_cols(conn, table):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))

def auto_migrate_columns():
    """필요 테이블 생성 + 모든 사용 칼럼 보강"""
    with db.engine.begin() as conn:
        # 최소 테이블 생성
        conn.execute(text("CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS substation_log (id INTEGER PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS work_file (id INTEGER PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS complaint (id INTEGER PRIMARY KEY)"))

        # settings 보강
        for name, ddl in [
            ("public_base_url","TEXT"),
            ("hv_factor","REAL"), ("ind_factor","REAL"), ("street_factor","REAL"),
            ("water_factor","REAL"), ("heat_factor","REAL"), ("flow_factor","REAL"),
            ("tariff_per_kwh","REAL"), ("base_charge","REAL"),
            ("allocation_method","TEXT"),
            ("kakao_rest_key","TEXT"), ("kakao_access_token","TEXT"), ("kakao_friend_uuid","TEXT"),
        ]:
            ensure_column(conn, "settings", name, ddl)

        # substation_log 보강 (폼/API와 1:1 매핑되는 모든 필드)
        for name, ddl in [
            ("log_date","DATE"), ("log_time","TIME"), ("operator","TEXT"),
            ("incomer_voltage","REAL"), ("incomer_curr","REAL"), ("vcb_p_factor","REAL"),
            ("electric_energy","REAL"),
            ("lv1_v","REAL"), ("lv1_a","REAL"), ("lv1_kw","REAL"),
            ("lv2_v","REAL"), ("lv2_a","REAL"), ("lv2_kw","REAL"),
            ("lv3_v","REAL"), ("lv3_a","REAL"), ("lv3_kw","REAL"),
            ("power_factor","REAL"),
            ("hv_acc_kwh","REAL"), ("ind_acc_kwh","REAL"), ("str_acc_kwh","REAL"),
            ("hv_use_kwh","REAL"), ("ind_use_kwh","REAL"), ("str_use_kwh","REAL"),
            ("acc_water","REAL"), ("day_water","REAL"),
            ("acc_heat","REAL"),  ("day_heat","REAL"),
            ("acc_flow","REAL"),  ("day_flow","REAL"),
            ("hst","REAL"), ("hrt","REAL"), ("lst","REAL"), ("lrt","REAL"),
            ("dhws","REAL"), ("dhwr","REAL"),
            ("air_temp","REAL"), ("winding_temp","REAL"),
            ("event","TEXT"), ("remarks","TEXT"), ("created_at","TEXT"),
        ]:
            ensure_column(conn, "substation_log", name, ddl)

        # work_file 보강
        for name, ddl in [
            ("created_at","TEXT"), ("title","TEXT"), ("description","TEXT"),
            ("category","TEXT"), ("tags","TEXT"),
            ("filename","TEXT"), ("ext","TEXT"), ("size","INTEGER"),
            ("uploader","TEXT"),
        ]:
            ensure_column(conn, "work_file", name, ddl)

        # complaint 보강
        for name, ddl in [
            ("created_at","TEXT"), ("name","TEXT"), ("unit","TEXT"),
            ("phone","TEXT"), ("channel","TEXT"), ("text","TEXT"),
            ("category","TEXT"), ("confidence","REAL"), ("tags","TEXT"),
            ("status","TEXT"), ("priority","TEXT"), ("assigned_to","TEXT"),
            ("media_filename","TEXT"), ("media_type","TEXT"),
        ]:
            ensure_column(conn, "complaint", name, ddl)

# ───────────────────────────────────────────────────────────────────
# 5) 자동 계산 로직
#    - 수전/저압 kW 자동계산
#    - 일사용량 = (금일 누적 - 전일 누적) * 계수(설정값)
#    - 음수 방지, 소수점 3자리 반올림
# ───────────────────────────────────────────────────────────────────
def compute_auto_fields(row: SubstationLog, prev: Optional[SubstationLog], s: Settings):
    # 5-1) 수전 및 저압 kW 계산
    row.electric_energy = calc_kw(row.incomer_voltage, row.incomer_curr, row.vcb_p_factor, is_kv=True)
    row.lv1_kw = calc_kw(row.lv1_v, row.lv1_a, row.power_factor, is_kv=False)
    row.lv2_kw = calc_kw(row.lv2_v, row.lv2_a, row.power_factor, is_kv=False)
    row.lv3_kw = calc_kw(row.lv3_v, row.lv3_a, row.power_factor, is_kv=False)

    # 5-2) 일 사용량 계산 도우미
    def diff_mul(today, yest, factor):
        if today is None or yest is None: return None
        val = (float(today) - float(yest)) * float(factor)
        return r3(max(val, 0.0))  # 누계 감소(계기 리셋 등)는 0으로 처리

    # 5-3) 전일 데이터가 있으면 일 사용량 산출
    if prev:
        row.hv_use_kwh  = diff_mul(row.hv_acc_kwh,  prev.hv_acc_kwh,  s.hv_factor or 1.0)
        row.ind_use_kwh = diff_mul(row.ind_acc_kwh, prev.ind_acc_kwh, s.ind_factor or 1.0)
        row.str_use_kwh = diff_mul(row.str_acc_kwh, prev.str_acc_kwh, s.street_factor or 1.0)

        row.day_water = diff_mul(row.acc_water, prev.acc_water, s.water_factor or 1.0)
        row.day_heat  = diff_mul(row.acc_heat,  prev.acc_heat,  s.heat_factor  or 1.0)
        row.day_flow  = diff_mul(row.acc_flow,  prev.acc_flow,  s.flow_factor  or 1.0)
    else:
        row.hv_use_kwh = row.ind_use_kwh = row.str_use_kwh = None
        row.day_water = row.day_heat = row.day_flow = None

# ───────────────────────────────────────────────────────────────────
# 6) 공통 라우트
# ───────────────────────────────────────────────────────────────────
@app.route("/health")
def health(): return jsonify(status="ok")

@app.route("/")
def home(): 
    return "Hello ka-part!" #redirect(url_for("ui_home"))


#app = Flask(__name__)

#@app.route("/health")
#def health():
#    return jsonify(status="ok")
# ───────────────────────────────────────────────────────────────────
# 7) UI 기본 레이아웃 (부트스트랩 + 탭 네비)
#    - 음성 입력 지원 버튼 포함
# ───────────────────────────────────────────────────────────────────
BASE = """
<!doctype html><html lang="ko"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{ title or '아파트 관리' }}</title>

<link rel="icon" href="{{ url_for('static', filename='favicon.ico') }}">

<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{background:#f7f8fb;padding-bottom:70px}.table-sm td,.table-sm th{padding:.35rem .5rem}</style>
</head><body>
<nav class="navbar navbar-dark bg-dark mb-3">
 <div class="container-fluid">
   <a class="navbar-brand" href="{{ url_for('ui_home') }}">🏢 아파트 관리</a>
   <div class="d-flex gap-2">
     <a class="btn btn-outline-light btn-sm" href="{{ url_for('ui_home') }}">수변전</a>
     <a class="btn btn-outline-light btn-sm" href="{{ url_for('ui_files') }}">업무파일</a>
     <a class="btn btn-outline-light btn-sm" href="{{ url_for('ui_complaints') }}">민원/고장</a>
     <a class="btn btn-outline-light btn-sm" href="{{ url_for('ui_settings') }}">설정</a>
   </div>
 </div>
</nav>
<div class="container">
 {% with messages=get_flashed_messages() %}
   {% if messages %}<div class="alert alert-info">{{ messages[0] }}</div>{% endif %}
 {% endwith %}
 {{ body|safe }}
</div>
<script>
function fillFromSpeech(id){
  if(!('webkitSpeechRecognition' in window)){alert('이 브라우저는 음성입력이 지원되지 않습니다.');return;}
  const r = new webkitSpeechRecognition(); r.lang='ko-KR'; r.interimResults=false; r.maxAlternatives=1;
  r.onresult = e => { document.getElementById(id).value = e.results[0][0].transcript; };
  r.start();
}
</script>
</body></html>
"""
def render(title, body, **ctx):
    return render_template_string(BASE, title=title, body=body, **ctx)

# ───────────────────────────────────────────────────────────────────
# 8) UI: 수변전/설비 일지 목록 + CSV/월별/재계산
# ───────────────────────────────────────────────────────────────────
@app.route("/ui")
def ui_home():
    rows = SubstationLog.query.order_by(
        func.coalesce(SubstationLog.log_date, date(1900,1,1)).desc(),
        func.coalesce(SubstationLog.log_time, time(0,0)).desc(),
        SubstationLog.id.desc()
    ).limit(200).all()
    body = render_template_string("""
<div class="d-flex justify-content-between align-items-center mb-2">
  <h5 class="m-0">수변전/설비 일지</h5>
  <div>
    <a class="btn btn-sm btn-primary" href="{{ url_for('ui_new_log') }}">+ 새 기록</a>
    <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('export_csv') }}">CSV</a>
    <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('ui_monthly') }}">월별집계</a>
    <a class="btn btn-sm btn-outline-danger" href="{{ url_for('recalc_all') }}" onclick="return confirm('전체 재계산을 실행할까요?')">일괄 재계산</a>
  </div>
</div>
<div class="table-responsive">
<table class="table table-sm table-hover">
  <thead class="table-light">
  <tr>
    <th>ID</th><th>일시</th><th>수전kW</th>
    <th>고압누적/일</th><th>산업누적/일</th><th>가로등누적/일</th>
    <th>급수누적/일</th><th>열량누적/일</th><th>유량누적/일</th>
    <th>비고</th><th></th>
  </tr>
  </thead>
  <tbody>
  {% for r in rows %}
    <tr>
      <td>{{ r.id }}</td>
      <td>{{ r.log_date }} {{ r.log_time or '' }}</td>
      <td>{{ r.electric_energy or '' }}</td>
      <td>{{ r.hv_acc_kwh or '' }}/<strong>{{ r.hv_use_kwh or '' }}</strong></td>
      <td>{{ r.ind_acc_kwh or '' }}/<strong>{{ r.ind_use_kwh or '' }}</strong></td>
      <td>{{ r.str_acc_kwh or '' }}/<strong>{{ r.str_use_kwh or '' }}</strong></td>
      <td>{{ r.acc_water or '' }}/<strong>{{ r.day_water or '' }}</strong></td>
      <td>{{ r.acc_heat or '' }}/<strong>{{ r.day_heat or '' }}</strong></td>
      <td>{{ r.acc_flow or '' }}/<strong>{{ r.day_flow or '' }}</strong></td>
      <td>{{ (r.event or '')[:12] }}</td>
      <td class="text-end">
        <a class="btn btn-sm btn-outline-primary" href="{{ url_for('ui_edit_log', lid=r.id) }}">수정</a>
        <form method="post" action="{{ url_for('del_log', lid=r.id) }}" style="display:inline" onsubmit="return confirm('삭제하시겠습니까?');">
          <button class="btn btn-sm btn-outline-danger">삭제</button>
        </form>
      </td>
    </tr>
  {% else %}
    <tr><td colspan="11" class="text-muted p-3">기록 없음</td></tr>
  {% endfor %}
  </tbody>
</table>
</div>
""", rows=rows)
    return render("수변전 일지", body)

# 신규/수정 폼 (폼 name이 모델 필드와 정확히 일치하도록 주의)
FORM = """
<div class="card"><div class="card-header"><strong>{{ title }}</strong></div>
<div class="card-body">
<form method="post">
<div class="row g-2">
  <!-- 날짜/시각/점검자 -->
  <div class="col-6 col-md-3"><label class="form-label">일자</label>
    <input type="date" name="log_date" class="form-control" value="{{v.log_date}}"></div>
  <div class="col-6 col-md-3"><label class="form-label">시각</label>
    <input type="time" name="log_time" class="form-control" value="{{v.log_time}}"></div>
  <div class="col-12 col-md-3"><label class="form-label">점검자</label>
    <select name="operator" class="form-select">
      {% for n in operators %}<option value="{{n}}" {% if v.operator==n %}selected{% endif %}>{{n}}</option>{% endfor %}
    </select>
  </div>

  <!-- 고압 수전 측정치 (kV, A, PF) -->
  <div class="col-4 col-md-2"><label class="form-label">HV(kV)</label>
    <input name="incomer_voltage" class="form-control" value="{{v.incomer_voltage}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">HV(A)</label>
    <input name="incomer_curr" class="form-control" value="{{v.incomer_curr}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">HV역률</label>
    <input name="vcb_p_factor" class="form-control" value="{{v.vcb_p_factor}}"></div>

  <!-- 저압 3회선 (V, A) + 공통 역률 -->
  <div class="col-4 col-md-2"><label class="form-label">LV1(V)</label>
    <input name="lv1_v" class="form-control" value="{{v.lv1_v}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">LV1(A)</label>
    <input name="lv1_a" class="form-control" value="{{v.lv1_a}}"></div>

  <div class="col-4 col-md-2"><label class="form-label">LV2(V)</label>
    <input name="lv2_v" class="form-control" value="{{v.lv2_v}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">LV2(A)</label>
    <input name="lv2_a" class="form-control" value="{{v.lv2_a}}"></div>

  <div class="col-4 col-md-2"><label class="form-label">LV3(V)</label>
    <input name="lv3_v" class="form-control" value="{{v.lv3_v}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">LV3(A)</label>
    <input name="lv3_a" class="form-control" value="{{v.lv3_a}}"></div>

  <div class="col-4 col-md-2"><label class="form-label">저압역률</label>
    <input name="power_factor" class="form-control" value="{{v.power_factor}}"></div>

  <!-- 전력 누적 계기값(당일 지시) -->
  <div class="col-4 col-md-2"><label class="form-label">고압 누적</label>
    <input name="hv_acc_kwh" class="form-control" value="{{v.hv_acc_kwh}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">산업 누적</label>
    <input name="ind_acc_kwh" class="form-control" value="{{v.ind_acc_kwh}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">가로등 누적</label>
    <input name="str_acc_kwh" class="form-control" value="{{v.str_acc_kwh}}"></div>

  <!-- 설비 누적 값 -->
  <div class="col-4 col-md-2"><label class="form-label">급수 누적</label>
    <input name="acc_water" class="form-control" value="{{v.acc_water}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">열량 누적</label>
    <input name="acc_heat" class="form-control" value="{{v.acc_heat}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">유량 누적</label>
    <input name="acc_flow" class="form-control" value="{{v.acc_flow}}"></div>

  <!-- 온도 -->
  <div class="col-4 col-md-2"><label class="form-label">HST</label><input name="hst" class="form-control" value="{{v.hst}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">HRT</label><input name="hrt" class="form-control" value="{{v.hrt}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">LST</label><input name="lst" class="form-control" value="{{v.lst}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">LRT</label><input name="lrt" class="form-control" value="{{v.lrt}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">DHWS</label><input name="dhws" class="form-control" value="{{v.dhws}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">DHWR</label><input name="dhwr" class="form-control" value="{{v.dhwr}}"></div>

  <!-- 기타 -->
  <div class="col-6 col-md-3"><label class="form-label">주변온도</label><input name="air_temp" class="form-control" value="{{v.air_temp}}"></div>
  <div class="col-6 col-md-3"><label class="form-label">권선온도</label><input name="winding_temp" class="form-control" value="{{v.winding_temp}}"></div>

  <div class="col-12"><label class="form-label">특이사항</label>
    <input id="event" name="event" class="form-control" value="{{v.event}}">
    <button type="button" class="btn btn-sm btn-outline-secondary mt-1" onclick="fillFromSpeech('event')">🎤 음성으로 입력</button>
  </div>
  <div class="col-12"><label class="form-label">비고</label>
    <textarea name="remarks" class="form-control" rows="2">{{v.remarks}}</textarea></div>
</div>
<div class="mt-3 d-flex gap-2">
  <button class="btn btn-primary">저장</button>
  <a class="btn btn-secondary" href="{{ url_for('ui_home') }}">목록</a>
</div>
</form>
</div></div>
"""

@app.route("/ui/new", methods=["GET","POST"])
def ui_new_log():
    """새 기록 생성(모든 칼럼 저장) + 자동계산"""
    if request.method == "POST":
        s = Settings.get()
        payload = {k: request.form.get(k) for k in request.form.keys()}

        # ① 폼 → 모델 매핑 (모든 칼럼 저장)
        r = SubstationLog(
            log_date=parse_date(payload.get("log_date")) or date.today(),
            log_time=parse_time(payload.get("log_time")),
            operator=payload.get("operator") or DEFAULT_OPERATOR,

            incomer_voltage=parse_float(payload.get("incomer_voltage")),
            incomer_curr=parse_float(payload.get("incomer_curr")),
            vcb_p_factor=parse_float(payload.get("vcb_p_factor")),

            lv1_v=parse_float(payload.get("lv1_v")), lv1_a=parse_float(payload.get("lv1_a")),
            lv2_v=parse_float(payload.get("lv2_v")), lv2_a=parse_float(payload.get("lv2_a")),
            lv3_v=parse_float(payload.get("lv3_v")), lv3_a=parse_float(payload.get("lv3_a")),
            power_factor=parse_float(payload.get("power_factor")),

            hv_acc_kwh=parse_float(payload.get("hv_acc_kwh")),
            ind_acc_kwh=parse_float(payload.get("ind_acc_kwh")),
            str_acc_kwh=parse_float(payload.get("str_acc_kwh")),

            acc_water=parse_float(payload.get("acc_water")),
            acc_heat=parse_float(payload.get("acc_heat")),
            acc_flow=parse_float(payload.get("acc_flow")),

            hst=parse_float(payload.get("hst")), hrt=parse_float(payload.get("hrt")),
            lst=parse_float(payload.get("lst")), lrt=parse_float(payload.get("lrt")),
            dhws=parse_float(payload.get("dhws")), dhwr=parse_float(payload.get("dhwr")),

            air_temp=parse_float(payload.get("air_temp")),
            winding_temp=parse_float(payload.get("winding_temp")),
            event=payload.get("event") or "", remarks=payload.get("remarks") or ""
        )

        # ② 전일 기록 조회(일 사용량 계산용) — 같은 달/연속 불문, 단순 직전 날짜
        prev = SubstationLog.query\
            .filter(SubstationLog.log_date < r.log_date)\
            .order_by(SubstationLog.log_date.desc(), SubstationLog.id.desc())\
            .first()

        # ③ 자동 계산(수전/저압 kW, 일 사용량)
        compute_auto_fields(r, prev, s)

        db.session.add(r); db.session.commit()
        flash(f"등록 완료 (ID {r.id})")
        return redirect(url_for("ui_home"))

    # GET: 폼 초기값
    v = dict(
        log_date=date.today().isoformat(), log_time=datetime.now().strftime("%H:%M"),
        operator=DEFAULT_OPERATOR, incomer_voltage="", incomer_curr="", vcb_p_factor="",
        lv1_v="", lv1_a="", lv2_v="", lv2_a="", lv3_v="", lv3_a="", power_factor="",
        hv_acc_kwh="", ind_acc_kwh="", str_acc_kwh="",
        acc_water="", acc_heat="", acc_flow="",
        hst="", hrt="", lst="", lrt="", dhws="", dhwr="", air_temp="", winding_temp="",
        event="", remarks=""
    )
    return render("새 기록", render_template_string(FORM, title="새 기록", v=v, operators=OPERATORS))

@app.route("/ui/edit/<int:lid>", methods=["GET","POST"])
def ui_edit_log(lid):
    """기존 기록 수정(모든 칼럼 저장) + 자동 재계산"""
    r = SubstationLog.query.get_or_404(lid)
    if request.method == "POST":
        s = Settings.get()

        # ① 폼 → 필드 갱신 (누락 없이 전부)
        r.log_date = parse_date(request.form.get("log_date")) or r.log_date
        r.log_time = parse_time(request.form.get("log_time"))
        r.operator  = request.form.get("operator") or DEFAULT_OPERATOR

        r.incomer_voltage = parse_float(request.form.get("incomer_voltage"))
        r.incomer_curr    = parse_float(request.form.get("incomer_curr"))
        r.vcb_p_factor    = parse_float(request.form.get("vcb_p_factor"))

        r.lv1_v = parse_float(request.form.get("lv1_v")); r.lv1_a = parse_float(request.form.get("lv1_a"))
        r.lv2_v = parse_float(request.form.get("lv2_v")); r.lv2_a = parse_float(request.form.get("lv2_a"))
        r.lv3_v = parse_float(request.form.get("lv3_v")); r.lv3_a = parse_float(request.form.get("lv3_a"))
        r.power_factor = parse_float(request.form.get("power_factor"))

        r.hv_acc_kwh  = parse_float(request.form.get("hv_acc_kwh"))
        r.ind_acc_kwh = parse_float(request.form.get("ind_acc_kwh"))
        r.str_acc_kwh = parse_float(request.form.get("str_acc_kwh"))

        r.acc_water = parse_float(request.form.get("acc_water"))
        r.acc_heat  = parse_float(request.form.get("acc_heat"))
        r.acc_flow  = parse_float(request.form.get("acc_flow"))

        r.hst = parse_float(request.form.get("hst")); r.hrt = parse_float(request.form.get("hrt"))
        r.lst = parse_float(request.form.get("lst")); r.lrt = parse_float(request.form.get("lrt"))
        r.dhws = parse_float(request.form.get("dhws")); r.dhwr = parse_float(request.form.get("dhwr"))

        r.air_temp = parse_float(request.form.get("air_temp"))
        r.winding_temp = parse_float(request.form.get("winding_temp"))
        r.event = request.form.get("event",""); r.remarks = request.form.get("remarks","")

        # ② 수정된 날짜 기준, 이전 레코드로 일사용량 재계산
        prev = SubstationLog.query\
            .filter(SubstationLog.log_date < r.log_date)\
            .order_by(SubstationLog.log_date.desc(), SubstationLog.id.desc())\
            .first()

        # ③ 자동 계산 반영
        compute_auto_fields(r, prev, s)

        db.session.commit()
        flash("수정 완료")
        return redirect(url_for("ui_home"))

    # GET: 폼에 현재 값 표시
    v = r.to_dict()
    return render("기록 수정", render_template_string(FORM, title=f"기록 수정 #{lid}", v=v, operators=OPERATORS))

@app.route("/ui/del/<int:lid>", methods=["POST"])
def del_log(lid):
    """기록 삭제"""
    r = SubstationLog.query.get_or_404(lid)
    db.session.delete(r); db.session.commit()
    flash("삭제되었습니다.")
    return redirect(url_for("ui_home"))

# CSV 내보내기
@app.route("/export.csv")
def export_csv():
    """모든 기록 CSV 저장 후 다운로드"""
    path = os.path.join(app.instance_path, f"substation_{date.today().isoformat()}.csv")
    import csv
    rows = SubstationLog.query.order_by(SubstationLog.log_date.asc(), SubstationLog.id.asc()).all()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","date","time","operator",
                    "elec_kw","hv_acc","hv_use","ind_acc","ind_use","str_acc","str_use",
                    "water_acc","water_day","heat_acc","heat_day","flow_acc","flow_day",
                    "event","remarks"])
        for r in rows:
            w.writerow([
                r.id, r.log_date, r.log_time, r.operator,
                r3(r.electric_energy), r3(r.hv_acc_kwh), r3(r.hv_use_kwh),
                r3(r.ind_acc_kwh), r3(r.ind_use_kwh), r3(r.str_acc_kwh), r3(r.str_use_kwh),
                r3(r.acc_water), r3(r.day_water), r3(r.acc_heat), r3(r.day_heat),
                r3(r.acc_flow), r3(r.day_flow), r.event, r.remarks
            ])
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))

# 월별 집계
@app.route("/monthly")
def ui_monthly():
    """연/월로 일사용량 합계를 보여줌"""
    y = int(request.args.get("y", date.today().year))
    m = int(request.args.get("m", date.today().month))
    start = date(y, m, 1)
    end = date(y + (m==12), (m%12)+1, 1)
    rows = SubstationLog.query.filter(SubstationLog.log_date >= start, SubstationLog.log_date < end).all()
    sums = dict(
        hv=sum([r.hv_use_kwh or 0 for r in rows]),
        ind=sum([r.ind_use_kwh or 0 for r in rows]),
        st=sum([r.str_use_kwh or 0 for r in rows]),
        water=sum([r.day_water or 0 for r in rows]),
        heat=sum([r.day_heat or 0 for r in rows]),
        flow=sum([r.day_flow or 0 for r in rows]),
    )
    body = render_template_string("""
<h5>{{y}}-{{'%02d'%m}} 월별 집계</h5>
<table class="table table-sm w-auto">
<tr><th>고압 사용량</th><td>{{ '%.3f' % sums.hv }}</td></tr>
<tr><th>산업용 사용량</th><td>{{ '%.3f' % sums.ind }}</td></tr>
<tr><th>가로등 사용량</th><td>{{ '%.3f' % sums.st }}</td></tr>
<tr><th>급수 일사용 합</th><td>{{ '%.3f' % sums.water }}</td></tr>
<tr><th>열량 일사용 합</th><td>{{ '%.3f' % sums.heat }}</td></tr>
<tr><th>유량 일사용 합</th><td>{{ '%.3f' % sums.flow }}</td></tr>
</table>
<p><a class="btn btn-secondary" href="{{ url_for('ui_home') }}">← 돌아가기</a></p>
""", y=y, m=m, sums=type("Obj",(object,),sums))
    return render("월별 집계", body)

# 일괄 재계산
@app.route("/recalc")
def recalc_all():
    """전체 레코드에 대해 자동 계산(일사용량 포함)을 재적용"""
    s = Settings.get()
    rows = SubstationLog.query.order_by(SubstationLog.log_date.asc(), SubstationLog.id.asc()).all()
    prev = None
    for r in rows:
        compute_auto_fields(r, prev, s)
        prev = r
    db.session.commit()
    flash("전체 재계산 완료")
    return redirect(url_for("ui_home"))

# ───────────────────────────────────────────────────────────────────
# 9) UI: 업무 파일 보관/검색/삭제
# ───────────────────────────────────────────────────────────────────
@app.route("/files", methods=["GET","POST"])
def ui_files():
    """업무 파일 업로드/검색/삭제"""
    if request.method == "POST":
        f = request.files.get("file")
        title = request.form.get("title") or (f.filename if f else "무제")
        cat = request.form.get("category") or "일반"
        tags = request.form.get("tags") or ""
        desc = request.form.get("description") or ""
        if not f:
            flash("파일이 없습니다."); return redirect(url_for("ui_files"))
        ext = os.path.splitext(f.filename)[1].lower()
        new_name = f"{uuid.uuid4().hex}{ext}"
        path = os.path.join(UPLOAD_DIR, new_name)
        f.save(path)
        st = os.stat(path)
        wf = WorkFile(title=title, description=desc, category=cat, tags=tags,
                      filename=new_name, ext=ext, size=st.st_size, uploader="관리자")
        db.session.add(wf); db.session.commit()
        flash("업로드 완료")
        return redirect(url_for("ui_files"))

    q = request.args.get("q","").strip()
    query = WorkFile.query
    if q:
        like = f"%{q}%"
        query = query.filter(
            (WorkFile.title.like(like)) | (WorkFile.description.like(like)) |
            (WorkFile.category.like(like)) | (WorkFile.tags.like(like))
        )
    rows = query.order_by(WorkFile.created_at.desc(), WorkFile.id.desc()).all()
    body = render_template_string("""
<h5 class="mb-2">업무 파일</h5>
<form class="row g-2 mb-3" method="post" enctype="multipart/form-data">
  <div class="col-12 col-md-3"><input class="form-control" name="title" placeholder="제목"></div>
  <div class="col-6 col-md-2"><input class="form-control" name="category" placeholder="분류"></div>
  <div class="col-6 col-md-3"><input class="form-control" name="tags" placeholder="태그(,구분)"></div>
  <div class="col-12"><input class="form-control" name="description" placeholder="설명"></div>
  <div class="col-8"><input class="form-control" type="file" name="file" required></div>
  <div class="col-4"><button class="btn btn-primary w-100">업로드</button></div>
</form>

<form class="input-group mb-2" method="get">
  <input class="form-control" name="q" value="{{ request.args.get('q','') }}" placeholder="검색(제목/설명/분류/태그)">
  <button class="btn btn-outline-secondary">검색</button>
</form>

<form id="kakao-files-form" method="post" action="{{ url_for('kakao_send_files') }}"></form>

<form class="d-flex gap-2 mb-2" method="post" action="{{ url_for('kakao_send_files') }}">
  <input class="form-control" name="message" placeholder="전송 메모(선택)">
  <button class="btn btn-warning">선택 항목 카카오 전송</button>
</form>

<div class="list-group">
{% for r in rows %}
  <div class="list-group-item">
    <div class="d-flex justify-content-between"><div>
      <strong>{{ r.title }}</strong>
      <span class="text-muted">[{{ r.category }}] {{ r.tags }}</span>
      <div class="small">{{ r.description }}</div>
    </div>
    <div class="text-end">
      <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('download_file', fid=r.id) }}">다운로드</a>
      <form method="post" action="{{ url_for('delete_file', fid=r.id) }}" style="display:inline" onsubmit="return confirm('삭제?');">
        <button class="btn btn-sm btn-outline-danger">삭제</button>
      </form>
    </div></div>
  </div>
{% else %}
  <div class="text-muted p-3">파일 없음</div>
{% endfor %}
</div>
""", rows=rows)
    return render("업무 파일", body)

@app.route("/kakao/send/files", methods=["POST"])
def kakao_send_files():
    s = Settings.get()
    if not s.kakao_access_token or not s.kakao_friend_uuid:
        flash("설정에서 Kakao Access Token과 Friend UUID를 입력하세요.")
        return redirect(url_for("ui_files"))

    ids = request.form.getlist("fid")
    if not ids:
        # 체크박스가 숨은 폼으로 들어오는 케이스 대비: 파일 목록 폼에서 message만 온 경우
        ids = request.form.getlist("fid[]")
    ids = [int(x) for x in ids if str(x).isdigit()]
    if not ids:
        flash("전송할 파일을 선택하세요.")
        return redirect(url_for("ui_files"))

    rows = WorkFile.query.filter(WorkFile.id.in_(ids)).order_by(WorkFile.id.asc()).all()
    msg = (request.form.get("message") or "").strip()

    # 메시지 본문 구성
    lines = []
    if msg: lines.append(f"[메모] {msg}")
    lines.append("📎 업무파일 전송")
    pub = s.public_base_url or ""
    for r in rows:
        link = (pub + r.filename) if pub else None
        base = f"- {r.title}{r.ext or ''}"
        lines.append(base + (f" 🔗 {link}" if link else ""))

    text = "\n".join(lines)[:990]
    uuids = [x.strip() for x in (s.kakao_friend_uuid or "").split(",") if x.strip()]
    ok, detail = kakao_send_default(s.kakao_access_token, uuids, text, link_url=s.public_base_url or None)
    flash("카카오 전송 성공" if ok else f"카카오 전송 실패: {detail}")
    return redirect(url_for("ui_files"))

@app.route("/files/<int:fid>/download")
def download_file(fid):
    r = WorkFile.query.get_or_404(fid)
    path = os.path.join(UPLOAD_DIR, r.filename)
    return send_file(path, as_attachment=True, download_name=r.title + (r.ext or ""))

@app.route("/files/<int:fid>/delete", methods=["POST"])
def delete_file(fid):
    r = WorkFile.query.get_or_404(fid)
    try:
        os.remove(os.path.join(UPLOAD_DIR, r.filename))
    except Exception:
        pass
    db.session.delete(r); db.session.commit()
    flash("삭제되었습니다.")
    return redirect(url_for("ui_files"))

# ───────────────────────────────────────────────────────────────────
# 10) UI: 민원/고장 (간단 자동분류 + 미디어 첨부)
# ───────────────────────────────────────────────────────────────────
COMPLAINT_CATEGORIES = {
    "전기": ["정전", "누전", "조명", "차단기", "콘센트", "승강기"],
    "배관": ["누수", "막힘", "수압", "배수", "악취"],
    "난방": ["보일러", "온수", "난방", "온도"],
    "시설": ["문고장", "파손", "도색", "청소"],
}
DEFAULT_CATEGORY = "기타"
ALL_TAGS = set(sum(COMPLAINT_CATEGORIES.values(), []))

def simple_classify(text: str, filename: Optional[str]) -> (str, float, List[str]):
    """아주 단순한 규칙 기반 분류(키워드 카운트) + 미디어 첨부 힌트"""
    txt = (text or "").lower()
    score = {}; tags = []
    for cat, keys in COMPLAINT_CATEGORIES.items():
        s = sum([1 for k in keys if k.lower() in txt])
        score[cat] = s
        if s: tags.extend([k for k in keys if k.lower() in txt])
    if filename:
        ext = os.path.splitext(filename)[1].lower()
        if ext in (".jpg",".jpeg",".png",".mp4",".mov",".avi"):
            tags.append("미디어첨부")
    best = max(score, key=score.get) if score else DEFAULT_CATEGORY
    conf = (score.get(best,0) / max(1,len(ALL_TAGS))) + (0.1 if "미디어첨부" in tags else 0.0)
    conf = r3(min(conf, 1.0))
    return best if score.get(best,0)>0 else DEFAULT_CATEGORY, conf, tags

@app.route("/c", methods=["GET","POST"])
def ui_complaints():
    """민원/고장 접수 + 목록"""
    if request.method == "POST":
        name = request.form.get("name") or "익명"
        unit = request.form.get("unit") or ""
        phone = request.form.get("phone") or ""
        textv = request.form.get("text") or ""
        ch = request.form.get("channel") or "웹"

        media = request.files.get("media")
        media_name, media_type = None, None
        if media and media.filename:
            ext = os.path.splitext(media.filename)[1].lower()
            media_name = f"cmp_{uuid.uuid4().hex}{ext}"
            media.save(os.path.join(UPLOAD_DIR, media_name))
            mt = mimetypes.guess_type(media_name)[0] or ""
            media_type = "image" if mt.startswith("image") else ("video" if mt.startswith("video") else "file")

        cat, conf, tags = simple_classify(textv, media_name)
        row = Complaint(name=name, unit=unit, phone=phone, text=textv, channel=ch,
                        category=cat, confidence=conf, tags=",".join(tags),
                        media_filename=media_name, media_type=media_type)
        db.session.add(row); db.session.commit()
        flash(f"접수 완료 (#{row.id}, {row.category})")
        return redirect(url_for("ui_complaints"))

    q = request.args.get("q","").strip()
    query = Complaint.query
    if q:
        like = f"%{q}%"
        query = query.filter( (Complaint.text.like(like)) | (Complaint.category.like(like)) | (Complaint.unit.like(like)) )
    rows = query.order_by(Complaint.created_at.desc(), Complaint.id.desc()).all()
    body = render_template_string("""
<h5 class="mb-2">민원/고장 접수</h5>
<form class="row g-2 mb-3" method="post" enctype="multipart/form-data">
  <div class="col-6 col-md-2"><input class="form-control" name="name" placeholder="이름"></div>
  <div class="col-6 col-md-2"><input class="form-control" name="unit" placeholder="동/호수"></div>
  <div class="col-6 col-md-2"><input class="form-control" name="phone" placeholder="연락처"></div>
  <div class="col-6 col-md-2"><input class="form-control" name="channel" value="웹"></div>
  <div class="col-12"><textarea id="ctext" class="form-control" name="text" rows="2" placeholder="내용"></textarea>
    <button type="button" class="btn btn-sm btn-outline-secondary mt-1" onclick="fillFromSpeech('ctext')">🎤 음성으로 입력</button>
  </div>
  <div class="col-8"><input class="form-control" type="file" name="media" accept="image/*,video/*"></div>
  <div class="col-4"><button class="btn btn-primary w-100">접수</button></div>
</form>

<form class="input-group mb-2" method="get">
  <input class="form-control" name="q" value="{{ request.args.get('q','') }}" placeholder="검색(내용/분류/호수)">
  <button class="btn btn-outline-secondary">검색</button>
</form>

<div class="list-group">
{% for r in rows %}
  <div class="list-group-item">
    <div class="d-flex justify-content-between">
      <div><strong>#{{ r.id }}</strong> [{{ r.category }}] <span class="text-muted small">{{ r.unit }} {{ r.name }}</span>
        <div class="small text-muted">{{ r.created_at.strftime("%Y-%m-%d %H:%M") }}</div>
        <div>{{ r.text }}</div>
        {% if r.media_filename %}
          <div class="small">첨부: <a href="{{ url_for('download_upload', name=r.media_filename) }}">{{ r.media_filename }}</a></div>
        {% endif %}
      </div>
      <div class="text-end">
        <span class="badge bg-secondary">{{ r.status }}</span>
      </div>
    </div>
  </div>
{% else %}
  <div class="text-muted p-3">접수 없음</div>
{% endfor %}
</div>
""", rows=rows)
    return render("민원/고장", body)

@app.route("/u/<path:name>")
def download_upload(name):
    """업로드 파일 내려받기(민원 첨부 포함)"""
    path = os.path.join(UPLOAD_DIR, name)
    return send_file(path, as_attachment=True)

# # ───────────────────────────────────────────────────────────────────
# Kakao 메시지 전송 헬퍼
# ───────────────────────────────────────────────────────────────────
def kakao_send_default(access_token: str, friend_uuids: list[str], text: str, link_url: str | None = None):
    """
    카카오 친구에게 기본 템플릿(텍스트) 메시지 전송.
    - friend_uuids: ["uuid1","uuid2", ...]
    - text: 본문 (최대 200자 권장)
    - link_url: 버튼 링크(선택). public_base_url이 있을 때 파일/첨부 링크로 사용 가능
    요구:
      - 카카오 개발자 콘솔 애플리케이션
      - 액세스토큰에 friends, talk_message 권한
      - 수신자 uuid 확보 (friends API 등)
    """
    url = "https://kapi.kakao.com/v1/api/talk/friends/message/default/send"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
    }
    template_obj = {
        "object_type": "text",
        "text": text[:990],  # 안전 여유
        "link": {"web_url": link_url or "https://developers.kakao.com"},
        "button_title": "열기" if link_url else "확인",
    }
    data = {
        "receiver_uuids": json.dumps(friend_uuids, ensure_ascii=False),
        "template_object": json.dumps(template_obj, ensure_ascii=False),
    }
    resp = requests.post(url, headers=headers, data=data, timeout=10)
    ok = (resp.status_code == 200)
    return ok, (resp.text if not ok else "OK")
# 11) UI: 설정 (보정계수/요금/카카오)
# ───────────────────────────────────────────────────────────────────
@app.route("/settings", methods=["GET","POST"])
def ui_settings():
    """보정계수/요금/카카오 전송 설정"""
    s = Settings.get()
    if request.method == "POST":
        # 빈칸은 기존값 유지 (parse_float(None) 방지)
        def keep(val, cur): 
            p = parse_float(val)
            return p if p is not None else cur

        s.hv_factor  = keep(request.form.get("hv_factor"), s.hv_factor)
        s.ind_factor = keep(request.form.get("ind_factor"), s.ind_factor)
        s.street_factor = keep(request.form.get("street_factor"), s.street_factor)
        s.water_factor = keep(request.form.get("water_factor"), s.water_factor)
        s.heat_factor  = keep(request.form.get("heat_factor"), s.heat_factor)
        s.flow_factor  = keep(request.form.get("flow_factor"), s.flow_factor)

        s.tariff_per_kwh = keep(request.form.get("tariff_per_kwh"), s.tariff_per_kwh)
        s.base_charge    = keep(request.form.get("base_charge"), s.base_charge)
        s.allocation_method = request.form.get("allocation_method") or s.allocation_method

        s.kakao_rest_key = request.form.get("kakao_rest_key") or s.kakao_rest_key
        s.kakao_access_token = request.form.get("kakao_access_token") or s.kakao_access_token
        s.kakao_friend_uuid  = request.form.get("kakao_friend_uuid") or s.kakao_friend_uuid

        db.session.commit()
        flash("저장되었습니다.")
        return redirect(url_for("ui_settings"))
        s.public_base_url = request.form.get("public_base_url") or s.public_base_url

    body = render_template_string("""
<h5>설정</h5>
<form method="post" class="row g-2">
  <div class="col-4 col-md-2"><label class="form-label">고압계수</label><input class="form-control" name="hv_factor" value="{{s.hv_factor}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">산업계수</label><input class="form-control" name="ind_factor" value="{{s.ind_factor}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">가로등계수</label><input class="form-control" name="street_factor" value="{{s.street_factor}}"></div>

  <div class="col-4 col-md-2"><label class="form-label">급수계수</label><input class="form-control" name="water_factor" value="{{s.water_factor}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">열량계수</label><input class="form-control" name="heat_factor" value="{{s.heat_factor}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">유량계수</label><input class="form-control" name="flow_factor" value="{{s.flow_factor}}"></div>

  <div class="col-4 col-md-2"><label class="form-label">kWh 단가</label><input class="form-control" name="tariff_per_kwh" value="{{s.tariff_per_kwh}}"></div>
  <div class="col-4 col-md-2"><label class="form-label">기본요금</label><input class="form-control" name="base_charge" value="{{s.base_charge}}"></div>
  <div class="col-4 col-md-3"><label class="form-label">배분방식</label><input class="form-control" name="allocation_method" value="{{s.allocation_method}}"></div>

  <div class="col-12"><hr></div>
  <div class="col-12"><strong>카카오 전송(선택)</strong></div>
  <div class="col-12 col-md-4"><label class="form-label">REST Key</label><input class="form-control" name="kakao_rest_key" value="{{s.kakao_rest_key or ''}}"></div>
  <div class="col-12 col-md-4"><label class="form-label">Access Token</label><input class="form-control" name="kakao_access_token" value="{{s.kakao_access_token or ''}}"></div>
  <div class="col-12 col-md-4"><label class="form-label">Friend UUID</label><input class="form-control" name="kakao_friend_uuid" value="{{s.kakao_friend_uuid or ''}}"></div>
  
<div class="col-12"><hr></div>
  <div class="col-12 col-md-6">
    <label class="form-label">공개 URL Prefix(선택)</label>
    <input class="form-control" name="public_base_url" value="{{s.public_base_url or ''}}" placeholder="예: https://files.example.com/apt/">
    <div class="form-text">여기에 설정하면 파일/첨부 전송 시 해당 URL로 링크를 붙여 보냅니다.</div>
  </div>
  <div class="col-12 mt-2"><button class="btn btn-primary">저장</button>
  <a class="btn btn-secondary" href="{{ url_for('ui_home') }}">돌아가기</a></div>
</form>
""", s=s)
    return render("설정", body)

# ───────────────────────────────────────────────────────────────────
# 12) 서버 시작: 테이블 생성→컬럼 보강→설정 1행 보장→실행
# ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        auto_migrate_columns()   # ← 누락 컬럼 자동 보강 (저장 누락 방지 핵심)
        Settings.get()           # ← ID=1 기본 설정 생성
    # 같은 폰 브라우저에서 접속: http://127.0.0.1:8000/
    app.run(host="127.0.0.1", port=8000, debug=False, use_reloader=False, threaded=False)