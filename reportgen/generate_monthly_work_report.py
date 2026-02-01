import os
import json
import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader, select_autoescape

# PDF 엔진
from weasyprint import HTML

# -----------------------------
# Config
# -----------------------------
DB_PATH = os.getenv("KA_DB_PATH", "ka.db")  # 업무 DB (work_orders 등이 있는 DB)
RUNS_DB_PATH = os.getenv("KA_REPORT_RUNS_DB_PATH", os.path.join(os.path.dirname(__file__), "report_runs.sqlite3"))

SITE_NAME = os.getenv("KA_SITE_NAME", "○○아파트")
DEPT_NAME = os.getenv("KA_DEPT_NAME", "관리사무소")
APPROVER_NAME = os.getenv("KA_APPROVER_NAME", "홍길동")

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
TEMPLATE_FILE = "report_work_monthly_a4.html"

OUTPUT_ROOT = os.getenv("KA_REPORT_OUTPUT_ROOT", os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports"))

TOP_WORKS_LIMIT = int(os.getenv("KA_REPORT_TOP_WORKS_LIMIT", "20"))


# -----------------------------
# Helpers
# -----------------------------
def ym_to_period(ym: str) -> tuple[date, date, str]:
    """ym='YYYY-MM' -> (date_from, date_to_exclusive, period_label)"""
    y, m = ym.split("-")
    y, m = int(y), int(m)
    d_from = date(y, m, 1)
    d_to = (d_from + relativedelta(months=1))
    period_label = f"{y}년 {m:02d}월"
    return d_from, d_to, period_label


def fmt_money(n) -> str:
    try:
        return f"{int(round(float(n))):,}"
    except Exception:
        return "0"


def fmt_float_1(n) -> str:
    try:
        return f"{float(n):.1f}"
    except Exception:
        return "0.0"


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# -----------------------------
# Report Runs DB (doc_no)
# -----------------------------
def init_runs_db():
    os.makedirs(os.path.dirname(RUNS_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(RUNS_DB_PATH)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS report_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_type TEXT NOT NULL,
        period_ym TEXT NOT NULL,
        seq INTEGER NOT NULL,
        doc_no TEXT NOT NULL UNIQUE,
        generated_at TEXT NOT NULL,
        generated_by TEXT,
        pdf_path TEXT,
        pdf_sha256 TEXT,
        json_path TEXT,
        html_path TEXT
      );
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_report_runs_period_seq ON report_runs(report_type, period_ym, seq);")
    conn.commit()
    conn.close()


def issue_doc_no(report_type: str, period_ym: str, generated_by: str = "system") -> tuple[str, int]:
    """
    문서번호: RPT-WORK-YYYY-MM-#### (월별 증가)
    """
    conn = sqlite3.connect(RUNS_DB_PATH)
    conn.isolation_level = None  # manual transaction
    conn.execute("BEGIN IMMEDIATE;")
    try:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM report_runs WHERE report_type=? AND period_ym=?",
            (report_type, period_ym),
        ).fetchone()
        next_seq = int(row[0]) + 1
        doc_no = f"{report_type}-{period_ym}-{next_seq:04d}"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "INSERT INTO report_runs(report_type, period_ym, seq, doc_no, generated_at, generated_by) VALUES (?,?,?,?,?,?)",
            (report_type, period_ym, next_seq, doc_no, now, generated_by),
        )
        conn.execute("COMMIT;")
        return doc_no, next_seq
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.close()


def finalize_run(doc_no: str, pdf_path: str, pdf_sha: str, json_path: str, html_path: str):
    conn = sqlite3.connect(RUNS_DB_PATH)
    conn.execute(
        "UPDATE report_runs SET pdf_path=?, pdf_sha256=?, json_path=?, html_path=? WHERE doc_no=?",
        (pdf_path, pdf_sha, json_path, html_path, doc_no),
    )
    conn.commit()
    conn.close()


# -----------------------------
# Data fetch (SQL)
# -----------------------------
@dataclass
class Period:
    ym: str
    date_from: str
    date_to: str
    label: str


def q_one(cur, sql: str, params: tuple):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else 0


def q_all(cur, sql: str, params: tuple):
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return rows


def fetch_report_data(db_path: str, period: Period):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    date_from = period.date_from
    date_to = period.date_to

    # --- KPI
    total_done = q_one(cur, """
        SELECT COUNT(*) FROM work_orders
        WHERE status='DONE' AND completed_at >= ? AND completed_at < ?
    """, (date_from, date_to))

    from_inspection = q_one(cur, """
        SELECT COUNT(*) FROM work_orders
        WHERE status='DONE' AND completed_at >= ? AND completed_at < ?
          AND source_type='INSPECTION'
    """, (date_from, date_to))

    from_complaint = q_one(cur, """
        SELECT COUNT(*) FROM work_orders
        WHERE status='DONE' AND completed_at >= ? AND completed_at < ?
          AND source_type='COMPLAINT'
    """, (date_from, date_to))

    emergency_cnt = q_one(cur, """
        SELECT COUNT(*) FROM work_orders
        WHERE status='DONE' AND completed_at >= ? AND completed_at < ?
          AND (is_emergency=1 OR priority=1)
    """, (date_from, date_to))

    # avg lead days
    avg_lead_days = q_one(cur, """
        SELECT AVG((julianday(completed_at) - julianday(created_at))) AS avg_days
        FROM work_orders
        WHERE status='DONE' AND completed_at >= ? AND completed_at < ?
    """, (date_from, date_to))
    avg_lead_days = fmt_float_1(avg_lead_days)

    # rejected_work_cnt (기간 내 반려 이벤트가 1회 이상인 작업 수)
    rejected_work_cnt = q_one(cur, """
        SELECT COUNT(DISTINCT entity_id) FROM events
        WHERE entity_type='WORK_ORDER'
          AND event_type IN ('REVIEW_REJECT','APPROVE_REJECT')
          AND created_at >= ? AND created_at < ?
    """, (date_from, date_to))

    # rework_cnt (반려 후 IN_PROGRESS로 복귀)
    rework_cnt = q_one(cur, """
        WITH rej AS (
          SELECT entity_id AS work_order_id, MIN(created_at) AS first_reject_at
          FROM events
          WHERE entity_type='WORK_ORDER'
            AND event_type IN ('REVIEW_REJECT','APPROVE_REJECT')
            AND created_at >= ? AND created_at < ?
          GROUP BY entity_id
        ),
        back_to_progress AS (
          SELECT DISTINCT e.entity_id AS work_order_id
          FROM events e
          JOIN rej r ON r.work_order_id = e.entity_id
          WHERE e.entity_type='WORK_ORDER'
            AND e.event_type='STATUS_CHANGE'
            AND e.to_status='IN_PROGRESS'
            AND e.created_at > r.first_reject_at
        )
        SELECT COUNT(*) FROM back_to_progress
    """, (date_from, date_to))

    # --- by_source_type
    by_source_type = q_all(cur, """
        SELECT source_type, COUNT(*) AS cnt
        FROM work_orders
        WHERE status='DONE' AND completed_at >= ? AND completed_at < ?
        GROUP BY source_type
        ORDER BY cnt DESC
    """, (date_from, date_to))

    # 라벨 정리
    source_label_map = {
        "INSPECTION": "점검 후 조치",
        "COMPLAINT": "민원 조치",
        "MAINTENANCE": "정기 정비",
        "OTHER": "기타",
    }
    by_source_type = [{"label": source_label_map.get(r["source_type"], r["source_type"]), "cnt": r["cnt"]} for r in by_source_type]

    # --- by_category
    by_category = q_all(cur, """
        SELECT c.name AS category_name, COUNT(*) AS cnt
        FROM work_orders w
        LEFT JOIN categories c ON c.id = w.category_id
        WHERE w.status='DONE' AND w.completed_at >= ? AND w.completed_at < ?
        GROUP BY c.name
        ORDER BY cnt DESC
    """, (date_from, date_to))

    # --- by_common_location
    by_common_location = q_all(cur, """
        SELECT l.name AS location_name, COUNT(*) AS cnt
        FROM work_orders w
        JOIN locations l ON l.id = w.location_id
        WHERE w.status='DONE' AND w.completed_at >= ? AND w.completed_at < ?
          AND l.type='COMMON'
        GROUP BY l.name
        ORDER BY cnt DESC
    """, (date_from, date_to))

    # --- by_assignee
    by_assignee = q_all(cur, """
        SELECT u.name AS assignee_name,
               COUNT(*) AS done_cnt,
               AVG((julianday(w.completed_at) - julianday(w.created_at))) AS avg_days
        FROM work_orders w
        LEFT JOIN users u ON u.id = w.assigned_to
        WHERE w.status='DONE' AND w.completed_at >= ? AND w.completed_at < ?
        GROUP BY u.name
        ORDER BY done_cnt DESC
    """, (date_from, date_to))
    for r in by_assignee:
        r["avg_lead_days"] = fmt_float_1(r.get("avg_days", 0))

    # --- procurement link
    done_with_pr_cnt = q_one(cur, """
        SELECT COUNT(DISTINCT w.id)
        FROM work_orders w
        JOIN purchase_requests pr ON pr.work_order_id = w.id
        WHERE w.status='DONE' AND w.completed_at >= ? AND w.completed_at < ?
          AND pr.status <> 'CANCELED'
    """, (date_from, date_to))

    total_po_amount = q_one(cur, """
        SELECT COALESCE(SUM(po.total_amount), 0)
        FROM purchase_orders po
        JOIN purchase_requests pr ON pr.id = po.pr_id
        JOIN work_orders w ON w.id = pr.work_order_id
        WHERE w.status='DONE' AND w.completed_at >= ? AND w.completed_at < ?
          AND po.order_date >= ? AND po.order_date < ?
          AND po.status <> 'CANCELED'
    """, (date_from, date_to, date_from, date_to))

    top_items = q_all(cur, """
        SELECT pol.item_name AS item_name,
               COALESCE(SUM(pol.line_amount),0) AS total_amount
        FROM purchase_order_lines pol
        JOIN purchase_orders po ON po.id = pol.po_id AND po.status <> 'CANCELED'
        JOIN purchase_requests pr ON pr.id = po.pr_id
        JOIN work_orders w ON w.id = pr.work_order_id
        WHERE w.status='DONE' AND w.completed_at >= ? AND w.completed_at < ?
          AND po.order_date >= ? AND po.order_date < ?
        GROUP BY pol.item_name
        ORDER BY total_amount DESC
        LIMIT 3
    """, (date_from, date_to, date_from, date_to))

    top_items_amount_sum = sum([float(r["total_amount"]) for r in top_items]) if top_items else 0
    top_items_labels = " / ".join([f"{r['item_name']}({fmt_money(r['total_amount'])})" for r in top_items]) if top_items else "-"

    # --- top works list
    top_works = q_all(cur, f"""
        SELECT
          w.work_code,
          l.name AS location_name,
          c.name AS category_name,
          w.title,
          u.name AS assignee_name,
          substr(w.completed_at,1,10) AS completed_date
        FROM work_orders w
        LEFT JOIN locations l ON l.id = w.location_id
        LEFT JOIN categories c ON c.id = w.category_id
        LEFT JOIN users u ON u.id = w.assigned_to
        WHERE w.status='DONE' AND w.completed_at >= ? AND w.completed_at < ?
        ORDER BY w.completed_at DESC, w.work_code DESC
        LIMIT {TOP_WORKS_LIMIT}
    """, (date_from, date_to))

    conn.close()

    return {
        "kpi": {
            "total_done": total_done,
            "from_inspection": from_inspection,
            "from_complaint": from_complaint,
            "emergency_cnt": emergency_cnt,
            "avg_lead_days": avg_lead_days,
            "rejected_work_cnt": rejected_work_cnt,
            "rework_cnt": rework_cnt,
        },
        "by_source_type": by_source_type,
        "by_category": by_category,
        "by_common_location": by_common_location,
        "by_assignee": [
            {"assignee_name": r["assignee_name"] or "-", "done_cnt": r["done_cnt"], "avg_lead_days": r["avg_lead_days"]}
            for r in by_assignee
        ],
        "proc": {
            "done_with_pr_cnt": done_with_pr_cnt,
            "total_po_amount": fmt_money(total_po_amount),
            "top_items_amount_sum": fmt_money(top_items_amount_sum),
            "top_items_labels": top_items_labels,
        },
        "top_works_limit": TOP_WORKS_LIMIT,
        "top_works": top_works,
    }


# -----------------------------
# Render + Export
# -----------------------------
def render_html(template_dir: str, template_file: str, data: dict) -> str:
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(["html", "xml"]),
    )
    # 템플릿이 Handlebars 스타일로 되어 있다면(Jinja가 아니라면),
    # Jinja 버전으로 바꾸거나 템플릿 문법을 통일해야 합니다.
    # 여기서는 Jinja 형태로 사용하는 것을 권장합니다.
    tpl = env.get_template(template_file)
    return tpl.render(**data)


def ensure_out_dir(ym: str) -> str:
    y, m = ym.split("-")
    out_dir = os.path.join(OUTPUT_ROOT, y, m)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def main():
    import argparse

    parser = argparse.ArgumentParser(description="월간 작업 실적 보고서 생성기")
    parser.add_argument("--ym", required=True, help="YYYY-MM (예: 2026-01)")
    parser.add_argument("--generated-by", default="system", help="생성자 표기(옵션)")
    parser.add_argument("--notes-special", default="", help="특이사항(줄바꿈 가능)")
    parser.add_argument("--notes-next", default="", help="다음 달 중점 관리(줄바꿈 가능)")
    args = parser.parse_args()

    init_runs_db()

    d_from, d_to, period_label = ym_to_period(args.ym)
    period = Period(
        ym=args.ym,
        date_from=d_from.isoformat(),
        date_to=d_to.isoformat(),
        label=period_label,
    )

    # 1) 집계
    agg = fetch_report_data(DB_PATH, period)

    # 2) 문서번호 발급
    doc_no, _seq = issue_doc_no("RPT-WORK", period.ym, generated_by=args.generated_by)

    # 3) 템플릿 데이터 구성
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    payload = {
        "period_label": period.label,
        "date_from": period.date_from,
        "date_to_exclusive": period.date_to,
        "site_name": SITE_NAME,
        "dept_name": DEPT_NAME,
        "generated_at": now_str,
        "approver_name": APPROVER_NAME,
        "doc_no": doc_no,

        "kpi": agg["kpi"],
        "by_source_type": agg["by_source_type"],
        "by_category": agg["by_category"],
        "by_common_location": agg["by_common_location"],
        "by_assignee": agg["by_assignee"],

        "proc": agg["proc"],

        "top_works_limit": agg["top_works_limit"],
        "top_works": agg["top_works"],

        "notes": {
            "special": args.notes_special,
            "next_focus": args.notes_next,
        },
    }

    # 4) 출력 경로
    out_dir = ensure_out_dir(period.ym)
    base_name = doc_no.replace(":", "-")
    json_path = os.path.join(out_dir, f"{base_name}.json")
    html_path = os.path.join(out_dir, f"{base_name}.html")
    pdf_path = os.path.join(out_dir, f"{base_name}.pdf")

    # 5) JSON 저장(재현성 확보)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # 6) HTML 렌더링
    html_str = render_html(TEMPLATE_DIR, TEMPLATE_FILE, payload)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_str)

    # 7) PDF 생성
    HTML(string=html_str, base_url=out_dir).write_pdf(pdf_path)

    # 8) 해시/이력 확정
    pdf_sha = sha256_file(pdf_path)
    finalize_run(doc_no, pdf_path, pdf_sha, json_path, html_path)

    print("OK")
    print(f"doc_no: {doc_no}")
    print(f"pdf:    {pdf_path}")
    print(f"sha256: {pdf_sha}")


if __name__ == "__main__":
    main()
