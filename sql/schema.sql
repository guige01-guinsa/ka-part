-- =========================================================
-- ka-part SQLite DDL v1.0
-- 범위: 점검(1순위) + 작업/이력/첨부 + 구매(PR/PO) + 기본 마스터
-- 주의: SQLite는 BOOLEAN이 없으므로 INTEGER(0/1) 사용
-- =========================================================
PRAGMA foreign_keys = ON;

-- -------------------------
-- 0) 공통: 코드 시퀀스(권장)
-- -------------------------
CREATE TABLE IF NOT EXISTS code_sequences (
  key TEXT PRIMARY KEY,                 -- 예: 'WO-2026', 'PR-2026'
  last_seq INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- =========================================================
-- 1) 사용자/권한
-- =========================================================
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  login TEXT NOT NULL UNIQUE,           -- 사번/아이디
  phone TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS roles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,            -- TECH/LEAD/MANAGER/ACCOUNTING
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_roles (
  user_id INTEGER NOT NULL,
  role_id INTEGER NOT NULL,
  PRIMARY KEY (user_id, role_id),
  FOREIGN KEY (user_id) REFERENCES users(id),
  FOREIGN KEY (role_id) REFERENCES roles(id)
);

-- =========================================================
-- 2) 위치/분류/설비(자산)
-- =========================================================
CREATE TABLE IF NOT EXISTS locations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL CHECK (type IN ('HOUSEHOLD','COMMON')),
  code TEXT NOT NULL UNIQUE,            -- LOC_101_12_1203, LOC_ELECROOM 등
  name TEXT NOT NULL,                   -- 표시명(전기실, 101동 12층 1203호 등)
  building TEXT,                        -- HOUSEHOLD일 때 사용(101동)
  floor TEXT,                           -- 12층
  unit TEXT,                            -- 1203호
  parent_id INTEGER,                    -- 계층(동→층→실)
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (parent_id) REFERENCES locations(id)
);

CREATE TABLE IF NOT EXISTS categories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,            -- ELEC/FIRE/ELEV/MECH/ARCH/COMM
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_code TEXT NOT NULL UNIQUE,      -- 설비관리번호
  name TEXT NOT NULL,
  category_id INTEGER NOT NULL,
  location_id INTEGER NOT NULL,
  manufacturer TEXT,
  model TEXT,
  installed_on TEXT,                    -- YYYY-MM-DD
  criticality INTEGER NOT NULL DEFAULT 3 CHECK (criticality BETWEEN 1 AND 5),
  inspection_cycle_days INTEGER,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (category_id) REFERENCES categories(id),
  FOREIGN KEY (location_id) REFERENCES locations(id)
);

-- =========================================================
-- 3) 점검(템플릿/결과/측정)
-- =========================================================
CREATE TABLE IF NOT EXISTS checklist_templates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,            -- ELEC_MONTHLY, FIRE_QUARTERLY 등
  name TEXT NOT NULL,
  category_id INTEGER NOT NULL,
  scope_type TEXT NOT NULL CHECK (scope_type IN ('ASSET','LOCATION')),
  requires_photo INTEGER NOT NULL DEFAULT 1,
  requires_signature INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE IF NOT EXISTS checklist_template_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  template_id INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  item_text TEXT NOT NULL,
  answer_type TEXT NOT NULL CHECK (answer_type IN ('OK_NG','OK_WARN_NG','TEXT','NUMBER')),
  is_required INTEGER NOT NULL DEFAULT 1,
  hint TEXT,
  FOREIGN KEY (template_id) REFERENCES checklist_templates(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_template_items_template_seq
ON checklist_template_items(template_id, seq);

CREATE TABLE IF NOT EXISTS inspections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  template_id INTEGER NOT NULL,
  category_id INTEGER NOT NULL,
  scope_type TEXT NOT NULL CHECK (scope_type IN ('ASSET','LOCATION')),
  asset_id INTEGER,                     -- scope_type=ASSET이면 필수(앱에서 강제)
  location_id INTEGER,                  -- scope_type=LOCATION이면 필수(앱에서 강제)
  performed_by INTEGER NOT NULL,
  performed_at TEXT NOT NULL DEFAULT (datetime('now')),
  overall_result TEXT NOT NULL CHECK (overall_result IN ('PASS','WARN','FAIL')),
  summary_note TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (template_id) REFERENCES checklist_templates(id),
  FOREIGN KEY (category_id) REFERENCES categories(id),
  FOREIGN KEY (asset_id) REFERENCES assets(id),
  FOREIGN KEY (location_id) REFERENCES locations(id),
  FOREIGN KEY (performed_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS inspection_answers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  inspection_id INTEGER NOT NULL,
  template_item_id INTEGER NOT NULL,
  answer_text TEXT,
  answer_num REAL,
  result TEXT CHECK (result IN ('OK','WARN','NG')),
  note TEXT,
  FOREIGN KEY (inspection_id) REFERENCES inspections(id),
  FOREIGN KEY (template_item_id) REFERENCES checklist_template_items(id)
);

CREATE TABLE IF NOT EXISTS measurements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  inspection_id INTEGER NOT NULL,
  measure_type TEXT NOT NULL,           -- LEAKAGE_CURRENT 등
  value REAL NOT NULL,
  unit TEXT NOT NULL,
  threshold REAL,
  judgement TEXT NOT NULL CHECK (judgement IN ('OK','WARN','NG')),
  measured_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (inspection_id) REFERENCES inspections(id)
);

-- =========================================================
-- 4) 작업(WorkOrder) + 이력(Event) + 첨부(Attachment)
-- =========================================================
CREATE TABLE IF NOT EXISTS work_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  work_code TEXT NOT NULL UNIQUE,       -- WO-YYYY-###### (서버에서 발급)
  source_type TEXT NOT NULL CHECK (source_type IN ('INSPECTION','COMPLAINT','MAINTENANCE','OTHER')),
  source_id INTEGER,                    -- inspections.id 등(다형 참조)
  category_id INTEGER NOT NULL,
  asset_id INTEGER,
  location_id INTEGER,
  title TEXT NOT NULL,
  description TEXT,
  priority INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
  is_emergency INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN (
    'NEW','ASSIGNED','IN_PROGRESS','REVIEW','APPROVED','DONE','HOLD','REJECTED','CANCELED'
  )),
  requested_by INTEGER NOT NULL,
  assigned_to INTEGER,                  -- 단일 담당(간단 운영)
  due_at TEXT,
  started_at TEXT,
  completed_at TEXT,
  closed_at TEXT,
  result_note TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (category_id) REFERENCES categories(id),
  FOREIGN KEY (asset_id) REFERENCES assets(id),
  FOREIGN KEY (location_id) REFERENCES locations(id),
  FOREIGN KEY (requested_by) REFERENCES users(id),
  FOREIGN KEY (assigned_to) REFERENCES users(id)
);

-- (선택) 다중 담당이 필요하면 사용
CREATE TABLE IF NOT EXISTS work_assignments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  work_order_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  role_in_work TEXT NOT NULL DEFAULT 'LEAD', -- LEAD/ASSIST
  assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS work_time_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  work_order_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  minutes INTEGER NOT NULL CHECK (minutes >= 0),
  work_date TEXT NOT NULL,              -- YYYY-MM-DD
  note TEXT,
  FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL,            -- WORK_ORDER/INSPECTION/PR/PO/...
  entity_id INTEGER NOT NULL,
  event_type TEXT NOT NULL,             -- CREATE/STATUS_CHANGE/...
  actor_id INTEGER NOT NULL,
  from_status TEXT,
  to_status TEXT,
  note TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (actor_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS ix_events_entity ON events(entity_type, entity_id, created_at);

CREATE TABLE IF NOT EXISTS attachments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  file_name TEXT NOT NULL,
  file_path TEXT NOT NULL,              -- 저장 경로/키
  mime_type TEXT,
  file_size INTEGER,
  sha256 TEXT,
  uploaded_by INTEGER NOT NULL,
  uploaded_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (uploaded_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS ix_attachments_entity ON attachments(entity_type, entity_id);

-- =========================================================
-- 5) 구매(Procurement) - 최소 PR/PO
-- =========================================================
CREATE TABLE IF NOT EXISTS vendors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  phone TEXT,
  email TEXT,
  is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT UNIQUE,
  name TEXT NOT NULL,
  spec TEXT,
  default_unit TEXT NOT NULL DEFAULT 'EA',
  category_id INTEGER,
  is_active INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE IF NOT EXISTS purchase_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pr_code TEXT NOT NULL UNIQUE,         -- PR-YYYY-######
  work_order_id INTEGER,                -- 작업과 연결(핵심)
  requested_by INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('DRAFT','REVIEW','APPROVED','REJECTED','CANCELED','ORDERED')),
  need_by TEXT,                         -- YYYY-MM-DD
  note TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (work_order_id) REFERENCES work_orders(id),
  FOREIGN KEY (requested_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS ix_pr_work_order ON purchase_requests(work_order_id);
CREATE INDEX IF NOT EXISTS ix_pr_status_created ON purchase_requests(status, created_at);

CREATE TABLE IF NOT EXISTS purchase_request_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pr_id INTEGER NOT NULL,
  item_id INTEGER,                      -- 마스터 품목 사용 시
  item_name TEXT NOT NULL,              -- 초기 운영은 자유 입력 허용
  qty REAL NOT NULL CHECK (qty > 0),
  unit TEXT NOT NULL DEFAULT 'EA',
  target_price REAL,
  spec_note TEXT,
  FOREIGN KEY (pr_id) REFERENCES purchase_requests(id),
  FOREIGN KEY (item_id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS purchase_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_code TEXT NOT NULL UNIQUE,         -- PO-YYYY-######
  pr_id INTEGER NOT NULL,
  vendor_id INTEGER,
  ordered_by INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('ISSUED','DELIVERING','DELIVERED','CANCELED')),
  order_date TEXT NOT NULL,             -- YYYY-MM-DD
  expected_date TEXT,                   -- YYYY-MM-DD
  total_amount REAL DEFAULT 0,
  FOREIGN KEY (pr_id) REFERENCES purchase_requests(id),
  FOREIGN KEY (vendor_id) REFERENCES vendors(id),
  FOREIGN KEY (ordered_by) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS ix_po_vendor_orderdate ON purchase_orders(vendor_id, order_date);

CREATE TABLE IF NOT EXISTS purchase_order_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  po_id INTEGER NOT NULL,
  item_id INTEGER,
  item_name TEXT NOT NULL,
  qty REAL NOT NULL CHECK (qty > 0),
  unit TEXT NOT NULL DEFAULT 'EA',
  unit_price REAL NOT NULL DEFAULT 0,
  line_amount REAL NOT NULL DEFAULT 0,
  FOREIGN KEY (po_id) REFERENCES purchase_orders(id),
  FOREIGN KEY (item_id) REFERENCES items(id)
);

-- (옵션) 납품/검수까지 확장
CREATE TABLE IF NOT EXISTS goods_receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  gr_code TEXT NOT NULL UNIQUE,         -- GR-YYYY-######
  po_id INTEGER NOT NULL,
  received_by INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('RECEIVED','INSPECTED','REJECTED')),
  received_at TEXT NOT NULL DEFAULT (datetime('now')),
  note TEXT,
  FOREIGN KEY (po_id) REFERENCES purchase_orders(id),
  FOREIGN KEY (received_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS goods_receipt_lines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  gr_id INTEGER NOT NULL,
  po_line_id INTEGER NOT NULL,
  received_qty REAL NOT NULL DEFAULT 0,
  accepted_qty REAL NOT NULL DEFAULT 0,
  reject_reason TEXT,
  FOREIGN KEY (gr_id) REFERENCES goods_receipts(id),
  FOREIGN KEY (po_line_id) REFERENCES purchase_order_lines(id)
);

-- =========================================================
-- 6) 보고서 실행 이력(월간 작업 실적 등)
-- =========================================================
CREATE TABLE IF NOT EXISTS report_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  report_type TEXT NOT NULL,            -- 예: RPT-WORK
  period_ym TEXT NOT NULL,              -- YYYY-MM
  seq INTEGER NOT NULL,
  doc_no TEXT NOT NULL UNIQUE,          -- RPT-WORK-YYYY-MM-####

  generated_at TEXT NOT NULL DEFAULT (datetime('now')),
  generated_by TEXT,

  pdf_path TEXT,
  pdf_sha256 TEXT,
  json_path TEXT,
  html_path TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_report_runs_period_seq
ON report_runs(report_type, period_ym, seq);

-- =========================================================
-- 7) 성능 인덱스(보고/검색)
-- =========================================================
-- work_orders: 보고서/대시보드
CREATE INDEX IF NOT EXISTS ix_wo_status_completed ON work_orders(status, completed_at);
CREATE INDEX IF NOT EXISTS ix_wo_location_completed ON work_orders(location_id, completed_at);
CREATE INDEX IF NOT EXISTS ix_wo_category_completed ON work_orders(category_id, completed_at);
CREATE INDEX IF NOT EXISTS ix_wo_assigned_completed ON work_orders(assigned_to, completed_at);
CREATE INDEX IF NOT EXISTS ix_wo_source_completed ON work_orders(source_type, completed_at);

-- inspections: 이력 조회
CREATE INDEX IF NOT EXISTS ix_insp_performed_at ON inspections(performed_at);
CREATE INDEX IF NOT EXISTS ix_insp_asset_performed ON inspections(asset_id, performed_at);
CREATE INDEX IF NOT EXISTS ix_insp_location_performed ON inspections(location_id, performed_at);

-- measurements: 유형별 조회
CREATE INDEX IF NOT EXISTS ix_meas_insp_type ON measurements(inspection_id, measure_type);

-- =========================================================
-- 8) 기본 시드(선택)
-- =========================================================
-- categories 기본값 예시(필요 시 사용)
-- INSERT OR IGNORE INTO categories(code, name) VALUES
-- ('ELEC','전기'),('FIRE','소방'),('ELEV','승강기'),('MECH','기계/설비'),('ARCH','건축'),('COMM','통신');

-- roles 기본값 예시(필요 시 사용)
-- INSERT OR IGNORE INTO roles(code, name) VALUES
-- ('TECH','시설기사'),('LEAD','시설과장'),('MANAGER','관리소장'),('ACCOUNTING','경리');
