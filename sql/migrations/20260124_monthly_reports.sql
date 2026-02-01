-- 월간 보고서 스냅샷/결재용 테이블
CREATE TABLE IF NOT EXISTS monthly_reports (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  report_code   TEXT NOT NULL UNIQUE,        -- 예: MWR-2026-01-0001
  yyyymm        TEXT NOT NULL,               -- 예: 2026-01
  status        TEXT NOT NULL DEFAULT 'DRAFT', -- DRAFT/SUBMITTED/APPROVED
  created_by    TEXT NOT NULL,               -- login
  created_at    TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at    TEXT NOT NULL DEFAULT (datetime('now')),
  submitted_at  TEXT,
  approved_at   TEXT,
  approved_by   TEXT,                        -- login
  payload_json  TEXT NOT NULL                -- 스냅샷 JSON
);

CREATE INDEX IF NOT EXISTS idx_monthly_reports_yyyymm ON monthly_reports(yyyymm);
CREATE INDEX IF NOT EXISTS idx_monthly_reports_status ON monthly_reports(status);

-- updated_at 자동 갱신 트리거(없으면 생성)
CREATE TRIGGER IF NOT EXISTS trg_monthly_reports_updated_at
AFTER UPDATE ON monthly_reports
FOR EACH ROW
BEGIN
  UPDATE monthly_reports SET updated_at = datetime('now') WHERE id = NEW.id;
END;
