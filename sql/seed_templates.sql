PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;

-- ---------------------------------------------------------
-- 전기(ELEC) category_id 확보
-- ---------------------------------------------------------
-- 없으면 categories는 seed.sql에서 이미 들어가 있어야 함
-- category_id를 동적으로 찾아서 template에 사용

-- ---------------------------------------------------------
-- 템플릿: 전기실 월간 점검 (LOCATION 기준)
-- ---------------------------------------------------------
INSERT OR IGNORE INTO checklist_templates(
  code, name, category_id, scope_type, requires_photo, requires_signature, is_active
)
SELECT
  'ELECROOM_MONTHLY',
  '전기실 월간 점검',
  c.id,
  'LOCATION',
  1,
  0,
  1
FROM categories c
WHERE c.code='ELEC';

-- template_id 조회용(후속 insert에서 사용)
-- ---------------------------------------------------------
-- 항목 구성 (OK/WARN/NG + TEXT + NUMBER 혼합)
-- ---------------------------------------------------------
INSERT OR IGNORE INTO checklist_template_items(template_id, seq, item_text, answer_type, is_required, hint)
SELECT t.id, 10, '수변전반 외관/표시 상태(오염·손상·표기)', 'OK_WARN_NG', 1, '표기훼손, 문 개폐, 누유 흔적 확인'
FROM checklist_templates t WHERE t.code='ELECROOM_MONTHLY';

INSERT OR IGNORE INTO checklist_template_items(template_id, seq, item_text, answer_type, is_required, hint)
SELECT t.id, 20, '차단기/개폐기 이상유무(이상음·과열·변색)', 'OK_WARN_NG', 1, '이상음/냄새/변색 시 즉시 조치'
FROM checklist_templates t WHERE t.code='ELECROOM_MONTHLY';

INSERT OR IGNORE INTO checklist_template_items(template_id, seq, item_text, answer_type, is_required, hint)
SELECT t.id, 30, '실내 청결/가연물 적치 여부', 'OK_NG', 1, '전기실 가연물 적치 금지'
FROM checklist_templates t WHERE t.code='ELECROOM_MONTHLY';

INSERT OR IGNORE INTO checklist_template_items(template_id, seq, item_text, answer_type, is_required, hint)
SELECT t.id, 40, '특이사항(서술)', 'TEXT', 0, '이상 발견 시 상세 기록'
FROM checklist_templates t WHERE t.code='ELECROOM_MONTHLY';

INSERT OR IGNORE INTO checklist_template_items(template_id, seq, item_text, answer_type, is_required, hint)
SELECT t.id, 50, '누설전류 측정값(mA) 기록', 'NUMBER', 0, '측정값 입력(필요 시 measurements에도 기록)'
FROM checklist_templates t WHERE t.code='ELECROOM_MONTHLY';

COMMIT;
