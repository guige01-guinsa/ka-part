from __future__ import annotations

import importlib
import io
import re
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


@pytest.fixture()
def app_client(tmp_path, monkeypatch):
    monkeypatch.setenv("ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("KA_STORAGE_ROOT", str(tmp_path))

    for name in (
        "app.main",
        "app.db",
        "app.engine_db",
        "app.legacy_import",
        "app.ops_db",
        "app.voice_db",
        "app.voice_service",
        "app.ai_service",
        "app.routes.core",
        "app.routes.engine",
        "app.routes.ops",
        "app.routes.voice",
    ):
        sys.modules.pop(name, None)

    db = importlib.import_module("app.db")
    engine_db = importlib.import_module("app.engine_db")
    ops_db = importlib.import_module("app.ops_db")
    voice_db = importlib.import_module("app.voice_db")

    monkeypatch.setattr(db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(engine_db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(ops_db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(voice_db, "DB_PATH", tmp_path / "engine_test.db")

    main = importlib.import_module("app.main")
    db.init_db()
    engine_db.init_engine_db()
    ops_db.init_ops_db()

    with TestClient(main.app) as client:
        yield client


def _bootstrap_admin_and_tenant(client: TestClient) -> str:
    created = client.post(
        "/api/auth/bootstrap",
        json={"login_id": "admin01", "name": "운영관리자", "password": "password123"},
    )
    assert created.status_code == 200

    tenant = client.post(
        "/api/admin/tenants",
        json={
            "tenant_id": "ys_thesharp",
            "name": "연산더샵",
            "site_code": "APT00001",
            "site_name": "연산더샵",
        },
    )
    assert tenant.status_code == 200
    return str(tenant.json()["item"]["api_key"])


def test_session_admin_can_run_new_mvp_engine_and_legacy_api_is_gone(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    classify = client.post(
        "/api/ai/classify",
        json={"tenant_id": "ys_thesharp", "text": "101동 엘리베이터가 멈췄어요"},
    )
    assert classify.status_code == 200
    assert classify.json()["item"]["type"] == "승강기"
    assert classify.json()["item"]["urgency"] == "긴급"

    created = client.post(
        "/api/complaints",
        json={
            "tenant_id": "ys_thesharp",
            "building": "101",
            "unit": "1203",
            "complainant_phone": "010-1111-2222",
            "channel": "전화",
            "content": "엘리베이터가 멈췄어요",
        },
    )
    assert created.status_code == 200
    item = created.json()["item"]
    assert item["type"] == "승강기"
    assert item["tenant_id"] == "ys_thesharp"
    assert item["complainant_phone"] == "010-1111-2222"

    listed = client.get("/api/complaints?tenant_id=ys_thesharp")
    assert listed.status_code == 200
    assert len(listed.json()["items"]) == 1

    report = client.get("/api/report/daily?tenant_id=ys_thesharp")
    assert report.status_code == 200
    assert report.json()["item"]["total"] == 1

    contracts = client.get("/api/modules/contracts")
    assert contracts.status_code == 200
    assert contracts.json()["allowed_modules"] == ["complaint_engine", "operations_admin"]

    assert client.get("/api/v1/complaints").status_code == 404


def test_api_key_flow_supports_complaints_dashboard_and_chat_digest(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    created = client.post(
        "/api/complaints",
        headers=headers,
        json={
            "building": "103",
            "unit": "805",
            "channel": "카톡",
            "content": "지하주차장 주차 민원이 계속 발생합니다",
        },
    )
    assert created.status_code == 200
    complaint_id = int(created.json()["item"]["id"])
    assert created.json()["item"]["tenant_id"] == "ys_thesharp"

    updated = client.put(
        f"/api/complaints/{complaint_id}",
        headers=headers,
        json={"status": "처리중", "manager": "김대리", "note": "현장 확인 예정"},
    )
    assert updated.status_code == 200
    assert updated.json()["item"]["status"] == "처리중"

    dashboard = client.get("/api/dashboard/summary", headers=headers)
    assert dashboard.status_code == 200
    assert dashboard.json()["item"]["pending_total"] >= 1

    digest = client.post(
        "/api/ai/kakao_digest",
        headers=headers,
        json={
            "text": "\n".join(
                [
                    "2026년 4월 7일 오전 9:00, 관리실 : 101동 엘리베이터 멈춤",
                    "2026년 4월 7일 오전 9:10, 관리실 : 103동 주차 문제 계속 발생",
                    "2026년 4월 7일 오전 10:10, 관리실 : 101동 엘리베이터 멈춤",
                ]
            )
        },
    )
    assert digest.status_code == 200
    item = digest.json()["item"]
    assert item["total"] >= 2
    assert "📊 일일 요약" in item["report_text"]


def test_kakao_digest_supports_images_with_filename_fallback(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    digest = client.post(
        "/api/ai/kakao_digest/images",
        headers=headers,
        data={"text": ""},
        files=[
            ("files", ("101동-엘리베이터-멈춤.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("files", ("103동-주차-민원.png", io.BytesIO(b"fake-image-2"), "image/png")),
        ],
    )
    assert digest.status_code == 200
    item = digest.json()["item"]
    assert item["input_image_count"] == 2
    assert item["total"] >= 2
    assert any(row["type"] == "승강기" for row in item["excel_rows"])
    assert "🖼 첨부 이미지 요약" in item["report_text"]
    assert "파일명 기반" in str(item["analysis_notice"])


def test_kakao_digest_explains_when_image_has_no_detected_issue(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    digest = client.post(
        "/api/ai/kakao_digest/images",
        headers=headers,
        data={"text": ""},
        files=[
            ("files", ("kakao-paste-plain.png", io.BytesIO(b"fake-image-1"), "image/png")),
        ],
    )
    assert digest.status_code == 200
    item = digest.json()["item"]
    assert item["total"] == 0
    assert item["image_analysis_model"] == "filename-fallback"
    assert "이미지 본문을 읽지 못해 파일명 기준으로만 확인했습니다." in str(item["analysis_notice"])


def test_kakao_digest_treats_building_and_unit_as_complaint_signal(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    digest = client.post(
        "/api/ai/kakao_digest/images",
        headers=headers,
        data={"text": ""},
        files=[
            ("files", ("101동-1203호-카톡캡처.png", io.BytesIO(b"fake-image-1"), "image/png")),
        ],
    )
    assert digest.status_code == 200
    item = digest.json()["item"]
    assert item["input_image_count"] == 1
    assert item["total"] == 1
    row = item["excel_rows"][0]
    assert row["building"] == "101"
    assert row["unit"] == "1203"
    assert row["status"] == "접수"


def test_kakao_digest_accepts_up_to_thirty_images(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    files = [
        ("files", (f"{index:02d}-현장사진.jpg", io.BytesIO(f"fake-image-{index}".encode("utf-8")), "image/jpeg"))
        for index in range(30)
    ]
    response = client.post(
        "/api/ai/kakao_digest/images",
        headers=headers,
        data={"text": ""},
        files=files,
    )
    assert response.status_code == 200
    assert response.json()["item"]["input_image_count"] == 30

    files = [
        ("files", (f"{index:02d}-현장사진.jpg", io.BytesIO(f"fake-image-{index}".encode("utf-8")), "image/jpeg"))
        for index in range(31)
    ]
    blocked = client.post(
        "/api/ai/kakao_digest/images",
        headers=headers,
        data={"text": ""},
        files=files,
    )
    assert blocked.status_code == 400
    assert "최대 30장" in blocked.json()["detail"]


def test_kakao_digest_pdf_supports_text_and_images(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/kakao_digest/pdf",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": "2026년 4월 8일 오전 9:00, 관리실 : 101동 엘리베이터 멈춤"},
        files=[
            ("files", ("101동-엘리베이터-멈춤.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert "attachment;" in response.headers.get("content-disposition", "")
    assert response.content.startswith(b"%PDF")


def test_kakao_digest_import_creates_complaints(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/kakao_digest/import",
        headers=headers,
        json={
            "tenant_id": "ys_thesharp",
            "channel": "카톡",
            "image_analysis_model": "gpt-5",
            "source_text": "카톡 테스트 원문",
            "rows": [
                {
                    "building": "101",
                    "unit": "1203",
                    "type": "누수",
                    "summary": "101동 1203호 누수 심함",
                    "urgency": "긴급",
                    "status": "접수",
                    "manager": "",
                    "content": "101동 1203호 누수 심함",
                },
                {
                    "building": "103",
                    "unit": "",
                    "type": "승강기",
                    "summary": "103동 엘리베이터 멈춤",
                    "urgency": "긴급",
                    "status": "접수",
                    "manager": "",
                    "content": "103동 엘리베이터 멈춤 긴급",
                },
            ],
        },
    )
    assert response.status_code == 200
    assert response.json()["created_count"] == 2

    listed = client.get("/api/complaints?tenant_id=ys_thesharp", headers=headers)
    assert listed.status_code == 200
    items = listed.json()["items"]
    assert len(items) == 2
    assert any(item["channel"] == "카톡" for item in items)
    for item in items:
        detail = client.get(f"/api/complaints/{item['id']}?tenant_id=ys_thesharp", headers=headers)
        assert detail.status_code == 200
        assert detail.json()["item"]["ai_model"] == "gpt-5"


def test_attachment_limit_and_group_delete(app_client, tmp_path) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    created = client.post(
        "/api/complaints",
        json={
            "tenant_id": "ys_thesharp",
            "building": "101",
            "unit": "1301",
            "channel": "방문",
            "content": "현관문 사진 첨부 테스트",
        },
    )
    assert created.status_code == 200
    complaint_id = int(created.json()["item"]["id"])

    for idx in range(6):
        upload = client.post(
            f"/api/complaints/{complaint_id}/attachments?tenant_id=ys_thesharp",
            files={"file": (f"photo-{idx}.jpg", io.BytesIO(b"fake-image"), "image/jpeg")},
        )
        assert upload.status_code == 200
        assert upload.json()["item"]["file_url"].startswith("/api/files/ys_thesharp/")

    blocked = client.post(
        f"/api/complaints/{complaint_id}/attachments?tenant_id=ys_thesharp",
        files={"file": ("photo-6.jpg", io.BytesIO(b"fake-image"), "image/jpeg")},
    )
    assert blocked.status_code == 400
    assert "max 6" in blocked.json()["detail"]

    detail = client.get(f"/api/complaints/{complaint_id}?tenant_id=ys_thesharp")
    assert detail.status_code == 200
    attachments = detail.json()["item"]["attachments"]
    assert len(attachments) == 6

    deleted = client.request(
        "DELETE",
        f"/api/complaints/{complaint_id}/attachments",
        json={"tenant_id": "ys_thesharp", "attachment_ids": [attachments[1]["id"], attachments[3]["id"]]},
    )
    assert deleted.status_code == 200
    assert len(deleted.json()["deleted"]) == 2
    assert len(deleted.json()["item"]["attachments"]) == 4

    delete_all = client.request(
        "DELETE",
        f"/api/complaints/{complaint_id}/attachments",
        json={"tenant_id": "ys_thesharp", "delete_all": True},
    )
    assert delete_all.status_code == 200
    assert len(delete_all.json()["item"]["attachments"]) == 0

    deleted_case = client.request(
        "DELETE",
        f"/api/complaints/{complaint_id}",
        json={"tenant_id": "ys_thesharp"},
    )
    assert deleted_case.status_code == 200
    assert client.get(f"/api/complaints/{complaint_id}?tenant_id=ys_thesharp").status_code == 404


def test_complaint_filters_and_admin_only_delete(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    first = client.post(
        "/api/complaints",
        json={
            "tenant_id": "ys_thesharp",
            "building": "101",
            "unit": "1203",
            "channel": "전화",
            "content": "101동 1203호 누수 신고",
        },
    )
    assert first.status_code == 200
    first_id = int(first.json()["item"]["id"])

    second = client.post(
        "/api/complaints",
        json={
            "tenant_id": "ys_thesharp",
            "building": "102",
            "unit": "905",
            "channel": "카톡",
            "content": "102동 905호 주차 문제",
        },
    )
    assert second.status_code == 200
    second_id = int(second.json()["item"]["id"])

    progressed = client.put(
        f"/api/complaints/{second_id}",
        json={"tenant_id": "ys_thesharp", "status": "처리중", "manager": "김대리"},
    )
    assert progressed.status_code == 200

    filtered = client.get("/api/complaints?tenant_id=ys_thesharp&status=접수&building=101")
    assert filtered.status_code == 200
    filtered_items = filtered.json()["items"]
    assert len(filtered_items) == 1
    assert filtered_items[0]["id"] == first_id
    assert filtered_items[0]["status"] == "접수"
    assert filtered_items[0]["building"] == "101"

    created_user = client.post(
        "/api/users",
        json={
            "tenant_id": "ys_thesharp",
            "login_id": "deskdelete01",
            "name": "삭제테스트직원",
            "password": "password123",
            "role": "desk",
        },
    )
    assert created_user.status_code == 200

    client.post("/api/auth/logout")
    desk_login = client.post("/api/auth/login", json={"login_id": "deskdelete01", "password": "password123"})
    assert desk_login.status_code == 200

    blocked_delete = client.request(
        "DELETE",
        f"/api/complaints/{first_id}",
        json={"tenant_id": "ys_thesharp"},
    )
    assert blocked_delete.status_code == 403

    client.post("/api/auth/logout")
    admin_login = client.post("/api/auth/login", json={"login_id": "admin01", "password": "password123"})
    assert admin_login.status_code == 200

    deleted = client.request(
        "DELETE",
        f"/api/complaints/{first_id}",
        json={"tenant_id": "ys_thesharp"},
    )
    assert deleted.status_code == 200
    assert client.get(f"/api/complaints/{first_id}?tenant_id=ys_thesharp").status_code == 404


def test_user_management_crud_and_permission_boundaries(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    created_admin = client.post(
        "/api/users",
        json={
            "tenant_id": "ys_thesharp",
            "login_id": "siteadmin01",
            "name": "현장관리자",
            "password": "password123",
            "role": "manager",
            "phone": "010-2222-3333",
            "note": "현장 관리자 계정",
            "is_site_admin": True,
        },
    )
    assert created_admin.status_code == 200
    assert created_admin.json()["item"]["is_site_admin"] == 1

    client.post("/api/auth/logout")
    login = client.post("/api/auth/login", json={"login_id": "siteadmin01", "password": "password123"})
    assert login.status_code == 200

    created_user = client.post(
        "/api/users",
        json={
            "login_id": "desk02",
            "name": "야간접수",
            "password": "password123",
            "role": "desk",
            "phone": "010-9999-0000",
            "note": "야간 접수 전용",
        },
    )
    assert created_user.status_code == 200
    user_id = int(created_user.json()["item"]["id"])
    assert created_user.json()["item"]["tenant_id"] == "ys_thesharp"

    blocked = client.post(
        "/api/users",
        json={
            "login_id": "blocked01",
            "name": "권한초과",
            "password": "password123",
            "role": "manager",
            "is_site_admin": True,
        },
    )
    assert blocked.status_code == 403

    listed = client.get("/api/users?active_only=false")
    assert listed.status_code == 200
    assert any(item["login_id"] == "desk02" for item in listed.json()["items"])

    updated = client.patch(
        f"/api/users/{user_id}",
        json={
            "name": "야간접수수정",
            "role": "reader",
            "phone": "010-0000-0000",
            "note": "읽기 전용으로 전환",
            "is_active": False,
        },
    )
    assert updated.status_code == 200
    assert updated.json()["item"]["is_active"] == 0
    assert updated.json()["item"]["role"] == "reader"

    reset = client.post(f"/api/users/{user_id}/reset_password", json={"password": "NewPass123!"})
    assert reset.status_code == 200

    deleted = client.delete(f"/api/users/{user_id}")
    assert deleted.status_code == 200
    assert client.get(f"/api/users/{user_id}").status_code == 404


def test_public_register_flow_creates_inactive_user_pending_approval(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    options = client.get("/api/auth/register_options")
    assert options.status_code == 200
    assert options.json()["enabled"] is True
    assert any(item["id"] == "ys_thesharp" for item in options.json()["items"])

    created = client.post(
        "/api/auth/register",
        json={
            "tenant_id": "ys_thesharp",
            "login_id": "request01",
            "name": "가입신청자",
            "phone": "010-7777-8888",
            "password": "password123",
        },
    )
    assert created.status_code == 200
    assert "관리자 승인" in created.json()["message"]
    user_id = int(created.json()["item"]["id"])
    assert created.json()["item"]["is_active"] == 0
    assert created.json()["item"]["tenant_id"] == "ys_thesharp"

    blocked = client.post("/api/auth/login", json={"login_id": "request01", "password": "password123"})
    assert blocked.status_code == 401

    activated = client.post(f"/api/users/{user_id}/approve")
    assert activated.status_code == 200
    assert activated.json()["item"]["is_active"] == 1

    approved = client.post("/api/auth/login", json={"login_id": "request01", "password": "password123"})
    assert approved.status_code == 200
    assert approved.json()["user"]["login_id"] == "request01"


def test_operations_admin_module_supports_crud_and_dashboard(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    vendor = client.post(
        "/api/ops/vendors",
        json={
            "tenant_id": "ys_thesharp",
            "company_name": "태성전기",
            "service_type": "전기 유지보수",
            "contact_name": "박기사",
            "phone": "051-111-2222",
            "status": "활성",
            "note": "야간 긴급 출동 가능",
        },
    )
    assert vendor.status_code == 200
    vendor_id = int(vendor.json()["item"]["id"])

    notice = client.post(
        "/api/ops/notices",
        json={
            "tenant_id": "ys_thesharp",
            "title": "4월 정기 소독 안내",
            "body": "4월 15일 오전 10시에 정기 소독이 진행됩니다.",
            "category": "기안",
            "status": "published",
            "pinned": True,
        },
    )
    assert notice.status_code == 200
    assert notice.json()["item"]["pinned"] == 1

    document = client.post(
        "/api/ops/documents",
        json={
            "tenant_id": "ys_thesharp",
            "title": "소방 점검 보고서",
            "summary": "소방 점검 결과 보고서 작성 필요",
            "category": "보고",
            "status": "검토중",
            "owner": "김과장",
            "due_date": "2026-04-09",
        },
    )
    assert document.status_code == 200
    document_id = int(document.json()["item"]["id"])
    assert str(document.json()["item"]["reference_no"]).startswith("RPT-")

    archived_document = client.post(
        "/api/ops/documents",
        json={
            "tenant_id": "ys_thesharp",
            "title": "외벽 보수 계약서",
            "summary": "외벽 보수 계약 완료본 보관",
            "category": "계약",
            "status": "보관",
            "owner": "관리소장",
            "reference_no": "CT-2026-001",
        },
    )
    assert archived_document.status_code == 200

    schedule = client.post(
        "/api/ops/schedules",
        json={
            "tenant_id": "ys_thesharp",
            "title": "지하 기계실 정기 점검",
            "schedule_type": "점검",
            "status": "예정",
            "due_date": "2026-04-10",
            "owner": "시설팀",
            "note": "점검 전 입주민 공지 필요",
            "vendor_id": vendor_id,
        },
    )
    assert schedule.status_code == 200
    schedule_id = int(schedule.json()["item"]["id"])
    assert schedule.json()["item"]["vendor_name"] == "태성전기"

    dashboard = client.get("/api/ops/dashboard?tenant_id=ys_thesharp")
    assert dashboard.status_code == 200
    item = dashboard.json()["item"]
    assert item["published_notices"] == 1
    assert item["open_documents"] == 1
    assert item["open_schedules"] == 1
    assert item["active_vendors"] == 1

    notices = client.get("/api/ops/notices?tenant_id=ys_thesharp")
    assert notices.status_code == 200
    assert notices.json()["items"][0]["title"] == "4월 정기 소독 안내"

    documents = client.get("/api/ops/documents?tenant_id=ys_thesharp&category=보고")
    assert documents.status_code == 200
    assert len(documents.json()["items"]) == 1
    assert documents.json()["items"][0]["title"] == "소방 점검 보고서"
    category_counts = {row["category"]: row for row in documents.json()["category_counts"]}
    assert category_counts["보고"]["total_count"] == 1
    assert category_counts["계약"]["total_count"] == 1

    next_reference = client.get("/api/ops/documents/next_reference?tenant_id=ys_thesharp&category=보고")
    assert next_reference.status_code == 200
    assert str(next_reference.json()["item"]["reference_no"]).startswith("RPT-")

    from openpyxl import load_workbook

    export = client.get("/api/ops/documents/export.xlsx?tenant_id=ys_thesharp&category=보고")
    assert export.status_code == 200
    assert export.headers["content-type"].startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    workbook = load_workbook(io.BytesIO(export.content))
    sheet = workbook.active
    assert sheet["A1"].value == "행정문서 관리대장"
    assert sheet["A3"].value == "분류: 보고"
    assert sheet["A6"].value == "소방 점검 보고서"

    updated_document = client.patch(
        f"/api/ops/documents/{document_id}",
        json={"tenant_id": "ys_thesharp", "status": "완료"},
    )
    assert updated_document.status_code == 200
    assert updated_document.json()["item"]["status"] == "완료"

    updated_schedule = client.patch(
        f"/api/ops/schedules/{schedule_id}",
        json={"tenant_id": "ys_thesharp", "status": "진행중", "owner": "시설주임"},
    )
    assert updated_schedule.status_code == 200
    assert updated_schedule.json()["item"]["status"] == "진행중"

    deleted_notice = client.request(
        "DELETE",
        f"/api/ops/notices/{int(notice.json()['item']['id'])}",
        json={"tenant_id": "ys_thesharp"},
    )
    assert deleted_notice.status_code == 200

    deleted_vendor = client.request(
        "DELETE",
        f"/api/ops/vendors/{vendor_id}",
        json={"tenant_id": "ys_thesharp"},
    )
    assert deleted_vendor.status_code == 200


def test_operations_admin_module_blocks_reader_write_access(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    created_user = client.post(
        "/api/users",
        json={
            "tenant_id": "ys_thesharp",
            "login_id": "reader01",
            "name": "열람전용",
            "password": "password123",
            "role": "reader",
        },
    )
    assert created_user.status_code == 200

    client.post("/api/auth/logout")
    login = client.post("/api/auth/login", json={"login_id": "reader01", "password": "password123"})
    assert login.status_code == 200

    blocked = client.post(
        "/api/ops/notices",
        json={
            "title": "읽기전용 차단 테스트",
            "body": "본문",
            "category": "공지",
            "status": "published",
        },
    )
    assert blocked.status_code == 403

    allowed_read = client.get("/api/ops/notices")
    assert allowed_read.status_code == 200


def test_document_numbering_config_is_tenant_configurable(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    default_config = client.get("/api/ops/documents/numbering_config?tenant_id=ys_thesharp")
    assert default_config.status_code == 200
    assert default_config.json()["item"]["config"]["date_mode"] == "yyyymmdd"
    assert default_config.json()["item"]["config"]["category_codes"]["보고"] == "RPT"

    updated = client.patch(
        "/api/ops/documents/numbering_config",
        json={
            "tenant_id": "ys_thesharp",
            "config": {
                "separator": "_",
                "date_mode": "yyyymm",
                "sequence_digits": 4,
                "category_codes": {
                    "계약": "CONT",
                    "공문": "LTR",
                    "보고": "REP",
                    "예산": "BUD",
                    "입주": "MOVE",
                    "점검": "CHK",
                    "기타": "ETC",
                },
            },
        },
    )
    assert updated.status_code == 200
    assert updated.json()["item"]["config"]["separator"] == "_"
    assert updated.json()["item"]["config"]["sequence_digits"] == 4
    assert updated.json()["item"]["preview_examples"]["보고"].startswith("REP_")

    next_reference = client.get("/api/ops/documents/next_reference?tenant_id=ys_thesharp&category=보고")
    assert next_reference.status_code == 200
    generated = str(next_reference.json()["item"]["reference_no"])
    assert re.match(r"^REP_\d{6}_\d{4}$", generated)

    created = client.post(
        "/api/ops/documents",
        json={
            "tenant_id": "ys_thesharp",
            "title": "커스텀 번호 보고서",
            "summary": "설정 변경 후 자동번호 테스트",
            "category": "보고",
            "status": "작성중",
        },
    )
    assert created.status_code == 200
    assert re.match(r"^REP_\d{6}_\d{4}$", str(created.json()["item"]["reference_no"]))

    reset = client.patch(
        "/api/ops/documents/numbering_config",
        json={"tenant_id": "ys_thesharp", "reset": True},
    )
    assert reset.status_code == 200
    assert reset.json()["item"]["config"]["separator"] == "-"
    assert reset.json()["item"]["config"]["date_mode"] == "yyyymmdd"


def test_operations_admin_document_pdf_generation_and_sample_reference(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    rendered = client.post(
        "/api/ops/documents/render_pdf",
        json={
            "tenant_id": "ys_thesharp",
            "title": "승강기 부품 교체의 건",
            "summary": "상가A동 25호기 승강기 부품 노후로 예방정비 차원의 교체를 진행하고 결재를 요청합니다.",
            "category": "점검",
            "owner": "시설과장",
            "reference_no": "관리-2026-001",
        },
    )
    assert rendered.status_code == 200
    assert rendered.headers["content-type"].startswith("application/pdf")
    assert rendered.content.startswith(b"%PDF")

    sample_pdf = client.post(
        "/api/ops/documents/sample_pdf",
        data={"tenant_id": "ys_thesharp", "title": "샘플 참조 기안서"},
        files={"source_file": ("sample.txt", io.BytesIO("제목\n승강기 부품 교체의 건\n내용\n교체 결재 요청".encode("utf-8")), "text/plain")},
    )
    assert sample_pdf.status_code == 200
    assert sample_pdf.headers["content-type"].startswith("application/pdf")
    assert sample_pdf.content.startswith(b"%PDF")


def test_legacy_import_supports_json_bundle_and_sqlite_aliases(app_client, tmp_path) -> None:
    import json
    import sqlite3

    import app.legacy_import as legacy_import

    _bootstrap_admin_and_tenant(app_client)

    bundle_path = tmp_path / "legacy_bundle.json"
    bundle_path.write_text(
        json.dumps(
            {
                "tenant": {"id": "ys_thesharp", "name": "연산더샵", "site_code": "APT00001", "site_name": "연산더샵"},
                "users": [
                    {
                        "login_id": "legacyops",
                        "name": "레거시행정",
                        "role": "manager",
                        "phone": "010-1000-2000",
                        "is_site_admin": True,
                        "is_active": True,
                    }
                ],
                "complaints": [
                    {
                        "building": "101",
                        "unit": "901",
                        "content": "지하주차장 조명이 나갔습니다.",
                        "channel": "방문",
                        "type": "전기",
                        "urgency": "당일",
                        "status": "처리중",
                        "created_at": "2026-04-07 09:00:00",
                    }
                ],
                "notices": [{"title": "정기 단수 안내", "body": "4월 20일 단수 예정", "category": "행정", "status": "published"}],
                "documents": [{"title": "월간 운영보고", "category": "보고", "status": "완료", "reference_no": "OPS-1"}],
                "vendors": [{"company_name": "한빛설비", "service_type": "설비 유지보수", "status": "활성"}],
                "schedules": [{"title": "옥상 방수 점검", "schedule_type": "점검", "status": "예정", "vendor_name": "한빛설비", "vendor_service_type": "설비 유지보수"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = legacy_import.import_legacy_source(
        source_path=bundle_path,
        tenant_id="ys_thesharp",
        tenant_name="연산더샵",
        default_user_password="TempPass123!",
    )
    assert summary["users"]["created"] == 1
    assert summary["complaints"]["created"] == 1
    assert summary["notices"]["created"] == 1
    assert summary["documents"]["created"] == 1
    assert summary["vendors"]["created"] == 1
    assert summary["schedules"]["created"] == 1

    legacy_sqlite = tmp_path / "legacy.sqlite"
    con = sqlite3.connect(str(legacy_sqlite))
    try:
        con.execute(
            """
            CREATE TABLE legacy_users (
              username TEXT,
              full_name TEXT,
              user_role TEXT,
              mobile TEXT,
              enabled INTEGER
            )
            """
        )
        con.execute(
            """
            CREATE TABLE legacy_notices (
              title TEXT,
              description TEXT,
              category TEXT,
              status TEXT
            )
            """
        )
        con.execute(
            """
            INSERT INTO legacy_users(username, full_name, user_role, mobile, enabled)
            VALUES('csvuser01', 'SQLite사용자', 'desk', '010-2222-3333', 1)
            """
        )
        con.execute(
            """
            INSERT INTO legacy_notices(title, description, category, status)
            VALUES('소방훈련 안내', '4월 30일 소방훈련 예정', '긴급', 'published')
            """
        )
        con.commit()
    finally:
        con.close()

    second = legacy_import.import_legacy_source(
        source_path=legacy_sqlite,
        tenant_id="ys_thesharp",
        tenant_name="연산더샵",
    )
    assert second["users"]["created"] == 1
    assert second["notices"]["created"] == 1

    users = app_client.get("/api/users?active_only=false&tenant_id=ys_thesharp")
    assert users.status_code == 200
    assert any(item["login_id"] == "legacyops" for item in users.json()["items"])
    assert any(item["login_id"] == "csvuser01" for item in users.json()["items"])

    notices = app_client.get("/api/ops/notices?tenant_id=ys_thesharp")
    assert notices.status_code == 200
    notice_map = {item["title"]: item for item in notices.json()["items"]}
    assert notice_map["정기 단수 안내"]["category"] == "기안"
    assert notice_map["소방훈련 안내"]["category"] == "작업내용"


def test_legacy_import_supports_facility_db_schema(app_client, tmp_path) -> None:
    import sqlite3

    import app.legacy_import as legacy_import

    _bootstrap_admin_and_tenant(app_client)

    legacy_sqlite = tmp_path / "facility.sqlite"
    con = sqlite3.connect(str(legacy_sqlite))
    try:
        con.executescript(
            """
            CREATE TABLE complaint_cases (
              id INTEGER PRIMARY KEY,
              case_key TEXT,
              site TEXT,
              building TEXT,
              unit_number TEXT,
              resident_name TEXT,
              contact_phone TEXT,
              complaint_type TEXT,
              title TEXT,
              description TEXT,
              status TEXT,
              priority TEXT,
              source_channel TEXT,
              reported_at TEXT,
              scheduled_visit_at TEXT,
              resolved_at TEXT,
              resident_confirmed_at TEXT,
              closed_at TEXT,
              recurrence_flag INTEGER,
              recurrence_count INTEGER,
              assignee TEXT,
              linked_work_order_id INTEGER,
              import_batch_id TEXT,
              source_workbook TEXT,
              source_sheet TEXT,
              source_row_number INTEGER,
              source_row_hash TEXT,
              created_by TEXT,
              created_at TEXT,
              updated_at TEXT
            );
            CREATE TABLE complaint_events (
              id INTEGER PRIMARY KEY,
              complaint_id INTEGER,
              event_type TEXT,
              from_status TEXT,
              to_status TEXT,
              note TEXT,
              detail_json TEXT,
              actor_username TEXT,
              created_at TEXT
            );
            CREATE TABLE admin_audit_logs (
              id INTEGER PRIMARY KEY,
              actor_user_id INTEGER,
              actor_username TEXT,
              action TEXT,
              resource_type TEXT,
              resource_id TEXT,
              status TEXT,
              detail_json TEXT,
              created_at TEXT,
              prev_hash TEXT,
              entry_hash TEXT
            );
            CREATE TABLE ops_checklist_sets (
              id INTEGER PRIMARY KEY,
              set_id TEXT,
              label TEXT,
              task_type TEXT,
              source TEXT,
              created_at TEXT,
              updated_at TEXT,
              version_no INTEGER,
              lifecycle_state TEXT
            );
            CREATE TABLE ops_checklist_set_items (
              id INTEGER PRIMARY KEY,
              set_id TEXT,
              seq INTEGER,
              item_text TEXT,
              created_at TEXT,
              updated_at TEXT
            );
            CREATE TABLE sla_policies (
              id INTEGER PRIMARY KEY,
              policy_key TEXT,
              policy_json TEXT,
              updated_at TEXT
            );
            """
        )
        con.execute(
            """
            INSERT INTO complaint_cases(
              id, case_key, site, building, unit_number, contact_phone, complaint_type, title, description,
              status, priority, source_channel, reported_at, assignee, import_batch_id, source_workbook,
              source_sheet, source_row_number, source_row_hash, created_by, created_at, updated_at
            )
            VALUES(
              10, 'case-10', '연산더샵', '110동', '705호', '010-1234-5678', 'glass_damage', '110동 705호 유리/창문 파손',
              '거실유리창 교체요', 'assigned', 'medium', 'legacy_excel', '2026-03-21 16:16:08', '현장3', 'paint-1',
              '추가세대 도색 민원내역.xlsx', '유리', 6, 'hash-1', 'excel-importer', '2026-03-21 16:16:08', '2026-03-22 00:11:16'
            )
            """
        )
        con.execute(
            """
            INSERT INTO complaint_events(
              id, complaint_id, event_type, from_status, to_status, note, detail_json, actor_username, created_at
            )
            VALUES(
              1, 10, 'status_changed', 'received', 'assigned', '민원 정보 수정',
              '{"changed_fields":["assignee","status"]}', '현장총괄', '2026-03-22 00:11:18'
            )
            """
        )
        con.execute(
            """
            INSERT INTO admin_audit_logs(
              id, actor_user_id, actor_username, action, resource_type, resource_id, status, detail_json, created_at, prev_hash, entry_hash
            )
            VALUES(
              1, NULL, '현장총괄', 'complaints.case.update', 'complaint_case', '10', 'success',
              '{"changed_fields":["assignee","status"]}', '2026-03-22 00:11:18', 'prev', 'next'
            )
            """
        )
        con.execute(
            """
            INSERT INTO ops_checklist_sets(
              id, set_id, label, task_type, source, created_at, updated_at, version_no, lifecycle_state
            )
            VALUES(1, 'electrical_60', '전기직무고시60항목', '전기점검', 'fallback', '2026-03-23 11:22:59', '2026-03-23 11:22:59', 1, 'active')
            """
        )
        con.execute(
            """
            INSERT INTO ops_checklist_set_items(
              id, set_id, seq, item_text, created_at, updated_at
            )
            VALUES(1, 'electrical_60', 1, '수변전실 출입통제 상태 확인', '2026-03-23 11:22:59', '2026-03-23 11:22:59')
            """
        )
        con.execute(
            """
            INSERT INTO sla_policies(id, policy_key, policy_json, updated_at)
            VALUES(1, 'default', '{"default_due_hours":{"medium":24}}', '2026-03-02 03:45:19')
            """
        )
        con.commit()
    finally:
        con.close()

    summary = legacy_import.import_legacy_source(
        source_path=legacy_sqlite,
        tenant_id="ys_thesharp",
        tenant_name="연산더샵",
    )
    assert summary["complaints"]["created"] == 1
    assert summary["documents"]["created"] == 2
    assert summary["audit_logs"]["created"] == 1

    complaints = app_client.get("/api/complaints?tenant_id=ys_thesharp")
    assert complaints.status_code == 200
    assert len(complaints.json()["items"]) == 1
    item = complaints.json()["items"][0]
    assert item["building"] == "110"
    assert item["unit"] == "705"
    assert item["type"] == "시설"
    assert item["status"] == "처리중"

    detail = app_client.get(f"/api/complaints/{item['id']}?tenant_id=ys_thesharp")
    assert detail.status_code == 200
    assert len(detail.json()["item"]["history"]) == 1
    assert detail.json()["item"]["history"][0]["to_status"] == "처리중"

    documents = app_client.get("/api/ops/documents?tenant_id=ys_thesharp")
    assert documents.status_code == 200
    assert any(doc["title"] == "[레거시 점검표] 전기직무고시60항목" for doc in documents.json()["items"])
    assert any(doc["title"] == "[레거시 SLA] default" for doc in documents.json()["items"])


def test_admin_legacy_import_upload_endpoint_supports_dry_run(app_client) -> None:
    import json

    _bootstrap_admin_and_tenant(app_client)

    payload = {
        "tenant": {"id": "ys_thesharp", "name": "연산더샵"},
        "complaints": [
            {
                "building": "101",
                "unit": "1201",
                "content": "공용등이 깜빡입니다.",
                "type": "전기",
                "urgency": "일반",
                "status": "접수",
            }
        ],
    }
    response = app_client.post(
        "/api/admin/legacy/import",
        data={
            "tenant_id": "ys_thesharp",
            "tenant_name": "연산더샵",
            "site_code": "APT00001",
            "site_name": "연산더샵",
            "dry_run": "true",
        },
        files={"source_file": ("bundle.json", io.BytesIO(json.dumps(payload, ensure_ascii=False).encode("utf-8")), "application/json")},
    )
    assert response.status_code == 200
    item = response.json()["item"]
    assert item["dry_run"] is True
    assert item["complaints"]["created"] == 1

    complaints = app_client.get("/api/complaints?tenant_id=ys_thesharp")
    assert complaints.status_code == 200
    assert complaints.json()["items"] == []


def test_voice_twilio_flow_creates_complaint_and_tracks_session(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    inbound = client.post(
        "/api/voice/twilio/inbound?tenant_id=ys_thesharp",
        data={"CallSid": "CA1001", "From": "+821055551234", "To": "+82212345678"},
    )
    assert inbound.status_code == 200
    assert "<Gather" in inbound.text
    assert "동과 호수" in inbound.text

    location = client.post(
        "/api/voice/twilio/gather?tenant_id=ys_thesharp&call_sid=CA1001",
        data={"CallSid": "CA1001", "SpeechResult": "101동 1203호입니다", "From": "+821055551234", "To": "+82212345678"},
    )
    assert location.status_code == 200
    assert "불편하신 내용을 말씀해 주세요" in location.text

    issue = client.post(
        "/api/voice/twilio/gather?tenant_id=ys_thesharp&call_sid=CA1001",
        data={"CallSid": "CA1001", "SpeechResult": "주차장 이중주차가 계속되고 있습니다", "From": "+821055551234", "To": "+82212345678"},
    )
    assert issue.status_code == 200
    assert "접수 내용을 확인" in issue.text

    confirm = client.post(
        "/api/voice/twilio/gather?tenant_id=ys_thesharp&call_sid=CA1001",
        data={"CallSid": "CA1001", "Digits": "1", "From": "+821055551234", "To": "+82212345678"},
    )
    assert confirm.status_code == 200
    assert "접수번호는" in confirm.text

    complaints = client.get("/api/complaints?tenant_id=ys_thesharp")
    assert complaints.status_code == 200
    assert any(item["channel"] == "전화" and item["building"] == "101" and item["unit"] == "1203" for item in complaints.json()["items"])

    config = client.get("/api/voice/config?tenant_id=ys_thesharp")
    assert config.status_code == 200
    assert config.json()["item"]["inbound_url"].endswith("/api/voice/twilio/inbound?tenant_id=ys_thesharp")

    sessions = client.get("/api/voice/sessions?tenant_id=ys_thesharp")
    assert sessions.status_code == 200
    assert sessions.json()["items"][0]["provider_call_id"] == "CA1001"
    assert sessions.json()["items"][0]["complaint_id"] is not None


def test_voice_twilio_emergency_handoff_flow(app_client, monkeypatch) -> None:
    monkeypatch.setenv("KA_VOICE_HANDOFF_NUMBER", "01012341234")
    client = app_client
    _bootstrap_admin_and_tenant(client)

    inbound = client.post(
        "/api/voice/twilio/inbound?tenant_id=ys_thesharp",
        data={"CallSid": "CA2001", "From": "+821012341234", "To": "+82212345678"},
    )
    assert inbound.status_code == 200

    handoff = client.post(
        "/api/voice/twilio/gather?tenant_id=ys_thesharp&call_sid=CA2001",
        data={
            "CallSid": "CA2001",
            "SpeechResult": "101동 1203호인데 누수가 심합니다 긴급으로 연결해 주세요",
            "From": "+821012341234",
            "To": "+82212345678",
        },
    )
    assert handoff.status_code == 200
    assert "<Dial>01012341234</Dial>" in handoff.text

    sessions = client.get("/api/voice/sessions?tenant_id=ys_thesharp")
    assert sessions.status_code == 200
    target = next(item for item in sessions.json()["items"] if item["provider_call_id"] == "CA2001")
    assert target["status"] == "handoff"
    assert target["complaint_id"] is not None


def test_startup_seed_supports_deployed_operator_bundle(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("KA_STORAGE_ROOT", str(tmp_path))
    monkeypatch.setenv("KA_BOOTSTRAP_ADMIN_LOGIN", "admin01")
    monkeypatch.setenv("KA_BOOTSTRAP_ADMIN_NAME", "운영관리자")
    monkeypatch.setenv("KA_BOOTSTRAP_ADMIN_PASSWORD", "SeedPass123!")
    monkeypatch.setenv("KA_BOOTSTRAP_TENANT_ID", "ys_thesharp")
    monkeypatch.setenv("KA_BOOTSTRAP_TENANT_NAME", "연산더샵")
    monkeypatch.setenv("KA_BOOTSTRAP_TENANT_SITE_CODE", "APT00001")
    monkeypatch.setenv("KA_BOOTSTRAP_TENANT_SITE_NAME", "연산더샵")
    monkeypatch.setenv("KA_BOOTSTRAP_TENANT_API_KEY", "sk-ka-seeded-api-key")
    monkeypatch.setenv("KA_BOOTSTRAP_MANAGER_LOGIN", "manager01")
    monkeypatch.setenv("KA_BOOTSTRAP_MANAGER_NAME", "현장담당")
    monkeypatch.setenv("KA_BOOTSTRAP_MANAGER_PASSWORD", "SeedPass123!")
    monkeypatch.setenv("KA_BOOTSTRAP_DESK_LOGIN", "desk01")
    monkeypatch.setenv("KA_BOOTSTRAP_DESK_NAME", "민원접수")
    monkeypatch.setenv("KA_BOOTSTRAP_DESK_PASSWORD", "SeedPass123!")

    for name in (
        "app.main",
        "app.db",
        "app.engine_db",
        "app.ai_service",
        "app.routes.core",
        "app.routes.engine",
    ):
        sys.modules.pop(name, None)

    db = importlib.import_module("app.db")
    engine_db = importlib.import_module("app.engine_db")

    monkeypatch.setattr(db, "DB_PATH", tmp_path / "seed_test.db")
    monkeypatch.setattr(engine_db, "DB_PATH", tmp_path / "seed_test.db")

    main = importlib.import_module("app.main")
    with TestClient(main.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["service"] == "ka-part-complaint-engine"
        assert client.head("/health").status_code == 200

        login = client.post("/api/auth/login", json={"login_id": "admin01", "password": "SeedPass123!"})
        assert login.status_code == 200

        me = client.get("/api/auth/me")
        assert me.status_code == 200
        assert me.json()["user"]["login_id"] == "admin01"

        tenants = client.get("/api/admin/tenants")
        assert tenants.status_code == 200
        assert any(item["id"] == "ys_thesharp" for item in tenants.json()["items"])
