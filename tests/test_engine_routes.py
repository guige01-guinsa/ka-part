from __future__ import annotations

import io
import importlib
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
        "app.ai_service",
        "app.routes.core",
        "app.routes.engine",
    ):
        sys.modules.pop(name, None)

    db = importlib.import_module("app.db")
    engine_db = importlib.import_module("app.engine_db")

    monkeypatch.setattr(db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(engine_db, "DB_PATH", tmp_path / "engine_test.db")

    main = importlib.import_module("app.main")
    db.init_db()
    engine_db.init_engine_db()

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
    assert contracts.json()["allowed_modules"] == ["complaint_engine"]

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
