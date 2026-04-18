from __future__ import annotations

import base64
import importlib
import io
import json
import re
import sys
import time
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
        "app.facility_db",
        "app.info_db",
        "app.legacy_import",
        "app.ops_db",
        "app.work_report_batch",
        "app.voice_db",
        "app.voice_service",
        "app.ai_service",
        "app.routes.core",
        "app.routes.engine",
        "app.routes.facility",
        "app.routes.info",
        "app.routes.ops",
        "app.routes.voice",
    ):
        sys.modules.pop(name, None)

    db = importlib.import_module("app.db")
    engine_db = importlib.import_module("app.engine_db")
    facility_db = importlib.import_module("app.facility_db")
    info_db = importlib.import_module("app.info_db")
    ops_db = importlib.import_module("app.ops_db")
    voice_db = importlib.import_module("app.voice_db")

    monkeypatch.setattr(db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(engine_db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(facility_db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(info_db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(ops_db, "DB_PATH", tmp_path / "engine_test.db")
    monkeypatch.setattr(voice_db, "DB_PATH", tmp_path / "engine_test.db")

    main = importlib.import_module("app.main")
    db.init_db()
    engine_db.init_engine_db()
    facility_db.init_facility_db()
    info_db.init_info_db()
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
    assert contracts.json()["allowed_modules"] == ["complaint_engine", "operations_admin", "facility_ops"]

    assert client.get("/api/v1/complaints").status_code == 404


def test_build_info_endpoints_expose_release_and_asset_versions(app_client) -> None:
    client = app_client

    api_response = client.get("/api/build_info")
    assert api_response.status_code == 200
    api_payload = api_response.json()
    assert api_payload["release_id"] == "2026-04-18-imagepreview-1"
    assert api_payload["static_assets"]["pwa_asset_version"] == "20260418g"
    assert api_payload["frontend_expectations"]["build_info_page"] == "/diag/build"
    assert api_response.headers["cache-control"].startswith("no-store")

    html_response = client.get("/diag/build")
    assert html_response.status_code == 200
    assert "text/html" in html_response.headers["content-type"]
    assert "2026-04-18-imagepreview-1" in html_response.text
    assert "20260418g" in html_response.text
    assert "/api/build_info" in html_response.text


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


def test_work_report_analysis_matches_chat_images_and_files(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    sample_text = "\n".join(
        [
            "시설팀 주요 업무 보고",
            "보고기간 : 7월01일~7월31일",
            "<작업내용 :><커뮤니티 헬스장 44번 사이클 손잡이 교체>",
            "<작업일자 :><7월 2일><업 체 :><관리실>",
        ]
    )
    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": "\n".join(
                [
                    "2026년 7월 2일 오전 9:00, 관리실 : 커뮤니티 헬스장 44번 사이클 손잡이 교체",
                    "2026년 7월 3일 오전 10:00, 관리실 : 110동 4/5라인 지하1층 방화문 기판 교체",
                ]
            ),
        },
        files=[
            ("images", ("7월2일-사이클-교체전.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("7월2일-사이클-교체후.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
            ("images", ("7월3일-방화문-교체전.jpg", io.BytesIO(b"fake-image-3"), "image/jpeg")),
            ("images", ("7월3일-방화문-교체후.jpg", io.BytesIO(b"fake-image-4"), "image/jpeg")),
            ("attachments", ("7월2일-사이클-작업내역서.txt", io.BytesIO("사이클 손잡이 교체 작업내역".encode("utf-8")), "text/plain")),
            ("sample_file", ("major-work-report.txt", io.BytesIO(sample_text.encode("utf-8")), "text/plain")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    assert item["report_title"] == "시설팀 주요 업무 보고"
    assert item["period_label"] == "7월 2일 ~ 7월 3일"
    assert item["item_count"] >= 2
    assert any("사이클" in str(row["title"]) for row in item["items"])
    cycle_item = next(row for row in item["items"] if "사이클" in str(row["title"]))
    assert len(cycle_item["images"]) >= 2
    assert any("작업 전" == str(image["stage_label"]) for image in cycle_item["images"])
    assert any("작업 후" == str(image["stage_label"]) for image in cycle_item["images"])
    assert any("작업내역서" in str(file["filename"]) for file in cycle_item["attachments"])


def test_work_report_analysis_supports_kakao_export_headers_and_notice_counts(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": "\n".join(
                [
                    "2026년 7월 2일 수요일",
                    "[관리실] [오전 9:00] 커뮤니티 헬스장 44번 사이클 손잡이 교체",
                    "[관리실] [오전 9:05] 사진 1장",
                    "[관리실] [오전 9:06] 작업내역서.pdf",
                    "2026년 7월 3일 목요일",
                    "[관리실] [오전 10:00] 110동 4/5라인 지하1층 방화문 기판 교체",
                    "[관리실] [오전 10:05] 사진 3장",
                ]
            ),
        },
        files=[
            ("images", ("IMG_0001.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("IMG_0002.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
            ("images", ("IMG_0003.jpg", io.BytesIO(b"fake-image-3"), "image/jpeg")),
            ("images", ("IMG_0004.jpg", io.BytesIO(b"fake-image-4"), "image/jpeg")),
            ("attachments", ("FILE_0001.pdf", io.BytesIO(b"fake-pdf"), "application/pdf")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    cycle_item = next(row for row in item["items"] if "사이클" in str(row["title"]))
    door_item = next(row for row in item["items"] if "방화문" in str(row["title"]))
    assert cycle_item["work_date"] == "2026-07-02"
    assert door_item["work_date"] == "2026-07-03"
    assert len(cycle_item["images"]) == 1
    assert len(door_item["images"]) == 3
    assert any(str(file["filename"]) == "FILE_0001.pdf" for file in cycle_item["attachments"])


def test_work_report_analysis_extracts_metadata_from_attachment_preview(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    preview_text = "\n".join(
        [
            "<제목><승강기 부품 교체의 건>",
            "<수리업체><한국미쓰비시엘리베이터(주)>",
            "<수리일시><2026.04.07>",
            "<대상><상가A동 25호기>",
        ]
    )
    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": ""},
        files=[
            ("attachments", ("repair-note.txt", io.BytesIO(preview_text.encode("utf-8")), "text/plain")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    assert item["item_count"] == 1
    first_item = item["items"][0]
    assert first_item["title"] == "승강기 부품 교체의 건"
    assert first_item["vendor_name"] == "한국미쓰비시엘리베이터(주)"
    assert first_item["work_date"] == "2026-04-07"
    assert first_item["location_name"] == "상가A동 25호기"


def test_work_report_analysis_accepts_source_file_without_text(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    source_text = "\n".join(
        [
            "2026년 4월 14일 화요일",
            "[김종훈(시설계장)] [오전 10:47] 사진",
            "[김종훈(시설계장)] [오전 10:48] 109동 놀이터 방치 자전거 및 스케이트 보드 회수함.",
        ]
    )
    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": ""},
        files=[
            ("source_file", ("kakao-source.txt", io.BytesIO(source_text.encode("utf-8")), "text/plain")),
            ("images", ("bike.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    assert item["item_count"] >= 1
    first_item = item["items"][0]
    assert "자전거" in str(first_item["title"])
    assert len(first_item["images"]) == 1


def test_work_report_analysis_accepts_multiple_source_files_with_text_and_images(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    source_text = "\n".join(
        [
            "2026년 4월 14일 화요일",
            "[김종훈(시설계장)] [오전 10:47] 사진",
            "[김종훈(시설계장)] [오전 10:48] 109동 놀이터 방치 자전거 및 스케이트 보드 회수함.",
        ]
    )
    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": ""},
        files=[
            ("source_files", ("kakao-source.txt", io.BytesIO(source_text.encode("utf-8")), "text/plain")),
            ("source_files", ("bike.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    assert item["item_count"] >= 1
    first_item = item["items"][0]
    assert "자전거" in str(first_item["title"])
    assert len(first_item["images"]) == 0
    assert item["image_item_count"] == 0


def test_work_report_analysis_rejects_more_than_twenty_source_files(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    files = [
        ("source_files", (f"source-{index}.txt", io.BytesIO(f"line-{index}".encode("utf-8")), "text/plain"))
        for index in range(21)
    ]
    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": ""},
        files=files,
    )
    assert response.status_code == 400
    assert "최대 20개" in response.json()["detail"]


def test_work_report_analysis_separates_text_only_items(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": "\n".join(
                [
                    "2026년 7월 2일 수요일",
                    "[관리실] [오전 9:00] 101동 계단등 교체",
                    "[관리실] [오전 9:01] 사진",
                    "[관리실] [오전 11:00] 102동 주차장 바닥 균열 보수 예정",
                ]
            ),
        },
        files=[
            ("images", ("KakaoTalk_20260702_090100.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    assert item["item_count"] == 2
    assert item["image_item_count"] == 1
    assert item["text_only_item_count"] == 1
    assert len(item["image_items"]) == 1
    assert len(item["text_only_items"]) == 1
    assert "계단등" in str(item["image_items"][0]["title"])
    assert "균열 보수" in str(item["text_only_items"][0]["title"])
    assert item["text_only_items"][0]["images"] == []


def test_work_report_analysis_merges_repeated_work_title_into_before_after_pair(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": "\n".join(
                [
                    "2026년 7월 2일 수요일",
                    "[관리실] [오전 9:00] 101동 계단등 교체",
                    "[관리실] [오전 9:01] 사진",
                    "[관리실] [오전 10:00] 101동 계단등 교체",
                    "[관리실] [오전 10:01] 사진",
                ]
            ),
        },
        files=[
            ("images", ("KakaoTalk_20260702_090100.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("KakaoTalk_20260702_100100.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    assert item["item_count"] == 1
    work_item = item["items"][0]
    assert work_item["title"] == "101동 계단등 교체"
    assert [str(image["stage_label"]) for image in work_item["images"]] == ["작업 전", "작업 후"]


def test_work_report_analysis_uses_notice_time_window_for_kakao_images(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": "\n".join(
                [
                    "2026년 7월 2일 수요일",
                    "[관리실] [오후 4:15] 집하장 센서등 10개 입고",
                    "[관리실] [오후 4:15] 사진",
                    "[관리실] [오후 4:32] 사진 4장",
                    "[관리실] [오후 4:32] 101동 집하장 센서등 2개교체",
                    "[관리실] [오후 4:37] 사진",
                    "[관리실] [오후 4:38] 104동 음식물처리기 키패드 As접수",
                ]
            ),
        },
        files=[
            ("images", ("KakaoTalk_20260702_163218930.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("KakaoTalk_20260702_163218930_01.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
            ("images", ("KakaoTalk_20260702_163218930_02.jpg", io.BytesIO(b"fake-image-3"), "image/jpeg")),
            ("images", ("KakaoTalk_20260702_163218930_03.jpg", io.BytesIO(b"fake-image-4"), "image/jpeg")),
            ("images", ("KakaoTalk_20260702_163750937.jpg", io.BytesIO(b"fake-image-5"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    stock_item = next(row for row in item["items"] if "10개 입고" in str(row["title"]))
    repair_item = next(row for row in item["items"] if "2개교체" in str(row["title"]))
    keypad_item = next(row for row in item["items"] if "키패드" in str(row["title"]))
    assert stock_item["images"] == []
    assert len(repair_item["images"]) == 4
    assert [str(image["filename"]) for image in repair_item["images"]] == [
        "KakaoTalk_20260702_163218930.jpg",
        "KakaoTalk_20260702_163218930_01.jpg",
        "KakaoTalk_20260702_163218930_02.jpg",
        "KakaoTalk_20260702_163218930_03.jpg",
    ]
    assert [str(image["stage_label"]) for image in repair_item["images"]] == ["작업 전", "작업 중", "작업 중", "작업 후"]
    assert [str(image["filename"]) for image in keypad_item["images"]] == ["KakaoTalk_20260702_163750937.jpg"]


def test_work_report_analysis_matches_generic_timestamp_images_to_nearest_items(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": "\n".join(
                [
                    "2026년 7월 2일 수요일",
                    "[관리실] [오전 9:10] 101동 집하장 센서등 2개교체",
                    "[관리실] [오전 9:46] 104동 음식물처리기 키패드 교체",
                ]
            ),
        },
        files=[
            ("images", ("KakaoTalk_20260702_091200.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("KakaoTalk_20260702_091245_01.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
            ("images", ("KakaoTalk_20260702_094630.jpg", io.BytesIO(b"fake-image-3"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    item = response.json()["item"]
    lighting_item = next(row for row in item["items"] if "센서등" in str(row["title"]))
    keypad_item = next(row for row in item["items"] if "키패드" in str(row["title"]))
    assert [str(image["filename"]) for image in lighting_item["images"]] == [
        "KakaoTalk_20260702_091200.jpg",
        "KakaoTalk_20260702_091245_01.jpg",
    ]
    assert [str(image["filename"]) for image in keypad_item["images"]] == ["KakaoTalk_20260702_094630.jpg"]


def test_work_report_visual_sampling_limits_large_cluster_batches() -> None:
    from app.work_report_service import _select_openai_visual_meta

    rows = [
        {"filename": "KakaoTalk_20260702_104711820.jpg"},
        {"filename": "KakaoTalk_20260702_112531576.jpg"},
        {"filename": "KakaoTalk_20260702_140610088.jpg"},
        {"filename": "KakaoTalk_20260702_163218930.jpg"},
        {"filename": "KakaoTalk_20260702_163218930_01.jpg"},
        {"filename": "KakaoTalk_20260702_163218930_02.jpg"},
        {"filename": "KakaoTalk_20260702_163218930_03.jpg"},
        {"filename": "KakaoTalk_20260702_163750937.jpg"},
    ]
    selected = _select_openai_visual_meta(rows)
    assert [str(row["filename"]) for row in selected] == [
        "KakaoTalk_20260702_104711820.jpg",
        "KakaoTalk_20260702_112531576.jpg",
        "KakaoTalk_20260702_140610088.jpg",
        "KakaoTalk_20260702_163218930.jpg",
        "KakaoTalk_20260702_163218930_01.jpg",
        "KakaoTalk_20260702_163218930_02.jpg",
        "KakaoTalk_20260702_163218930_03.jpg",
        "KakaoTalk_20260702_163750937.jpg",
    ]


def test_work_report_analysis_uses_chunked_ai_for_large_image_batches(monkeypatch) -> None:
    import app.work_report_service as service

    openai_calls: list[int] = []

    def _fake_openai_work_report(
        *,
        text,
        image_inputs,
        reference_image_inputs,
        attachment_inputs,
        sample_title,
        sample_lines,
        feedback_profile=None,
    ):
        openai_calls.append(len(list(image_inputs or [])))
        return {
            "report_title": "시설팀 주요 업무 보고",
            "period_label": "4월 15일",
            "items": [
                {
                    "index": 1,
                    "title": "커뮤니티 유리 깨짐 교체 작업",
                    "work_date": "2026-04-15",
                    "work_date_label": "4월 15일",
                    "vendor_name": "",
                    "location_name": "커뮤니티",
                    "summary": "커뮤니티 유리 깨짐 교체 작업",
                    "confidence": "high",
                    "images": [],
                    "attachments": [],
                }
            ],
            "unmatched_image_indexes": [],
            "unmatched_attachment_indexes": [],
            "analysis_notice": "",
            "analysis_model": "gpt-5.4",
        }

    def _fake_openai_match_image_chunks(*, text, image_inputs, items, progress_callback=None, feedback_profile=None):
        assert len(list(image_inputs or [])) == 13
        return {
            "items": [
                {
                    **dict(items[0]),
                    "images": [
                        {"index": 1, "filename": "KakaoTalk_20260415_093628492.jpg", "stage": "before", "stage_label": "작업 전"},
                        {"index": 2, "filename": "KakaoTalk_20260415_093628492_05.jpg", "stage": "after", "stage_label": "작업 후"},
                    ],
                }
            ],
            "unmatched_image_indexes": [3],
            "analysis_notice": "군집 1건은 unmatched로 남겼습니다.",
            "analysis_model": "gpt-5.4",
        }

    monkeypatch.setattr(service, "_openai_work_report", _fake_openai_work_report)
    monkeypatch.setattr(service, "_openai_match_image_chunks", _fake_openai_match_image_chunks)

    image_inputs = [
        {"filename": f"KakaoTalk_20260415_093628492_{index:02d}.jpg", "bytes": b"fake-image", "content_type": "image/jpeg"}
        for index in range(13)
    ]
    result = service.analyze_work_report(
        "2026년 4월 15일 오전 9:21, 관리실 : 커뮤니티 유리 깨짐 교체 작업 시작한다고 함.",
        image_inputs=image_inputs,
    )

    assert openai_calls == [0]
    assert result["analysis_model"] == "gpt-5.4"
    assert result["item_count"] == 1
    assert len(result["items"][0]["images"]) == 2
    assert result["items"][0]["images"][0]["stage"] == "before"
    assert result["items"][0]["images"][1]["stage"] == "after"
    assert len(result["unmatched_images"]) == 1
    assert "단계적으로 매칭" in str(result["analysis_notice"])


def test_work_report_batch_candidate_items_limits_large_item_context() -> None:
    import app.work_report_service as service

    items = []
    for index in range(1, 31):
        items.append(
            {
                "index": index,
                "title": f"{index}동 공용부 센서등 점검",
                "work_date": "2026-04-15",
                "work_date_label": "4월 15일",
                "vendor_name": "관리실",
                "location_name": f"{index}동 공동현관",
                "summary": f"{index}동 공동현관 센서등 점검",
                "_minute_of_day": 540 + index,
                "_image_notices": [],
            }
        )

    cluster = [
        {
            "index": 1,
            "filename": "KakaoTalk_20260415_091000_07동_공동현관_센서등.jpg",
            "date": "2026-04-15",
            "minute_of_day": 547,
        }
    ]
    work_events = service._work_report_events("2026년 4월 15일 오전 9:07, 관리실 : 7동 공동현관 센서등 점검")

    batch_items = service._batch_candidate_items([cluster], items, work_events, total_limit=8)

    assert len(batch_items) <= 8
    assert any(int(item["index"]) == 7 for item in batch_items)
    assert len(batch_items) < len(items)


def test_work_report_analysis_exposes_timeout_diagnostics_on_heuristic_fallback(monkeypatch) -> None:
    import app.work_report_service as service

    def _fake_openai_work_report(
        *,
        text,
        image_inputs,
        reference_image_inputs,
        attachment_inputs,
        sample_title,
        sample_lines,
        feedback_profile=None,
    ):
        service._set_openai_error_state(
            "api_timeout",
            "OpenAI 응답 시간이 초과되어 AI 분석이 중단됐습니다.",
            details="ReadTimeout while waiting for response body",
        )
        return None

    monkeypatch.setattr(service, "_openai_work_report", _fake_openai_work_report)

    result = service.analyze_work_report("2026년 4월 15일 오전 9:21, 관리실 : 커뮤니티 유리 깨짐 교체 작업 시작한다고 함.")

    assert result["analysis_model"] == "heuristic"
    assert result["analysis_mode_label"] == "규칙 기반"
    assert result["analysis_reason"] == "api_timeout"
    assert result["analysis_reason_label"] == "응답 시간 초과"
    assert "OpenAI 응답 시간이 초과" in str(result["analysis_notice"])
    failures = result["analysis_diagnostics"]["openai_failures"]
    assert failures[0]["stage"] == "direct_extract"
    assert failures[0]["reason"] == "api_timeout"


def test_work_report_cluster_candidate_lines_prioritize_nearby_location_match() -> None:
    from app.work_report_service import _cluster_item_candidate_lines

    cluster = [
        {
            "index": 1,
            "filename": "KakaoTalk_20260415_093628492.jpg",
            "date": "2026-04-15",
            "minute_of_day": 561,
            "second_of_day": 33660,
            "stage_hint": "",
        }
    ]
    items = [
        {
            "index": 1,
            "title": "107동 천장 센서등 교체",
            "summary": "107동 복도 천장 센서등 교체 진행",
            "location_name": "107동 복도",
            "vendor_name": "",
            "work_date": "2026-04-15",
            "work_date_label": "4월 15일",
        },
        {
            "index": 2,
            "title": "109동 음식물처리기 키패드 AS 접수",
            "summary": "109동 음식물처리기 키패드 이상 접수",
            "location_name": "109동 음식물처리기",
            "vendor_name": "",
            "work_date": "2026-04-15",
            "work_date_label": "4월 15일",
        },
    ]
    work_events = [
        {"index": 1, "date": "2026-04-15", "minute_of_day": 560, "text": "107동 천장 센서등 교체"},
        {"index": 2, "date": "2026-04-15", "minute_of_day": 700, "text": "109동 음식물처리기 키패드 AS 접수"},
    ]

    lines = _cluster_item_candidate_lines(cluster, items, work_events)

    assert lines
    assert lines[0].startswith("T1 107동 천장 센서등 교체")
    assert "위치 107동 복도" in lines[0]


def test_work_report_heuristic_anchor_filters_low_signal_lines() -> None:
    from app.work_report_service import _looks_like_heuristic_anchor

    assert _looks_like_heuristic_anchor("105동 3.4라인 피난유도선 설치", image_heavy=True) is True
    assert _looks_like_heuristic_anchor("교체 완료", image_heavy=True) is False
    assert _looks_like_heuristic_anchor("통화완료함. 볼륨 조정 안내드림", image_heavy=True) is False
    assert _looks_like_heuristic_anchor("(부품사오면 교체해주시나요)", image_heavy=True) is False


def test_work_report_match_score_does_not_use_date_only_match() -> None:
    from app.work_report_service import _match_score

    item = {
        "title": "커뮤니티 유리 교체",
        "summary": "커뮤니티 유리 교체",
        "location_name": "커뮤니티",
        "vendor_name": "",
        "work_date": "2026-04-13",
        "work_date_label": "4월 13일",
    }
    entry = {
        "filename": "KakaoTalk_20260413_105500045.jpg",
        "preview_text": "",
        "metadata": {},
    }

    assert _match_score(item, entry) == 0


def test_work_report_match_score_uses_nearby_time_for_generic_kakao_filename() -> None:
    from app.work_report_service import _match_score

    item = {
        "title": "101동 집하장 센서등 2개교체",
        "summary": "101동 집하장 센서등 2개교체",
        "location_name": "101동 집하장",
        "vendor_name": "관리실",
        "work_date": "2026-07-02",
        "work_date_label": "7월 2일",
        "_minute_of_day": (9 * 60) + 10,
    }
    entry = {
        "filename": "KakaoTalk_20260702_091200.jpg",
        "date": "2026-07-02",
        "minute_of_day": (9 * 60) + 12,
        "second_of_day": (9 * 3600) + (12 * 60),
    }

    assert _match_score(item, entry) >= 8


def test_work_report_image_candidate_matches_return_ranked_top_three_with_reasons() -> None:
    from app.work_report_service import _image_candidate_matches

    items = [
        {
            "index": 1,
            "title": "101동 집하장 센서등 2개교체",
            "summary": "101동 집하장 센서등 2개교체",
            "location_name": "101동 집하장",
            "vendor_name": "관리실",
            "work_date": "2026-07-02",
            "work_date_label": "7월 2일",
            "_minute_of_day": (9 * 60) + 10,
        },
        {
            "index": 2,
            "title": "104동 음식물처리기 키패드 교체",
            "summary": "104동 음식물처리기 키패드 교체",
            "location_name": "104동 음식물처리기",
            "vendor_name": "관리실",
            "work_date": "2026-07-02",
            "work_date_label": "7월 2일",
            "_minute_of_day": (9 * 60) + 18,
        },
        {
            "index": 3,
            "title": "지하주차장 출입문 보수",
            "summary": "지하주차장 출입문 보수",
            "location_name": "지하주차장",
            "vendor_name": "관리실",
            "work_date": "2026-07-02",
            "work_date_label": "7월 2일",
            "_minute_of_day": (9 * 60) + 24,
        },
    ]
    entry = {
        "index": 1,
        "filename": "KakaoTalk_20260702_091200.jpg",
        "date": "2026-07-02",
        "minute_of_day": (9 * 60) + 12,
        "second_of_day": (9 * 3600) + (12 * 60),
    }

    candidates = _image_candidate_matches(items, entry, limit=3)

    assert [int(row["item_index"]) for row in candidates] == [1, 2, 3]
    assert candidates[0]["rank"] == 1
    assert candidates[0]["score"] >= candidates[1]["score"] >= candidates[2]["score"]
    assert "촬영 시각" in str(candidates[0]["reason_text"])


def test_work_report_image_review_decision_flags_low_gap_candidates() -> None:
    from app.work_report_service import _image_review_decision

    decision = _image_review_decision(
        1,
        [
            {"item_index": 1, "score": 7},
            {"item_index": 2, "score": 6},
            {"item_index": 3, "score": 1},
        ],
    )

    assert decision["needed"] is True
    assert decision["confidence"] == "low"
    assert "점수 차가 작습니다" in str(decision["reason"])


def test_work_report_analysis_exposes_review_queue_for_ambiguous_generic_image(monkeypatch) -> None:
    import app.work_report_service as service

    monkeypatch.setattr(service, "_openai_work_report", lambda **kwargs: None)

    result = service.analyze_work_report(
        "\n".join(
            [
                "2026년 7월 2일 수요일",
                "[관리실] [오전 9:10] 101동 집하장 센서등 2개교체",
                "[관리실] [오전 9:12] 104동 집하장 센서등 1개교체",
            ]
        ),
        image_inputs=[
            {
                "filename": "KakaoTalk_20260702_091100.jpg",
                "bytes": b"fake-image",
                "content_type": "image/jpeg",
            }
        ],
    )

    assert result["review_queue_count"] == 1
    review = result["review_queue"][0]
    assert review["filename"] == "KakaoTalk_20260702_091100.jpg"
    assert len(review["candidate_items"]) >= 2
    assert "점수 차가 작습니다" in str(review["review_reason"])
    first_item = result["items"][0]
    assert first_item["images"][0]["review_needed"] is True


def test_work_report_analysis_uses_tenant_feedback_to_rerank_ambiguous_candidates(monkeypatch) -> None:
    import app.work_report_service as service

    monkeypatch.setattr(service, "_openai_work_report", lambda **kwargs: None)

    db_module = importlib.import_module("app.db")
    monkeypatch.setattr(
        db_module,
        "list_work_report_image_feedback",
        lambda *, tenant_id, limit=100: [
            {
                "feedback_type": "reassign_item",
                "to_item_index": 2,
                "to_item_title": "104동 집하장 센서등 1개교체",
                "from_item_index": 1,
                "from_item_title": "101동 집하장 센서등 2개교체",
                "review_confidence": "low",
                "candidate_items_json": json.dumps(
                    [
                        {"item_index": 1, "title": "101동 집하장 센서등 2개교체", "score": 13},
                        {"item_index": 2, "title": "104동 집하장 센서등 1개교체", "score": 13},
                    ],
                    ensure_ascii=False,
                ),
            }
        ],
    )

    result = service.analyze_work_report(
        "\n".join(
            [
                "2026년 7월 2일 수요일",
                "[관리실] [오전 9:10] 101동 집하장 센서등 2개교체",
                "[관리실] [오전 9:12] 104동 집하장 센서등 1개교체",
            ]
        ),
        tenant_id="ys_thesharp",
        image_inputs=[
            {
                "filename": "KakaoTalk_20260702_091100.jpg",
                "bytes": b"fake-image",
                "content_type": "image/jpeg",
            }
        ],
    )

    second_item = next(row for row in result["items"] if "104동" in str(row["title"]))
    assert [str(image["filename"]) for image in second_item["images"]] == ["KakaoTalk_20260702_091100.jpg"]
    assert result["analysis_diagnostics"]["tenant_feedback_rows_used"] == 1


def test_work_report_finalize_image_stages_keeps_all_matched_images() -> None:
    from app.work_report_service import _finalize_image_stages

    rows = [
        {"index": 1, "filename": "before.jpg", "stage": ""},
        {"index": 2, "filename": "during-1.jpg", "stage": ""},
        {"index": 3, "filename": "during-2.jpg", "stage": ""},
        {"index": 4, "filename": "after.jpg", "stage": ""},
    ]

    finalized = _finalize_image_stages(rows)

    assert [int(row["index"]) for row in finalized] == [1, 2, 3, 4]
    assert [str(row["stage"]) for row in finalized] == ["before", "during", "during", "after"]
    assert [str(row["stage_label"]) for row in finalized] == ["작업 전", "작업 중", "작업 중", "작업 후"]


def test_work_report_pdf_output_items_respect_selected_images() -> None:
    from app.report_pdf import _work_report_output_items

    report = {
        "items": [
            {
                "index": 1,
                "title": "커뮤니티 유리 깨짐 교체 작업",
                "summary": "커뮤니티 창호 유리 교체 진행",
                "images": [
                    {"index": 1, "filename": "before.jpg", "stage": "before", "include_in_output": True},
                    {"index": 2, "filename": "after.jpg", "stage": "after", "include_in_output": False},
                ],
            },
            {
                "index": 2,
                "title": "음식물처리기 키패드 불량 AS 접수",
                "summary": "키패드 불량으로 AS 접수",
                "include_in_output": False,
                "images": [
                    {"index": 3, "filename": "keypad.jpg", "stage": "general", "include_in_output": False},
                ],
            },
        ]
    }

    items, image_items, text_only_items = _work_report_output_items(report)

    assert len(items) == 1
    assert [str(image["filename"]) for image in items[0]["images"]] == ["before.jpg"]
    assert [str(item["title"]) for item in image_items] == ["커뮤니티 유리 깨짐 교체 작업"]
    assert text_only_items == []


def test_work_report_feedback_records_manual_image_corrections(app_client) -> None:
    from app.db import list_audit_logs, list_work_report_image_feedback

    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/work_report/feedback",
        headers=headers,
        json={
            "tenant_id": "ys_thesharp",
            "job_id": "job-manual-match-1",
            "corrections": [
                {
                    "feedback_type": "reassign_item",
                    "image_index": 3,
                    "filename": "KakaoTalk_20260702_094630.jpg",
                    "from_item_index": 0,
                    "from_item_title": "미매칭",
                    "to_item_index": 2,
                    "to_item_title": "104동 음식물처리기 키패드 교체",
                    "from_stage": "general",
                    "from_stage_label": "현장 이미지",
                    "to_stage": "after",
                    "to_stage_label": "작업 후",
                }
            ],
            "report": {
                "report_title": "시설팀 주요 업무 보고",
                "period_label": "7월 2일",
                "analysis_model": "gpt-5.4",
                "analysis_reason": "",
                "items": [
                    {
                        "index": 2,
                        "title": "104동 음식물처리기 키패드 교체",
                        "summary": "104동 음식물처리기 키패드 교체 완료",
                        "images": [
                            {
                                "index": 3,
                                "filename": "KakaoTalk_20260702_094630.jpg",
                                "stage": "after",
                                "stage_label": "작업 후",
                            }
                        ],
                    }
                ],
                "unmatched_images": [],
            },
        },
    )

    assert response.status_code == 200
    assert response.json()["item"]["correction_count"] == 1

    logs = list_audit_logs(tenant_id="ys_thesharp", limit=1)
    assert logs
    assert logs[0]["action"] == "ai_work_report_feedback"
    payload = json.loads(str(logs[0]["data_json"] or "{}"))
    assert payload["job_id"] == "job-manual-match-1"
    assert payload["correction_count"] == 1
    assert payload["corrections"][0]["feedback_type"] == "reassign_item"
    assert payload["corrections"][0]["to_item_title"] == "104동 음식물처리기 키패드 교체"
    assert payload["report"]["item_count"] == 1
    assert payload["report"]["unmatched_image_count"] == 0

    feedback_rows = list_work_report_image_feedback(tenant_id="ys_thesharp", limit=1)
    assert feedback_rows
    assert feedback_rows[0]["feedback_type"] == "reassign_item"
    assert feedback_rows[0]["to_item_title"] == "104동 음식물처리기 키패드 교체"
    stored_candidates = json.loads(str(feedback_rows[0]["candidate_items_json"] or "[]"))
    assert stored_candidates == []


def test_admin_work_report_learning_dashboard_summarizes_feedback(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    saved = client.post(
        "/api/ai/work_report/feedback",
        headers=headers,
        json={
            "tenant_id": "ys_thesharp",
            "job_id": "job-learning-dashboard-1",
            "corrections": [
                {
                    "feedback_type": "reassign_item",
                    "image_index": 1,
                    "filename": "KakaoTalk_20260702_091100.jpg",
                    "from_item_index": 1,
                    "from_item_title": "101동 집하장 센서등 2개교체",
                    "to_item_index": 2,
                    "to_item_title": "104동 집하장 센서등 1개교체",
                    "from_stage": "before",
                    "to_stage": "after",
                    "review_reason": "점수 차가 작습니다",
                    "review_confidence": "low",
                    "candidate_items": [
                        {"item_index": 1, "title": "101동 집하장 센서등 2개교체", "score": 13},
                        {"item_index": 2, "title": "104동 집하장 센서등 1개교체", "score": 13},
                    ],
                },
                {
                    "feedback_type": "confirm_current",
                    "image_index": 2,
                    "filename": "KakaoTalk_20260702_092100.jpg",
                    "from_item_index": 2,
                    "from_item_title": "104동 집하장 센서등 1개교체",
                    "to_item_index": 2,
                    "to_item_title": "104동 집하장 센서등 1개교체",
                    "from_stage": "after",
                    "to_stage": "after",
                    "review_reason": "현재 선택이 맞습니다",
                    "review_confidence": "medium",
                    "candidate_items": [
                        {"item_index": 2, "title": "104동 집하장 센서등 1개교체", "score": 14},
                    ],
                },
            ],
            "report": {
                "report_title": "시설팀 주요 업무 보고",
                "period_label": "7월 2일",
                "analysis_model": "gpt-5.4",
                "analysis_reason": "",
            },
        },
    )
    assert saved.status_code == 200

    response = client.get("/api/admin/work_report_learning?tenant_id=ys_thesharp&limit=300")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["meta"]["tenant_count"] == 1
    assert payload["meta"]["total_feedback_rows"] == 2
    item = payload["items"][0]
    assert item["tenant_id"] == "ys_thesharp"
    assert item["total_feedback_rows"] == 2
    assert item["inspected_feedback_rows"] == 2
    assert item["inspected_learning_dataset_rows"] == 2
    assert item["few_shot_example_count"] >= 1
    assert item["summary"]["choice_feedback_rows"] == 2
    assert item["readiness"]["ready"] is False
    assert item["latest_feedback_at"]


def test_work_report_image_layout_uses_three_columns_for_three_images() -> None:
    from app.report_pdf import _work_report_image_layout

    two_image_layout = _work_report_image_layout(2)
    three_image_layout = _work_report_image_layout(3)

    assert two_image_layout["columns"] == 2
    assert len(two_image_layout["col_widths"]) == 2
    assert three_image_layout["columns"] == 3
    assert len(three_image_layout["col_widths"]) == 3
    assert three_image_layout["draw_width"] < two_image_layout["draw_width"]
    assert three_image_layout["draw_height"] < two_image_layout["draw_height"]


def test_work_report_pdf_supports_sample_and_uploads(app_client) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    response = client.post(
        "/api/ai/work_report/pdf",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": "2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소",
        },
        files=[
            ("images", ("7월4일-수영장-작업전.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("7월4일-수영장-작업후.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
            ("sample_file", ("major-work-report.txt", io.BytesIO("시설팀 주요 업무 보고\n보고기간 : 7월01일~7월31일".encode("utf-8")), "text/plain")),
        ],
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert "attachment;" in response.headers.get("content-disposition", "")
    assert response.content.startswith(b"%PDF")


def test_work_report_pdf_can_reuse_cached_preview_result(app_client, monkeypatch) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}
    text = "2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"

    preview = client.post(
        "/api/ai/work_report",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": text},
        files=[
            ("images", ("7월4일-수영장-작업전.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("7월4일-수영장-작업후.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
        ],
    )
    assert preview.status_code == 200
    cached_report = preview.json()["item"]
    if cached_report.get("items") and cached_report["items"][0].get("images"):
        cached_report["items"][0]["images"][-1]["include_in_output"] = False

    import app.routes.engine as engine

    def _unexpected_reanalysis(*args, **kwargs):
        raise AssertionError("cached report should skip analyze_work_report")

    monkeypatch.setattr(engine, "analyze_work_report", _unexpected_reanalysis)

    response = client.post(
        "/api/ai/work_report/pdf",
        headers=headers,
        data={
            "tenant_id": "ys_thesharp",
            "text": text,
            "report_json": json.dumps(cached_report, ensure_ascii=False),
        },
        files=[
            ("images", ("7월4일-수영장-작업전.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
            ("images", ("7월4일-수영장-작업후.jpg", io.BytesIO(b"fake-image-2"), "image/jpeg")),
        ],
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert response.content.startswith(b"%PDF")


def test_work_report_batch_job_can_poll_until_completed(app_client, monkeypatch) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    import app.routes.engine as engine

    def _fake_analyze_work_report(
        text,
        *,
        tenant_id="",
        image_inputs=None,
        reference_image_inputs=None,
        attachment_inputs=None,
        sample_title="",
        sample_lines=None,
        progress_callback=None,
    ):
        if progress_callback:
            progress_callback(
                {
                    "current_step": 1,
                    "total_steps": 5,
                    "summary": "원문에서 작업 항목을 추출하고 있습니다.",
                    "hint": "대량 배치 작업을 모사하는 테스트입니다.",
                }
            )
            progress_callback(
                {
                    "current_step": 3,
                    "total_steps": 5,
                    "summary": "이미지 군집 1/1개를 매칭하고 있습니다.",
                    "hint": "현장 사진과 대화 내용을 연결합니다.",
                }
            )
        return {
            "report_title": "시설팀 주요 업무 보고",
            "period_label": "7월 4일",
            "template_title": "시설팀 주요 업무 보고",
            "analysis_model": "gpt-5.4",
            "analysis_mode_label": "OpenAI (gpt-5.4)",
            "analysis_reason": "",
            "analysis_reason_label": "",
            "analysis_notice": "",
            "analysis_diagnostics": {"openai_failures": []},
            "item_count": 1,
            "image_item_count": 1,
            "text_only_item_count": 0,
            "items": [
                {
                    "index": 1,
                    "title": "어린이 수영장 청소",
                    "work_date": "2026-07-04",
                    "work_date_label": "7월 4일",
                    "vendor_name": "관리실",
                    "location_name": "어린이 수영장",
                    "summary": "어린이 수영장 청소 진행",
                    "images": [{"index": 1, "filename": "7월4일-수영장-작업전.jpg", "stage": "general"}],
                    "attachments": [],
                }
            ],
            "image_items": [
                {
                    "index": 1,
                    "title": "어린이 수영장 청소",
                    "work_date": "2026-07-04",
                    "work_date_label": "7월 4일",
                    "vendor_name": "관리실",
                    "location_name": "어린이 수영장",
                    "summary": "어린이 수영장 청소 진행",
                    "images": [{"index": 1, "filename": "7월4일-수영장-작업전.jpg", "stage": "general"}],
                    "attachments": [],
                }
            ],
            "text_only_items": [],
            "unmatched_images": [],
            "unmatched_attachments": [],
            "source_text_preview": ["2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"],
            "report_text": "어린이 수영장 청소",
        }

    monkeypatch.setattr(engine, "analyze_work_report", _fake_analyze_work_report)

    created = client.post(
        "/api/ai/work_report/jobs",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": "2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"},
        files=[
            ("images", ("7월4일-수영장-작업전.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
        ],
    )
    assert created.status_code == 200
    created_item = created.json()["item"]
    assert created_item["status"] in {"queued", "running"}
    assert created_item["image_count"] == 1

    detail_item = None
    for _ in range(40):
        detail = client.get(f"/api/ai/work_report/jobs/{created_item['id']}", headers=headers)
        assert detail.status_code == 200
        detail_item = detail.json()["item"]
        if detail_item["status"] == "completed":
            break
        time.sleep(0.05)
    assert detail_item is not None
    assert detail_item["status"] == "completed"
    assert detail_item["result"]["analysis_model"] == "gpt-5.4"
    assert detail_item["result"]["analysis_mode_label"] == "OpenAI (gpt-5.4)"
    assert detail_item["result"]["analysis_diagnostics"]["openai_failures"] == []
    assert detail_item["result"]["item_count"] == 1
    assert detail_item["result"]["template_source_name"] == ""


def test_work_report_batch_job_exposes_preview_image_endpoint(app_client, monkeypatch) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    import app.routes.engine as engine

    def _fake_analyze_work_report(
        text,
        *,
        tenant_id="",
        image_inputs=None,
        reference_image_inputs=None,
        attachment_inputs=None,
        sample_title="",
        sample_lines=None,
        progress_callback=None,
    ):
        return {
            "report_title": "시설팀 주요 업무 보고",
            "period_label": "7월 4일",
            "template_title": "시설팀 주요 업무 보고",
            "analysis_model": "gpt-5.4",
            "analysis_mode_label": "OpenAI (gpt-5.4)",
            "analysis_reason": "",
            "analysis_reason_label": "",
            "analysis_notice": "",
            "analysis_diagnostics": {"openai_failures": []},
            "item_count": 1,
            "image_item_count": 1,
            "text_only_item_count": 0,
            "items": [
                {
                    "index": 1,
                    "title": "어린이 수영장 청소",
                    "work_date": "2026-07-04",
                    "work_date_label": "7월 4일",
                    "vendor_name": "관리실",
                    "location_name": "어린이 수영장",
                    "summary": "어린이 수영장 청소 진행",
                    "images": [{"index": 1, "filename": "7월4일-수영장-작업전.png", "stage": "general"}],
                    "attachments": [],
                }
            ],
            "image_items": [],
            "text_only_items": [],
            "unmatched_images": [],
            "unmatched_attachments": [],
            "source_text_preview": ["2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"],
            "report_text": "어린이 수영장 청소",
        }

    monkeypatch.setattr(engine, "analyze_work_report", _fake_analyze_work_report)

    png_bytes = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO9W3ioAAAAASUVORK5CYII=")
    created = client.post(
        "/api/ai/work_report/jobs",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": "2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"},
        files=[
            ("images", ("7월4일-수영장-작업전.png", io.BytesIO(png_bytes), "image/png")),
        ],
    )
    assert created.status_code == 200
    created_item = created.json()["item"]

    detail_item = None
    for _ in range(40):
        detail = client.get(f"/api/ai/work_report/jobs/{created_item['id']}", headers=headers)
        assert detail.status_code == 200
        detail_item = detail.json()["item"]
        if detail_item["status"] == "completed":
            break
        time.sleep(0.05)

    assert detail_item is not None
    assert detail_item["status"] == "completed"
    preview = client.get(f"/api/ai/work_report/jobs/{created_item['id']}/images/1", headers=headers)
    assert preview.status_code == 200
    assert preview.headers["content-type"].startswith("image/jpeg")
    assert preview.content[:2] == b"\xff\xd8"


def test_work_report_batch_cleanup_preserves_result_but_removes_staged_files(app_client, monkeypatch) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    import app.routes.engine as engine
    import app.work_report_batch as batch

    def _fake_analyze_work_report(
        text,
        *,
        tenant_id="",
        image_inputs=None,
        reference_image_inputs=None,
        attachment_inputs=None,
        sample_title="",
        sample_lines=None,
        progress_callback=None,
    ):
        return {
            "report_title": "시설팀 주요 업무 보고",
            "period_label": "7월 4일",
            "template_title": "시설팀 주요 업무 보고",
            "analysis_model": "gpt-5.4",
            "analysis_mode_label": "OpenAI (gpt-5.4)",
            "analysis_reason": "",
            "analysis_reason_label": "",
            "analysis_notice": "",
            "analysis_diagnostics": {"openai_failures": []},
            "item_count": 1,
            "image_item_count": 1,
            "text_only_item_count": 0,
            "items": [
                {
                    "index": 1,
                    "title": "어린이 수영장 청소",
                    "work_date": "2026-07-04",
                    "work_date_label": "7월 4일",
                    "vendor_name": "관리실",
                    "location_name": "어린이 수영장",
                    "summary": "어린이 수영장 청소 진행",
                    "images": [{"index": 1, "filename": "7월4일-수영장-작업전.jpg", "stage": "general"}],
                    "attachments": [],
                }
            ],
            "image_items": [],
            "text_only_items": [],
            "unmatched_images": [],
            "unmatched_attachments": [],
            "source_text_preview": ["2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"],
            "report_text": "어린이 수영장 청소",
        }

    monkeypatch.setattr(engine, "analyze_work_report", _fake_analyze_work_report)

    created = client.post(
        "/api/ai/work_report/jobs",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": "2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"},
        files=[
            ("images", ("7월4일-수영장-작업전.jpg", io.BytesIO(b"fake-image-1"), "image/jpeg")),
        ],
    )
    assert created.status_code == 200
    created_item = created.json()["item"]

    detail_item = None
    for _ in range(40):
        detail = client.get(f"/api/ai/work_report/jobs/{created_item['id']}", headers=headers)
        assert detail.status_code == 200
        detail_item = detail.json()["item"]
        if detail_item["status"] == "completed":
            break
        time.sleep(0.05)

    assert detail_item is not None
    assert detail_item["status"] == "completed"
    record = batch.get_work_report_job_record(created_item["id"])
    assert record is not None
    job_dir = Path(str(record["job_dir"]))
    result_path = Path(str(record["result_path"]))
    assert result_path.exists()
    assert not (job_dir / "images").exists()
    assert not (job_dir / "reference_images").exists()
    assert not (job_dir / "job-input.json").exists()


def test_work_report_batch_job_create_returns_507_when_storage_is_full(app_client, monkeypatch) -> None:
    client = app_client
    api_key = _bootstrap_admin_and_tenant(client)
    headers = {"Authorization": f"Bearer {api_key}"}

    import app.routes.engine as engine

    monkeypatch.setattr(engine, "reclaim_work_report_job_storage", lambda: None)

    def _raise_no_space(*args, **kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(engine, "_write_work_report_batch_payload", _raise_no_space)

    response = client.post(
        "/api/ai/work_report/jobs",
        headers=headers,
        data={"tenant_id": "ys_thesharp", "text": "2026년 7월 4일 오전 9:00, 관리실 : 어린이 수영장 청소"},
    )

    assert response.status_code == 507
    assert "저장 공간" in response.json()["detail"]


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
            "category": "월업무보고(작업 보고서)",
            "status": "검토중",
            "owner": "김과장",
            "due_date": "2026-04-09",
            "target_label": "소방설비 월간 점검",
            "vendor_name": "태성전기",
            "amount_total": 298320,
            "basis_date": "2026-04-09",
        },
    )
    assert document.status_code == 200
    document_id = int(document.json()["item"]["id"])
    assert str(document.json()["item"]["reference_no"]).startswith("MWR-")
    assert document.json()["item"]["vendor_name"] == "태성전기"
    assert document.json()["item"]["amount_total"] == 298320.0

    archived_document = client.post(
        "/api/ops/documents",
        json={
            "tenant_id": "ys_thesharp",
            "title": "외벽 보수 계약서",
            "summary": "외벽 보수 계약 완료본 보관",
            "category": "계약서관리",
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

    catalog = client.get("/api/ops/documents/catalog?tenant_id=ys_thesharp")
    assert catalog.status_code == 200
    assert "기안지(10만원 이상)" in catalog.json()["item"]["categories"]

    documents = client.get("/api/ops/documents?tenant_id=ys_thesharp&category=월업무보고(작업 보고서)")
    assert documents.status_code == 200
    assert len(documents.json()["items"]) == 1
    assert documents.json()["items"][0]["title"] == "소방 점검 보고서"
    category_counts = {row["category"]: row for row in documents.json()["category_counts"]}
    assert category_counts["월업무보고(작업 보고서)"]["total_count"] == 1
    assert category_counts["계약서관리"]["total_count"] == 1

    next_reference = client.get("/api/ops/documents/next_reference?tenant_id=ys_thesharp&category=월업무보고(작업 보고서)")
    assert next_reference.status_code == 200
    assert str(next_reference.json()["item"]["reference_no"]).startswith("MWR-")

    from openpyxl import load_workbook

    export = client.get("/api/ops/documents/export.xlsx?tenant_id=ys_thesharp&category=월업무보고(작업 보고서)")
    assert export.status_code == 200
    assert export.headers["content-type"].startswith("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    workbook = load_workbook(io.BytesIO(export.content))
    sheet = workbook.active
    assert sheet["A1"].value == "행정문서 관리대장"
    assert sheet["A3"].value == "분류: 월업무보고(작업 보고서)"
    assert sheet["A6"].value == "소방 점검 보고서"
    assert sheet["G6"].value == "소방설비 월간 점검"
    assert sheet["H6"].value == "태성전기"

    updated_document = client.patch(
        f"/api/ops/documents/{document_id}",
        json={"tenant_id": "ys_thesharp", "status": "완료", "period_start": "2026-04-01", "period_end": "2026-04-30"},
    )
    assert updated_document.status_code == 200
    assert updated_document.json()["item"]["status"] == "완료"
    assert updated_document.json()["item"]["period_start"] == "2026-04-01"

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


def test_facility_ops_module_supports_crud_and_dashboard(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    asset = client.post(
        "/api/facility/assets",
        json={
            "tenant_id": "ys_thesharp",
            "asset_code": "ELV-A-25",
            "asset_name": "상가A동 25호기 승강기",
            "category": "승강기",
            "location_name": "상가A동 1층",
            "vendor_name": "오티스",
            "installed_on": "2024-01-01",
            "inspection_cycle_days": 30,
            "lifecycle_state": "운영중",
            "next_inspection_date": "2026-04-30",
        },
    )
    assert asset.status_code == 200
    asset_id = int(asset.json()["item"]["id"])
    assert asset.json()["item"]["inspection_cycle_days"] == 30
    assert asset.json()["item"]["vendor_name"] == "오티스"

    checklist = client.post(
        "/api/facility/checklists",
        json={
            "tenant_id": "ys_thesharp",
            "checklist_key": "ELV-MONTHLY",
            "title": "승강기 월간 점검표",
            "task_type": "월간점검",
            "version_no": "1",
            "items": ["도어 상태 확인", "비상통화 확인"],
        },
    )
    assert checklist.status_code == 200
    assert checklist.json()["item"]["items"] == ["도어 상태 확인", "비상통화 확인"]

    qr_asset = client.post(
        "/api/facility/qr_assets",
        json={
            "tenant_id": "ys_thesharp",
            "qr_id": "QR-ELV-A-25",
            "asset_id": asset_id,
            "asset_name_snapshot": "상가A동 25호기 승강기",
            "location_snapshot": "상가A동 1층",
            "checklist_key": "ELV-MONTHLY",
        },
    )
    assert qr_asset.status_code == 200
    qr_asset_id = int(qr_asset.json()["item"]["id"])

    inspection = client.post(
        "/api/facility/inspections",
        json={
            "tenant_id": "ys_thesharp",
            "title": "상가A동 25호기 월간 점검",
            "asset_id": asset_id,
            "qr_asset_id": qr_asset_id,
            "checklist_key": "ELV-MONTHLY",
            "inspector": "시설과장",
            "inspected_at": "2026-04-09 10:30:00",
            "result_status": "주의",
            "notes": "도어 개폐 속도 점검 필요",
            "measurement": {"door_speed": "slow"},
        },
    )
    assert inspection.status_code == 200
    inspection_id = int(inspection.json()["item"]["id"])
    assert inspection.json()["item"]["measurement"]["door_speed"] == "slow"
    assert inspection.json()["item"]["result_status"] == "주의"

    refreshed_asset = client.get("/api/facility/assets?tenant_id=ys_thesharp&category=승강기")
    assert refreshed_asset.status_code == 200
    assert refreshed_asset.json()["items"][0]["last_result_status"] == "주의"
    assert refreshed_asset.json()["items"][0]["next_inspection_date"] == "2026-05-09"

    work_order = client.post(
        f"/api/facility/inspections/{inspection_id}/issue_work_order",
        json={"tenant_id": "ys_thesharp"},
    )
    assert work_order.status_code == 200
    work_order_id = int(work_order.json()["item"]["id"])
    assert work_order.json()["created"] is True
    assert work_order.json()["item"]["priority"] == "높음"
    assert work_order.json()["item"]["category"] == "점검후속"

    duplicate_work_order = client.post(
        f"/api/facility/inspections/{inspection_id}/issue_work_order",
        json={"tenant_id": "ys_thesharp"},
    )
    assert duplicate_work_order.status_code == 200
    assert duplicate_work_order.json()["created"] is False
    assert int(duplicate_work_order.json()["item"]["id"]) == work_order_id

    dashboard = client.get("/api/facility/dashboard?tenant_id=ys_thesharp")
    assert dashboard.status_code == 200
    item = dashboard.json()["item"]
    assert item["active_assets"] == 1
    assert item["active_qr_assets"] == 1
    assert item["open_work_orders"] == 1
    assert item["month_inspections"] == 1
    assert len(item["urgent_work_orders"]) == 0

    listed_assets = client.get("/api/facility/assets?tenant_id=ys_thesharp&category=승강기")
    assert listed_assets.status_code == 200
    assert listed_assets.json()["items"][0]["asset_code"] == "ELV-A-25"

    listed_checklists = client.get("/api/facility/checklists?tenant_id=ys_thesharp")
    assert listed_checklists.status_code == 200
    assert listed_checklists.json()["items"][0]["item_count"] == 2

    updated_work_order = client.patch(
        f"/api/facility/work_orders/{work_order_id}",
        json={"tenant_id": "ys_thesharp", "status": "진행중", "assignee": "현장2"},
    )
    assert updated_work_order.status_code == 200
    assert updated_work_order.json()["item"]["status"] == "진행중"

    complaint = client.post(
        f"/api/facility/work_orders/{work_order_id}/create_complaint",
        json={"tenant_id": "ys_thesharp"},
    )
    assert complaint.status_code == 200
    assert complaint.json()["created"] is True
    assert complaint.json()["item"]["type"] == "승강기"
    assert complaint.json()["work_order"]["complaint_id"] == complaint.json()["item"]["id"]

    existing_complaint = client.post(
        f"/api/facility/work_orders/{work_order_id}/create_complaint",
        json={"tenant_id": "ys_thesharp"},
    )
    assert existing_complaint.status_code == 200
    assert existing_complaint.json()["created"] is False
    assert existing_complaint.json()["item"]["id"] == complaint.json()["item"]["id"]

    deleted_qr = client.request(
        "DELETE",
        f"/api/facility/qr_assets/{qr_asset_id}",
        json={"tenant_id": "ys_thesharp"},
    )
    assert deleted_qr.status_code == 200


def test_facility_asset_list_supports_keyword_search_and_filters(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    first = client.post(
        "/api/facility/assets",
        json={
            "tenant_id": "ys_thesharp",
            "asset_code": "ELV-A-25",
            "asset_name": "상가A동 25호기 승강기",
            "category": "승강기",
            "location_name": "상가A동 1층",
            "vendor_name": "오티스",
            "lifecycle_state": "운영중",
            "qr_id": "QR-ELV-A-25",
            "note": "메인 출입구 쪽",
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/api/facility/assets",
        json={
            "tenant_id": "ys_thesharp",
            "asset_code": "PUMP-B1-01",
            "asset_name": "지하1층 급수펌프",
            "category": "기계",
            "location_name": "지하1층 기계실",
            "vendor_name": "효성",
            "lifecycle_state": "점검중",
            "checklist_key": "PUMP-MONTHLY",
            "note": "베어링 소음 점검 필요",
        },
    )
    assert second.status_code == 200

    by_vendor = client.get("/api/facility/assets?tenant_id=ys_thesharp&query=오티스")
    assert by_vendor.status_code == 200
    assert [item["asset_code"] for item in by_vendor.json()["items"]] == ["ELV-A-25"]

    by_keyword_and_state = client.get("/api/facility/assets?tenant_id=ys_thesharp&query=소음&lifecycle_state=점검중")
    assert by_keyword_and_state.status_code == 200
    assert [item["asset_code"] for item in by_keyword_and_state.json()["items"]] == ["PUMP-B1-01"]

    by_qr = client.get("/api/facility/assets?tenant_id=ys_thesharp&query=QR-ELV-A-25&category=승강기")
    assert by_qr.status_code == 200
    assert [item["asset_code"] for item in by_qr.json()["items"]] == ["ELV-A-25"]


def test_document_numbering_config_is_tenant_configurable(app_client) -> None:
    client = app_client
    _bootstrap_admin_and_tenant(client)

    default_config = client.get("/api/ops/documents/numbering_config?tenant_id=ys_thesharp")
    assert default_config.status_code == 200
    assert default_config.json()["item"]["config"]["date_mode"] == "yyyymmdd"
    assert default_config.json()["item"]["config"]["category_codes"]["월업무보고(작업 보고서)"] == "MWR"

    updated = client.patch(
        "/api/ops/documents/numbering_config",
        json={
            "tenant_id": "ys_thesharp",
            "config": {
                "separator": "_",
                "date_mode": "yyyymm",
                "sequence_digits": 4,
                "category_codes": {
                    "기안지(10만원 이상)": "DRF",
                    "구매요청서(10만원 이하)": "BUY",
                    "견적서와 발주서": "ORD",
                    "월업무보고(작업 보고서)": "REP",
                    "계약서관리": "CONT",
                    "배상보험": "LIA",
                    "주요업무일정관리": "SCH",
                    "전기수도검침": "MTR",
                    "전기수도부과": "BIL",
                    "직무고시": "DUTY",
                    "안전관리대장관리": "SAFE",
                    "법정 정기점검": "LCHK",
                    "수질검사": "WQ",
                    "소방정기점검": "FIRE",
                    "기계설비유지관리": "MECH",
                    "기계설비성능점검": "MPT",
                    "승강기안전점검": "ELV",
                    "안전점검하자보수완료보고": "RPR",
                    "기타": "ETC",
                },
            },
        },
    )
    assert updated.status_code == 200
    assert updated.json()["item"]["config"]["separator"] == "_"
    assert updated.json()["item"]["config"]["sequence_digits"] == 4
    assert updated.json()["item"]["preview_examples"]["월업무보고(작업 보고서)"].startswith("REP_")

    next_reference = client.get("/api/ops/documents/next_reference?tenant_id=ys_thesharp&category=월업무보고(작업 보고서)")
    assert next_reference.status_code == 200
    generated = str(next_reference.json()["item"]["reference_no"])
    assert re.match(r"^REP_\d{6}_\d{4}$", generated)

    created = client.post(
        "/api/ops/documents",
        json={
            "tenant_id": "ys_thesharp",
            "title": "커스텀 번호 보고서",
            "summary": "설정 변경 후 자동번호 테스트",
            "category": "월업무보고(작업 보고서)",
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
            "category": "기안지(10만원 이상)",
            "owner": "시설과장",
            "reference_no": "관리-2026-001",
            "target_label": "상가A동 25호기 승강기",
            "vendor_name": "한국미쓰비시엘리베이터(주)",
            "amount_total": 298320,
            "basis_date": "2026-04-07",
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


def test_information_management_dashboard_and_crud(app_client) -> None:
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
        },
    )
    assert vendor.status_code == 200

    staff = client.post(
        "/api/users",
        json={
            "tenant_id": "ys_thesharp",
            "login_id": "facilitystaff",
            "name": "시설주임",
            "role": "staff",
            "phone": "010-9999-0000",
            "password": "TempPass123!",
        },
    )
    assert staff.status_code == 200

    asset = client.post(
        "/api/facility/assets",
        json={
            "tenant_id": "ys_thesharp",
            "asset_code": "ELV-A-25",
            "asset_name": "상가A동 25호기 승강기",
            "category": "승강기",
            "location_name": "상가A동",
            "vendor_name": "태성전기",
            "lifecycle_state": "운영중",
        },
    )
    assert asset.status_code == 200

    building = client.post(
        "/api/info/buildings",
        json={
            "tenant_id": "ys_thesharp",
            "building_code": "101",
            "building_name": "101동",
            "usage_type": "아파트동",
            "status": "운영중",
            "floors_above": 20,
            "floors_below": 2,
            "household_count": 120,
        },
    )
    assert building.status_code == 200
    building_id = int(building.json()["item"]["id"])

    registration = client.post(
        "/api/info/registrations",
        json={
            "tenant_id": "ys_thesharp",
            "record_type": "보험",
            "title": "영업배상 책임보험",
            "reference_no": "POL-2026-001",
            "status": "유효",
            "issuer_name": "DB손해보험",
            "issued_on": "2026-04-01",
            "expires_on": "2027-03-31",
        },
    )
    assert registration.status_code == 200
    registration_id = int(registration.json()["item"]["id"])

    dashboard = client.get("/api/info/dashboard?tenant_id=ys_thesharp")
    assert dashboard.status_code == 200
    item = dashboard.json()["item"]
    assert item["vendor_count"] == 1
    assert item["staff_count"] >= 1
    assert item["asset_count"] == 1
    assert item["building_count"] == 1
    assert item["registration_count"] == 1

    buildings = client.get("/api/info/buildings?tenant_id=ys_thesharp")
    assert buildings.status_code == 200
    assert buildings.json()["items"][0]["building_name"] == "101동"

    registrations = client.get("/api/info/registrations?tenant_id=ys_thesharp")
    assert registrations.status_code == 200
    assert registrations.json()["items"][0]["title"] == "영업배상 책임보험"

    updated_building = client.patch(
        f"/api/info/buildings/{building_id}",
        json={"tenant_id": "ys_thesharp", "household_count": 122, "note": "관리동 포함"},
    )
    assert updated_building.status_code == 200
    assert updated_building.json()["item"]["household_count"] == 122

    updated_registration = client.patch(
        f"/api/info/registrations/{registration_id}",
        json={"tenant_id": "ys_thesharp", "status": "만료예정"},
    )
    assert updated_registration.status_code == 200
    assert updated_registration.json()["item"]["status"] == "만료예정"


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
                "facility_assets": [
                    {"asset_code": "MECH-01", "asset_name": "지하 기계실 펌프", "category": "기계", "location_name": "지하1층"}
                ],
                "facility_checklists": [
                    {"checklist_key": "PUMP-MONTHLY", "title": "펌프 월간 점검표", "task_type": "월간점검", "items": ["진동 확인", "누수 확인"]}
                ],
                "facility_work_orders": [
                    {"title": "펌프 진동 보수", "category": "점검후속", "priority": "높음", "status": "접수"}
                ],
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
    assert summary["facility_assets"]["created"] == 1
    assert summary["facility_checklists"]["created"] == 1
    assert summary["facility_work_orders"]["created"] == 1

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

    facility_assets = app_client.get("/api/facility/assets?tenant_id=ys_thesharp")
    assert facility_assets.status_code == 200
    assert any(item["asset_code"] == "MECH-01" for item in facility_assets.json()["items"])

    facility_checklists = app_client.get("/api/facility/checklists?tenant_id=ys_thesharp")
    assert facility_checklists.status_code == 200
    assert any(item["checklist_key"] == "PUMP-MONTHLY" for item in facility_checklists.json()["items"])


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
