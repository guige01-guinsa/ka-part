from __future__ import annotations

import importlib
import sys

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def parking_main(tmp_path, monkeypatch):
    monkeypatch.setenv("ALLOW_INSECURE_DEFAULTS", "1")
    monkeypatch.setenv("PARKING_API_KEY", "A" * 24)
    monkeypatch.setenv("PARKING_SECRET_KEY", "B" * 24)
    monkeypatch.setenv("PARKING_CONTEXT_SECRET", "C" * 32)
    monkeypatch.setenv("PARKING_DEFAULT_SITE_CODE", "COMMON")
    monkeypatch.setenv("PARKING_DB_PATH", str(tmp_path / "parking_test.db"))
    monkeypatch.setenv("PARKING_UPLOAD_DIR", str(tmp_path / "uploads"))

    for name in (
        "services.parking.app.main",
        "services.parking.app.db",
        "services.parking.app.auth",
    ):
        sys.modules.pop(name, None)

    db = importlib.import_module("services.parking.app.db")
    main = importlib.import_module("services.parking.app.main")
    db.init_db()
    db.seed_demo()
    db.seed_users()
    return main, db


def _manager_cookie(main_mod) -> dict[str, str]:
    token = main_mod.make_session("pytest-admin", "admin", site_code="COMMON")
    return {"parking_session": token}


def test_normalize_plate_supports_jamo_middle(parking_main) -> None:
    main_mod, _ = parking_main
    assert main_mod.normalize_plate("12ㄱ3456") == "12가3456"
    assert main_mod.normalize_plate("12ᄂ5678") == "12나5678"


def test_check_plate_record_matches_jamo_middle(parking_main) -> None:
    main_mod, _ = parking_main
    out = main_mod.check_plate_record("COMMON", "12ㄱ3456")
    assert out.verdict == "OK"
    assert out.plate == "12가3456"


def test_check_plate_record_matches_rear4_when_unique(parking_main) -> None:
    main_mod, _ = parking_main
    out = main_mod.check_plate_record("COMMON", "3456")
    assert out.verdict == "OK"
    assert out.plate == "12가3456"
    assert "뒷자리 4자리 조회" in out.message


def test_check_plate_record_rear4_ambiguous_returns_unregistered(parking_main) -> None:
    main_mod, db_mod = parking_main
    with db_mod.connect() as con:
        con.execute(
            "INSERT INTO vehicles(site_code, plate, status) VALUES (?,?,?)",
            ("COMMON", "99하3456", "active"),
        )
        con.commit()

    out = main_mod.check_plate_record("COMMON", "3456")
    assert out.verdict == "UNREGISTERED"
    assert "여러 대" in out.message


def test_session_vehicle_search_supports_rear4(parking_main) -> None:
    main_mod, _ = parking_main
    client = TestClient(main_mod.app)
    client.cookies.update(_manager_cookie(main_mod))
    res = client.get("/api/session/vehicles", params={"q": "3456"})
    assert res.status_code == 200
    plates = [str(x.get("plate") or "") for x in res.json().get("items") or []]
    assert "12가3456" in plates


def test_session_vehicle_search_supports_front_rear_hint(parking_main) -> None:
    main_mod, _ = parking_main
    client = TestClient(main_mod.app)
    client.cookies.update(_manager_cookie(main_mod))
    res = client.get("/api/session/vehicles", params={"q": "12A3456"})
    assert res.status_code == 200
    plates = [str(x.get("plate") or "") for x in res.json().get("items") or []]
    assert "12가3456" in plates
