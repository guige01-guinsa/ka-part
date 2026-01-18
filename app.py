import os
from pathlib import Path

from flask import Flask, render_template, redirect, url_for, request, jsonify
from flask_sqlalchemy import SQLAlchemy

# ------------------------------------------------------------
# DB (Flask-SQLAlchemy)
# ------------------------------------------------------------
db = SQLAlchemy()


def _normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    # psycopg3 드라이버 명시 (python 3.13 안정)
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql+psycopg://") and "sslmode=" not in url:
        join = "&" if "?" in url else "?"
        url = url + join + "sslmode=require"
    return url



def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")

    # instance 폴더(로컬 sqlite 대비)
    app.instance_path = os.path.join(os.getcwd(), "instance")
    os.makedirs(app.instance_path, exist_ok=True)

    # 기본 SQLite (DATABASE_URL 없을 때)
    db_path = os.path.join(app.instance_path, "apartment.db")
    default_sqlite = "sqlite:///" + db_path

    raw_db_url = os.environ.get("DATABASE_URL", "").strip()
    if raw_db_url:
        db_url = _normalize_database_url(raw_db_url)
    else:
        db_url = default_sqlite

    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # SQLite만 check_same_thread 필요
    if db_url.startswith("sqlite:///"):
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
            "connect_args": {"check_same_thread": False}
        }
    else:
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {}

    # init db
    db.init_app(app)

    # ------------------------------------------------------------
    # Blueprint: tool-search (있을 때만 등록)
    # ------------------------------------------------------------
    try:
        from blueprints.tool_search.main import tool_search_bp
        app.register_blueprint(tool_search_bp, url_prefix="/ts")
        tool_search_ok = True
    except Exception as e:
        # Render 부팅을 “죽이지 않고” 원인만 로그로 남긴다.
        print("[WARN] tool_search blueprint not loaded:", repr(e))
        tool_search_ok = False

    # ------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------
    @app.get("/")
    def root():
        # 상위 메뉴로
        return redirect("/ka-part")

    @app.get("/health")
    def health():
        return jsonify({
            "status": "ok",
            "db": "postgres" if app.config["SQLALCHEMY_DATABASE_URI"].startswith("postgresql") else "sqlite",
            "tool_search": tool_search_ok,
        })

    # 상위 프로그램(메뉴) - /ka-part
    @app.get("/ka-part")
    def ka_part_home():
        # 메뉴 화면(이미 만들어둔 home/base/dashboard 흐름을 연결)
        # templates/home.html 에 맞춰 변수명 넘김 (필요하면 여기서 추가)
        return render_template(
            "home.html",
            page_title="아파트 관리",
            active_menu="home",
        )

    # 기존 /ui 경로 쓰고 있으면 유지
    @app.get("/ui")
    def ui_alias():
        return redirect("/ka-part")

    return app


# gunicorn app:app 대응
app = create_app()

if __name__ == "__main__":
    # 로컬 실행용
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")), debug=True)
