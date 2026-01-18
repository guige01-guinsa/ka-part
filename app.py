import os
from flask import Flask, render_template, redirect, jsonify
from flask_sqlalchemy import SQLAlchemy

# ------------------------------------------------------------
# DB (Flask-SQLAlchemy)
# ------------------------------------------------------------
db = SQLAlchemy()


def _normalize_database_url(url: str) -> str:
    """
    Render/Heroku 계열에서 postgres:// 로 주는 경우 보정
    Python 3.13 호환을 위해 psycopg3 드라이버 사용 (psycopg[binary])
    """
    url = (url or "").strip()

    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]

    # psycopg3 드라이버 명시
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg://"):
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)

    # Render Postgres: sslmode=require 권장
    if url.startswith("postgresql+psycopg://") and "sslmode=" not in url:
        join = "&" if "?" in url else "?"
        url = url + join + "sslmode=require"

    return url


def create_app() -> Flask:
    # templates 폴더는 app.py 기준 상대경로로 확정
    base_dir = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.join(base_dir, "templates")
    static_dir = os.path.join(base_dir, "static")

    app = Flask(__name__, template_folder=templates_dir, static_folder=static_dir)

    # instance 폴더(로컬 sqlite 대비)
    instance_dir = os.path.join(base_dir, "instance")
    os.makedirs(instance_dir, exist_ok=True)

    # 기본 SQLite (DATABASE_URL 없을 때)
    sqlite_path = os.path.join(instance_dir, "apartment.db")
    default_sqlite = "sqlite:///" + sqlite_path

    raw_db_url = os.environ.get("DATABASE_URL", "").strip()
    db_url = _normalize_database_url(raw_db_url) if raw_db_url else default_sqlite

    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # SQLite만 check_same_thread 필요
    if db_url.startswith("sqlite:///"):
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"connect_args": {"check_same_thread": False}}
    else:
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {}

    # init db
    db.init_app(app)

    # ------------------------------------------------------------
    # Blueprint: tool-search (있을 때만 등록)
    # ------------------------------------------------------------
    tool_search_ok = False
    try:
        from blueprints.tool_search.main import tool_search_bp  # noqa
        app.register_blueprint(tool_search_bp, url_prefix="/ts")
        tool_search_ok = True
    except Exception as e:
        print("[WARN] tool_search blueprint not loaded:", repr(e))

    # ------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------
    @app.get("/")
    def root():
        return redirect("/ka-part")

    @app.get("/health")
    def health():
        return jsonify(
            {
                "status": "ok",
                "db": "postgres" if app.config["SQLALCHEMY_DATABASE_URI"].startswith("postgresql") else "sqlite",
                "tool_search": tool_search_ok,
                "templates_dir": templates_dir,
            }
        )

    @app.get("/ka-part")
    def ka_part_home():
        # 반드시 templates/home.html 존재해야 함
        return render_template(
            "home.html",
            page_title="아파트 관리",
            active_menu="home",
            tool_search_ok=tool_search_ok,  # 메뉴에서 조건부로 쓸 수 있게
        )

    @app.get("/ui")
    def ui_alias():
        return redirect("/ka-part")

    return app


# gunicorn app:app 대응
app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")), debug=True)
