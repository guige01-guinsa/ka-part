import os
import sqlite3
from contextlib import contextmanager
from typing import Generator, Optional

DB_PATH = os.getenv("DB_PATH", os.path.join(os.getcwd(), "ka.db"))

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # FK 사용(원하면 유지)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

@contextmanager
def db_conn() -> Generator[sqlite3.Connection, None, None]:
    """
    동기 컨텍스트 매니저.
    FastAPI async 라우트에서도 'with db_conn()' 으로 안전하게 사용.
    """
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def get_db() -> Generator[sqlite3.Connection, None, None]:
    """
    FastAPI Depends 용(동기 generator).
    """
    with db_conn() as conn:
        yield conn
