import sqlite3

conn = sqlite3.connect("ka.db")
with open("schema.sql", "r", encoding="utf-8") as f:
    conn.executescript(f.read())
conn.commit()
conn.close()

print("DB 생성 완료")
