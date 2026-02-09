from .db import init_db, seed_demo, seed_users

def main():
    init_db()
    seed_demo()
    seed_users()
    print("[OK] DB initialized")

if __name__ == "__main__":
    main()
