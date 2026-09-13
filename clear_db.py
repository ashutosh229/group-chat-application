import os
import sqlite3

DB_PATH = os.environ.get(
    "SQLITE_DB_PATH",
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "db-server",
        "chat.db",
    ),
)


def clear_database():
    if not os.path.exists(DB_PATH):
        print(f"[!] Database file does not exist at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        with conn:
            msg_count = conn.execute("SELECT count(*) FROM messages").fetchone()[0]

            conn.execute("DELETE FROM messages")
            conn.execute("DELETE FROM user_keys")

        conn.execute("VACUUM")

        print("DATABASE CLEARED SUCCESSFULLY")
        print(f"Deleted {msg_count} message(s) from 'messages' table.")
        print("Cleared 'user_keys' table.")
        print(f"Database at {DB_PATH} is now fresh and empty.")

    finally:
        conn.close()


if __name__ == "__main__":
    clear_database()
