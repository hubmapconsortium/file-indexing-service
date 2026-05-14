import sqlite3
from argparse import ArgumentParser
from configparser import ConfigParser


def parse_config() -> str:
    parser = ArgumentParser(description="Check file integrity in local SQLite database.")
    parser.add_argument(
        "--config", default="config.ini", help="Path to the configuration file (ini format)"
    )
    args = parser.parse_args()

    c = ConfigParser()
    c.read(args.config)

    return c.get("Local", "DATABASE_FILEPATH")


def is_db_corrupted(db_path: str) -> bool:
    try:
        with sqlite3.connect(db_path) as conn:
            result = conn.execute("PRAGMA integrity_check;").fetchone()
            return result[0] != "ok"
    except sqlite3.DatabaseError:
        return True  # If the DB can't be opened, it's likely corrupted


def main():
    db_path = parse_config()
    is_corrupted = is_db_corrupted(db_path)
    if is_corrupted:
        print(f"Database {db_path} is likely corrupted.")
    else:
        print(f"Database {db_path} is intact.")


if __name__ == "__main__":
    main()
