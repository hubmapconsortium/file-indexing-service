import logging
import os
import sqlite3
from collections import namedtuple

DBFile = namedtuple("DBFile", ["path", "rel_path", "size", "last_modified_at"])


class Database:
    def __init__(self, db_path: str, logger: logging.Logger):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file {db_path} does not exist.")

        self.logger = logger
        self.conn = sqlite3.connect(
            f"file:{db_path}?mode=ro",
            uri=True,
            timeout=5.0,
            check_same_thread=True,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query_files(self, path_prefix: str) -> list[DBFile]:
        query = """
            SELECT path, size, last_modified_at
            FROM files
            WHERE (path, last_modified_at) IN (
                SELECT path, MAX(last_modified_at)
                FROM files
                WHERE path LIKE ?
                GROUP BY path
            )
        """
        return [
            DBFile(row[0], os.path.relpath(row[0], path_prefix), row[1], row[2])
            for row in self.conn.execute(query, (f"{path_prefix}%",))
        ]

    def close(self):
        self.conn.close()
