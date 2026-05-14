import logging
import sqlite3
from collections import namedtuple

FileInfo = namedtuple("FileInfo", ["path", "size", "last_modified_at"])


class Database:
    def __init__(self, db_path: str, logger: logging.Logger):
        self.logger = logger
        self.conn = sqlite3.connect(db_path, check_same_thread=True)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    aws_version TEXT,
                    size INTEGER NOT NULL,
                    last_modified_at INTEGER NOT NULL,
                    PRIMARY KEY (path, version)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS timestamps (
                    timestamp INTEGER NOT NULL PRIMARY KEY,
                    event TEXT NOT NULL CHECK(event IN ('backup', 'restore'))
                )
                """
            )
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)")
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_last_modified_at_aws_version "
                "ON files(last_modified_at, aws_version)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_timestamps_event ON timestamps(event)"
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def insert_files(self, files: list[FileInfo]):
        try:
            with self.conn:
                # transaction
                paths = [f.path for f in files]
                placeholders = ",".join(["?"] * len(paths))
                cur = self.conn.execute(
                    "SELECT path, MAX(version), size, last_modified_at FROM files "
                    f"WHERE path IN ({placeholders}) GROUP BY path",
                    paths,
                )
                db_files_by_path = {f[0]: (f[1], f[2], f[3]) for f in cur.fetchall()}

                for f in files:
                    db_file = db_files_by_path.get(f.path)
                    if db_file:
                        # file exists, check if it needs updating
                        db_version, db_size, db_last_modified_at = db_file
                        if db_size != f.size or db_last_modified_at != f.last_modified_at:
                            # update existing file
                            self.conn.execute(
                                "INSERT INTO files (path, version, size, last_modified_at) "
                                "VALUES (?, ?, ?, ?)",
                                (f.path, db_version + 1, f.size, f.last_modified_at),
                            )
                            self.logger.debug(f"Updated file {f.path} to version {db_version + 1}")
                    else:
                        # insert new file
                        self.conn.execute(
                            "INSERT INTO files (path, version, size, last_modified_at) "
                            "VALUES (?, ?, ?, ?)",
                            (f.path, 1, f.size, f.last_modified_at),
                        )
                        self.logger.debug(f"Inserted new file {f.path}")
        except Exception as e:
            self.logger.error(f"Database insert error: {e}")

    def reindex(self):
        with self.conn:
            self.conn.execute("REINDEX")

    def close(self):
        self.conn.close()