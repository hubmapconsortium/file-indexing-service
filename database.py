import logging
import os
import sqlite3
from collections import namedtuple
from typing import List, Optional

FileInfo = namedtuple("FileInfo", ["path", "size", "last_modified_at", "dataset_uuid", "uuid_api_md5", "uuid_api_sha256"])
DBFile = namedtuple("DBFile", ["path", "rel_path", "size", "last_modified_at"])
VersionedFileInfo = namedtuple("VersionedFileInfo", ["path", "version", "aws_version"])

logger = logging.getLogger("database")


class Database:
    def __init__(self, db_path: str, read_only: bool = False):
        if read_only:
            if not os.path.exists(db_path):
                raise FileNotFoundError(f"Database file {db_path} does not exist.")
            self.conn = sqlite3.connect(
                f"file:{db_path}?mode=ro",
                uri=True,
                timeout=5.0,
                check_same_thread=True,
            )
        else:
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
                    dataset_uuid TEXT,
                    uuid_api_md5 TEXT,
                    uuid_api_sha256 TEXT,
                    es_upsert_dt TEXT,
                    es_id TEXT,
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

    def insert_files(self, files: List[FileInfo]):
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
                                "INSERT INTO files (path, version, size, last_modified_at, dataset_uuid, uuid_api_md5, uuid_api_sha256) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (f.path, db_version + 1, f.size, f.last_modified_at, f.dataset_uuid, f.uuid_api_md5, f.uuid_api_sha256),
                            )
                            logger.debug(f"Updated file {f.path} to version {db_version + 1}")
                    else:
                        # insert new file
                        self.conn.execute(
                            "INSERT INTO files (path, version, size, last_modified_at, dataset_uuid, uuid_api_md5, uuid_api_sha256) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (f.path, 1, f.size, f.last_modified_at, f.dataset_uuid, f.uuid_api_md5, f.uuid_api_sha256),
                        )
                        logger.debug(f"Inserted new file {f.path}")
        except Exception as e:
            logger.error(f"Database insert error: {e}")

    def query_files(self, path_prefix: str) -> List[DBFile]:
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

    def count_files(self, since: int) -> int:
        """Count files modified since a given timestamp that have not yet been backed up to AWS.
        Used by file_backup.py."""
        query = "SELECT COUNT(path) FROM files WHERE last_modified_at >= ? AND aws_version IS NULL"
        cur = self.conn.execute(query, (since,))
        result = cur.fetchone()
        return result[0] if result else 0

    def update_files(self, files: List[VersionedFileInfo]):
        """Update the aws_version for a list of files after backup.
        Used by file_backup.py."""
        try:
            with self.conn:
                self.conn.executemany(
                    "UPDATE files SET aws_version = ? WHERE path = ? AND version = ?",
                    [(f.aws_version, f.path, f.version) for f in files],
                )
        except Exception as e:
            logger.error(f"Database update error: {e}")

    def get_last_backup_timestamp(self) -> int:
        """Return the timestamp of the most recent backup event, or 0 if none.
        Used by file_backup.py."""
        query = "SELECT MAX(timestamp) FROM timestamps WHERE event = 'backup'"
        return self.conn.execute(query).fetchone()[0] or 0

    def insert_backup_timestamp(self, timestamp: int):
        """Record a backup event timestamp.
        Used by file_backup.py."""
        try:
            with self.conn:
                self.conn.execute(
                    "INSERT INTO timestamps (timestamp, event) VALUES (?, 'backup')",
                    (timestamp,),
                )
        except Exception as e:
            logger.error(f"Database insert error: {e}")

    def reindex(self):
        with self.conn:
            self.conn.execute("REINDEX")

    def close(self):
        self.conn.close()
