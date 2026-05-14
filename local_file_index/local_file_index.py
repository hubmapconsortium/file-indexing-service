import logging
import os
import queue
import signal
import threading
import time
from argparse import ArgumentParser
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from contextlib import contextmanager

import requests
from database import Database, FileInfo

BATCH_SIZE = 10000
MAX_WORKERS = 8

Config = namedtuple("Config", ["paths", "database", "log_id", "log_level", "slack_webhook_url"])


def parse_config() -> Config:
    parser = ArgumentParser(description="Backup file info to local SQLite database.")
    parser.add_argument(
        "--config", default="config.ini", help="Path to the configuration file (ini format)"
    )
    args = parser.parse_args()

    c = ConfigParser()
    c.read(args.config)

    # Validate arguments
    paths = tuple(
        p.strip() for p in c.get("Local", "INDEX_PATHS", fallback="").split(",") if p.strip()
    )
    invalid_paths = [p for p in paths if not os.path.isdir(p)]
    if len(invalid_paths) > 0:
        raise NotADirectoryError(f"Invalid directories to index: {', '.join(invalid_paths)}")

    return Config(
        paths=paths,
        database=c.get("Local", "DATABASE_FILEPATH", fallback="local_file_index.db"),
        log_id=c.get("Local", "LOG_ID", fallback="default"),
        log_level=c.get("Local", "LOG_LEVEL", fallback="info"),
        slack_webhook_url=c.get("Slack", "SLACK_WEBHOOK_URL", fallback=None),
    )


@contextmanager
def setup_logger(log_id: str, log_level: str):
    if not os.path.exists("logs"):
        os.makedirs("logs")
    log_file = os.path.join(
        "logs", f"local-file-index-{log_id}-{time.strftime('%Y-%m-%d-%H-%M-%S')}.log"
    )
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file)],
    )
    logger = logging.getLogger("local_file_index")
    try:
        yield logger
    finally:
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)


@contextmanager
def setup_lock_file(db_path: str, logger: logging.Logger):
    lock_file = db_path + ".lock"
    if os.path.exists(lock_file):
        logger.error(f"Lock file {lock_file} already exists. Another process may be running.")
        raise RuntimeError(f"Lock file {lock_file} already exists. Another process may be running.")
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))
    try:
        yield
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)


def file_walk_worker(
    dir_path: str,
    file_queue: queue.Queue,
    terminate_event: threading.Event,
    logger: logging.Logger,
) -> int:
    logger.debug(f"Started indexing of {dir_path}")
    error_count = 0
    for root, _, files in os.walk(dir_path, followlinks=False):
        for fname in files:
            if terminate_event.is_set():
                logger.debug(f"Termination signal received, stopping indexing of {dir_path}")
                return error_count

            fpath = os.path.join(root, fname)
            try:
                stat = os.lstat(fpath)
                if os.path.islink(fpath):
                    continue
                file_queue.put(FileInfo(fpath, stat.st_size, int(stat.st_mtime)))
            except Exception as e:
                logger.error(f"Error getting info for {fpath}: {e}")
                error_count += 1
    logger.debug(f"Finished indexing of {dir_path}")
    return error_count


def database_batch_insert_worker(
    db_path: str,
    file_queue: queue.Queue,
    stop_event: threading.Event,
    terminate_event: threading.Event,
    logger: logging.Logger,
) -> int:
    error_count = 0
    with Database(db_path, logger) as db:
        batch = []

        while True:
            if (stop_event.is_set() or terminate_event.is_set()) and file_queue.empty():
                break
            try:
                file_info = file_queue.get(timeout=0.1)
                batch.append(file_info)
                if len(batch) >= BATCH_SIZE:
                    db.insert_files(batch)
                    batch.clear()
                file_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                file_queue.task_done()
                logger.error(f"Error inserting files into database: {e}")
                error_count += 1
                continue
        if batch:
            try:
                db.insert_files(batch)
            except Exception as e:
                logger.error(f"Error inserting final batch into database: {e}")
                error_count += 1

        db.reindex()

    return error_count


def main():
    config = parse_config()
    terminate_event = threading.Event()

    def handle_termination(signum, frame):
        print("Received termination signal. Cleaning up and exiting...")
        terminate_event.set()

    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)

    with (
        setup_logger(config.log_id, config.log_level) as logger,
        setup_lock_file(config.database, logger),
    ):
        start = time.time()
        num_errors = 0
        file_queue = queue.Queue(maxsize=BATCH_SIZE * 10)
        stop_event = threading.Event()

        num_workers = min(len(config.paths) + 1, MAX_WORKERS)
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            db_future = executor.submit(
                database_batch_insert_worker,
                config.database,
                file_queue,
                stop_event,
                terminate_event,
                logger,
            )
            futures = [
                executor.submit(file_walk_worker, d, file_queue, terminate_event, logger)
                for d in config.paths
            ]
            for future in futures:
                num_errors += future.result()

            stop_event.set()
            file_queue.join()
            num_errors += db_future.result()

        if config.slack_webhook_url and num_errors > 0:
            # Send a Slack message if there were errors
            requests.post(
                config.slack_webhook_url,
                json={
                    "text": (
                        f"{num_errors} errors occurred during {config.log_id} local "
                        "file indexing."
                    )
                },
            )

        elapsed = round(time.time() - start, 2)
        logger.setLevel(logging.INFO)
        logger.info(f"Finished indexing all directories: {elapsed} seconds")


if __name__ == "__main__":
    main()