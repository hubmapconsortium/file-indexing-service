import logging
import re
import sys
import os
import queue
import signal
import threading
import time
import ast
from argparse import ArgumentParser
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from contextlib import contextmanager
from pathlib import Path

import requests

# Import service resources
from database import Database, FileInfo
from file_indexing_utils import FileIndexingUtils

BATCH_SIZE = 10000
MAX_WORKERS = 8
UUID_API_TIMEOUT = 30  # seconds

Config = namedtuple(
    "Config",
    [
        "database",
        "log_id",
        "log_level",
        "slack_notifications",
        "paths",
        "create_new_database",
        "uuid_api_url",
        "globus_token",
    ],
)

log_file_name = "Log filename not set"
logger = logging.getLogger("local_file_index")
terminate_event = threading.Event()

# Thread-safe cache of UUID API responses keyed by dataset_uuid.
# Each entry is a dict mapping rel_path -> {md5_checksum, sha256_checksum}.
# Populated on first encounter of a dataset_uuid; shared across file_walk_worker threads.
_uuid_api_cache: dict = {}
_uuid_api_cache_lock = threading.Lock()

# Regex for a 32-character lowercase hex UUID
_UUID_RE = re.compile(r'[0-9a-f]{32}')


def parse_config() -> Config:
    parser = ArgumentParser(description="Backup file info to local SQLite database.")
    parser.add_argument(
        "--config", default="config.ini", help="Path to the configuration file (ini format)"
    )
    parser.add_argument(
        "--create-new-database", action="store_true",
        help="Create a new SQLite database. Exits with an error if the database file already exists."
    )
    args = parser.parse_args()

    c = ConfigParser()
    c.read(args.config)

    # Validate arguments
    paths = tuple(
        p.strip() for p in c.get("Globus", "INDEX_PATHS", fallback="").split(",") if p.strip()
    )
    invalid_paths = [p for p in paths if not os.path.isdir(p)]
    if len(invalid_paths) > 0:
        raise NotADirectoryError(f"Invalid directories to index: {', '.join(invalid_paths)}")

    return Config(
        paths=paths,
        database=c.get("Local", "DATABASE_FILEPATH", fallback="local_file_index.db"),
        log_id=c.get("DEFAULT", "LOG_ID", fallback="default"),
        log_level=c.get("Local", "LOG_LEVEL", fallback="info"),
        slack_notifications=c.get("Slack", "SLACK_NOTIFICATIONS", fallback="ENABLED"),
        create_new_database=args.create_new_database,
        uuid_api_url=c.get("Service", "UUID_API_URL", fallback=None),
        globus_token=c.get("Globus", "GLOBUS_GROUPS_TOKEN", fallback=None),
    )


def setup_logger(log_id: str, log_level: str):
    global log_file_name

    if not os.path.exists("exec_info"):
        os.makedirs("exec_info")

    log_file_name = os.path.join(
        "exec_info", f"local-file-index-{log_id}-{time.strftime('%Y-%m-%d-%H-%M-%S')}.log"
    )
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file_name)],
    )
    print(f"Logging to {log_file_name}")


@contextmanager
def setup_lock_file(db_path: str):
    global err
    lock_file = db_path + ".lock"
    if os.path.exists(lock_file):
        err_msg = f"Lock file {lock_file} already exists. Another process may be running."
        logger.error(err_msg)
        service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                          , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                          , mentions_dict=slack_user_id_mentions_on_error_dict
                                          , process_bad_news_emoji=':bangbang:'
                                          , exit_code=2)
        raise RuntimeError(err_msg)
    with open(lock_file, "w") as f:
        f.write(str(os.getpid()))
    try:
        yield
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)


def extract_dataset_uuid(fpath: str) -> str | None:
    """Extract the first 32-character lowercase hex component from a file path.
    Returns None and logs a warning if no such component is found."""
    for part in Path(fpath).parts:
        if _UUID_RE.fullmatch(part):
            return part
    logger.warning(f"Could not extract dataset UUID from path: {fpath}")
    return None


def get_uuid_api_checksums(dataset_uuid: str) -> dict:
    """Fetch checksums for all files in a dataset from the UUID API.
    Returns a dict mapping rel_path -> {md5_checksum, sha256_checksum}.
    Returns an empty dict on any error so callers can proceed without checksums."""
    if not config.uuid_api_url or not config.globus_token:
        return {}

    url = f"{config.uuid_api_url}/{dataset_uuid}/files"
    try:
        res = requests.get(
            url,
            headers={"Authorization": f"Bearer {config.globus_token}"},
            timeout=UUID_API_TIMEOUT,
        )
        if res.status_code != 200:
            logger.warning(
                f"UUID API returned {res.status_code} for dataset {dataset_uuid}: {url}"
            )
            return {}
        files = res.json()
        return {
            f["path"]: {
                "md5_checksum": f.get("md5_checksum"),
                "sha256_checksum": f.get("sha256_checksum"),
            }
            for f in files
            if "path" in f
        }
    except Exception as e:
        logger.warning(f"UUID API call failed for dataset {dataset_uuid}: {e}")
        return {}


def get_checksums_for_file(dataset_uuid: str, fpath: str) -> tuple[str | None, str | None]:
    """Return (md5_checksum, sha256_checksum) for a file, using the UUID API cache.
    Fetches from UUID API on first encounter of dataset_uuid, then caches for subsequent files."""
    with _uuid_api_cache_lock:
        if dataset_uuid in _uuid_api_cache:
            checksum_map = _uuid_api_cache[dataset_uuid]
            for rel_path, checksums in checksum_map.items():
                if fpath.endswith(rel_path):
                    return checksums["md5_checksum"], checksums["sha256_checksum"]
            return None, None

    # Fetch outside the lock so other threads are not blocked during the API call
    checksum_map = get_uuid_api_checksums(dataset_uuid)
    with _uuid_api_cache_lock:
        # Another thread may have fetched while we were calling the API — keep first result
        if dataset_uuid not in _uuid_api_cache:
            _uuid_api_cache[dataset_uuid] = checksum_map
            logger.info(
                f"UUID API: cached {len(checksum_map)} file checksums for dataset {dataset_uuid}"
            )

    for rel_path, checksums in checksum_map.items():
        if fpath.endswith(rel_path):
            return checksums["md5_checksum"], checksums["sha256_checksum"]
    return None, None


def _process_file(fpath: str, file_queue: queue.Queue) -> int:
    """Stat a single file, look up checksums from UUID API cache, and put it on the queue.
    Returns 1 on error, 0 on success."""
    try:
        stat = os.lstat(fpath)
        if os.path.islink(fpath):
            return 0

        dataset_uuid = extract_dataset_uuid(fpath)
        md5, sha256 = (None, None)
        if dataset_uuid:
            md5, sha256 = get_checksums_for_file(dataset_uuid, fpath)

        file_queue.put(FileInfo(
            path=fpath,
            size=stat.st_size,
            last_modified_at=int(stat.st_mtime),
            dataset_uuid=dataset_uuid,
            uuid_api_md5=md5,
            uuid_api_sha256=sha256,
        ))
        return 0
    except Exception as e:
        logger.error(f"Error getting info for {fpath}: {e}")
        return 1


def file_walk_worker(
    dir_path: str,
    file_queue: queue.Queue,
) -> int:
    logger.debug(f"Started indexing of {dir_path}")
    error_count = 0
    for root, dirs, files in os.walk(dir_path, followlinks=False):
        if terminate_event.is_set():
            logger.debug(f"Termination signal received, stopping indexing of {dir_path}")
            return error_count

        # Identify zarr subdirectories before descending into them.
        # For each zarr directory: walk its entire subtree looking only for *.zarr.zip files,
        # then prune it so os.walk does not descend into it again.
        zarr_dirs = [d for d in dirs if 'zarr' in d]
        for zarr_dir in zarr_dirs:
            zarr_root = os.path.join(root, zarr_dir)
            for zroot, _, zfiles in os.walk(zarr_root, followlinks=False):
                if terminate_event.is_set():
                    logger.debug(f"Termination signal received, stopping indexing of {dir_path}")
                    return error_count
                for zfname in zfiles:
                    if zfname.endswith('.zarr.zip'):
                        error_count += _process_file(os.path.join(zroot, zfname), file_queue)
            dirs.remove(zarr_dir)  # prune — os.walk will not descend here

        # Process non-zarr files in the current directory normally
        for fname in files:
            if terminate_event.is_set():
                logger.debug(f"Termination signal received, stopping indexing of {dir_path}")
                return error_count
            error_count += _process_file(os.path.join(root, fname), file_queue)

    logger.debug(f"Finished indexing of {dir_path}")
    return error_count


def database_batch_insert_worker(
    db_path: str,
    file_queue: queue.Queue,
    stop_event: threading.Event,
) -> int:
    error_count = 0
    with Database(db_path) as db:
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


config = parse_config()
setup_logger(config.log_id, config.log_level)

service_utils = None
try:
    service_utils = FileIndexingUtils(config_file_name=Path('fileIndexingService.ini')
                                      , disable_slack_notifications=(config.slack_notifications == 'DISABLED'))
    print('FileIndexingUtils instantiated.')
except Exception as e:
    print("Error instantiating a FileIndexingUtils during startup.")
    print(str(e))
    sys.exit(3)


util_config = service_utils.get_config()
# Create a usable Python dict globals from the str in the INI file
slack_user_id_mentions_on_error_dict = ast.literal_eval(util_config['SLACK_USER_ID_MENTIONS_ON_ERROR'])
slack_user_id_mentions_on_success_dict = ast.literal_eval(util_config['SLACK_USER_ID_MENTIONS_ON_SUCCESS'])


def main():

    num_workers = min(len(config.paths) + 1, MAX_WORKERS)
    msg = f"{util_config['SLACK_NEUTRAL_INFO_EMOJI']}" \
          f" The {Path(__file__).name} process to gather data for local files started with" \
          f" {num_workers} workers."
    logger.info(msg)
    service_utils.postToSlackChannel(channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                     , msg=msg)

    def handle_termination(signum, frame):
        err_msg = f"The {Path(__file__).name} process received a termination signal. Cleaning up and exiting..."
        print(err_msg)
        logger.error(err_msg)
        service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                          , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                          , mentions_dict=slack_user_id_mentions_on_error_dict
                                          , process_bad_news_emoji=':bangbang:'
                                          , exit_code=2)
        terminate_event.set()

    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)

    # Handle --create-new-database before entering the lock context,
    # so failures are clean and no lock file is left behind.
    if config.create_new_database:
        db_path = config.database
        if os.path.exists(db_path):
            print(f"ERROR: Database file {db_path} already exists. "
                  f"Remove it manually before creating a new one.", file=sys.stderr)
            sys.exit(1)
        # Create and immediately close a fresh database to initialize the schema.
        Database(db_path).close()
        print(f"Created new database: {db_path}")

    num_errors = 0
    start = time.time()

    with setup_lock_file(config.database):
        file_queue = queue.Queue(maxsize=BATCH_SIZE * 10)
        stop_event = threading.Event()

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            db_future = executor.submit(
                database_batch_insert_worker,
                config.database,
                file_queue,
                stop_event,
            )
            futures = [
                executor.submit(file_walk_worker, d, file_queue)
                for d in config.paths
            ]
            for future in futures:
                num_errors += future.result()

            stop_event.set()
            file_queue.join()
            num_errors += db_future.result()

    elapsed = round(time.time() - start, 2)
    logger.setLevel(logging.INFO)
    if config.slack_notifications != 'DISABLED':
        if num_errors > 0:
            err_msg =   f"{num_errors} errors occurred during the {config.log_id} " \
                        f" process to gather data for local files." \
                        f" See {log_file_name}." \
                        f"{util_config['SLACK_BAD_NEWS_EMOJI']}"
            service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                              , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                              , mentions_dict=slack_user_id_mentions_on_error_dict
                                              , process_bad_news_emoji=':bangbang:'
                                              , exit_code=2)
        else:
            success_msg =   f"The {config.log_id} process to gather data for" \
                            f" local files completed successfully." \
                            f"{util_config['SLACK_GOOD_NEWS_EMOJI']}"
            service_utils.postToSlackChannel(channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                             , msg=success_msg
                                             , mentions_dict=slack_user_id_mentions_on_success_dict)
    logger.info(f"Finished indexing all directories: {elapsed} seconds")


if __name__ == "__main__":
    main()
