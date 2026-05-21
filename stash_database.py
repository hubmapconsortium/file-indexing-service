import ast
import logging
import os
import signal
import sys
import threading
import time
from argparse import ArgumentParser
from collections import namedtuple
from configparser import ConfigParser
from contextlib import contextmanager
from pathlib import Path

from database import Database, VersionedFileInfo
from file_indexing_utils import FileIndexingUtils
from uploader import AWSS3Uploader

Config = namedtuple(
    "Config",
    [
        "database",
        "temp_path",
        "log_id",
        "log_level",
        "slack_notifications",
        "aws_backup_bucket",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_region_name",
    ],
)

log_file_name = "Log filename not set"
logger = logging.getLogger("database_backup")
terminate_event = threading.Event()


def parse_config() -> Config:
    parser = ArgumentParser(description="Stash copy of local SQLite database in versioned AWS S3 Bucket.")
    parser.add_argument(
        "--config", default="config.ini", help="Path to the configuration file (ini format)"
    )
    args = parser.parse_args()

    c = ConfigParser()
    c.read(args.config)

    return Config(
        database=c.get("Local", "DATABASE_FILEPATH"),
        temp_path=c.get("Local", "TEMP_PATH", fallback="./tmp"),
        log_id=c.get("Local", "LOG_ID", fallback="default"),
        log_level=c.get("Local", "LOG_LEVEL", fallback="info"),
        slack_notifications=c.get("Slack", "SLACK_NOTIFICATIONS", fallback="ENABLED"),
        aws_backup_bucket=c.get("AWSSettings", "AWS_BACKUP_BUCKET"),
        aws_access_key_id=c.get("AWSSettings", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=c.get("AWSSettings", "AWS_SECRET_ACCESS_KEY"),
        aws_region_name=c.get("AWSSettings", "AWS_REGION_NAME")
    )


def setup_logger(log_id: str, log_level: str):
    global log_file_name

    if not os.path.exists("exec_info"):
        os.makedirs("exec_info")
    log_file_name = os.path.join(
        "exec_info", f"stash-database-{log_id}-{time.strftime('%Y-%m-%d-%H-%M-%S')}.log"
    )
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file_name)],
    )
    print(f"Logging to {log_file_name}")


@contextmanager
def setup_lock_file(db_path: str):
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


config = parse_config()
setup_logger(config.log_id, config.log_level)

service_utils = None
try:
    service_utils = FileIndexingUtils(config_file_name=str(Path('fileIndexingService.ini'))
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
    msg =   f"{util_config['SLACK_NEUTRAL_INFO_EMOJI']}" \
            f" The {Path(__file__).name} process is launching to" \
            f" stash the {os.path.basename(config.database)} SQLite database" \
            f" in AWS S3 Bucket {config.aws_backup_bucket}"
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

    num_errors = 0

    with setup_lock_file(config.database):
        start = time.time()

        try:
            uploader = AWSS3Uploader(
                bucket_name=config.aws_backup_bucket,
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                aws_region_name=config.aws_region_name,
            )
            # Upload a copy of the SQLite database to the versioned AWS S3 Bucket for DEEP_ARCHIVE storage.
            uploader.upload_file(
                key=str(os.path.basename(config.database)),
                filepath=config.database,
                last_modified_at=int(start),
                storage_class="DEEP_ARCHIVE",
            )
            elapsed = round(time.time() - start, 2)
            logger.info(f"Finished stashing {os.path.basename(config.database)}"
                        f" in AWS S3 Bucket {config.aws_backup_bucket}"
                        f" in {elapsed} seconds.")
        except Exception as e:
            logger.error(   f"Failed to stash {os.path.basename(config.database)}"
                            f" in AWS S3 Bucket {config.aws_backup_bucket}:"
                            f" {e}")
            num_errors = 1

    if config.slack_notifications != 'DISABLED':
        if num_errors > 0:
            err_msg =   f"{num_errors} errors occurred during {config.log_id}" \
                        f" stash of SQLite database {os.path.basename(config.database)}" \
                        f" to AWS S3 Bucket {config.aws_backup_bucket}." \
                        f" See {log_file_name}." \
                        f"{util_config['SLACK_BAD_NEWS_EMOJI']}"
            service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                              , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                              , mentions_dict=slack_user_id_mentions_on_error_dict
                                              , process_bad_news_emoji=':bangbang:'
                                              , exit_code=2)
        else:
            success_msg =   f"Finished stashing {os.path.basename(config.database)}" \
                            f" in AWS S3 Bucket {config.aws_backup_bucket}" \
                            f" in {elapsed} seconds." \
                            f"{util_config['SLACK_GOOD_NEWS_EMOJI']}"
            service_utils.postToSlackChannel(channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                             , msg=success_msg
                                             , mentions_dict=slack_user_id_mentions_on_success_dict)


if __name__ == "__main__":
    main()
