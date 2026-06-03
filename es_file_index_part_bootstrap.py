######################################################################################################################
# This file is for bootstrapping the indices of the file-indexing-service prior to scheduling the regular jobs which #
# keep them updated.  This program is crudely designed to work with a portion of the lines in the SQLite database    #
# built by the local_file_index.py program.  It is assumed the database files table has a dataset_uuid column set    #
# during the file system crawl.  The PARTITION_KEY constant in this file limits it to working with a subset of rows  #
# in which dataset_uuid ends with the specified character.  It is assumed multiple copies of this program will run   #
# simultaneously, as reflected in the keys of the PARTITION_CLAUSES dict, each filling different ElasticSearch       #
# indices, maintaining different tracking files, etc.  After each is complete, the multiple ES indices will be       #
# merged so the needed indices of file-index-service are bootstrapped.                                               #
######################################################################################################################
import sys
import ast
import hashlib
import json
import logging
import os
import re
import signal

import threading
import time
from argparse import ArgumentParser
from collections import namedtuple
from configparser import ConfigParser
from typing import Dict, List, Optional, Tuple
# While we're still using a Python 3.9 interpreter prior to upgrading our
# platform, alias LiteralString as needed to satisfy static type checking.
if sys.version_info >= (3, 11):
    from typing import LiteralString
else:
    LiteralString = str
from pathlib import Path

from neo4j import Driver, GraphDatabase, Record
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Import service resources
# SQLite database is opened read-only - this process never writes to SQLite,
# so multiple processes can work on different partitions of the dataset simultaneously.
from database import Database, DBFilePart
from file_manager import FileManager
from file_indexing_utils import FileIndexingUtils

Config = namedtuple(
    "Config",
    [
        "database",
        "log_id",
        "log_level",
        "elastic_search_url",
        "elastic_search_public_index",
        "elastic_search_private_index",
        "globus_groups_token",
        "globus_public_endpoint_filepath",
        "globus_consortium_endpoint_filepath",
        "globus_protected_endpoint_filepath",
        "neo4j_uri",
        "neo4j_username",
        "neo4j_password",
        "slack_notifications",
        "ingest_api_url",
        "ubkg_url",
        "ubkg_application_context",
        "uuid_api_url",
    ],
)

UUIDFileInfo = namedtuple(
    "UUIDFileInfo", ["path", "md5_checksum", "sha256_checksum", "base_dir", "size"]
)

# When es_inserts reaches this size, flush to ElasticSearch immediately rather than
# waiting for the end of the dataset loop. Limits memory usage and reduces the risk
# of losing work if the process is interrupted.
FLUSH_ES_DOCS_LEVEL = 50  # KBKBKB @TODO set to 1000 before production

# Set to a list of Dataset UUIDs to limit indexing to specific datasets during development.
# Set to an empty list or None to index all datasets.
DEBUG_DATASET_UUID_LIST = []

# Partition settings — controls which subset of datasets this instance processes.
# Each copy of this script should set PARTITION_KEY to one of the keys in PARTITION_CLAUSES.
# PARTITION_CLAUSES maps a short label to a SQL fragment used by query_files_part().
# The SQL fragment filters on the 32nd character of dataset_uuid (SUBSTR position 32, 1-indexed).
# The C-F partition also catches rows where dataset_uuid IS NULL so no rows are lost.
PARTITION_CLAUSES = {
    '0-3': "(SUBSTR(dataset_uuid, 32, 1) IN ('0','1','2','3'))",
    '4-7': "(SUBSTR(dataset_uuid, 32, 1) IN ('4','5','6','7'))",
    '8-B': "(SUBSTR(dataset_uuid, 32, 1) IN ('8','9','a','b'))",
    'C-F': "(SUBSTR(dataset_uuid, 32, 1) IN ('c','d','e','f') OR dataset_uuid IS NULL)",
}
PARTITION_KEY = '0-3'   # set this to the key for this instance

# Cypher query for HuBMAP to get primary and processed Datasets with certain creation_action Activity,
# supporting substitutable Dataset status value lists for different functions.
# Note: ds.metadata and ds.files are returned as raw strings rather than parsed via APOC.
# This avoids Neo.ClientError.Procedure.ProcedureCallFailed errors caused by invalid JSON
# (e.g. unescaped control characters) in individual dataset records which would otherwise
# kill the entire Neo4j session. Parsing is done per-record in Python instead.
DATASETS_TO_INDEX_QUERY: LiteralString = """
    MATCH (donor:Donor)-[:ACTIVITY_INPUT]->(organ_activity:Activity)-[:ACTIVITY_OUTPUT]->(organ:Sample {sample_category:'organ'})-[*]->(a:Activity)-[:ACTIVITY_OUTPUT]->(ds:Dataset)
    WHERE a.creation_action IN ['Create Dataset Activity', 'Central Process','Lab Process','External Process']
    AND ds.status IN $statuses
    AND NOT (ds)<-[:REVISION_OF]-(:Entity)
    RETURN ds.uuid AS uuid, ds.hubmap_id AS hubmap_id, ds.group_name AS group_name,
    ds.status AS status, ds.dataset_type AS dataset_type, ds.data_access_level AS data_access_level,
    ds.contains_human_genetic_sequences AS contains_human_genetic_sequences,
    ds.metadata AS metadata_str,
    ds.files AS files_str,
    a.creation_action AS creation_action,
    COLLECT(apoc.map.fromValues(['uuid', donor.uuid, 'entity_type', donor.entity_type])) AS donors,
    COLLECT(apoc.map.fromValues(['uuid', donor.uuid, 'code', organ.organ])) AS organs
    ORDER BY rand()
"""

log_file_name = "Log filename not set"
logger = logging.getLogger("es-file-index")


def parse_config() -> Config:
    parser = ArgumentParser(description="Bootstrap initial ElasticSearch index from SQLite file index.")
    parser.add_argument(
        "--config", default="config.ini", help="Path to the configuration file (ini format)"
    )
    args = parser.parse_args()

    c = ConfigParser()
    c.read(args.config)

    return Config(
        database=c.get("Local", "DATABASE_FILEPATH", fallback="local_file_index.db"),
        log_id=c.get("DEFAULT", "LOG_ID", fallback="default"),
        log_level=c.get("Local", "LOG_LEVEL", fallback="info"),
        elastic_search_url=c.get("ElasticSearch", "ELASTIC_SEARCH_URL"),
        elastic_search_public_index=c.get("ElasticSearch", "ELASTIC_SEARCH_PUBLIC_INDEX"),
        elastic_search_private_index=c.get("ElasticSearch", "ELASTIC_SEARCH_PRIVATE_INDEX"),
        globus_groups_token=c.get("Globus", "GLOBUS_GROUPS_TOKEN"),
        globus_public_endpoint_filepath=c.get("Globus", "GLOBUS_PUBLIC_ENDPOINT_FILEPATH"),
        globus_consortium_endpoint_filepath=c.get("Globus", "GLOBUS_CONSORTIUM_ENDPOINT_FILEPATH"),
        globus_protected_endpoint_filepath=c.get("Globus", "GLOBUS_PROTECTED_ENDPOINT_FILEPATH"),
        neo4j_uri=c.get("Neo4J", "NEO4J_URI"),
        neo4j_username=c.get("Neo4J", "NEO4J_USERNAME"),
        neo4j_password=c.get("Neo4J", "NEO4J_PASSWORD"),
        slack_notifications=c.get("Slack", "SLACK_NOTIFICATIONS", fallback="ENABLED"),
        ingest_api_url=c.get("Service", "INGEST_API_URL"),
        ubkg_url=c.get("Service", "UBKG_URL"),
        ubkg_application_context=c.get("Service", "UBKG_APPLICATION_CONTEXT"),
        uuid_api_url=c.get("Service", "UUID_API_URL"),
    )


def setup_logger(log_id: str, log_level: str):
    global log_file_name

    if not os.path.exists("exec_info"):
        os.makedirs("exec_info")

    log_file_name = os.path.join(
        "exec_info", f"es-file-index-{config.log_id}-{PARTITION_KEY}-{time.strftime('%Y-%m-%d-%H-%M-%S')}.log"
    )
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file_name)],
    )
    print(f"Logging to {log_file_name}")


TIMEOUT = 30  # seconds

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

terminate_event = threading.Event()

session = Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[408, 429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)

try:
    file_manager = FileManager(
        ingest_api_url=config.ingest_api_url,
        ubkg_url=config.ubkg_url,
        ubkg_application_context=config.ubkg_application_context,
        token=config.globus_groups_token,
        session=session,
        logger=logger,
    )
except Exception as e:
    err_msg = f"Failed to initialize FileManager: {e}"
    logger.critical(err_msg)
    service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                      , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                      , mentions_dict=slack_user_id_mentions_on_error_dict
                                      , process_bad_news_emoji=':bangbang:'
                                      , exit_code=3)


def get_ubkg_organs() -> List[dict]:
    res = session.get(url=f"{config.ubkg_url}/organs?application_context={config.ubkg_application_context}",
                      timeout=TIMEOUT)
    if res.status_code != 200:
        msg = f"Error fetching UBKG organs: {res.status_code}"
        raise Exception(msg)
    return res.json()


def get_files_from_uuid_api(dataset_uuid: str) -> List[dict]:
    res = session.get(
        f"{config.uuid_api_url}/{dataset_uuid}/files",
        headers={"Authorization": f"Bearer {config.globus_groups_token}"},
        timeout=TIMEOUT,
    )
    if res.status_code >= 200 and res.status_code < 300:
        return res.json()
    elif res.status_code in [303]:
        s3_response = session.get(res.content.decode(), timeout=TIMEOUT)
        if s3_response.status_code >= 200 and s3_response.status_code < 300:
            return s3_response.json()
        else:
            err_msg =   f"Error fetching the large response content from AWS S3 at:" \
                        f" {res.content.decode()}"
    else:
        err_msg = f"HTTP {res.status_code} error fetching files using:" \
                  f" {config.uuid_api_url}/{dataset_uuid}/files"
    raise Exception(err_msg)


def get_dataset_globus_path(dataset: Record) -> str:
    if dataset["contains_human_genetic_sequences"] is True:
        return os.path.join(
            config.globus_protected_endpoint_filepath, dataset["group_name"], dataset["uuid"]
        )

    if dataset["status"] == "Published":
        return os.path.join(config.globus_public_endpoint_filepath, dataset["uuid"])

    return os.path.join(
        config.globus_consortium_endpoint_filepath, dataset["group_name"], dataset["uuid"]
    )


def bulk_insert_es_indices(
        indices: List[str], upserts: List[dict]
) -> Optional[List[str]]:
    """Insert documents into one or more ElasticSearch indices using the bulk API.
    Send documents to one or more ElasticSearch indices using the bulk API.
    Uses doc_as_upsert=true - ES inserts if the document does not exist, updates if it does.
    During bootstrap into an empty index all operations will be inserts in practice."""
    es_upserts = [
        f'{{"update":{{"_id":"{doc["dataset_uuid"]}/{doc["rel_path"]}"}}}}\n{{"doc":{json.dumps(doc, separators=(",", ":"))},"doc_as_upsert":true}}'
        for doc in upserts
    ]

    # split upserts into chunks to avoid exceeding the request size limit
    error_msgs = []
    chunk_size = 100
    chunks = [es_upserts[i: i + chunk_size] for i in range(0, len(es_upserts), chunk_size)]
    for chunk in chunks:
        body = "\n".join(chunk) + "\n"
        for index in indices:
            url = f"{config.elastic_search_url}/{index}/_bulk"
            res = session.post(
                url,
                headers={"Content-Type": "application/x-ndjson"},
                data=body,
                timeout=TIMEOUT,
            )
            if res.status_code != 200:
                raise Exception(
                    f"Error inserting documents for dataset in {index}: "
                    f"{res.status_code}, {res.text}"
                )

            res_body = res.json().get("items", [])
            result_values = [item.get("update") for item in res_body if "update" in item]
            msgs = [
                f"{item['_id']}: Insert - {item.get('error', {}).get('reason')}"
                for item in result_values
                if item["status"] not in [200, 201]
            ]
            if msgs:
                error_msgs.extend(msgs)

    return error_msgs if error_msgs else None


def bulk_create_file_uuids(
        file_info: List[UUIDFileInfo], parent_uuid: str
):
    # split inserts into chunks to avoid exceeding gateway timeouts
    chunk_size = 1000
    chunks = [file_info[i: i + chunk_size] for i in range(0, len(file_info), chunk_size)]
    error = None
    for idx, chunk in enumerate(chunks):
        res = session.post(
            f"{config.uuid_api_url}/uuid?entity_count={len(chunk)}",
            headers={"Authorization": f"Bearer {config.globus_groups_token}"},
            json={
                "entity_type": "FILE",
                "parent_ids": [parent_uuid],
                "file_info": [item._asdict() for item in chunk],
            },
            timeout=TIMEOUT,
        )
        if res.status_code != 200:
            error = f"Error creating file uuids in uuid-api: {res.status_code}, {res.text}"

        if idx < len(chunks) - 1:
            time.sleep(1)

    if error:
        raise Exception(error)


def generate_checksums(filepath: str) -> Tuple[str, str]:
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    read_size = 65536
    with open(filepath, "rb") as f:
        while chunk := f.read(read_size):
            md5.update(chunk)
            sha256.update(chunk)

    return md5.hexdigest(), sha256.hexdigest()


def create_file_info(file: DBFile) -> UUIDFileInfo:
    md5_checksum, sha256_checksum = generate_checksums(file.path)

    return UUIDFileInfo(
        path=file.rel_path,
        md5_checksum=md5_checksum,
        sha256_checksum=sha256_checksum,
        base_dir="DATA_UPLOAD",
        size=os.path.getsize(file.path),
    )


def get_organ_hierarchy(organ: dict) -> str:
    if category := organ.get("category"):
        return category["term"]

    return organ["term"]


def parse_dataset_record(record) -> Optional[dict]:
    """Parse a raw Neo4j dataset record, converting metadata_str and files_str from
    Python repr strings to Python objects. Returns None if the record cannot be parsed,
    allowing the caller to skip it without aborting the entire result set."""
    try:
        dataset = dict(record)
        raw_metadata = dataset.pop("metadata_str", None)
        raw_files = dataset.pop("files_str", None)
        dataset["metadata"] = ast.literal_eval(raw_metadata) if raw_metadata else {}
        dataset["files"] = ast.literal_eval(raw_files) if raw_files else []
        return dataset
    except Exception as e:
        uuid = dict(record).get("uuid", "unknown")
        logger.error(f"Skipping dataset {uuid} — failed to parse record: {e}")
        return None


BOOTSTRAP_CHECKPOINT_FILE = f"exec_info/bootstrap_checkpoint_{PARTITION_KEY}.txt"


def load_checkpoint() -> set:
    """Load the set of ES document _ids already successfully indexed during a previous
    run of this complete bootstrap load. Returns an empty set if no checkpoint exists.
    Delete or archive the checkpoint file before starting a new complete bootstrap load."""
    if not os.path.exists(BOOTSTRAP_CHECKPOINT_FILE):
        logger.info(f"No checkpoint file found at {BOOTSTRAP_CHECKPOINT_FILE} — starting fresh.")
        return set()
    try:
        with open(BOOTSTRAP_CHECKPOINT_FILE, "r") as f:
            ids = {line.rstrip("\n") for line in f if line.strip()}
        logger.info(
            f"Loaded checkpoint from {BOOTSTRAP_CHECKPOINT_FILE}: "
            f"{len(ids):,} previously indexed documents will be skipped."
        )
        return ids
    except Exception as e:
        logger.error(f"Failed to load checkpoint file {BOOTSTRAP_CHECKPOINT_FILE}: {e} — starting fresh.")
        return set()


def append_checkpoint(doc_ids: List[str]):
    """Append a list of successfully indexed ES document _ids to the checkpoint file.
    Called after each successful flush to ES so progress survives interruption."""
    try:
        with open(BOOTSTRAP_CHECKPOINT_FILE, "a") as f:
            for doc_id in doc_ids:
                f.write(doc_id + "\n")
    except Exception as e:
        logger.error(f"Failed to write to checkpoint file {BOOTSTRAP_CHECKPOINT_FILE}: {e}")


def index_published_datasets(
        ubkg_organs: dict,
        driver: Driver,
        db: Database,
        checkpoint_ids: set,
) -> Tuple[List[str], int]:
    es_indices = [
        f"{config.elastic_search_public_index}_{PARTITION_KEY}",
        f"{config.elastic_search_private_index}_{PARTITION_KEY}",
    ]

    num_errors = 0
    dataset_uuids = []
    es_inserts = []
    with driver.session() as neo4j_session:
        # Query for primary and processed datasets with a status of Published.
        # Avoid using APOC in the Cypher query so that bad data causing exceptions
        # is handled in Python rather than aborting the Neo4j session.
        datasets = neo4j_session.run(DATASETS_TO_INDEX_QUERY, statuses=['Published'])
        for raw_record in datasets:
            if terminate_event.is_set():
                logger.info("Termination signal received, stopping indexing.")
                return dataset_uuids, num_errors

            dataset = parse_dataset_record(raw_record)
            if dataset is None:
                err_msg = f"Skipping dataset — failed to parse Neo4j record for uuid: {dict(raw_record).get('uuid', 'unknown')}"
                logger.error(err_msg)
                num_errors += 1
                continue
            if DEBUG_DATASET_UUID_LIST and dataset["uuid"] not in DEBUG_DATASET_UUID_LIST:
                continue
            time.sleep(1)
            dataset_uuids.append(dataset["uuid"])
            logger.info(f"Processing Dataset {dataset['uuid']}")

            # add organ label and hierarchy from ubkg to each organ
            organs = [
                dict(
                    {
                        "label": ubkg_organs[o["code"]]["term"],
                        "hierarchy": get_organ_hierarchy(ubkg_organs[o["code"]]),
                    },
                    **o,
                )
                for o in dataset["organs"]
            ]

            dataset_globus_path = get_dataset_globus_path(dataset)
            try:
                # get files in local database for the dataset, filter out files with blank or
                # numeric extensions
                local_filepath_map = {
                    file.rel_path: file
                    for file in db.query_files_part(dataset_globus_path, PARTITION_CLAUSES[PARTITION_KEY])
                    if (ext := os.path.splitext(file.path)[1]) and not re.match(r"^\.\d+$", ext)
                }
            except Exception as e:
                logger.error(
                    f"Error fetching files from local database for dataset {dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            try:
                # get files in uuid-api for the dataset
                uuid_filepath_map = {
                    item["path"]: item
                    for item in get_files_from_uuid_api(dataset_uuid=dataset["uuid"])
                }
            except Exception as e:
                logger.error(
                    f"Error fetching files from UUID API for dataset {dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            # log the files that are in uuid-api but not in the local database
            diff_uuid_files = set(uuid_filepath_map.keys()) - set(local_filepath_map.keys())
            if diff_uuid_files:
                logger.warning(
                    f"UUID API returns {str(len(diff_uuid_files))} not in local database for Dataset {dataset['uuid']}: "
                    f"{', '.join(diff_uuid_files)}"
                )

            try:
                # get files that are in the database but not the uuid-api
                diff_uuid_files = set(local_filepath_map.keys()) - set(uuid_filepath_map.keys())
                logger.debug(   f"For {str(len(diff_uuid_files))} files in the database for the local filesystem but"
                                f" not known by UUID API, gather file info.")
                file_info = [
                    create_file_info(local_filepath_map[filepath]) for filepath in diff_uuid_files
                ]
                logger.debug(f"Gathered info for {str(len(file_info))} files to add to UUID API.")
            except Exception as e:
                logger.error(
                    "Error creating file info for UUID API creation for dataset "
                    f"{dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            if file_info:
                # update the uuid-api with the new files
                try:
                    logger.info(
                        f"Creating file uuids in UUID API for dataset {dataset['uuid']}: "
                        f"{', '.join([f.path for f in file_info])}"
                    )
                    bulk_create_file_uuids(file_info=file_info, parent_uuid=dataset["uuid"])
                except Exception as e:
                    logger.error(
                        f"Error creating file uuids in UUID API for dataset {dataset['uuid']}: {e}"
                    )
                    num_errors += 1
                    continue

                # get updated files in uuid-api for the dataset
                uuid_filepath_map = {
                    item["path"]: item
                    for item in get_files_from_uuid_api(dataset_uuid=dataset["uuid"])
                }

            es_document_skipped_count = 0
            for idx, (rel_path, local_file) in enumerate(local_filepath_map.items()):
                if idx > 0 and idx % 100 == 0:
                    try:
                        # keep-alive ping to Neo4j to prevent timeout
                        neo4j_session.run("RETURN 1")
                    except Exception as e:
                        logger.warning(f"Neo4j keep-alive ping failed: {e}")

                # skip files already successfully indexed in a previous run of this bootstrap load
                doc_id = f"{dataset['uuid']}/{rel_path}"
                if doc_id in checkpoint_ids:
                    es_document_skipped_count += 1
                    continue

                uuid_file = uuid_filepath_map[rel_path]

                # build the ES document — all files are inserted since this is a bootstrap load
                file_ext = os.path.splitext(local_file.path)[1].lower()
                additional_info = None
                try:
                    additional_info = file_manager.get_additional_info(
                        dataset=dataset,
                        path=rel_path,
                    )
                except Exception as e:
                    logger.error(
                        f"Error fetching file description for {local_file.rel_path} in dataset "
                        f"{dataset['uuid']}: {e}"
                    )
                    num_errors += 1

                logger.info(
                    f"Buffering ES Document for insert for file {local_file.rel_path} in Dataset {dataset['uuid']}"
                )
                doc = {
                    "sha256_checksum": uuid_file["sha256_checksum"],
                    "md5_checksum": uuid_file["md5_checksum"],
                    "dataset_uuid": dataset["uuid"],
                    "dataset_hubmap_id": dataset["hubmap_id"],
                    "dataset_status": dataset["status"],
                    "dataset_type": dataset["dataset_type"],
                    "data_access_level": dataset["data_access_level"],
                    "file_extension": file_ext,
                    "file_uuid": uuid_file["file_uuid"],
                    "organs": organs,
                    "rel_path": uuid_file["path"],
                    "size": uuid_file["size"],
                    "donors": dataset["donors"],
                    "last_modified_at": local_file.last_modified_at,
                }
                if additional_info is not None:
                    doc.update(additional_info)

                es_inserts.append(doc)

                # flush to ES if inserts have reached FLUSH_ES_DOCS_LEVEL
                if len(es_inserts) >= FLUSH_ES_DOCS_LEVEL:
                    try:
                        logger.info(
                            f"Flushing {len(es_inserts)} inserts to ES "
                            f"(mid-dataset flush at file {idx + 1} of {len(local_filepath_map)} "
                            f"for dataset {dataset['uuid']})"
                        )
                        err_msgs = bulk_insert_es_indices(
                            indices=es_indices, upserts=es_inserts
                        )
                        if err_msgs:
                            for msg in err_msgs:
                                logger.error(f"Error inserting document: {msg}")
                        flushed_ids = [f"{doc['dataset_uuid']}/{doc['rel_path']}" for doc in es_inserts]
                        append_checkpoint(flushed_ids)
                        checkpoint_ids.update(flushed_ids)
                        es_inserts = []
                    except Exception as e:
                        logger.error(
                            f"Error flushing inserts to ES for dataset {dataset['uuid']}: {e}"
                        )
                        num_errors += 1

            if es_document_skipped_count > 0:
                logger.info(
                    f"Skipped {es_document_skipped_count:,} already-checkpointed files for dataset {dataset['uuid']}"
                )

            # flush any remaining documents for this dataset
            if es_inserts:
                try:
                    logger.info(
                        f"Flushing {len(es_inserts)} remaining inserts "
                        f"to ES for dataset {dataset['uuid']}"
                    )
                    err_msgs = bulk_insert_es_indices(
                        indices=es_indices, upserts=es_inserts
                    )
                    if err_msgs:
                        for msg in err_msgs:
                            logger.error(f"Error inserting document: {msg}")
                    flushed_ids = [f"{doc['dataset_uuid']}/{doc['rel_path']}" for doc in es_inserts]
                    append_checkpoint(flushed_ids)
                    checkpoint_ids.update(flushed_ids)
                    es_inserts = []
                except Exception as e:
                    logger.error(f"Error flushing remaining inserts for dataset {dataset['uuid']}: {e}")
                    num_errors += 1

    return dataset_uuids, num_errors


def index_qa_datasets(
        ubkg_organs: dict,
        driver: Driver,
        db: Database,
        checkpoint_ids: set,
) -> Tuple[List[str], int]:
    es_indices = [f"{config.elastic_search_private_index}_{PARTITION_KEY}"]

    num_errors = 0
    dataset_uuids = []
    es_inserts = []
    with driver.session() as neo4j_session:
        # Query for primary and processed datasets with a status of QA or Submitted.
        # Avoid using APOC in the Cypher query so that bad data causing exceptions
        # is handled in Python rather than aborting the Neo4j session.
        datasets = neo4j_session.run(DATASETS_TO_INDEX_QUERY, statuses=['QA', 'Submitted', 'Approval'])
        for raw_record in datasets:
            if terminate_event.is_set():
                logger.info("Termination signal received, stopping indexing.")
                return dataset_uuids, num_errors

            dataset = parse_dataset_record(raw_record)
            if dataset is None:
                err_msg = f"Skipping dataset — failed to parse Neo4j record for uuid: {dict(raw_record).get('uuid', 'unknown')}"
                logger.error(err_msg)
                num_errors += 1
                continue
            if DEBUG_DATASET_UUID_LIST and dataset["uuid"] not in DEBUG_DATASET_UUID_LIST:
                continue
            time.sleep(1)
            dataset_uuids.append(dataset["uuid"])
            logger.info(f"Processing Dataset {dataset['uuid']}")

            # add organ label and hierarchy from ubkg to each organ
            organs = [
                dict(
                    {
                        "label": ubkg_organs[o["code"]]["term"],
                        "hierarchy": get_organ_hierarchy(ubkg_organs[o["code"]]),
                    },
                    **o,
                )
                for o in dataset["organs"]
            ]

            dataset_globus_path = get_dataset_globus_path(dataset)
            try:
                # get files in local database for the dataset, filter out files with blank or
                # numeric extensions
                local_filepath_map = {
                    file.rel_path: file
                    for file in db.query_files_part(dataset_globus_path, PARTITION_CLAUSES[PARTITION_KEY])
                    if (ext := os.path.splitext(file.path)[1]) and not re.match(r"^\.\d+$", ext)
                }
            except Exception as e:
                logger.error(
                    f"Error fetching files from local database for dataset {dataset['uuid']}: {e}"
                )
                num_errors += 1
                continue

            es_document_skipped_count = 0
            for idx, (rel_path, local_file) in enumerate(local_filepath_map.items()):
                if idx > 0 and idx % 100 == 0:
                    try:
                        # keep-alive ping to Neo4j to prevent timeout
                        neo4j_session.run("RETURN 1")
                    except Exception as e:
                        logger.warning(f"Neo4j keep-alive ping failed: {e}")

                # skip files already successfully indexed in a previous run of this bootstrap load
                doc_id = f"{dataset['uuid']}/{rel_path}"
                if doc_id in checkpoint_ids:
                    es_document_skipped_count += 1
                    continue

                # build the ES document — all files are inserted since this is a bootstrap load
                file_ext = os.path.splitext(local_file.path)[1].lower()
                additional_info = None
                try:
                    additional_info = file_manager.get_additional_info(
                        dataset=dataset,
                        path=rel_path,
                    )
                except Exception as e:
                    logger.error(
                        f"Error fetching file description for {local_file.rel_path} in dataset "
                        f"{dataset['uuid']}: {e}"
                    )
                    num_errors += 1

                logger.info(
                    f"Buffering ES Document for insert for file {local_file.rel_path} in Dataset {dataset['uuid']}"
                )
                doc = {
                    "dataset_uuid": dataset["uuid"],
                    "dataset_hubmap_id": dataset["hubmap_id"],
                    "dataset_status": dataset["status"],
                    "dataset_type": dataset["dataset_type"],
                    "data_access_level": dataset["data_access_level"],
                    "file_extension": file_ext,
                    "organs": organs,
                    "rel_path": rel_path,
                    "size": local_file.size,
                    "donors": dataset["donors"],
                    "last_modified_at": local_file.last_modified_at,
                }
                if additional_info is not None:
                    doc.update(additional_info)

                es_inserts.append(doc)

                # flush to ES if inserts have reached FLUSH_ES_DOCS_LEVEL
                if len(es_inserts) >= FLUSH_ES_DOCS_LEVEL:
                    try:
                        logger.info(
                            f"Flushing {len(es_inserts)} inserts to ES "
                            f"(mid-dataset flush at file {idx + 1} of {len(local_filepath_map)} "
                            f"for dataset {dataset['uuid']})"
                        )
                        err_msgs = bulk_insert_es_indices(
                            indices=es_indices, upserts=es_inserts
                        )
                        if err_msgs:
                            for msg in err_msgs:
                                logger.error(f"Error inserting document: {msg}")
                        flushed_ids = [f"{doc['dataset_uuid']}/{doc['rel_path']}" for doc in es_inserts]
                        append_checkpoint(flushed_ids)
                        checkpoint_ids.update(flushed_ids)
                        es_inserts = []
                    except Exception as e:
                        logger.error(
                            f"Error flushing inserts to ES for dataset {dataset['uuid']}: {e}"
                        )
                        num_errors += 1

            if es_document_skipped_count > 0:
                logger.info(
                    f"Skipped {es_document_skipped_count:,} already-checkpointed files for dataset {dataset['uuid']}"
                )

            # flush any remaining documents for this dataset
            if es_inserts:
                try:
                    logger.info(
                        f"Flushing {len(es_inserts)} remaining inserts "
                        f"to ES for dataset {dataset['uuid']}"
                    )
                    err_msgs = bulk_insert_es_indices(
                        indices=es_indices, upserts=es_inserts
                    )
                    if err_msgs:
                        for msg in err_msgs:
                            logger.error(f"Error inserting document: {msg}")
                    flushed_ids = [f"{doc['dataset_uuid']}/{doc['rel_path']}" for doc in es_inserts]
                    append_checkpoint(flushed_ids)
                    checkpoint_ids.update(flushed_ids)
                    es_inserts = []
                except Exception as e:
                    logger.error(f"Error flushing remaining inserts for dataset {dataset['uuid']}: {e}")
                    num_errors += 1

    return dataset_uuids, num_errors


def main():

    msg = f"{util_config['SLACK_NEUTRAL_INFO_EMOJI']}" \
          f" The {Path(__file__).name} process is launching to fill" \
          f" ElasticSearch indices {config.elastic_search_public_index} and {config.elastic_search_private_index}."
    logger.info(msg)
    service_utils.postToSlackChannel(channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                     , msg=msg)

    try:
        ubkg_organs = {o["rui_code"]: o for o in get_ubkg_organs()}
    except Exception as e:
        err_msg = f"Error fetching UBKG organs: {e}"
        logger.critical(err_msg)
        service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                          , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                          , mentions_dict=slack_user_id_mentions_on_error_dict
                                          , process_bad_news_emoji=':bangbang:'
                                          , exit_code=3)

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

    log_level = logger.level
    num_errors = 0
    dataset_uuids = []
    auth = (config.neo4j_username, config.neo4j_password)

    # SQLite database is opened read-only - this process never writes to SQLite,
    # so multiple processes can work on different partitions of the dataset simultaneously.
    with (
        GraphDatabase.driver(config.neo4j_uri, auth=auth) as driver,
        Database(config.database, read_only=True) as db,
    ):
        driver.verify_connectivity()

        if terminate_event.is_set():
            return

        # load checkpoint from any previous run of this complete bootstrap load
        checkpoint_ids = load_checkpoint()

        # index Published datasets
        start_time = time.time()
        uuids, errors = index_published_datasets(
            ubkg_organs=ubkg_organs, driver=driver, db=db, checkpoint_ids=checkpoint_ids
        )
        logger.setLevel(logging.INFO)
        logger.info(f"Published dataset indexing took {time.time() - start_time:.2f} seconds")
        logger.setLevel(log_level)
        num_errors += errors
        dataset_uuids.extend(uuids)

        if terminate_event.is_set():
            return

        # index QA datasets
        start_time = time.time()
        uuids, errors = index_qa_datasets(
            ubkg_organs=ubkg_organs, driver=driver, db=db, checkpoint_ids=checkpoint_ids
        )
        logger.setLevel(logging.INFO)
        logger.info(f"QA dataset indexing took {time.time() - start_time:.2f} seconds")
        logger.setLevel(log_level)
        num_errors += errors
        dataset_uuids.extend(uuids)

    if terminate_event.is_set():
        return

    if config.slack_notifications != 'DISABLED':
        if num_errors > 0:
            err_msg =   f"{num_errors} errors occurred during {config.log_id} " \
                        f" ElasticSearch bootstrap file indexing." \
                        f" See {log_file_name}." \
                        f"{util_config['SLACK_BAD_NEWS_EMOJI']}"
            service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                              , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                              , mentions_dict=slack_user_id_mentions_on_error_dict
                                              , process_bad_news_emoji=':bangbang:'
                                              , exit_code=2)
        else:
            success_msg = f"ElasticSearch {config.log_id} bootstrap file indexing completed successfully." \
                          f"{util_config['SLACK_GOOD_NEWS_EMOJI']}"
            service_utils.postToSlackChannel(channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                     , msg=success_msg
                                     , mentions_dict=slack_user_id_mentions_on_success_dict)


if __name__ == "__main__":
    main()
