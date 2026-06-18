######################################################################################################################
# Bootstraps the ElasticSearch indices of file-indexing-service prior to scheduling the regular incremental jobs.   #
# Processes one partition of the SQLite database built by local_file_index.py.                                      #
# The PARTITION_KEY argument selects which subset of dataset UUIDs this instance handles.                           #
# Run one instance per partition key simultaneously; after all complete, merge the partition indices into the        #
# final hm_consortium_files and hm_public_files indices.                                                            #
#                                                                                                                   #
# SQLite database is opened read-only - this process never writes to SQLite,                                        #
# so multiple processes can work on different partitions of the dataset simultaneously.                              #
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
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Tuple

if sys.version_info >= (3, 11):
    from typing import LiteralString
else:
    LiteralString = str

from neo4j import GraphDatabase, Record
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from database import Database, DBFilePart
from file_manager import FileManager
from file_indexing_utils import FileIndexingUtils

Config = namedtuple(
    "Config",
    [
        "database",
        "log_id",
        "log_level",
        "service_config",
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
FLUSH_ES_DOCS_LEVEL = 1000

# The task of flushing es_inserts to ElasticSearch when FLUSH_ES_DOCS_LEVEL is reached
# is performed in chunks with this many documents per Bulk API call.
FLUSH_ES_DOCS_CHUNK_SIZE = 100

# Partition settings - controls which subset of datasets this instance processes.
# PARTITION_KEY is set by --partition-key argument validated against PARTITION_CHAR_SPANS.
PARTITION_KEY = ''

# Character spans for each partition key, used for both Python-side filtering
# and generating the PARTITION_CLAUSES SQL fragments.
PARTITION_CHAR_SPANS = {
    '0-3': {'0', '1', '2', '3'},
    '4-7': {'4', '5', '6', '7'},
    '8-B': {'8', '9', 'a', 'b'},
    'C-F': {'c', 'd', 'e', 'f'},
}

# SQL fragments generated from PARTITION_CHAR_SPANS.
# The last partition also catches rows where dataset_uuid could not be identified
# during the file system crawl, so they are indexed by exactly one partition.
PARTITION_CLAUSES = {
    key: f"(SUBSTR(dataset_uuid, 32, 1) IN ({', '.join(repr(c) for c in sorted(chars))}))"
    for key, chars in PARTITION_CHAR_SPANS.items()
}
last_key = list(PARTITION_CLAUSES.keys())[-1]
PARTITION_CLAUSES[last_key] = PARTITION_CLAUSES[last_key][:-1] + ' OR dataset_uuid IS NULL)'

# Cypher query base for HuBMAP datasets.
# ds.metadata and ds.files are returned as raw strings rather than parsed via APOC
# to avoid Neo.ClientError.Procedure.ProcedureCallFailed errors from invalid JSON
# in individual dataset records killing the entire Neo4j session.
# ORDER BY rand() is intentionally omitted - the checkpoint file handles restartability
# regardless of order, and rand() forces Neo4j to materialize the full result set
# before streaming, which is extremely slow for large graphs.
_QUERY_BASE: LiteralString = """
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
"""


def build_datasets_query() -> str:
    """Build the Cypher query, injecting a partition clause based on PARTITION_KEY
    to limit Neo4j to only the datasets this instance will process."""
    extra_clauses = ""
    if PARTITION_KEY and PARTITION_CHAR_SPANS.get(PARTITION_KEY):
        chars = sorted(PARTITION_CHAR_SPANS[PARTITION_KEY])
        char_list = ", ".join("'" + c + "'" for c in chars)
        extra_clauses += "    AND SUBSTRING(ds.uuid, 31, 1) IN [" + char_list + "]\n"
        logger.info(f"Partition clause injected for PARTITION_KEY={PARTITION_KEY}: chars {chars}")
    return _QUERY_BASE.replace("    RETURN ds.uuid", extra_clauses + "    RETURN ds.uuid", 1)


def fetch_datasets(statuses: List[str]) -> List[dict]:
    """Fetch and materialize all dataset records from Neo4j for the given statuses.
    Uses a lazy streaming loop with keep-alive pings to prevent connection timeout
    during long-running queries. Returns a plain list; the session is closed on return."""
    auth = (config.neo4j_username, config.neo4j_password)
    datasets = []
    ping_count = 0
    with timed_step(f"Neo4j fetch_datasets for statuses {statuses}"):
        with GraphDatabase.driver(config.neo4j_uri, auth=auth) as neo4j_driver:
            with neo4j_driver.session() as neo4j_session:
                result = neo4j_session.run(build_datasets_query(), statuses=statuses)
                for raw_record in result:
                    datasets.append(raw_record)
                    if len(datasets) % 10 == 0:
                        try:
                            neo4j_session.run("RETURN 'neo4j warm query'")
                            ping_count += 1
                            if ping_count % 100 == 0:
                                logger.debug(f"Neo4j keep-alive ping #{ping_count} after {len(datasets):,} records fetched.")
                        except Exception as e:
                            logger.warning(f"Neo4j keep-alive ping during fetch failed: {e}")
    logger.info(
        f"Fetched {len(datasets):,} datasets from Neo4j for statuses {statuses} "
        f"with {ping_count} keep-alive pings."
    )
    return datasets


log_file_name = "Log filename not set"
logger = logging.getLogger("es-file-index")


def parse_config() -> Config:
    parser = ArgumentParser(description="Bootstrap ElasticSearch indices from SQLite file index, one partition at a time.")
    parser.add_argument(
        "--config", default="config.ini", help="Path to the configuration file (ini format)"
    )
    parser.add_argument(
        "--partition-key", required=True,
        help=f"Partition key to process. Must be one of: {list(PARTITION_CHAR_SPANS.keys())}"
    )
    parser.add_argument(
        "--service-config", default="fileIndexingService.ini",
        help="Path to the fileIndexingService.ini configuration file"
    )
    args = parser.parse_args()

    if args.partition_key not in PARTITION_CHAR_SPANS:
        print(f"ERROR: --partition-key '{args.partition_key}' is not a valid partition key.")
        print(f"Valid keys are: {list(PARTITION_CHAR_SPANS.keys())}")
        sys.exit(1)

    global PARTITION_KEY
    PARTITION_KEY = args.partition_key

    if not Path(args.config).exists():
        print(f"ERROR: config file not found: {args.config}")
        sys.exit(2)

    c = ConfigParser()
    c.read(args.config)

    if not Path(args.service_config).exists():
        print(f"ERROR: service config file not found: {args.service_config}")
        sys.exit(2)

    return Config(
        database=c.get("Local", "DATABASE_FILEPATH", fallback="local_file_index.db"),
        log_id=c.get("DEFAULT", "LOG_ID", fallback="default"),
        log_level=c.get("Local", "LOG_LEVEL", fallback="info"),
        service_config=args.service_config,
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
    # exec_info is created relative to the current working directory, which the
    # shell script sets to the bootstrapping/ directory via cd "${SCRIPT_DIR}".
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


@contextmanager
def timed_step(label: str):
    """Context manager for timing suspect steps. Logs at INFO level if the step
    takes more than 0.1 seconds, so routine fast calls stay out of the log."""
    start = time.time()
    yield
    elapsed = time.time() - start
    if elapsed > 0.1:
        logger.info(f"TIMING: {label} took {elapsed:.2f}s")


config = parse_config()
setup_logger(config.log_id, config.log_level)

service_utils = None
try:
    service_utils = FileIndexingUtils(config_file_name=Path(config.service_config)
                                      , disable_slack_notifications=(config.slack_notifications == 'DISABLED'))
    print('FileIndexingUtils instantiated.')
except Exception as e:
    print("Error instantiating a FileIndexingUtils during startup.")
    print(str(e))
    sys.exit(3)

util_config = service_utils.get_config()
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
    """Send documents to one or more ElasticSearch indices using the bulk API.
    Uses doc_as_upsert=true - ES inserts if the document does not exist, updates if it does.
    During bootstrap into an empty index all operations will be inserts in practice."""
    es_upserts = [
        f'{{"update":{{"_id":"{doc["dataset_uuid"]}/{doc["rel_path"]}"}}}}\n{{"doc":{json.dumps(doc, separators=(",", ":"))},"doc_as_upsert":true}}'
        for doc in upserts
    ]
    error_msgs = []
    chunks = [es_upserts[i: i + FLUSH_ES_DOCS_CHUNK_SIZE] for i in range(0, len(es_upserts), FLUSH_ES_DOCS_CHUNK_SIZE)]
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


def create_file_info(file: DBFilePart) -> UUIDFileInfo:
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
        logger.error(f"Skipping dataset {uuid} - failed to parse record: {e}")
        return None


# ---------------------------------------------------------------------------
# Checkpoint mechanism - may be removed in a future version if bootstrap
# completion times remain reliable enough that interrupted-run recovery
# is not needed in practice. The feature has not been exercised.
# ---------------------------------------------------------------------------
BOOTSTRAP_CHECKPOINT_FILE = f"exec_info/bootstrap_checkpoint_{PARTITION_KEY}.txt"


def load_checkpoint() -> set:
    """Load the set of ES document _ids already successfully indexed during a previous
    run of this complete bootstrap load. Returns an empty set if no checkpoint exists.
    Delete or archive the checkpoint file before starting a new complete bootstrap load."""
    if not os.path.exists(BOOTSTRAP_CHECKPOINT_FILE):
        logger.info(f"No checkpoint file found at {BOOTSTRAP_CHECKPOINT_FILE} - starting fresh.")
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
        logger.error(f"Failed to load checkpoint file {BOOTSTRAP_CHECKPOINT_FILE}: {e} - starting fresh.")
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
        checkpoint_ids: set,
) -> Tuple[List[str], int]:
    es_indices = [
        f"{config.elastic_search_public_index}_{PARTITION_KEY.lower()}",
        f"{config.elastic_search_private_index}_{PARTITION_KEY.lower()}",
    ]

    num_errors = 0
    dataset_uuids = []
    es_inserts = []
    datasets = fetch_datasets(['Published'])

    with Database(config.database, read_only=True) as db:
        for raw_record in datasets:
            if terminate_event.is_set():
                logger.info("Termination signal received, stopping indexing.")
                return dataset_uuids, num_errors

            dataset = parse_dataset_record(raw_record)
            if dataset is None:
                err_msg = f"Skipping dataset - failed to parse Neo4j record for uuid: {dict(raw_record).get('uuid', 'unknown')}"
                logger.error(err_msg)
                num_errors += 1
                continue
            if dataset["uuid"][31] not in PARTITION_CHAR_SPANS[PARTITION_KEY]:
                logger.info(f"Skipping dataset {dataset['uuid']} - does not end with one of {sorted(PARTITION_CHAR_SPANS[PARTITION_KEY])}.")
                continue

            time.sleep(1)
            dataset_uuids.append(dataset["uuid"])
            logger.info(f"Processing Dataset {dataset['uuid']}")

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
                with timed_step(f"SQLite query_files_part for dataset {dataset['uuid']}"):
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
                with timed_step(f"UUID API fetch for dataset {dataset['uuid']}"):
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

            diff_uuid_files = set(uuid_filepath_map.keys()) - set(local_filepath_map.keys())
            if diff_uuid_files:
                sorted_diff = sorted(diff_uuid_files)
                if len(sorted_diff) <= 2:
                    diff_str = ', '.join(sorted_diff)
                else:
                    diff_str = f"{sorted_diff[0]},...,{sorted_diff[-1]}"
                logger.warning(
                    f"UUID API returns {len(sorted_diff)} files not in local database for Dataset {dataset['uuid']}: {diff_str}"
                )

            try:
                diff_uuid_files = set(local_filepath_map.keys()) - set(uuid_filepath_map.keys())
                logger.debug(f"For {str(len(diff_uuid_files))} files in the database for the local filesystem but"
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

                uuid_filepath_map = {
                    item["path"]: item
                    for item in get_files_from_uuid_api(dataset_uuid=dataset["uuid"])
                }

            dataset_additional_info = None
            if local_filepath_map:
                try:
                    with timed_step(f"get_additional_info for dataset {dataset['uuid']}"):
                        dataset_additional_info = file_manager.get_additional_info(
                            dataset=dataset,
                            path=next(iter(local_filepath_map)),
                        )
                except Exception as e:
                    logger.error(
                        f"Error fetching additional info for dataset {dataset['uuid']}: {e}"
                    )
                    num_errors += 1
            else:
                logger.warning(
                    f"No files in local_filepath_map for dataset {dataset['uuid']} - skipping get_additional_info."
                )

            es_document_skipped_count = 0
            for idx, (rel_path, local_file) in enumerate(local_filepath_map.items()):
                doc_id = f"{dataset['uuid']}/{rel_path}"
                if doc_id in checkpoint_ids:
                    es_document_skipped_count += 1
                    continue

                uuid_file = uuid_filepath_map[rel_path]
                file_ext = os.path.splitext(local_file.path)[1].lower()
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
                if dataset_additional_info is not None:
                    doc.update(dataset_additional_info)

                es_inserts.append(doc)

                if len(es_inserts) >= FLUSH_ES_DOCS_LEVEL:
                    try:
                        logger.info(
                            f"Flushing {len(es_inserts)} inserts to ES "
                            f"(mid-dataset flush at file {idx + 1} of {len(local_filepath_map)} "
                            f"for dataset {dataset['uuid']})"
                        )
                        with timed_step(f"ES bulk insert {len(es_inserts)} docs (mid-dataset flush)"):
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

            if es_inserts:
                try:
                    logger.info(
                        f"Flushing {len(es_inserts)} remaining inserts "
                        f"to ES for dataset {dataset['uuid']}"
                    )
                    with timed_step(f"ES bulk insert {len(es_inserts)} docs (end-of-dataset flush)"):
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
        checkpoint_ids: set,
) -> Tuple[List[str], int]:
    es_indices = [f"{config.elastic_search_private_index}_{PARTITION_KEY.lower()}"]

    num_errors = 0
    dataset_uuids = []
    es_inserts = []
    datasets = fetch_datasets(['QA', 'Submitted', 'Approval'])

    with Database(config.database, read_only=True) as db:
        for raw_record in datasets:
            if terminate_event.is_set():
                logger.info("Termination signal received, stopping indexing.")
                return dataset_uuids, num_errors

            dataset = parse_dataset_record(raw_record)
            if dataset is None:
                err_msg = f"Skipping dataset - failed to parse Neo4j record for uuid: {dict(raw_record).get('uuid', 'unknown')}"
                logger.error(err_msg)
                num_errors += 1
                continue
            if dataset["uuid"][31] not in PARTITION_CHAR_SPANS[PARTITION_KEY]:
                logger.info(f"Skipping dataset {dataset['uuid']} - does not end with one of {sorted(PARTITION_CHAR_SPANS[PARTITION_KEY])}.")
                continue

            time.sleep(1)
            dataset_uuids.append(dataset["uuid"])
            logger.info(f"Processing Dataset {dataset['uuid']}")

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
                with timed_step(f"SQLite query_files_part for dataset {dataset['uuid']}"):
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

            dataset_additional_info = None
            if local_filepath_map:
                try:
                    with timed_step(f"get_additional_info for dataset {dataset['uuid']}"):
                        dataset_additional_info = file_manager.get_additional_info(
                            dataset=dataset,
                            path=next(iter(local_filepath_map)),
                        )
                except Exception as e:
                    logger.error(
                        f"Error fetching additional info for dataset {dataset['uuid']}: {e}"
                    )
                    num_errors += 1
            else:
                logger.warning(
                    f"No files in local_filepath_map for dataset {dataset['uuid']} - skipping get_additional_info."
                )

            es_document_skipped_count = 0
            for idx, (rel_path, local_file) in enumerate(local_filepath_map.items()):
                doc_id = f"{dataset['uuid']}/{rel_path}"
                if doc_id in checkpoint_ids:
                    es_document_skipped_count += 1
                    continue

                file_ext = os.path.splitext(local_file.path)[1].lower()
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
                if dataset_additional_info is not None:
                    doc.update(dataset_additional_info)

                es_inserts.append(doc)

                if len(es_inserts) >= FLUSH_ES_DOCS_LEVEL:
                    try:
                        logger.info(
                            f"Flushing {len(es_inserts)} inserts to ES "
                            f"(mid-dataset flush at file {idx + 1} of {len(local_filepath_map)} "
                            f"for dataset {dataset['uuid']})"
                        )
                        with timed_step(f"ES bulk insert {len(es_inserts)} docs (mid-dataset flush)"):
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

            if es_inserts:
                try:
                    logger.info(
                        f"Flushing {len(es_inserts)} remaining inserts "
                        f"to ES for dataset {dataset['uuid']}"
                    )
                    with timed_step(f"ES bulk insert {len(es_inserts)} docs (end-of-dataset flush)"):
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
          f" ElasticSearch indices prefixed {config.elastic_search_public_index}" \
          f" and {config.elastic_search_private_index}" \
          f" with PARTITION_KEY {PARTITION_KEY}."
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

    try:
        auth = (config.neo4j_username, config.neo4j_password)
        with GraphDatabase.driver(config.neo4j_uri, auth=auth) as driver:
            driver.verify_connectivity()
    except Exception as e:
        logger.critical(f"Neo4j connectivity check failed for {config.neo4j_uri}: {e}")
        sys.exit(2)

    if terminate_event.is_set():
        return

    checkpoint_ids = load_checkpoint()

    start_time = time.time()
    uuids, errors = index_published_datasets(
        ubkg_organs=ubkg_organs, checkpoint_ids=checkpoint_ids
    )
    logger.setLevel(logging.INFO)
    logger.info(f"Published dataset indexing took {time.time() - start_time:.2f} seconds")
    logger.setLevel(log_level)
    num_errors += errors
    dataset_uuids.extend(uuids)

    if terminate_event.is_set():
        return

    start_time = time.time()
    uuids, errors = index_qa_datasets(
        ubkg_organs=ubkg_organs, checkpoint_ids=checkpoint_ids
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
            err_msg =   f"ElasticSearch {config.log_id} bootstrap file indexing" \
                        f" by {Path(__file__).name}" \
                        f" completed with {num_errors} errors." \
                        f" PARTITION_KEY {PARTITION_KEY}." \
                        f" See {log_file_name}." \
                        f"{util_config['SLACK_BAD_NEWS_EMOJI']}"
            service_utils.exit_if_halt_reason(halt_reasons=[err_msg]
                                              , slack_channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                              , mentions_dict=slack_user_id_mentions_on_error_dict
                                              , process_bad_news_emoji=':bangbang:'
                                              , exit_code=2)
        else:
            success_msg = f"ElasticSearch {config.log_id}" \
                          f" bootstrap file indexing by {Path(__file__).name}" \
                          f" completed successfully." \
                          f" PARTITION_KEY {PARTITION_KEY}." \
                          f"{util_config['SLACK_GOOD_NEWS_EMOJI']}"
            service_utils.postToSlackChannel(channel=util_config['SLACK_NOTIFICATION_CHANNEL']
                                     , msg=success_msg
                                     , mentions_dict=slack_user_id_mentions_on_success_dict)


if __name__ == "__main__":
    main()
