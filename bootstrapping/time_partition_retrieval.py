######################################################################################################################
# Diagnostic utility for timing the Neo4j dataset query used by es_file_index_partition_bootstrap.py.              #
# Strips out all SQLite, UUID API, and ElasticSearch operations, retaining only the Neo4j fetch and reporting       #
# timing, record counts, and keep-alive ping activity.                                                              #
#                                                                                                                   #
# Run on any server to characterize Neo4j query performance and connection stability for a given partition.         #
######################################################################################################################

import logging
import os
import signal
import sys
import time
from argparse import ArgumentParser
from configparser import ConfigParser
from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path
from typing import List

from neo4j import GraphDatabase

if sys.version_info >= (3, 11):
    from typing import LiteralString
else:
    LiteralString = str

# ---------------------------------------------------------------------------
# Partition settings - same as es_file_index_partition_bootstrap.py
# ---------------------------------------------------------------------------
PARTITION_CHAR_SPANS = {
    '0-3': {'0', '1', '2', '3'},
    '4-7': {'4', '5', '6', '7'},
    '8-B': {'8', '9', 'a', 'b'},
    'C-F': {'c', 'd', 'e', 'f'},
}
PARTITION_KEY = '8-B'   # set this to the key for this instance

# ---------------------------------------------------------------------------
# Neo4j query base - identical to es_file_index_partition_bootstrap.py
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
Config = namedtuple("Config", ["neo4j_uri", "neo4j_username", "neo4j_password", "log_level"])

log_file_name = "Log filename not set"
logger = logging.getLogger("time_partition_retrieval")
terminate_event = False


def parse_config() -> Config:
    parser = ArgumentParser(description="Time the Neo4j partition retrieval query.")
    parser.add_argument("--config", default="config.ini", help="Path to config.ini")
    args = parser.parse_args()
    c = ConfigParser()
    c.read(args.config)
    return Config(
        neo4j_uri=c.get("Neo4J", "NEO4J_URI"),
        neo4j_username=c.get("Neo4J", "NEO4J_USERNAME"),
        neo4j_password=c.get("Neo4J", "NEO4J_PASSWORD"),
        log_level=c.get("Local", "LOG_LEVEL", fallback="info"),
    )


def setup_logger(log_level: str):
    global log_file_name
    if not os.path.exists("../exec_info"):
        os.makedirs("../exec_info")
    log_file_name = os.path.join(
        "../exec_info", f"time-partition-retrieval-{PARTITION_KEY}-{time.strftime('%Y-%m-%d-%H-%M-%S')}.log"
    )
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file_name), logging.StreamHandler(sys.stdout)],
    )
    print(f"Logging to {log_file_name}")


@contextmanager
def timed_step(label: str):
    start = time.time()
    yield
    elapsed = time.time() - start
    if elapsed > 0.1:
        logger.info(f"TIMING: {label} took {elapsed:.2f}s")


def build_datasets_query() -> str:
    """Build the Cypher query with the partition clause for PARTITION_KEY."""
    extra_clauses = ""

    if PARTITION_KEY and PARTITION_CHAR_SPANS.get(PARTITION_KEY):
        chars = sorted(PARTITION_CHAR_SPANS[PARTITION_KEY])
        char_list = ", ".join("'" + c + "'" for c in chars)
        extra_clauses += "    AND SUBSTRING(ds.uuid, 31, 1) IN [" + char_list + "]\n"
        logger.info(f"Partition clause injected for PARTITION_KEY={PARTITION_KEY}: chars {chars}")

    return _QUERY_BASE.replace("    RETURN ds.uuid", extra_clauses + "    RETURN ds.uuid", 1)


def fetch_datasets(config: Config, statuses: List[str]) -> List[dict]:
    """Fetch and materialize all dataset records from Neo4j for the given statuses.
    Uses a lazy streaming loop with keep-alive pings to prevent connection timeout.
    Reports timing, record count, and ping count."""
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


def main():
    global terminate_event

    def handle_termination(signum, frame):
        print("Received termination signal. Exiting...")
        logger.error("Termination signal received.")
        terminate_event = True
        sys.exit(2)

    signal.signal(signal.SIGTERM, handle_termination)
    signal.signal(signal.SIGINT, handle_termination)

    logger.info(f"Starting time_partition_retrieval.py with PARTITION_KEY={PARTITION_KEY}")
    logger.info(f"Neo4j URI: {config.neo4j_uri}")

    try:
        auth = (config.neo4j_username, config.neo4j_password)
        with GraphDatabase.driver(config.neo4j_uri, auth=auth) as driver:
            driver.verify_connectivity()
        logger.info("Neo4j connectivity verified.")
    except Exception as e:
        logger.critical(f"Neo4j connectivity check failed for {config.neo4j_uri}: {e}")
        sys.exit(2)

    start = time.time()

    published = fetch_datasets(config, ['Published'])
    logger.info(f"Published: {len(published):,} datasets")

    qa = fetch_datasets(config, ['QA', 'Submitted', 'Approval'])
    logger.info(f"QA/Submitted/Approval: {len(qa):,} datasets")

    elapsed = round(time.time() - start, 2)
    logger.info(f"Total retrieval time: {elapsed}s for {len(published) + len(qa):,} datasets.")


if __name__ == "__main__":
    config = parse_config()
    setup_logger(config.log_level)
    main()
