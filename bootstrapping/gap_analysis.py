import sqlite3
import configparser
import requests
import sys
import datetime
from argparse import ArgumentParser
from neo4j import GraphDatabase

c = configparser.ConfigParser()
c.read('config.ini')

NEO4J_URI  = c.get('Neo4J', 'NEO4J_URI')
NEO4J_USER = c.get('Neo4J', 'NEO4J_USERNAME')
NEO4J_PASS = c.get('Neo4J', 'NEO4J_PASSWORD')
ES_URL     = c.get('ElasticSearch', 'ELASTIC_SEARCH_URL')
DB_PATH    = c.get('Local', 'DATABASE_FILEPATH')

TIMEOUT = 60

# ---------------------------------------------------------------------------
# Hardcoded totals are stored in gap_analysis.ini and updated automatically
# when --full-refresh is run.
# ---------------------------------------------------------------------------
ini = configparser.ConfigParser()
ini.read('gap_analysis.ini')
HARDCODED_SQLITE_TOTAL        = ini.getint('hardcoded', 'sqlite_total', fallback=0)
HARDCODED_ES_CONSORTIUM_TOTAL = ini.getint('hardcoded', 'es_consortium_total', fallback=0)
HARDCODED_DATE                = ini.get('hardcoded', 'date', fallback='unknown')

parser = ArgumentParser(description="Gap analysis between SQLite, Neo4j, and ElasticSearch.")
parser.add_argument(
    '--full-refresh',
    action='store_true',
    help='Recompute all values live including ES scroll. Without this flag, '
         'SQLite total and ES consortium total are taken from hardcoded constants.'
)
args = parser.parse_args()

today = datetime.date.today().isoformat()

print("=== Gap Analysis: SQLite vs Neo4j vs ElasticSearch ===")
if args.full_refresh:
    print(f"Full refresh mode: all values computed live on {today}.")
else:
    print(f"SQLite total and ES consortium total are hardcoded from {HARDCODED_DATE}.")
    print(f"All other values are computed live.")
    print(f"Run with --full-refresh to recompute all values from scratch.")
print()

# ---------------------------------------------------------------------------
# 1. Fetch dataset UUIDs from Neo4j
# ---------------------------------------------------------------------------
print("Fetching dataset UUIDs from Neo4j...", flush=True)
neo4j_uuids = set()
QUERY = """
    MATCH (donor:Donor)-[:ACTIVITY_INPUT]->(organ_activity:Activity)-[:ACTIVITY_OUTPUT]->(organ:Sample {sample_category:'organ'})-[*]->(a:Activity)-[:ACTIVITY_OUTPUT]->(ds:Dataset)
    WHERE a.creation_action IN ['Create Dataset Activity', 'Central Process', 'Lab Process', 'External Process']
    AND ds.status IN ['Published', 'QA', 'Submitted', 'Approval']
    AND NOT (ds)<-[:REVISION_OF]-(:Entity)
    RETURN DISTINCT ds.uuid AS uuid
"""
with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS)) as driver:
    with driver.session() as session:
        for record in session.run(QUERY):
            neo4j_uuids.add(record["uuid"])
# ---------------------------------------------------------------------------
# 2. Fetch dataset UUIDs from SQLite
# ---------------------------------------------------------------------------
print("Fetching dataset UUIDs from SQLite...", flush=True)
conn = sqlite3.connect(DB_PATH)
sqlite_uuids = set(
    row[0] for row in conn.execute(
        "SELECT DISTINCT dataset_uuid FROM files WHERE dataset_uuid IS NOT NULL"
    )
)
# ---------------------------------------------------------------------------
# 3. Fetch dataset UUIDs from ES via scroll (full refresh only)
# ---------------------------------------------------------------------------
if args.full_refresh:
    print("Fetching dataset UUIDs from ES (hm_consortium_files) via scroll...", flush=True)
    es_uuids = set()
    res = requests.post(
        f"{ES_URL}/hm_consortium_files/_search?scroll=2m",
        headers={"Content-Type": "application/json"},
        json={"_source": ["dataset_uuid"], "size": 5000, "query": {"match_all": {}}},
        timeout=TIMEOUT,
    )
    res.raise_for_status()
    body = res.json()
    scroll_id = body["_scroll_id"]
    hits = body["hits"]["hits"]
    while hits:
        for hit in hits:
            uuid = hit.get("_source", {}).get("dataset_uuid")
            if uuid:
                es_uuids.add(uuid)
        if len(es_uuids) % 100000 == 0:
            print(f"  ...{len(es_uuids):,} unique UUIDs so far", flush=True)
        res = requests.post(
            f"{ES_URL}/_search/scroll",
            headers={"Content-Type": "application/json"},
            json={"scroll": "2m", "scroll_id": scroll_id},
            timeout=TIMEOUT,
        )
        res.raise_for_status()
        body = res.json()
        scroll_id = body["_scroll_id"]
        hits = body["hits"]["hits"]
    requests.delete(
        f"{ES_URL}/_search/scroll",
        headers={"Content-Type": "application/json"},
        json={"scroll_id": scroll_id},
        timeout=TIMEOUT,
    )
    # ES UUID count printed in results section below
    # Get actual ES total document count and SQLite total
    res = requests.get(f"{ES_URL}/hm_consortium_files/_count",
                       headers={"Content-Type": "application/json"},
                       timeout=TIMEOUT)
    ES_CONSORTIUM_TOTAL = res.json()["count"]
    SQLITE_TOTAL = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    # Totals printed in results section below
else:
    es_uuids = None
    SQLITE_TOTAL        = HARDCODED_SQLITE_TOTAL
    ES_CONSORTIUM_TOTAL = HARDCODED_ES_CONSORTIUM_TOTAL

GAP = SQLITE_TOTAL - ES_CONSORTIUM_TOTAL

# ---------------------------------------------------------------------------
# 4. Compute comparisons
# ---------------------------------------------------------------------------
in_sqlite_not_neo4j             = sqlite_uuids - neo4j_uuids
in_neo4j_not_sqlite             = neo4j_uuids - sqlite_uuids
in_sqlite_and_neo4j             = sqlite_uuids & neo4j_uuids

if es_uuids is not None:
    in_neo4j_not_es                 = neo4j_uuids - es_uuids
    in_es_not_neo4j                 = es_uuids - neo4j_uuids
    in_neo4j_not_es_but_in_sqlite   = in_neo4j_not_es & sqlite_uuids
    in_neo4j_not_es_not_sqlite      = in_neo4j_not_es - sqlite_uuids
else:
    # Use hardcoded values from last full refresh (2026-06-17)
    in_neo4j_not_es_but_in_sqlite   = None  # 3,063 datasets
    in_neo4j_not_es_not_sqlite      = None  # 860 datasets

# ---------------------------------------------------------------------------
# 5. File counts from SQLite
# ---------------------------------------------------------------------------
print("Computing SQLite file counts...", flush=True)

files_no_uuid = conn.execute(
    "SELECT COUNT(*) FROM files WHERE dataset_uuid IS NULL"
).fetchone()[0]

files_in_sqlite_only = 0
if in_sqlite_not_neo4j:
    placeholders = ','.join('?' * len(in_sqlite_not_neo4j))
    files_in_sqlite_only = conn.execute(
        f"SELECT COUNT(*) FROM files WHERE dataset_uuid IN ({placeholders})",
        list(in_sqlite_not_neo4j)
    ).fetchone()[0]

files_neo4j_not_es = 0
n_neo4j_not_es_in_sqlite = 0
if es_uuids is not None and in_neo4j_not_es_but_in_sqlite:
    n_neo4j_not_es_in_sqlite = len(in_neo4j_not_es_but_in_sqlite)
    placeholders = ','.join('?' * n_neo4j_not_es_in_sqlite)
    files_neo4j_not_es = conn.execute(
        f"SELECT COUNT(*) FROM files WHERE dataset_uuid IN ({placeholders})",
        list(in_neo4j_not_es_but_in_sqlite)
    ).fetchone()[0]
else:
    # Hardcoded from 2026-06-17 full refresh
    n_neo4j_not_es_in_sqlite = 3_063
    files_neo4j_not_es = 132_754

files_numeric_ext = conn.execute("""
    SELECT COUNT(*) FROM files
    WHERE path GLOB '*.[0-9]'
       OR path GLOB '*.[0-9][0-9]'
       OR path GLOB '*.[0-9][0-9][0-9]'
""").fetchone()[0]

files_no_ext = conn.execute("""
    SELECT COUNT(*) FROM files
    WHERE INSTR(path, '.') = 0
       OR path LIKE '%/'
       OR SUBSTR(path, INSTR(path, '.')) = ''
""").fetchone()[0]

LAB_PROCESS_UUIDS = (
    '9f37a9b1f6073e6e588ff7e0dd9493b5',
    'ceab8754162592b4014093f09881a47c',
    'f91b925ab975d7f5997c51a72d9b1329',
    '65b92f0191dc73e9470f46ceb217054d',
    '6d094503393d6d41b6193c9d10e33d9e',
    '851d76f833dc8dc3debb8eb2d73d543b',
    '7d7ecee7a88e406abe95f5ce206fe722',
    '9ad464b54a0087db94bfca405d4ad968',
    '0a39994eca4db587a2106f766dc7a8c7',
    '7f0e5c10babfa368c9fbaff2e52f79f4',
    '137cfbe04757d99ae0763c1f387ccea7',
    'a8172784e0670b5ab59e880f315bd56a',
    'b9824e6d57a2e861fb990b7a27ba7008',
)
EXTERNAL_PROCESS_UUIDS = (
    '728d02be5fada1541decd1091a4b17e3',
    '94af143bfe62c14c6f49888c1ede71d3',
    '8736c260ad3b6d6ccc4ed5840e78312f',
)
placeholders = ','.join('?' * len(LAB_PROCESS_UUIDS))
files_lab_process = conn.execute(
    f"SELECT COUNT(*) FROM files WHERE dataset_uuid IN ({placeholders})",
    LAB_PROCESS_UUIDS
).fetchone()[0]

placeholders = ','.join('?' * len(EXTERNAL_PROCESS_UUIDS))
files_external_process = conn.execute(
    f"SELECT COUNT(*) FROM files WHERE dataset_uuid IN ({placeholders})",
    EXTERNAL_PROCESS_UUIDS
).fetchone()[0]

# ---------------------------------------------------------------------------
# 6. Print results
# ---------------------------------------------------------------------------
print()
print(f"Neo4j datasets of all statuses (but no revisions): {len(neo4j_uuids):,}")
print(f"SQLite distinct dataset_uuids:                     {len(sqlite_uuids):,}")
if args.full_refresh:
    print(f"SQLite total files:                                {SQLITE_TOTAL:,}")
    print(f"Distinct dataset_uuids in hm_consortium_files:    {len(es_uuids):,}")
    print(f"ES hm_consortium_files total:                      {ES_CONSORTIUM_TOTAL:,}")
    print()
    # Write updated constants to gap_analysis.ini
    ini.set('hardcoded', 'sqlite_total', str(SQLITE_TOTAL))
    ini.set('hardcoded', 'es_consortium_total', str(ES_CONSORTIUM_TOTAL))
    ini.set('hardcoded', 'date', today)
    with open('gap_analysis.ini', 'w') as ini_file:
        ini.write(ini_file)
    print(f"gap_analysis.ini updated with values from {today}.")
print()
print(f"In SQLite but not Neo4j:                      {len(in_sqlite_not_neo4j):,}")
print(f"  ({files_in_sqlite_only:,} files for these datasets)")
print()
print(f"In SQLite and in Neo4j:                       {len(in_sqlite_and_neo4j):,}")
print(f"In Neo4j but not SQLite:                      {len(in_neo4j_not_sqlite):,}")
print(f"  (Datasets probably added to Neo4j since the last crawl of the")
print(f"   file system to fill SQLite; file count unknown)")
print()
if es_uuids is not None:
    print(f"In Neo4j but not ES:                          {len(in_neo4j_not_es):,}")
    print(f"  of which also in SQLite:                    {len(in_neo4j_not_es_but_in_sqlite):,}")
    print(f"  of which not in SQLite either:              {len(in_neo4j_not_es_not_sqlite):,}")
    print(f"In ES but not Neo4j:                          {len(in_es_not_neo4j):,}")
else:
    print(f"(ES comparison not computed - run with --full-refresh for live ES values)")
print()
print(f"Files in SQLite but not ES for {n_neo4j_not_es_in_sqlite:,} datasets")
print(f"  found in both SQLite and Neo4j:               {files_neo4j_not_es:,}")
if es_uuids is None:
    print(f"  (hardcoded from {HARDCODED_DATE}; run with --full-refresh to recompute)")
print()

# ---------------------------------------------------------------------------
# 7. Final summary
# ---------------------------------------------------------------------------
accounted = files_no_uuid + files_in_sqlite_only + files_neo4j_not_es + files_lab_process + files_external_process

print("=" * 70)
if args.full_refresh:
    print(f"=== Final Gap Summary (computed live on {today}) ===")
else:
    print(f"=== Final Gap Summary ===")
    print(f"=== SQLite and ES totals hardcoded from {HARDCODED_DATE} ===")
    print(f"=== Run with --full-refresh to recompute all values ===")
print("=" * 70)
print()
print(f"SQLite total files:                           {SQLITE_TOTAL:,}")
print(f"hm_consortium_files total:                    {ES_CONSORTIUM_TOTAL:,}")
print(f"Gap to explain:                               {GAP:,}")
print()
print("Buckets (hierarchical, minimal overlap):")
print()
print(f"  1. No dataset_uuid extracted from path      {files_no_uuid:,}")
print(f"     (files whose path had no 32-char hex component)")
print()
# Note: these files land in the C-F partition via OR dataset_uuid IS NULL
# in PARTITION_CLAUSES, but are then excluded by query_files_part since
# the path prefix does not match any dataset globus path.
print(f"  2. In SQLite but not in Neo4j               {files_in_sqlite_only:,}")
print(f"     ({len(in_sqlite_not_neo4j):,} datasets: likely revisions or")
print(f"     datasets without qualifying graph path)")
print()
print(f"  3. In SQLite but not ES for {n_neo4j_not_es_in_sqlite:,} datasets")
print(f"     found in both SQLite and Neo4j:          {files_neo4j_not_es:,}")
print(f"     (these files were present in SQLite but excluded from")
print(f"     ES indexing; buckets 5 and 6 below identify the two")
print(f"     filtering rules responsible for most of these exclusions)")
print()
print(f"  4. In Neo4j but not in SQLite               {len(in_neo4j_not_sqlite):,} datasets")
print(f"     (not crawled by local_file_index.py,")
# Note: likely added to Neo4j after the last file system crawl.
# Could also include datasets whose paths are not under INDEX_PATHS.
print(f"     probably added since the last crawl - file count unknown)")
print()
print(f"  5. Numeric-only extensions (SenNet logic)   {files_numeric_ext:,}")
print(f"     (these files are within bucket 3; their numeric-only")
print(f"     extensions caused them to be filtered out of ES indexing)")
print()
print(f"  6. No extension (SenNet logic)              {files_no_ext:,}")
print(f"     (these files are within bucket 3; their missing")
print(f"     extensions caused them to be filtered out of ES indexing)")
print()
print(f"  7. Lab Process datasets                     {files_lab_process:,}")
print(f"     ({len(LAB_PROCESS_UUIDS)} datasets; ignoring and expecting")
print(f"     no further Lab Process datasets)")
print()
print(f"  8. External Process datasets                {files_external_process:,}")
print(f"     ({len(EXTERNAL_PROCESS_UUIDS)} datasets; these should be")
print(f"     processed as Primary Datasets)")
print()
print(f"  Buckets 1+2+3+7+8 total:                    {accounted:,}")
print(f"  Gap:                                        {GAP:,}")
print(f"  Unaccounted:                                {GAP - accounted:,}")
print()
if GAP - accounted == 0:
    print("  Gap fully explained.")
elif GAP - accounted > 0:
    print(f"  {GAP - accounted:,} files still unaccounted for.")
else:
    overage = accounted - GAP
    print(f"  Note: buckets 1+2+3+7+8 slightly exceed the gap by {overage:,} files.")
    print(f"  Most likely overlap: some no-dataset_uuid files (bucket 1) also")
    print(f"  belong to SQLite-only datasets (bucket 2), since a file with no")
    print(f"  extractable UUID could still sit under a dataset directory that")
    print(f"  Neo4j does not know about. Similarly, some Lab/External Process")
    print(f"  files (buckets 7+8) may also fall into bucket 2 or 3 if those")
    print(f"  datasets were excluded by Neo4j or ES filters as well.")
