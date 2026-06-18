# Bootstrap Guide for file-indexing-service ElasticSearch Indices

The bootstrap code is only for initial creation of the ElasticSearch indices, using
several processes to fill separate partition indices which are then merged. This is
prior to scheduling the three regular jobs, particularly `file_indexing.sh` /
`es_file_index.py`.

---

## @TODO - Explain DTN deployment

---

## Partitioning and Scheduling

The bootstrap is split across four partitions keyed on the 32nd character of each
dataset UUID (`0-3`, `4-7`, `8-B`, `C-F`). Running one process per partition
simultaneously completes the work roughly four times faster than a single process.

The wrapper script `file_index_partition_bootstrap.sh` takes the partition key as its
first argument and passes it to `es_file_index_partition_bootstrap.py` via
`--partition-key`. Each process writes its own log and checkpoint file under `exec_info/`.

### Crontab entries

The server (dtn03) runs on UTC. Verify the current time with `date` before setting
scheduled times. The entries below stagger the four partitions five minutes apart:

```
SHELL=/usr/bin/bash
PATH=/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/hive/users/hive/bin

# Bootstrap runs - staggered 5 minutes apart
# Update the date fields (day month) before scheduling
# Remember DTN03 runs on UTC, and these settings need to
# align with the output of the date command!
00 20 17 06 *  PART=0-3; /hive/users/hive/scripts/PittCronJobs/file-indexing-service/file_index_partition_bootstrap.sh $PART >> /hive/users/hive/scripts/PittCronJobs/file-indexing-service/exec_info/cron_log_$PART 2>&1
05 20 17 06 *  PART=4-7; /hive/users/hive/scripts/PittCronJobs/file-indexing-service/file_index_partition_bootstrap.sh $PART >> /hive/users/hive/scripts/PittCronJobs/file-indexing-service/exec_info/cron_log_$PART 2>&1
10 20 17 06 *  PART=8-B; /hive/users/hive/scripts/PittCronJobs/file-indexing-service/file_index_partition_bootstrap.sh $PART >> /hive/users/hive/scripts/PittCronJobs/file-indexing-service/exec_info/cron_log_$PART 2>&1
15 20 17 06 *  PART=C-F; /hive/users/hive/scripts/PittCronJobs/file-indexing-service/file_index_partition_bootstrap.sh $PART >> /hive/users/hive/scripts/PittCronJobs/file-indexing-service/exec_info/cron_log_$PART 2>&1

# Nightly incremental job (disabled during bootstrap)
#30 0 * * *  /hive/users/hive/scripts/PittCronJobs/file-indexing-service/file_indexing.sh >> /hive/users/hive/scripts/PittCronJobs/file-indexing-service/exec_info/cron_log 2>&1
```

Edit with `crontab -e` as the `hive` user. After all four bootstrap runs complete,
comment out the bootstrap entries and re-enable the regular incremental job.

### Checkpoint files

Each partition writes an append-only checkpoint file recording the ES document `_id`
of every successfully indexed file. If a run is interrupted it can be restarted from
the same crontab entry and will skip already-indexed documents.

Checkpoint files are written to:
```
exec_info/bootstrap_checkpoint_{PARTITION_KEY}.txt
```

Delete or archive the checkpoint file before starting a fresh bootstrap run. Do not
delete it when restarting an interrupted run.

### Log files

Each run writes a timestamped log to:
```
exec_info/es-file-index-{log_id}-{PARTITION_KEY}-{timestamp}.log
```

To monitor a running partition:
```bash
tail -f exec_info/es-file-index-prod-0-3-*.log
```

To extract a summary after completion:
```bash
for f in es-file-index-prod-*.log; do
    echo -n "$f: "
    echo -n "datasets=$(grep -c 'Processing Dataset' $f) "
    echo -n "errors=$(grep -c ' ERROR ' $f) "
    echo "$(grep 'dataset indexing took' $f | tail -2 | tr '\n' ' ')"
done
```

---

## Merging and Cleaning Up Indices

After all four partition runs complete, merge the partition indices into the final
`hm_consortium_files` and `hm_public_files` indices using the ElasticSearch Reindex API.

Run these from the ElasticSearch DevTools console (or equivalent):

```
DELETE hm_consortium_files

POST _reindex
{
  "source": {
    "index": [
      "hm_consortium_files_0-3",
      "hm_consortium_files_4-7",
      "hm_consortium_files_8-b",
      "hm_consortium_files_c-f"
    ]
  },
  "dest": {
    "index": "hm_consortium_files"
  }
}

GET hm_consortium_files/_count
GET hm_consortium_files/_search
```

```
DELETE hm_public_files

POST _reindex
{
  "source": {
    "index": [
      "hm_public_files_0-3",
      "hm_public_files_4-7",
      "hm_public_files_8-b",
      "hm_public_files_c-f"
    ]
  },
  "dest": {
    "index": "hm_public_files"
  }
}

GET hm_public_files/_count
```

The reindex requests will time out at the HTTP gateway but continue running on the
server. Monitor progress with:

```
GET _tasks?actions=*reindex&detailed=true
```

The `status.created` field shows documents written so far. The reindex is complete
when the task no longer appears and the document counts match expectations.

### @TODO - Explain simplifying down by dropping Dataset file count thresholds

The previous bootstrap approach split each partition further by dataset file count
(e.g. `0-49999` and `50000-500000`) requiring 8 runs and 8 sets of partition indices.
The old crontab and reindex commands using that approach are preserved here for reference:

```
# Old crontab (8 runs, split by file count thresholds)
15 03 16 06 *  PART=0-3; /path/to/file_index_part_bootstrap_by_ds_size.sh $PART >> /path/to/exec_info/cron_log_$PART 2>&1
20 03 16 06 *  PART=4-7; /path/to/file_index_part_bootstrap_by_ds_size.sh $PART >> /path/to/exec_info/cron_log_$PART 2>&1
25 03 16 06 *  PART=8-B; /path/to/file_index_part_bootstrap_by_ds_size.sh $PART >> /path/to/exec_info/cron_log_$PART 2>&1
30 03 16 06 *  PART=C-F; /path/to/file_index_part_bootstrap_by_ds_size.sh $PART >> /path/to/exec_info/cron_log_$PART 2>&1
```

```
# Old reindex commands (16 indices, split by file count thresholds)
POST _reindex
{
  "source": {
    "index": [
      "hm_consortium_files_0-3_0-500000",
      "hm_consortium_files_4-7_0-500000",
      "hm_consortium_files_8-b_0-500000",
      "hm_consortium_files_c-f_0-500000"
    ]
  },
  "dest": { "index": "hm_consortium_files" }
}

POST _reindex
{
  "source": {
    "index": [
      "hm_public_files_0-3_0-500000",
      "hm_public_files_4-7_0-500000",
      "hm_public_files_8-b_0-500000",
      "hm_public_files_c-f_0-500000"
    ]
  },
  "dest": { "index": "hm_public_files" }
}
```

`es_file_index_partition_bootstrap.py` eliminates this split, requiring only 4 runs
and producing cleaner index names. Document here when the transition is confirmed
stable.

---

## Gap Analysis After Bootstrapping

After merging, run `gap_analysis.py` to validate completeness and account for expected
differences between SQLite, Neo4j, and ElasticSearch.

### Configuration

`gap_analysis.py` reads database and service connection settings from `config.ini`
(same file used by the bootstrap scripts). It also reads and writes hardcoded totals
to `gap_analysis.ini` in the same directory:

```ini
[hardcoded]
sqlite_total = 11633619
es_consortium_total = 10903886
date = 2026-06-17
```

Both files must be present in the working directory.

### Running scripts from the bootstrapping directory

Scripts in `bootstrapping/` import modules (`database.py`, `file_manager.py`,
`file_indexing_utils.py`) that live in the parent directory. `file_index_partition_bootstrap.sh`
sets `PYTHONPATH` automatically. When running Python scripts directly, set it first:

```bash
cd bootstrapping
export PYTHONPATH=$(pwd)/..
```

Or prefix each command:

```bash
PYTHONPATH=.. .venv/bin/python gap_analysis.py
```

### Options

**Fast run** (Neo4j and SQLite queried live; ES total read from `gap_analysis.ini`):
```bash
PYTHONPATH=.. .venv/bin/python gap_analysis.py
```

**Full refresh** (all three sources queried live; `gap_analysis.ini` updated automatically):
```bash
PYTHONPATH=.. .venv/bin/python gap_analysis.py --full-refresh
```

Use `--full-refresh` after each bootstrap run to update the hardcoded totals.
Subsequent fast runs use those totals without the slow ES scroll.

### Interpreting the output

The summary section accounts for the gap between SQLite total files and
`hm_consortium_files` total documents in five non-overlapping buckets:

1. Files with no dataset UUID extractable from their path
2. Files whose dataset UUID appears in SQLite but not in Neo4j (likely revisions)
3. Files in datasets found in both SQLite and Neo4j but not indexed to ES (all files
   filtered out by numeric-only or blank extension rules inherited from SenNet)
4. Datasets in Neo4j not yet crawled by `local_file_index.py` (file count unknown)
5. Lab Process and External Process datasets handled specially

A small overlap between buckets is expected and explained in the output.
