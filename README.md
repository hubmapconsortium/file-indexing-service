# File Indexing scripts and programs

## file_indexing.sh
The `file_indexing.sh` Bash script executes in order the Python programs which make up the
file indexing process.  This script is scheduled for `cron` execution for the user `hive`.

### local_file_index.py

`local_file_index.py` walks the directories of the configuration file field, `INDEX_PATHS`.
It indexes the files found in those directories into an SQLite database.
If the filepath exists in the database under the column `path`, a new item is created with the same `path` and an incremented `version`. Items are not deleted from the database.

Scripts arguments

| Argument   | Required | Default       | Type | Description                                         |
|------------|----------|---------------|------|-----------------------------------------------------|
| --config   | No       | config.ini    | str  | Path to the configuration file (ini format)         |
| --create-new-database | No | (not set) | flag | Create a new SQLite database. Exits with an error if the database file already exists. |

#### SQLite database schema

| Column           | Type            | Constraints         | Description             |
| ---------------- | --------------- | ------------------- | ----------------------- |
| path             | TEXT            | NOT NULL            | File path               |
| version          | INTEGER         | NOT NULL, DEFAULT 1 | Version number          |
| aws_version      | TEXT            |                     | AWS version (optional)  |
| size             | INTEGER         | NOT NULL            | File size               |
| last_modified_at | INTEGER         | NOT NULL            | Last modified timestamp |
| PRIMARY KEY      | (path, version) |                     | Composite primary key   |

### stash_database.py

`stash_database.py` script puts a copy of the SQLite database populated by
`local_file_index.py` in the AWS versioned S3 Bucket with a Storage Type of `DEEP_ARCHIVE`.

### es_file_index.py

`es_file_index.py` script queries the Neo4J database for Published Primary Datasets,
Published Processed Datasets, and QA Datasets. The files for each Dataset are retrieved from
the SQLite database. File information is then generated and indexed into ElasticSearch.

Published Datasets are indexed into both the public and consortium indices.
QA Datasets are indexed into the consortium index.

Scripts arguments

| Argument   | Required | Default       | Type | Description                                         |
|------------|----------|---------------|------|-----------------------------------------------------|
| --config   | No       | config.ini    | str  | Path to the configuration file (ini format)         |


# Running scripts

## Configuration files

Configuration files are required to run the Python programs. Example configuration files can be found
in `config.ini.example` and `fileIndexingService.ini.example`.

Copy each example file, remove the `.example` suffix, and fill in the secrets, tokens, and
passwords. These files are gitignored and must never be committed.

```bash
cp config.ini.example config.ini
cp fileIndexingService.ini.example fileIndexingService.ini
```

## Dependencies

Dependencies are found in the `requirements.txt` file.

### Setting up a virtual environment

A virtual environment is recommended to isolate dependencies from the system Python and from
other projects. Create and populate it once per deployment:

```bash
cd /path/to/file-indexing-service
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Use whichever `python3` version is available and appropriate for the deployment (Python 3.9 or
higher). Once the `.venv` exists, `file_indexing.sh` will detect and activate it automatically.

To verify the environment is set up correctly:
```bash
.venv/bin/python --version
.venv/bin/pip list
```

## Running the script or a program

### file_indexing.sh
This Bash script will execute the Python files in order. It detects whether a `.venv` is
present and activates it automatically. It is typically scheduled using `cron`, but can be
launched manually:
```bash
    bash file_indexing.sh
```

### local_file_index.py
```bash
    # config parameter defaults to 'config.ini'
    python local_file_index.py --config config.ini

    # To create a fresh database (exits with error if the file already exists):
    python local_file_index.py --config config.ini --create-new-database
```

### stash_database.py
```bash
    # config parameter defaults to 'config.ini'
    python stash_database.py --config config.ini
```

### es_file_index.py
```bash
    # config parameter defaults to 'config.ini'
    python es_file_index.py --config config.ini
```

## Scheduling with cron

The `file_indexing.sh` script is designed to be run as a cron job. Add an entry to the
crontab of the user who owns the deployment directory:

```bash
crontab -e
```

Add a line in the format `minute hour * * * /full/path/to/file_indexing.sh >> /full/path/to/exec_info/cron_log 2>&1`.
For example, to run daily at 2:00am:

```
0 2 * * *  /path/to/file-indexing-service/file_indexing.sh >> /path/to/file-indexing-service/exec_info/cron_log 2>&1
```

Notes:
- Use full absolute paths in crontab entries — cron does not inherit your shell's `PATH` or working directory.
- The `exec_info/` directory must exist before the first run, or the log redirection will fail silently. Create it with `mkdir -p exec_info`.
- To test the cron entry without waiting for the scheduled time, run the command directly from your shell first and confirm it exits cleanly.
