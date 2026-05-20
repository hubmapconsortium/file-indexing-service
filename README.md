# File Indexing Scripts

# Scripts and programs

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

## Configuration file

Configuration files are required to run the Python programs. Example configuration files can be found
in `config.ini.example` and `fileIndexingService.ini.example`. 

## Dependencies

Dependencies are found in the `requirements.txt` file and can be installed with `pip install -r requirements.txt`.

## Running the script or a program

### file_indexing.sh
This Bash script will execute the Python files in order.  It is typically scheduled
using `cron`, but can be launched manually with the correct environment setup.
```bash
    file_indexing.sh
```
#### local_file_index.py
```bash
    # config parameter defaults to 'config.ini'
    python local_file_index.py --config config.ini
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
