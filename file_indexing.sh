#!/bin/bash

####################################################################################################
# Wrapper script for the file-indexing-service pipeline.
#
# Runs three Python scripts in sequence:
#   1. local_file_index.py  — walks the filesystem and populates a local SQLite database
#   2. stash_database.py    — backs up the SQLite database to a versioned AWS S3 bucket
#   3. es_file_index.py     — reads the SQLite database and indexes file data to ElasticSearch
#
# This script runs code from the GitHub repo https://github.com/hubmapconsortium/file-indexing-service
#
# Execution environments supported:
#   VENV_ENV   — a .venv exists alongside this script (PyCharm, server venv, etc.)
#                Python and all dependencies come from the .venv.
#   IMAGE_ENV  — no .venv present; assumes a Docker image where the system Python
#                already has all dependencies installed.
#
# In both cases the script executes from its own directory, so config.ini,
# fileIndexingService.ini, and exec_info/ are resolved relative to that location.
####################################################################################################

function enter_script() {
    echo "Begin execution $0 at $(date) by $(whoami)"
}

function exit_script() {
    echo "End execution $0 at $(date) by $(whoami)"
    exit $1
}

enter_script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/exec_info"
mkdir -p "${LOG_DIR}"

# Determine execution environment and set PYTHON_CMD accordingly
VENV_ACTIVATE="${SCRIPT_DIR}/.venv/bin/activate"
if [ -f "${VENV_ACTIVATE}" ]; then
    EXEC_ENV=VENV_ENV
    source "${VENV_ACTIVATE}"
    PYTHON_CMD="python"
    echo "Activated virtual environment: ${SCRIPT_DIR}/.venv"
else
    EXEC_ENV=IMAGE_ENV
    PYTHON_CMD="python3"
    echo "No .venv found — using system Python: $(which python3)"
fi

echo "Execution environment: ${EXEC_ENV}"
echo "Python: $(which ${PYTHON_CMD}) -- $(${PYTHON_CMD} --version)"
echo "Logging to ${LOG_DIR}/file_indexing_python_output.log"

# Write header to log, then append all subsequent output
echo "=== $(date) | ${EXEC_ENV} | $(${PYTHON_CMD} --version) | $(which ${PYTHON_CMD}) ===" \
    > "${LOG_DIR}/file_indexing_python_output.log" 2>&1

# Execute all Python scripts from the directory where this script resides
cd "${SCRIPT_DIR}"

echo "Executing local_file_index.py to fill SQLite database with local Dataset file info"
$PYTHON_CMD local_file_index.py --config config.ini >> "${LOG_DIR}/file_indexing_python_output.log" 2>&1
if [ $? -ne 0 ]; then
    echo "local_file_index.py failed — SQLite database may be incomplete or missing. Aborting."
    exit_script 1
fi

echo "Executing stash_database.py to store SQLite database in versioned S3 Bucket"
# Continue even if stash fails — the SQLite database is still valid for ES indexing.
# A failure here means the S3 backup is missing but does not affect ElasticSearch population.
$PYTHON_CMD stash_database.py --config config.ini >> "${LOG_DIR}/file_indexing_python_output.log" 2>&1

echo "Executing es_file_index.py to fill ElasticSearch indices with local Dataset file info"
$PYTHON_CMD es_file_index.py --config config.ini >> "${LOG_DIR}/file_indexing_python_output.log" 2>&1

exit_script 0
