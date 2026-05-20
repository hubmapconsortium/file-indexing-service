#!/bin/bash

####################################################################################################
# This script is to be run in a Docker container invoked by a cron job. It serves as a wrapper so
# the local_file_index.py script can manage data about the Dataset files in a SQLite database, and
# the es_file_index.py script can subsequently index that data to ElasticSearch while maintaining
# separate INI configuration files, and logging to separate logs.
#
# This script runs code from the GitHub repo https://github.com/hubmapconsortium/file-indexing-service
#
# This job reads JSON files stored in AWS S3, placed there by processes running on a PSC Hive
# server which parses the logs Globus provides for authenticated, high-availability
# file transfers and for HTTP file transfers.
#
# This job creates ElasticSearch documents for each JSON Object in the S3 Objects it processes.
# Each S3 Object contains a single JSON Array filled with JSON Objects corresponding to logged
# file download events.
#
#
####################################################################################################

function enter_script() {
    echo Begin execution $0 at `date` by `whoami`
}

function exit_script() {
    echo End execution $0 at `date` by `whoami`
    exit $1
}

enter_script

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The execution configuration may be different during development on
# localhost than Docker deployment. Determine where this script is being
# executed by checking for a virtual environment directory
EXEC_ENV=UNKNOWN
if [ -d "${SCRIPT_DIR}/.venv" ]; then
  EXEC_ENV=LOCAL_ENV # PyCharm, Visual Studio Code, filesystem .venv, etc.
else
  EXEC_ENV=IMAGE_ENV # WORKDIR of Docker Image, with system pip3.13, python3.13, etc
fi

# Set the location of resources for the execution environment
if [ ${EXEC_ENV} == LOCAL_ENV ]; then
  LOG_DIR="${SCRIPT_DIR}/exec_info"
  PYTHON_CMD="$SCRIPT_DIR/.venv/bin/python3.13"
elif [ ${EXEC_ENV} == IMAGE_ENV ]; then
  LOG_DIR="${SCRIPT_DIR}/exec_info"
  PYTHON_CMD="python3.13"
else
  echo "Unable to determine execution environment based on EXEC_ENV=${EXEC_ENV}"
  exit 1
fi

echo Logging script output to ${LOG_DIR}/file_indexing_python_output.log.
echo Execute using Python 3.13 at ${PYTHON_CMD}.
echo Execute using Python 3.13 at ${PYTHON_CMD}. > $LOG_DIR/file_indexing_python_output.log 2>&1

# Execute the Python scripts from the directory where this script resides, which is
# below PyCharm's project directory, but is WORKDIR for Docker.
cd "${SCRIPT_DIR}"

echo executing "python3 local_file_index.py to fill SQLite database with local Dataset file info"
$PYTHON_CMD local_file_index.py --config config.ini >> $LOG_DIR/file_indexing_python_output.log 2>&1
if [ $? -ne 0 ]; then
  echo "local_file_index.py failed — SQLite database may be incomplete or missing. Aborting."
  exit_script 1
fi

echo executing "python3 stash_database.py to store SQLite database in versioned S3 Bucket"
# Continue even if stash fails — the SQLite database is still valid for ES indexing.
# A failure here means the S3 backup is missing but does not affect ElasticSearch population.
$PYTHON_CMD stash_database.py --config config.ini >> $LOG_DIR/file_indexing_python_output.log 2>&1

echo executing "python3 es_file_index.py to fill ElasticSearch indices with local Dataset file info"
$PYTHON_CMD es_file_index.py --config config.ini >> $LOG_DIR/file_indexing_python_output.log 2>&1

exit_script 0
