#!/bin/bash

####################################################################################################
# Wrapper script to drive the partitioned bootstrapping process from crontab entries.
#   es_file_index_partition_bootstrap.py  - reads the SQLite database and indexes
#   a partition of the file data to ElasticSearch.
#
# Usage: file_index_partition_bootstrap.sh <PARTITION_KEY>
# Example: file_index_partition_bootstrap.sh C-F
#
# Crontab example:
#   PART=C-F; /path/to/file_index_partition_bootstrap.sh $PART >> /path/to/exec_info/cron_log_$PART 2>&1
####################################################################################################

function enter_script() {
    echo "Begin execution $0 at $(date) by $(whoami)"
}

function exit_script() {
    echo "End execution $0 at $(date) by $(whoami)"
    exit $1
}

enter_script

# Validate partition key argument
PARTITION_KEY="$1"
if [ -z "${PARTITION_KEY}" ]; then
    echo "A valid partition key is required."
    exit_script 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/exec_info"
mkdir -p "${LOG_DIR}"

# Determine execution environment and set PYTHON_CMD accordingly
VENV_ACTIVATE="${SCRIPT_DIR}/../.venv/bin/activate"
if [ -f "${VENV_ACTIVATE}" ]; then
    EXEC_ENV=VENV_ENV
    source "${VENV_ACTIVATE}"
    PYTHON_CMD="python"
    echo "Activated virtual environment: ${SCRIPT_DIR}/.venv"
else
    EXEC_ENV=IMAGE_ENV
    PYTHON_CMD="python3"
    echo "No .venv found - using system Python: $(which python3)"
fi

echo "Execution environment: ${EXEC_ENV}"
echo "Python: $(which ${PYTHON_CMD}) -- $(${PYTHON_CMD} --version)"
echo "Logging to ${LOG_DIR}/file_index_partition_bootstrap_python_output_${PARTITION_KEY}.log"

# Write header to log, then append all subsequent output
echo "=== $(date) | ${EXEC_ENV} | $(${PYTHON_CMD} --version) | $(which ${PYTHON_CMD}) ===" \
    > "${LOG_DIR}/file_index_partition_bootstrap_python_output_${PARTITION_KEY}.log" 2>&1

# Execute Python script from the directory where this script resides
cd "${SCRIPT_DIR}"

PYTHON_SCRIPT="es_file_index_partition_bootstrap.py"
if [ ! -f "${PYTHON_SCRIPT}" ]; then
    echo "Python script not found: ${SCRIPT_DIR}/${PYTHON_SCRIPT}"
    exit_script 1
fi

echo "Executing ${PYTHON_SCRIPT} --partition-key ${PARTITION_KEY} to bootstrap ElasticSearch indices with local Dataset file info"
# PYTHONPATH is set inline (not exported) so it applies only to this Python process.
# It adds the parent directory so database.py, file_manager.py, and file_indexing_utils.py
# are found when running from the bootstrapping/ subdirectory.
PYTHONPATH="${SCRIPT_DIR}/.." $PYTHON_CMD "${PYTHON_SCRIPT}" --config "${SCRIPT_DIR}/../config.ini" --service-config "${SCRIPT_DIR}/../fileIndexingService.ini" --partition-key "${PARTITION_KEY}" >> "${LOG_DIR}/file_index_partition_bootstrap_python_output_${PARTITION_KEY}.log" 2>&1
if [ $? -ne 0 ]; then
    echo "${PYTHON_SCRIPT} failed."
    exit_script 1
fi

exit_script 0

