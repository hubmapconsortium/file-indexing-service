#!/bin/bash

####################################################################################################
# Script to evaluate timing for Neo4j fulfillment of the query used by es_file_index*.py.
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
echo "Logging to ${LOG_DIR}/time_partition_retrieval.log"

# Write header to log, then append all subsequent output
echo "=== $(date) | ${EXEC_ENV} | $(${PYTHON_CMD} --version) | $(which ${PYTHON_CMD}) ===" \
    > "${LOG_DIR}/time_partition_retrieval.log" 2>&1

# Execute all Python scripts from the directory where this script resides
cd "${SCRIPT_DIR}"

echo "Executing time_partition_retrieval.py to get Neo4j retrieval timing info"
$PYTHON_CMD time_partition_retrieval.py --config config.ini >> "${LOG_DIR}/time_partition_retrieval.log" 2>&1

exit_script 0
