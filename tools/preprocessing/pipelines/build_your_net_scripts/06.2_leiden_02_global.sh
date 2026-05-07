#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 6.2_leiden_02_global.sh
#
# Purpose:
#   Run global Leiden clustering on the integer edge file from
#   script 6.1. Both CPU and GPU Python implementations are supported.
#
# Input:
#   $WORK_ROOT/Leiden/integer_edges.csv
#
# Outputs, by default:
#   $WORK_ROOT/Leiden/networks/partition.csv
#   $WORK_ROOT/Leiden/networks/metrics.csv
#   $WORK_ROOT/Leiden/networks/subnetworks/subnet.*
#
# Example:
#   bash 6.2_leiden_02_global.sh
#
# Note:
#   The uploaded leiden_02_cpu_global.py and leiden_02_gpu_global.py do not
#   expose -j or -p options, so this wrapper does not add them.
# ============================================================

LEIDEN_MODE="cpu"

LEIDEN_02_CPU="/protDC/Scripts/leiden_02_cpu_global.py"
LEIDEN_02_GPU="/protDC/Scripts/leiden_02_gpu_global.py"

VENV_ACT=""

WORK_ROOT="/protDC/data/work/all_proteomes"

RESOLUTION=1.0
MAX_ITER=100
SEED=42
VALIDATION_ROWS=10

INTEGER_EDGES_HAS_HEADER=false
SKIP_SUBNETWORKS=false
VERBOSE_CC=false

LEIDEN_ROOT="${WORK_ROOT%/}/Leiden"
INTEGER_EDGES="${LEIDEN_ROOT}/integer_edges.csv"
NETWORKS_DIR="${LEIDEN_ROOT}/networks"

activate_python_env() {
    if [[ -z "$VENV_ACT" ]]; then
        echo "No Python environment specified. Continuing without environment activation."
        return 0
    fi

    if [[ ! -f "$VENV_ACT" ]]; then
        echo "Error: Python environment activation script not found: $VENV_ACT" >&2
        exit 1
    fi

    # This function is called with no arguments, so command-line parameters
    # passed to the main script are not visible as $1 inside the sourced
    # activation script.
    # shellcheck disable=SC1090
    source "$VENV_ACT"
}

if [[ ! -s "$INTEGER_EDGES" ]]; then
    echo "Error: integer edge file was not found or is empty: $INTEGER_EDGES" >&2
    echo "Please run script 6.1 first." >&2
    exit 1
fi

if [[ "$LEIDEN_MODE" == "cpu" ]]; then
    LEIDEN_02="$LEIDEN_02_CPU"
elif [[ "$LEIDEN_MODE" == "gpu" ]]; then
    LEIDEN_02="$LEIDEN_02_GPU"
else
    echo "Error: LEIDEN_MODE must be either 'cpu' or 'gpu'. Current value: $LEIDEN_MODE" >&2
    exit 1
fi

if [[ ! -f "$LEIDEN_02" ]]; then
    echo "Error: selected Leiden step 02 script was not found: $LEIDEN_02" >&2
    exit 1
fi

mkdir -p "$NETWORKS_DIR"

activate_python_env

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi

cd "$LEIDEN_ROOT"

cmd=(
    python "$LEIDEN_02"
    --edges "$INTEGER_EDGES"
    --output_dir "$NETWORKS_DIR"
    --resolution "$RESOLUTION"
    --max_iter "$MAX_ITER"
    --seed "$SEED"
    --validation_rows "$VALIDATION_ROWS"
)

if [[ "$INTEGER_EDGES_HAS_HEADER" == true ]]; then
    cmd+=(--has_header)
fi

if [[ "$SKIP_SUBNETWORKS" == true ]]; then
    cmd+=(--skip_subnetworks)
fi

if [[ "$VERBOSE_CC" == true ]]; then
    cmd+=(--verbose_cc)
fi

echo "Running Leiden step 02:"
echo "  Mode          : $LEIDEN_MODE"
echo "  Script        : $LEIDEN_02"
echo "  Integer edges : $INTEGER_EDGES"
echo "  Output dir    : $NETWORKS_DIR"
echo "  Resolution    : $RESOLUTION"
echo "  Max iter      : $MAX_ITER"
echo "  Seed          : $SEED"

"${cmd[@]}"

if [[ ! -s "${NETWORKS_DIR}/partition.csv" ]]; then
    echo "Error: partition.csv was not created: ${NETWORKS_DIR}/partition.csv" >&2
    exit 1
fi

if [[ ! -s "${NETWORKS_DIR}/metrics.csv" ]]; then
    echo "Error: metrics.csv was not created: ${NETWORKS_DIR}/metrics.csv" >&2
    exit 1
fi

echo
echo "Script 6.2 finished."
echo "Partition: ${NETWORKS_DIR}/partition.csv"
echo "Metrics  : ${NETWORKS_DIR}/metrics.csv"
