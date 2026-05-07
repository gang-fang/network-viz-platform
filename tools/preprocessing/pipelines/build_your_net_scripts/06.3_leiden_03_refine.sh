#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 6.3_leiden_03_refine.sh
#
# Purpose:
#   Run leiden_03_refine.py twice.
#
#   First run:
#     - test run
#     - no --auto_recursive
#     - density threshold 0.1
#     - --jobs 1
#
#   Second run:
#     - --auto_recursive
#     - user-editable density threshold
#     - --prune_weak_pairs
#     - --jobs 1
#
# Outputs:
#   $WORK_ROOT/Leiden/networks/partition.csv
#   $WORK_ROOT/Leiden/networks/metrics.csv
#
# Example:
#   bash 6.3_leiden_03_refine.sh
# ============================================================

LEIDEN_03="/protDC/Scripts/leiden_03_refine.py"

VENV_ACT=""

WORK_ROOT="/protDC/data/work/all_proteomes"

USE_GPU_LEIDEN=false

JOBS=1
TEST_DENSITY_THRESHOLD=0.1
RESOLUTION=1.0
MIN_NODES=3
PREFER_THREADS=false

# Tune this parameter as needed. A higher value, such as 0.5, or even higher
# is recommended if the goal is to ensure that the clusters are functionally 
# consistent.
RECURSIVE_DENSITY_THRESHOLD=0.5


LEIDEN_ROOT="${WORK_ROOT%/}/Leiden"
NETWORKS_DIR="${LEIDEN_ROOT}/networks"
PARTITION="${NETWORKS_DIR}/partition.csv"
METRICS="${NETWORKS_DIR}/metrics.csv"
SUBNETWORKS_DIR="${NETWORKS_DIR}/subnetworks"

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

if [[ ! -f "$LEIDEN_03" ]]; then
    echo "Error: leiden_03_refine.py was not found: $LEIDEN_03" >&2
    exit 1
fi

if [[ ! -s "$PARTITION" ]]; then
    echo "Error: partition.csv was not found or is empty: $PARTITION" >&2
    echo "Please run script 6.2 first." >&2
    exit 1
fi

if [[ ! -s "$METRICS" ]]; then
    echo "Error: metrics.csv was not found or is empty: $METRICS" >&2
    echo "Please run script 6.2 first." >&2
    exit 1
fi

if [[ ! -d "$SUBNETWORKS_DIR" ]]; then
    echo "Error: subnetworks directory was not found: $SUBNETWORKS_DIR" >&2
    echo "Please rerun script 6.2 without SKIP_SUBNETWORKS=true." >&2
    exit 1
fi

activate_python_env

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi

cd "$LEIDEN_ROOT"

common_opts=(
    --output_dir "$NETWORKS_DIR"
    --edges_dir "$SUBNETWORKS_DIR"
    --partition "$PARTITION"
    --metrics "$METRICS"
    --base_network_name "subnet"
    --resolution "$RESOLUTION"
    --min_nodes "$MIN_NODES"
    --jobs "$JOBS"
)

if [[ "$USE_GPU_LEIDEN" == true ]]; then
    common_opts+=(--use_gpu_leiden)
fi

if [[ "$PREFER_THREADS" == true ]]; then
    common_opts+=(--prefer_threads)
fi

echo "Running Leiden step 03 test pass:"
echo "  Script             : $LEIDEN_03"
echo "  Output dir         : $NETWORKS_DIR"
echo "  Density threshold  : $TEST_DENSITY_THRESHOLD"
echo "  Auto recursive     : false"
echo "  Prune weak pairs   : false"
echo "  Jobs               : $JOBS"
echo "  Use GPU Leiden     : $USE_GPU_LEIDEN"

python "$LEIDEN_03"     "${common_opts[@]}"     --density_threshold "$TEST_DENSITY_THRESHOLD"

echo
echo "Running Leiden step 03 auto-recursive pass:"
echo "  Script             : $LEIDEN_03"
echo "  Output dir         : $NETWORKS_DIR"
echo "  Density threshold  : $RECURSIVE_DENSITY_THRESHOLD"
echo "  Auto recursive     : true"
echo "  Prune weak pairs   : true"
echo "  Jobs               : $JOBS"
echo "  Use GPU Leiden     : $USE_GPU_LEIDEN"

python "$LEIDEN_03"     "${common_opts[@]}"     --density_threshold "$RECURSIVE_DENSITY_THRESHOLD"     --auto_recursive     --prune_weak_pairs

if [[ ! -s "$PARTITION" ]]; then
    echo "Error: final partition.csv was not found or is empty: $PARTITION" >&2
    exit 1
fi

if [[ ! -s "$METRICS" ]]; then
    echo "Error: final metrics.csv was not found or is empty: $METRICS" >&2
    exit 1
fi

echo
echo "Script 6.3 finished."
echo "Final partition: $PARTITION"
echo "Final metrics  : $METRICS"
