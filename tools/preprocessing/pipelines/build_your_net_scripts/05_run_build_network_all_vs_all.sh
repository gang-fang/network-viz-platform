#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 05_run_build_network_all_vs_all.sh
#
# Purpose:
#   Run build_network_single_node.py using the signal files from
#   script 04 as input.
#
# Input:
#   $WORK_ROOT/Signals
#
# Output:
#   $WORK_ROOT/SJI_network/all_proteomes.sji.network.csv
#
# Example:
#   bash 05_run_build_network_all_vs_all.sh
# ============================================================

SJI_NET="/protDC/Scripts/build_network_single_node.py"
VENV_ACT=""
WORK_ROOT="/protDC/data/work/all_proteomes"

SIGNAL_DIR="${WORK_ROOT%/}/Signals"
OUT_DIR="${WORK_ROOT%/}/SJI_network"
NETWORK_CSV="${OUT_DIR}/all_proteomes.sji.network.csv"

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

if [[ ! -f "$SJI_NET" ]]; then
    echo "Error: build_network_single_node.py was not found: $SJI_NET" >&2
    exit 1
fi

if [[ ! -d "$SIGNAL_DIR" ]]; then
    echo "Error: signal folder was not found: $SIGNAL_DIR" >&2
    echo "Please run script 04 first." >&2
    exit 1
fi

signal_count=$(find "$SIGNAL_DIR" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')
if [[ "$signal_count" -eq 0 ]]; then
    echo "Error: no signal files were found in: $SIGNAL_DIR" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"

activate_python_env

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi

echo "Running build_network_single_node.py:"
echo "  Script       : $SJI_NET"
echo "  Signal DB    : $SIGNAL_DIR"
echo "  Signal files : $signal_count"
echo "  Output CSV   : $NETWORK_CSV"

python "$SJI_NET" \
    --db "$SIGNAL_DIR" \
    --output "$NETWORK_CSV"

if [[ ! -s "$NETWORK_CSV" ]]; then
    echo "Error: network CSV was not created or is empty: $NETWORK_CSV" >&2
    exit 1
fi

row_count=$(awk 'NF > 0 { count++ } END { print count + 0 }' "$NETWORK_CSV")

echo
echo "Script 05 finished."
echo "Network CSV: $NETWORK_CSV"
echo "Rows written: $row_count"
