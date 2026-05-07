#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 03_run_make_2d_all_vs_all.sh
#
# Purpose:
#   Run make_2d.py using the all-against-all topN output from
#   script 02 as input.
#
# Input:
#   $WORK_ROOT/topN_out
#
# Output:
#   $WORK_ROOT/2D_out
#
# Example:
#   bash 03_run_make_2d_all_vs_all.sh
# ============================================================

MAKE_2D="/protDC/Scripts/make_2d.py"

# Suggested values:
#   N=20 for bacteria
#   N=50 for eukaryotes
N=20

VENV_ACT=""
WORK_ROOT="/protDC/data/work/all_proteomes"

TOPN_OUT="${WORK_ROOT%/}/topN_out"
OUT_FOLDER="${WORK_ROOT%/}/2D_out"

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

if [[ ! -f "$MAKE_2D" ]]; then
    echo "Error: make_2d.py was not found: $MAKE_2D" >&2
    exit 1
fi

if [[ ! -d "$TOPN_OUT" ]]; then
    echo "Error: topN output folder was not found: $TOPN_OUT" >&2
    echo "Please run script 02 first." >&2
    exit 1
fi

topn_file_count=$(find "$TOPN_OUT" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')
if [[ "$topn_file_count" -eq 0 ]]; then
    echo "Error: no topN output files were found in: $TOPN_OUT" >&2
    exit 1
fi

mkdir -p "$OUT_FOLDER"

activate_python_env

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi

echo "Running make_2d.py:"
echo "  Input folder : $TOPN_OUT"
echo "  Output folder: $OUT_FOLDER"
echo "  N            : $N"
echo "  Input files  : $topn_file_count"

python "$MAKE_2D" \
    --o "$OUT_FOLDER" \
    -n "$N" \
    "$TOPN_OUT"

echo
echo "Script 03 finished."
echo "2D output folder: $OUT_FOLDER"
