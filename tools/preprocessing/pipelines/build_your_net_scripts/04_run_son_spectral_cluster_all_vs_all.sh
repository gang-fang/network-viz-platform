#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 04_run_son_spectral_cluster_all_vs_all.sh
#
# Purpose:
#   Run son_spectral_final.py using the 2D output from script 03.
#
# Input:
#   $WORK_ROOT/2D_out
#
# Output:
#   $WORK_ROOT/Signals
#
# Example:
#   bash 04_run_son_spectral_cluster_all_vs_all.sh
#
# Memory-safety note:
#   The Python command uses -j 1 intentionally.
# ============================================================

SON="/protDC/Scripts/son_spectral_final.py"
VENV_ACT=""
WORK_ROOT="/protDC/data/work/all_proteomes"
JOBS=1

TWO_D_ROOT="${WORK_ROOT%/}/2D_out"
OUT_FOLDER="${WORK_ROOT%/}/Signals"

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

if [[ ! -f "$SON" ]]; then
    echo "Error: son_spectral_final.py was not found: $SON" >&2
    exit 1
fi

if [[ ! -d "$TWO_D_ROOT" ]]; then
    echo "Error: 2D output folder was not found: $TWO_D_ROOT" >&2
    echo "Please run script 03 first." >&2
    exit 1
fi

mkdir -p "$OUT_FOLDER"

activate_python_env

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi

TMP_INPUT_LIST=$(mktemp)
trap 'rm -f "$TMP_INPUT_LIST"' EXIT

find "$TWO_D_ROOT" -mindepth 1 -maxdepth 1 -type d | sort > "$TMP_INPUT_LIST"

if [[ ! -s "$TMP_INPUT_LIST" ]]; then
    printf '%s\n' "$TWO_D_ROOT" > "$TMP_INPUT_LIST"
fi

input_count=$(wc -l < "$TMP_INPUT_LIST" | tr -d ' ')

echo "Running son_spectral_final.py:"
echo "  2D root folder       : $TWO_D_ROOT"
echo "  Signal output folder : $OUT_FOLDER"
echo "  Input folders        : $input_count"
echo "  Jobs                 : $JOBS"

while IFS= read -r two_d_input; do
    [[ -n "$two_d_input" ]] || continue
    input_name=$(basename "$two_d_input")

    echo
    echo "Processing 2D folder:"
    echo "  Name  : $input_name"
    echo "  Input : $two_d_input"
    echo "  Output: $OUT_FOLDER"
    echo "  Jobs  : $JOBS"

    python "$SON" \
        --o "$OUT_FOLDER" \
        -j "$JOBS" \
        "$two_d_input"

done < "$TMP_INPUT_LIST"

echo
echo "Script 04 finished."
echo "Signal output folder: $OUT_FOLDER"
