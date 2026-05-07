#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 08_run_son_spectral_T.sh
#
# Purpose:
#   After script 07 has merged existing package 2D files with the
#   newly generated 2D_out.t files, run son_spectral_final.py on
#   the cleaned 2D_out.t folder.
#
# Workflow:
#   1. Check the script 07 report files.
#   2. Stop if script 07 reported missing files or duplicate matches.
#   3. Remove script 07 backup files:
#        *.before_script07
#   4. Run son_spectral_final.py on:
#        $WORK_ROOT/$PROTEOME_ID/2D_out.t
#   5. Write signal files to:
#        $WORK_ROOT/$PROTEOME_ID/Signals.t
#
# Example:
#   bash 08_run_son_spectral_T.sh UP000000556
#
# Notes:
#   - This script assumes that script 07 has already been run.
#   - Users should inspect script 07 reports before running this script.
#   - This script also performs automatic report checks before deleting
#     *.before_script07 backup files.
# ============================================================


# -------------------------
# Proteome ID
# -------------------------

PROTEOME_ID="${1:-UP000000556}"


# -------------------------
# Editable variables
# -------------------------

# Edit this variable to point to the location where son_spectral_final.py is stored.
SON="/protDC/Scripts/son_spectral_final.py"

# Optional Python environment activation.
# Leave VENV_ACT empty if no Python environment needs to be activated.
# Examples:
#   VENV_ACT="/protDC/venv/bin/activate"
#   VENV_ACT="/protDC/miniconda3/envs/protDC/bin/activate"
VENV_ACT=""

# Edit this variable to the main working folder.
WORK_ROOT="/protDC/data/work"

# Number of jobs used by son_spectral_final.py.
# Use 1 when memory is a concern.
JOBS=1

# If true, require script 07 reports to be clean before deleting backups
# and running son_spectral_final.py.
REQUIRE_SCRIPT07_REPORTS_CLEAN=true


# -------------------------
# Derived paths
# -------------------------

WORK_DIR="${WORK_ROOT%/}/${PROTEOME_ID}"
TWO_D_ROOT="${WORK_DIR}/2D_out.t"
OUT_FOLDER="${WORK_DIR}/Signals.t"

REPORT_DIR="${WORK_DIR}/script07_merge_reports"
MERGED_REPORT="${REPORT_DIR}/merged.tsv"
MISSING_EXISTING_REPORT="${REPORT_DIR}/missing_existing_2D_HOME.txt"
MISSING_NEW_REPORT="${REPORT_DIR}/missing_new_2D_out.t.txt"
DUPLICATE_REPORT="${REPORT_DIR}/duplicate_matches.tsv"


# -------------------------
# Functions
# -------------------------

activate_python_env() {
    if [[ -z "$VENV_ACT" ]]; then
        echo "No Python environment specified. Continuing without environment activation."
        return 0
    fi

    if [[ ! -f "$VENV_ACT" ]]; then
        echo "Error: Python environment activation script not found: $VENV_ACT" >&2
        exit 1
    fi

    # This function is called with no arguments, so the proteome ID passed to
    # the main script is not visible as $1 inside the sourced activation script.
    # shellcheck disable=SC1090
    source "$VENV_ACT"
}

count_nonempty_lines() {
    local file="$1"

    if [[ ! -f "$file" ]]; then
        echo "0"
        return 0
    fi

    awk 'NF > 0 { count++ } END { print count + 0 }' "$file"
}

count_data_lines_after_header() {
    local file="$1"

    if [[ ! -f "$file" ]]; then
        echo "0"
        return 0
    fi

    awk 'NR > 1 && NF > 0 { count++ } END { print count + 0 }' "$file"
}

check_script07_reports() {
    echo "Checking script 07 reports:"
    echo "  Report folder                : $REPORT_DIR"
    echo "  Merged report                : $MERGED_REPORT"
    echo "  Missing existing 2D_HOME      : $MISSING_EXISTING_REPORT"
    echo "  Missing new 2D_out.t          : $MISSING_NEW_REPORT"
    echo "  Duplicate match report        : $DUPLICATE_REPORT"

    if [[ ! -d "$REPORT_DIR" ]]; then
        echo "Error: script 07 report folder was not found: $REPORT_DIR" >&2
        echo "Please run script 07 first." >&2
        exit 1
    fi

    if [[ ! -s "$MERGED_REPORT" ]]; then
        echo "Error: merged report is missing or empty: $MERGED_REPORT" >&2
        echo "Please check whether script 07 finished successfully." >&2
        exit 1
    fi

    if [[ ! -f "$MISSING_EXISTING_REPORT" ]]; then
        echo "Error: missing-existing report was not found: $MISSING_EXISTING_REPORT" >&2
        exit 1
    fi

    if [[ ! -f "$MISSING_NEW_REPORT" ]]; then
        echo "Error: missing-new report was not found: $MISSING_NEW_REPORT" >&2
        exit 1
    fi

    if [[ ! -f "$DUPLICATE_REPORT" ]]; then
        echo "Error: duplicate match report was not found: $DUPLICATE_REPORT" >&2
        exit 1
    fi

    missing_existing_count=$(count_nonempty_lines "$MISSING_EXISTING_REPORT")
    missing_new_count=$(count_nonempty_lines "$MISSING_NEW_REPORT")
    duplicate_count=$(count_data_lines_after_header "$DUPLICATE_REPORT")

    echo "Report summary:"
    echo "  Missing existing 2D_HOME files : $missing_existing_count"
    echo "  Missing new 2D_out.t files     : $missing_new_count"
    echo "  Duplicate matches              : $duplicate_count"

    if [[ "$missing_existing_count" -gt 0 ]]; then
        echo "Error: script 07 reported missing existing 2D_HOME files." >&2
        echo "Inspect: $MISSING_EXISTING_REPORT" >&2
        exit 1
    fi

    if [[ "$missing_new_count" -gt 0 ]]; then
        echo "Error: script 07 reported missing new 2D_out.t files." >&2
        echo "Inspect: $MISSING_NEW_REPORT" >&2
        exit 1
    fi

    if [[ "$duplicate_count" -gt 0 ]]; then
        echo "Error: script 07 reported duplicate matches." >&2
        echo "Inspect: $DUPLICATE_REPORT" >&2
        exit 1
    fi

    echo "Script 07 reports look clean."
}

remove_script07_backups() {
    echo
    echo "Removing script 07 backup files from: $TWO_D_ROOT"

    backup_count=$(find "$TWO_D_ROOT" -type f -name '*.before_script07' | wc -l | tr -d ' ')

    echo "Backup files found: $backup_count"

    if [[ "$backup_count" -gt 0 ]]; then
        find "$TWO_D_ROOT" -type f -name '*.before_script07' -delete
    fi

    remaining_count=$(find "$TWO_D_ROOT" -type f -name '*.before_script07' | wc -l | tr -d ' ')

    if [[ "$remaining_count" -ne 0 ]]; then
        echo "Error: some .before_script07 files could not be removed." >&2
        find "$TWO_D_ROOT" -type f -name '*.before_script07' -print >&2
        exit 1
    fi

    echo "Backup cleanup finished."
}


# -------------------------
# Basic checks
# -------------------------

if [[ ! -f "$SON" ]]; then
    echo "Error: son_spectral_final.py was not found: $SON" >&2
    exit 1
fi

if [[ ! -d "$TWO_D_ROOT" ]]; then
    echo "Error: 2D_out.t folder was not found: $TWO_D_ROOT" >&2
    echo "Please run scripts 06 and 07 first." >&2
    exit 1
fi

mkdir -p "$OUT_FOLDER"


# -------------------------
# Activate Python environment, if needed
# -------------------------

activate_python_env

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi


# -------------------------
# Check script 07 reports and remove backups
# -------------------------

if [[ "$REQUIRE_SCRIPT07_REPORTS_CLEAN" == true ]]; then
    check_script07_reports
else
    echo "Warning: REQUIRE_SCRIPT07_REPORTS_CLEAN=false; skipping automatic script 07 report checks." >&2
fi

remove_script07_backups


# -------------------------
# Collect 2D input folders
# -------------------------

# make_2d.py often creates one subfolder per T chunk under 2D_out.t.
# This script processes all immediate subfolders.
# If no subfolders exist, it falls back to using TWO_D_ROOT itself.

TMP_INPUT_LIST=$(mktemp)
trap 'rm -f "$TMP_INPUT_LIST"' EXIT

find "$TWO_D_ROOT" -mindepth 1 -maxdepth 1 -type d | sort -V > "$TMP_INPUT_LIST"

if [[ ! -s "$TMP_INPUT_LIST" ]]; then
    printf '%s\n' "$TWO_D_ROOT" > "$TMP_INPUT_LIST"
fi

input_count=$(wc -l < "$TMP_INPUT_LIST" | tr -d ' ')

echo
echo "Proteome ID: $PROTEOME_ID"
echo "2D input root: $TWO_D_ROOT"
echo "Signal output folder: $OUT_FOLDER"
echo "Number of 2D input folders to process: $input_count"
echo "Jobs: $JOBS"


# -------------------------
# Run son_spectral_final.py
# -------------------------

while IFS= read -r two_d_input; do
    [[ -n "$two_d_input" ]] || continue

    input_name=$(basename "$two_d_input")

    usable_file_count=$(find "$two_d_input" -mindepth 1 -maxdepth 1 -type f \
        ! -name '*.before_script07' \
        ! -name '*.tmp_script07' | wc -l | tr -d ' ')

    if [[ "$usable_file_count" -eq 0 ]]; then
        echo "Warning: no usable 2D files found in $two_d_input. Skipping." >&2
        continue
    fi

    echo
    echo "Running son_spectral_final.py:"
    echo "  Input name       : $input_name"
    echo "  Input folder     : $two_d_input"
    echo "  Usable file count: $usable_file_count"
    echo "  Output           : $OUT_FOLDER"
    echo "  Jobs             : $JOBS"

    python "$SON" \
        --o "$OUT_FOLDER" \
        -j "$JOBS" \
        "$two_d_input"

done < "$TMP_INPUT_LIST"


echo
echo "Script 08 finished."
echo "Signal files are in: $OUT_FOLDER"
