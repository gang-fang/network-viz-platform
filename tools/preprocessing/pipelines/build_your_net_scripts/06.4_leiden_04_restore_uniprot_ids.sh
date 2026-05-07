#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 6.4_leiden_04_restore_uniprot_ids.sh
#
# Purpose:
#   Restore integer protein IDs in networks/partition.csv back to
#   UniProt accession codes using protein_id_mapping.csv from script 6.1.
#
# Equivalent command from inside $WORK_ROOT/Leiden:
#   python ../../leiden_04_restore_uniprot_ids.py \
#       --partition networks/partition.csv \
#       --mapping protein_id_mapping.csv
#
# Input:
#   $WORK_ROOT/Leiden/networks/partition.csv
#   $WORK_ROOT/Leiden/protein_id_mapping.csv
#
# Output:
#   $WORK_ROOT/Leiden/restored_partition.csv
#
# Example:
#   bash 6.4_leiden_04_restore_uniprot_ids.sh
# ============================================================

LEIDEN_04="/protDC/Scripts/leiden_04_restore_uniprot_ids.py"

VENV_ACT=""

WORK_ROOT="/protDC/data/work/all_proteomes"

LEIDEN_ROOT="${WORK_ROOT%/}/Leiden"
NETWORKS_DIR="${LEIDEN_ROOT}/networks"
PARTITION_REL="networks/partition.csv"
MAPPING_REL="protein_id_mapping.csv"
OUT_REL="restored_partition.csv"

PARTITION_ABS="${NETWORKS_DIR}/partition.csv"
MAPPING_ABS="${LEIDEN_ROOT}/protein_id_mapping.csv"
OUT_ABS="${LEIDEN_ROOT}/restored_partition.csv"

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

if [[ ! -f "$LEIDEN_04" ]]; then
    echo "Error: leiden_04_restore_uniprot_ids.py was not found: $LEIDEN_04" >&2
    exit 1
fi

if [[ ! -s "$PARTITION_ABS" ]]; then
    echo "Error: partition.csv was not found or is empty: $PARTITION_ABS" >&2
    echo "Please run scripts 6.2 and 6.3 first." >&2
    exit 1
fi

if [[ ! -s "$MAPPING_ABS" ]]; then
    echo "Error: protein_id_mapping.csv was not found or is empty: $MAPPING_ABS" >&2
    echo "Please run script 6.1 first." >&2
    exit 1
fi

activate_python_env

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi

cd "$LEIDEN_ROOT"

echo "Restoring UniProt IDs:"
echo "  Script     : $LEIDEN_04"
echo "  Partition  : $PARTITION_ABS"
echo "  Mapping    : $MAPPING_ABS"
echo "  Output     : $OUT_ABS"

python "$LEIDEN_04"     --partition "$PARTITION_REL"     --mapping "$MAPPING_REL"     --out "$OUT_REL"

if [[ ! -s "$OUT_ABS" ]]; then
    echo "Error: restored partition was not created or is empty: $OUT_ABS" >&2
    exit 1
fi

echo
echo "Script 6.4 finished."
echo "Restored partition: $OUT_ABS"
