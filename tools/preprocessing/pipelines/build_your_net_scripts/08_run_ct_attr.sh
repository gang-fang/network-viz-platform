#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 08_run_ct_attr.sh
#
# Purpose:
#   Run ct_attr_v1.py to create the final node-attribute file
#   for network visualization.
#
# Inputs:
#   From Leiden workflow:
#     $WORK_ROOT/Leiden/restored_partition.csv
#
#   From script 07:
#     $WORK_ROOT/lists/uAC_taxonomy_mapping
#
# Output:
#   $WORK_ROOT/nodes_attr/<PREFIX>.nodes.attr
#
# Example:
#   bash 08_run_ct_attr.sh
#
# Output format:
#   node_id,NCBI_txID,NH_ID,NH_Size,,,,,
#
# Notes:
#   ct_attr_v1.py requires:
#     --clusters  restored partition file with header: uniprotAC,cluster_id
#     --mapping   UniProt AC to NCBI taxonomy ID mapping, no header
#     --prefix    1-3 letters
#     --out       output attribute file
# ============================================================


# -------------------------
# Editable variables
# -------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Override CT_ATTR to use a custom ct_attr_v1.py.
CT_ATTR="${CT_ATTR:-${TOOLS_DIR}/preprocessing/ct_attr_v1.py}"

# Optional Python environment activation.
# Leave VENV_ACT empty if no Python environment needs to be activated.
# Examples:
#   VENV_ACT="/protDC/venv/bin/activate"
#   VENV_ACT="/protDC/miniconda3/envs/protDC/bin/activate"
VENV_ACT=""

# Edit this variable to the main working folder used in scripts 01-07.
WORK_ROOT="/protDC/data/work/all_proteomes"

# Three-letter abbreviation used as the NH_ID prefix and output file name.
# ct_attr_v1.py accepts 1-3 letters only.
#
# Examples:
#   ATTR_PREFIX="bac"
#   ATTR_PREFIX="euk"
#   ATTR_PREFIX="all"
ATTR_PREFIX="all"

# If true, ct_attr_v1.py will write an empty NCBI_txID instead of failing
# when a UniProt accession is missing from uAC_taxonomy_mapping.
#
# Recommended default:
#   ALLOW_MISSING_TAXID=false
ALLOW_MISSING_TAXID=false


# -------------------------
# Derived paths
# -------------------------

# restored_partition.csv is produced by Leiden script 6.4.
# If this script is stored under a scripts/ folder, this corresponds conceptually
# to ../Leiden/restored_partition.csv relative to the working analysis area.
RESTORED_PARTITION="${WORK_ROOT%/}/Leiden/restored_partition.csv"

# uAC_taxonomy_mapping is produced by script 07.
UAC_TAXID_MAPPING="${WORK_ROOT%/}/lists/uAC_taxonomy_mapping"

OUT_DIR="${WORK_ROOT%/}/nodes_attr"
OUT_FILE="${OUT_DIR}/${ATTR_PREFIX}.nodes.attr"


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

    # This function is called with no arguments, so command-line parameters
    # passed to the main script are not visible as $1 inside the sourced
    # activation script.
    # shellcheck disable=SC1090
    source "$VENV_ACT"
}

validate_prefix_in_bash() {
    local prefix="$1"

    if [[ -z "$prefix" ]]; then
        echo "Error: ATTR_PREFIX cannot be empty." >&2
        exit 1
    fi

    if [[ ! "$prefix" =~ ^[A-Za-z]{1,3}$ ]]; then
        echo "Error: ATTR_PREFIX must contain letters only and must be 1-3 letters long." >&2
        echo "Current value: $prefix" >&2
        exit 1
    fi
}


# -------------------------
# Basic checks
# -------------------------

validate_prefix_in_bash "$ATTR_PREFIX"

if [[ ! -f "$CT_ATTR" ]]; then
    echo "Error: ct_attr_v1.py was not found: $CT_ATTR" >&2
    exit 1
fi

if [[ ! -s "$RESTORED_PARTITION" ]]; then
    echo "Error: restored partition file was not found or is empty: $RESTORED_PARTITION" >&2
    echo "Please run Leiden script 6.4 first." >&2
    exit 1
fi

if [[ ! -s "$UAC_TAXID_MAPPING" ]]; then
    echo "Error: uAC_taxonomy_mapping was not found or is empty: $UAC_TAXID_MAPPING" >&2
    echo "Please run script 07 first." >&2
    exit 1
fi

mkdir -p "$OUT_DIR"


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
# Run ct_attr_v1.py
# -------------------------

cmd=(
    python "$CT_ATTR"
    --clusters "$RESTORED_PARTITION"
    --mapping "$UAC_TAXID_MAPPING"
    --prefix "$ATTR_PREFIX"
    --out "$OUT_FILE"
)

if [[ "$ALLOW_MISSING_TAXID" == true ]]; then
    cmd+=(--allow-missing-taxid)
fi

echo "Running ct_attr_v1.py:"
echo "  Script              : $CT_ATTR"
echo "  Restored partition  : $RESTORED_PARTITION"
echo "  uAC-taxID mapping   : $UAC_TAXID_MAPPING"
echo "  Prefix              : $ATTR_PREFIX"
echo "  Output              : $OUT_FILE"
echo "  Allow missing taxID : $ALLOW_MISSING_TAXID"

"${cmd[@]}"

if [[ ! -s "$OUT_FILE" ]]; then
    echo "Error: node attribute output was not created or is empty: $OUT_FILE" >&2
    exit 1
fi

row_count=$(wc -l < "$OUT_FILE" | tr -d ' ')

echo
echo "Script 08 finished."
echo "Node attribute file: $OUT_FILE"
echo "Rows including header: $row_count"
