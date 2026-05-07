#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 02_run_make_2d_proteome.sh
#
# Purpose:
#   Run make_2d.py on the topN output generated from a complete
#   UniProt proteome. The input is the whole topN_out.q folder from
#   script 01.
#
# Example:
#   bash 02_run_make_2d_proteome.sh UP000000556
# ============================================================

# UniProt proteome ID. Use the same ID used in script 01.
PROTEOME_ID="${1:-UP000000556}"

# Edit this variable to point to the location where make_2d.py is stored.
MAKE_2D="/protDC/Scripts/make_2d.py"

# N is the number of homolog candidates selected from each target proteome.
# Suggested values: N=50 for eukaryotes and N=20 for bacteria.
N=20

# Optional Python environment activation.
# Leave VENV_ACT empty if no Python environment needs to be activated.
# Set VENV_ACT to the activation script of the exact environment you want to use.
# Examples:
#   VENV_ACT="/protDC/venv/bin/activate"
#   VENV_ACT="/protDC/miniconda3/envs/protDC/bin/activate"
VENV_ACT=""

# Edit this variable to the root working directory used in script 01.
WORK_ROOT="/protDC/data/work"

# Working paths derived from PROTEOME_ID.
WORK_DIR="${WORK_ROOT%/}/${PROTEOME_ID}"
TOPN_OUT="${WORK_DIR}/topN_out.q"
OUT_FOLDER="${WORK_DIR}/2D_out.q"

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

# -------------------------
# Basic checks
# -------------------------

if [[ ! -f "$MAKE_2D" ]]; then
    echo "Error: make_2d.py was not found: $MAKE_2D" >&2
    exit 1
fi

if [[ ! -d "$TOPN_OUT" ]]; then
    echo "Error: topN output folder was not found: $TOPN_OUT" >&2
    echo "Run script 01 first, or edit TOPN_OUT." >&2
    exit 1
fi

mkdir -p "$OUT_FOLDER"

# -------------------------
# Activate Python environment, if needed
# -------------------------

activate_python_env

# -------------------------
# Run make_2d.py
# -------------------------

python "$MAKE_2D" \
    --o "$OUT_FOLDER" \
    -n "$N" \
    "$TOPN_OUT"

echo "make_2d.py finished for proteome $PROTEOME_ID."
echo "2D output folder: $OUT_FOLDER"
