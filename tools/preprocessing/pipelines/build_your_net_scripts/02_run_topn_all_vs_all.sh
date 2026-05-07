#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 02_run_topn_all_vs_all.sh
#
# Purpose:
#   Run all-against-all topN comparisons between every pair of
#   downloaded proteomes, including self-to-self comparisons.
#
# Input:
#   $WORK_ROOT/lists/proteome_ids.clean
#   $WORK_ROOT/proteomes/<PROTEOME_ID>/<PROTEOME_ID>.fasta
#
# Output:
#   $WORK_ROOT/topN_out/<QUERY_PROTEOME>_<TARGET_PROTEOME>
#
# Example:
#   bash 02_run_topn_all_vs_all.sh
#
# Note:
#   topn is a precompiled C++ program here. No Python environment
#   activation is used in this script.
# ============================================================

# -------------------------
# Editable variables
# -------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Override TOPN to use a custom topn executable.
TOPN="${TOPN:-${TOOLS_DIR}/bin/topn}"
WORK_ROOT="/protDC/data/work/all_proteomes"

# Suggested values:
#   TOPN_N=20 for bacteria
#   TOPN_N=50 for eukaryotes
TOPN_N=20

# The requested workflow uses -n. If your topn uses uppercase -N,
# change this variable to TOPN_N_FLAG="-N".
TOPN_N_FLAG="-n"

OVERWRITE_OUTPUT=true

# -------------------------
# Derived paths
# -------------------------

PROTEOME_LIST="${WORK_ROOT%/}/lists/proteome_ids.clean"
PROTEOME_FASTA_ROOT="${WORK_ROOT%/}/proteomes"
OUT_FOLDER="${WORK_ROOT%/}/topN_out"

# -------------------------
# Basic checks
# -------------------------

if [[ ! -x "$TOPN" ]]; then
    echo "Error: topn is not executable or was not found: $TOPN" >&2
    exit 1
fi

if [[ ! -s "$PROTEOME_LIST" ]]; then
    echo "Error: clean proteome list was not found or is empty: $PROTEOME_LIST" >&2
    echo "Please run script 01 first." >&2
    exit 1
fi

if [[ ! -d "$PROTEOME_FASTA_ROOT" ]]; then
    echo "Error: proteome FASTA root was not found: $PROTEOME_FASTA_ROOT" >&2
    echo "Please run script 01 first." >&2
    exit 1
fi

mkdir -p "$OUT_FOLDER"

proteome_count=$(wc -l < "$PROTEOME_LIST" | tr -d ' ')
echo "Proteomes to compare: $proteome_count"
echo "topn output folder: $OUT_FOLDER"
echo "topn candidate option: $TOPN_N_FLAG $TOPN_N"

# Validate input FASTA files.
while IFS= read -r proteome_id; do
    [[ -n "$proteome_id" ]] || continue

    if [[ "$proteome_id" == *_* ]]; then
        echo "Error: proteome ID contains an underscore: $proteome_id" >&2
        echo "Underscores are reserved for query_target output names." >&2
        exit 1
    fi

    fasta="${PROTEOME_FASTA_ROOT}/${proteome_id}/${proteome_id}.fasta"
    if [[ ! -s "$fasta" ]]; then
        echo "Error: FASTA file was not found or is empty: $fasta" >&2
        echo "Please run script 01 first." >&2
        exit 1
    fi
done < "$PROTEOME_LIST"

# -------------------------
# Run all-against-all topn comparisons
# -------------------------

pair_count=0

while IFS= read -r query_id; do
    [[ -n "$query_id" ]] || continue
    query_fasta="${PROTEOME_FASTA_ROOT}/${query_id}/${query_id}.fasta"

    while IFS= read -r target_id; do
        [[ -n "$target_id" ]] || continue
        target_fasta="${PROTEOME_FASTA_ROOT}/${target_id}/${target_id}.fasta"
        output="${OUT_FOLDER%/}/${query_id}_${target_id}"
        pair_count=$((pair_count + 1))

        if [[ -s "$output" && "$OVERWRITE_OUTPUT" != true ]]; then
            echo "Skipping existing output: $output"
            continue
        fi

        echo
        echo "Running topn pair ${pair_count}:"
        echo "  Query proteome : $query_id"
        echo "  Target proteome: $target_id"
        echo "  Output         : $output"
        echo "  topn candidates: $TOPN_N_FLAG $TOPN_N"

        "$TOPN" \
            -q "$query_fasta" \
            -t "$target_fasta" \
            -o "$output" \
            "$TOPN_N_FLAG" "$TOPN_N"

    done < "$PROTEOME_LIST"

done < "$PROTEOME_LIST"

echo
echo "Script 02 finished."
echo "Total topn pairs processed: $pair_count"
echo "topn output folder: $OUT_FOLDER"
