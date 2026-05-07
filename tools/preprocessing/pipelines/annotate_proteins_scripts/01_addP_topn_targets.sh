#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Override TOPN to use a custom topn executable.
TOPN="${TOPN:-${TOOLS_DIR}/bin/topn}"

# Edit this variable to the folder containing the downloaded UniProt reference proteomes.
# Check the ProtDC help document for the full list of species proteomes included in the 
# package, and download the relevant proteomes as needed.
DB="/protDC/data/Bacteria_Ref_Proteome/"

# Edit this variable to the query FASTA file.
# The query file should contain the proteins of interest in FASTA format.
QUERY="/protDC/data/work/test/query.fasta"

# Edit this variable to the output folder.
OUT_FOLDER="/protDC/data/work/test/topN_out/"

# -------------------------
# Basic checks
# -------------------------

if [[ ! -x "$TOPN" ]]; then
    echo "Error: topn is not executable or was not found: $TOPN" >&2
    exit 1
fi

if [[ ! -d "$DB" ]]; then
    echo "Error: target database folder was not found: $DB" >&2
    exit 1
fi

if [[ ! -f "$QUERY" ]]; then
    echo "Error: query FASTA file was not found: $QUERY" >&2
    exit 1
fi

mkdir -p "$OUT_FOLDER"

# -------------------------
# Generate query name
# -------------------------

query_file=$(basename "$QUERY")

# Keep only the part before the first period, then remove underscores.
query_name="${query_file%%.*}"
query_name="${query_name//_/}"

# -------------------------
# Run topn against each target file
# -------------------------

for target in "$DB"/*; do
    # Skip if there are no files, or if the item is not a regular file.
    [[ -f "$target" ]] || continue

    target_file=$(basename "$target")

    # Keep only the part before the first period, then remove underscores.
    target_name="${target_file%%.*}"
    target_name="${target_name//_/}"

    output="${OUT_FOLDER%/}/${query_name}_${target_name}"

    echo "Running:"
    echo "  Query : $QUERY"
    echo "  Target: $target"
    echo "  Output: $output"

    "$TOPN" \
        -q "$QUERY" \
        -t "$target" \
        -o "$output"
done

echo "All jobs finished."
