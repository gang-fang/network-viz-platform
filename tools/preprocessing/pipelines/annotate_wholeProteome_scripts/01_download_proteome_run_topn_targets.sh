#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 01_download_proteome_run_topn_targets.sh
#
# Purpose:
#   Download one complete UniProt proteome by proteome ID, simplify
#   UniProt FASTA headers to accession-only identifiers, and run topn
#   using the whole proteome FASTA as the query against every target
#   proteome/file in the local ProtDC reference database.
#
# Example:
#   bash 01_download_proteome_run_topn_targets.sh UP000000556
#
# Notes:
#   - This script is proteome-level, not single-query-level.
#   - The query FASTA may contain thousands of proteins.
#   - Override TOPN, DB, and WORK_ROOT for your local installation.
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# UniProt proteome ID. You may pass it as the first command-line argument.
PROTEOME_ID="${1:-UP000000556}"

# Override TOPN to use a custom topn executable.
TOPN="${TOPN:-${TOOLS_DIR}/bin/topn}"

# Edit this variable to the folder containing target proteome FASTA files.
DB="/protDC/data/Bacteria_Ref_Proteome/"

# Edit this variable to the root working directory for this analysis.
# A proteome-specific folder will be created under this root.
WORK_ROOT="/protDC/data/work"

# If true, download the proteome FASTA from UniProt when the raw FASTA is missing.
# If false, the script expects RAW_FASTA to already exist.
DOWNLOAD_IF_MISSING=true

# If true, overwrite the downloaded raw FASTA even if it already exists.
FORCE_DOWNLOAD=false

# Working paths derived from PROTEOME_ID.
WORK_DIR="${WORK_ROOT%/}/${PROTEOME_ID}"
RAW_FASTA="${WORK_DIR}/${PROTEOME_ID}.raw.fasta"
QUERY_FASTA="${WORK_DIR}/${PROTEOME_ID}.accession.fasta"
OUT_FOLDER="${WORK_DIR}/topN_out.q"

# UniProt REST stream URL for all UniProtKB entries assigned to this proteome.
UNIPROT_URL="https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=fasta&query=%28proteome%3A${PROTEOME_ID}%29"

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

mkdir -p "$WORK_DIR" "$OUT_FOLDER"

# -------------------------
# Download proteome FASTA from UniProt, if needed
# -------------------------

if [[ "$FORCE_DOWNLOAD" == true || ! -s "$RAW_FASTA" ]]; then
    if [[ "$DOWNLOAD_IF_MISSING" != true && "$FORCE_DOWNLOAD" != true ]]; then
        echo "Error: raw proteome FASTA does not exist and DOWNLOAD_IF_MISSING=false: $RAW_FASTA" >&2
        exit 1
    fi

    echo "Downloading UniProt proteome: $PROTEOME_ID"
    echo "Output raw FASTA: $RAW_FASTA"

    tmp_fasta="${RAW_FASTA}.tmp"
    rm -f "$tmp_fasta"

    if command -v curl >/dev/null 2>&1; then
        curl -L --fail --retry 3 --retry-delay 5 "$UNIPROT_URL" -o "$tmp_fasta"
    elif command -v wget >/dev/null 2>&1; then
        wget -O "$tmp_fasta" "$UNIPROT_URL"
    else
        echo "Error: neither curl nor wget was found. Install one of them or download the FASTA manually." >&2
        exit 1
    fi

    if ! grep -q '^>' "$tmp_fasta"; then
        echo "Error: downloaded file does not look like FASTA: $tmp_fasta" >&2
        echo "Check the proteome ID or UniProt connection." >&2
        exit 1
    fi

    mv "$tmp_fasta" "$RAW_FASTA"
else
    echo "Using existing raw FASTA: $RAW_FASTA"
fi

# -------------------------
# Normalize FASTA headers
# -------------------------

# UniProt headers look like:
#   >sp|P69905|HBA_HUMAN Hemoglobin subunit alpha ...
# This block writes accession-only headers:
#   >P69905
# For conventional FASTA headers, it keeps only the first token after '>'.
awk '
    /^>/ {
        header = substr($0, 2)
        split(header, fields, /[[:space:]]+/)
        token = fields[1]
        n = split(token, pipe_fields, "|")
        if (n >= 3) {
            id = pipe_fields[2]
        } else {
            id = token
        }
        print ">" id
        next
    }
    { print }
' "$RAW_FASTA" > "$QUERY_FASTA"

query_count=$(grep -c '^>' "$QUERY_FASTA" || true)
if [[ "$query_count" -eq 0 ]]; then
    echo "Error: no protein sequences were found in normalized FASTA: $QUERY_FASTA" >&2
    exit 1
fi

echo "Normalized query FASTA: $QUERY_FASTA"
echo "Number of query proteins: $query_count"


# -------------------------
# Run topn for the query proteome against itself
# -------------------------

# This self-comparison step adds the proteome itself as both the query and
# the target before comparing the query proteome against the reference DB.
# topn requires FASTA file paths, so both -q and -t use the normalized FASTA
# corresponding to PROTEOME_ID.
SELF_OUTPUT="${OUT_FOLDER%/}/${PROTEOME_ID}_${PROTEOME_ID}"

echo "Running topn self-comparison:"
echo "  Query proteome : $PROTEOME_ID"
echo "  Query FASTA    : $QUERY_FASTA"
echo "  Target FASTA   : $QUERY_FASTA"
echo "  Output         : $SELF_OUTPUT"

"$TOPN" \
    -q "$QUERY_FASTA" \
    -t "$QUERY_FASTA" \
    -o "$SELF_OUTPUT"

# -------------------------
# Run topn against each target file
# -------------------------

shopt -s nullglob
Targets=("$DB"/*)
if [[ ${#Targets[@]} -eq 0 ]]; then
    echo "Error: no target files were found under DB: $DB" >&2
    exit 1
fi

for target in "${Targets[@]}"; do
    [[ -f "$target" ]] || continue

    target_file=$(basename "$target")

    # Keep only the part before the first period, then remove underscores.
    target_name="${target_file%%.*}"
    target_name="${target_name//_/}"

    output="${OUT_FOLDER%/}/${PROTEOME_ID}_${target_name}"

    echo "Running topn:"
    echo "  Query proteome : $PROTEOME_ID"
    echo "  Query FASTA    : $QUERY_FASTA"
    echo "  Target         : $target"
    echo "  Output         : $output"

    "$TOPN" \
        -q "$QUERY_FASTA" \
        -t "$target" \
        -o "$output"

done

echo "topn finished for proteome $PROTEOME_ID."
echo "topn output folder: $OUT_FOLDER"
