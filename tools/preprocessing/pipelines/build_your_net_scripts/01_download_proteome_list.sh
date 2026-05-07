#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 01_download_proteome_list.sh
#
# Purpose:
#   Download a list of UniProt proteomes and save each proteome
#   FASTA into a designated folder.
#
# Input:
#   A plain-text file containing one UniProt proteome ID per row.
#
# Example:
#   bash 01_download_proteome_list.sh proteome_ids.txt
#
# Example proteome_ids.txt:
#   UP000000556
#   UP000005640
#
# Output layout:
#   $WORK_ROOT/proteomes/UP000000556/UP000000556.raw.fasta
#   $WORK_ROOT/proteomes/UP000000556/UP000000556.fasta
#
# Important:
#   Proteome FASTA file names should not contain underscores because
#   script 02 uses an underscore to separate query and target proteome
#   names in topN output files.
# ============================================================

PROTEOME_LIST="${1:-proteome_ids.txt}"

# -------------------------
# Editable variables
# -------------------------

WORK_ROOT="/protDC/data/work/all_proteomes"
DOWNLOAD_IF_MISSING=true
FORCE_DOWNLOAD=false

# -------------------------
# Derived paths
# -------------------------

PROTEOME_FASTA_ROOT="${WORK_ROOT%/}/proteomes"
LIST_DIR="${WORK_ROOT%/}/lists"
CLEAN_PROTEOME_LIST="${LIST_DIR}/proteome_ids.clean"

# -------------------------
# Basic checks
# -------------------------

if [[ ! -f "$PROTEOME_LIST" ]]; then
    echo "Error: proteome ID list was not found: $PROTEOME_LIST" >&2
    exit 1
fi

if ! command -v awk >/dev/null 2>&1; then
    echo "Error: awk was not found in PATH." >&2
    exit 1
fi

if ! command -v grep >/dev/null 2>&1; then
    echo "Error: grep was not found in PATH." >&2
    exit 1
fi

if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
    echo "Error: neither curl nor wget was found in PATH." >&2
    exit 1
fi

mkdir -p "$PROTEOME_FASTA_ROOT" "$LIST_DIR"

# -------------------------
# Clean proteome ID list
# -------------------------

awk '
    NF == 0 { next }
    $1 ~ /^#/ { next }
    {
        gsub(/\r/, "", $1)
        print $1
    }
' "$PROTEOME_LIST" | sort -u > "$CLEAN_PROTEOME_LIST"

proteome_count=$(wc -l < "$CLEAN_PROTEOME_LIST" | tr -d ' ')

if [[ "$proteome_count" -eq 0 ]]; then
    echo "Error: no proteome IDs were found in: $PROTEOME_LIST" >&2
    exit 1
fi

echo "Clean proteome ID list: $CLEAN_PROTEOME_LIST"
echo "Number of proteomes: $proteome_count"

# -------------------------
# Download and normalize each proteome
# -------------------------

while IFS= read -r proteome_id; do
    [[ -n "$proteome_id" ]] || continue

    if [[ "$proteome_id" == *_* ]]; then
        echo "Error: proteome ID contains an underscore: $proteome_id" >&2
        echo "Underscores are reserved for query_target output names in script 02." >&2
        exit 1
    fi

    proteome_dir="${PROTEOME_FASTA_ROOT}/${proteome_id}"
    raw_fasta="${proteome_dir}/${proteome_id}.raw.fasta"
    clean_fasta="${proteome_dir}/${proteome_id}.fasta"
    uniprot_url="https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=fasta&query=%28proteome%3A${proteome_id}%29"

    mkdir -p "$proteome_dir"

    echo
    echo "Processing proteome: $proteome_id"
    echo "  Directory   : $proteome_dir"
    echo "  Raw FASTA   : $raw_fasta"
    echo "  Clean FASTA : $clean_fasta"

    if [[ "$FORCE_DOWNLOAD" == true || ! -s "$raw_fasta" ]]; then
        if [[ "$DOWNLOAD_IF_MISSING" != true && "$FORCE_DOWNLOAD" != true ]]; then
            echo "Error: raw FASTA is missing and DOWNLOAD_IF_MISSING=false: $raw_fasta" >&2
            exit 1
        fi

        tmp_fasta="${raw_fasta}.tmp"
        rm -f "$tmp_fasta"

        echo "  Downloading from UniProt"

        if command -v curl >/dev/null 2>&1; then
            curl -L --fail --retry 3 --retry-delay 5 "$uniprot_url" -o "$tmp_fasta"
        else
            wget -O "$tmp_fasta" "$uniprot_url"
        fi

        if ! grep -q '^>' "$tmp_fasta"; then
            echo "Error: downloaded file does not look like FASTA: $tmp_fasta" >&2
            echo "Check the proteome ID or UniProt connection." >&2
            exit 1
        fi

        mv "$tmp_fasta" "$raw_fasta"
    else
        echo "  Using existing raw FASTA"
    fi

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
    ' "$raw_fasta" > "$clean_fasta"

    seq_count=$(grep -c '^>' "$clean_fasta" || true)
    if [[ "$seq_count" -eq 0 ]]; then
        echo "Error: no sequences were found after FASTA normalization: $clean_fasta" >&2
        exit 1
    fi

    echo "  Sequences: $seq_count"

done < "$CLEAN_PROTEOME_LIST"

echo
echo "Script 01 finished."
echo "Proteome FASTA root: $PROTEOME_FASTA_ROOT"
echo "Clean proteome list : $CLEAN_PROTEOME_LIST"
