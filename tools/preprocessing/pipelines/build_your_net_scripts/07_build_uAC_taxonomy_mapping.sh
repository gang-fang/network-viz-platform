#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 07_build_uAC_taxonomy_mapping.sh
#
# Purpose:
#   1. Run upid_to_taxid.py to map UniProt proteome IDs (UPIDs)
#      to NCBI taxonomy IDs.
#
#   2. Also generate:
#        NCBI_txID.csv
#        commontree.txt
#        warnings_and_not_found.txt
#
#   3. Read all downloaded UniProt reference proteome FASTA files
#      from script 01.
#
#   4. Extract every UniProt accession from each proteome FASTA.
#
#   5. Create a protein-level mapping file:
#        UniProt_accession,NCBI_taxonomy_ID
#
# Input files:
#   $WORK_ROOT/lists/proteome_ids.clean
#   $WORK_ROOT/proteomes/<UPID>/<UPID>.fasta
#
# Output files:
#   $WORK_ROOT/lists/upid_to_taxid.csv
#   $WORK_ROOT/lists/NCBI_txID.csv
#   $WORK_ROOT/lists/commontree.txt
#   $WORK_ROOT/lists/warnings_and_not_found.txt
#   $WORK_ROOT/lists/uAC_taxonomy_mapping
#
# Example:
#   bash 07_build_uAC_taxonomy_mapping.sh
#
# Output format of uAC_taxonomy_mapping:
#   K8E1K5,1234679
#   K8E1Y0,1234679
#   K8E1Y8,1234679
#
# Notes:
#   - upid_to_taxid.py expects one UniProt proteome ID per line.
#   - script 01 writes the cleaned proteome list to:
#       $WORK_ROOT/lists/proteome_ids.clean
#   - script 01 writes normalized accession-only FASTA files to:
#       $WORK_ROOT/proteomes/<UPID>/<UPID>.fasta
# ============================================================


# -------------------------
# Editable variables
# -------------------------

# Edit this variable to point to upid_to_taxid.py.
UPID_TO_TAXID="/protDC/Scripts/upid_to_taxid.py"

# Optional Python environment activation.
# Leave VENV_ACT empty if no Python environment needs to be activated.
# Examples:
#   VENV_ACT="/protDC/venv/bin/activate"
#   VENV_ACT="/protDC/miniconda3/envs/protDC/bin/activate"
VENV_ACT=""

# Edit this variable to the main working folder used in scripts 01-06.
WORK_ROOT="/protDC/data/work/all_proteomes"

# Python executable after optional environment activation.
PYTHON_BIN="python"

# UniProt query settings for upid_to_taxid.py.
BATCH_SIZE=100
SLEEP_SECONDS=0.2

# NCBI taxonomy-query settings used by upid_to_taxid.py when generating commontree.txt.
NCBI_BATCH_SIZE=200

# Leave empty unless you want to provide these to NCBI E-utilities.
NCBI_EMAIL=""
NCBI_API_KEY=""

# If true, skip NCBI Common Tree generation.
# This avoids NCBI E-utilities calls and skips commontree.txt generation.
SKIP_COMMON_TREE=false


# -------------------------
# Derived paths
# -------------------------

LIST_DIR="${WORK_ROOT%/}/lists"
PROTEOME_LIST="${LIST_DIR}/proteome_ids.clean"

UPID_TAXID_CSV="${LIST_DIR}/upid_to_taxid.csv"
NCBI_TXID_CSV="${LIST_DIR}/NCBI_txID.csv"
COMMON_TREE_TXT="${LIST_DIR}/commontree.txt"
DIAGNOSTICS_TXT="${LIST_DIR}/warnings_and_not_found.txt"

UAC_TAXID_MAPPING="${LIST_DIR}/uAC_taxonomy_mapping"

PROTEOME_FASTA_ROOT="${WORK_ROOT%/}/proteomes"

REPORT_DIR="${WORK_ROOT%/}/script07_reports"
MISSING_FASTA_REPORT="${REPORT_DIR}/missing_proteome_fasta.txt"
MISSING_TAXID_REPORT="${REPORT_DIR}/missing_or_invalid_taxid.txt"
DUPLICATE_UAC_REPORT="${REPORT_DIR}/duplicate_uAC_assignments.tsv"


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


# -------------------------
# Basic checks
# -------------------------

if [[ ! -f "$UPID_TO_TAXID" ]]; then
    echo "Error: upid_to_taxid.py was not found: $UPID_TO_TAXID" >&2
    exit 1
fi

if [[ ! -s "$PROTEOME_LIST" ]]; then
    echo "Error: proteome ID list was not found or is empty: $PROTEOME_LIST" >&2
    echo "Please run script 01 first." >&2
    exit 1
fi

if [[ ! -d "$PROTEOME_FASTA_ROOT" ]]; then
    echo "Error: proteome FASTA root was not found: $PROTEOME_FASTA_ROOT" >&2
    echo "Please run script 01 first." >&2
    exit 1
fi

mkdir -p "$LIST_DIR" "$REPORT_DIR"


# -------------------------
# Activate Python environment, if needed
# -------------------------

activate_python_env

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "Error: $PYTHON_BIN was not found in PATH." >&2
    echo "Please activate the correct Python environment or set VENV_ACT." >&2
    exit 1
fi


# -------------------------
# Step 1: Generate UPID-to-taxID mapping and taxonomy reports
# -------------------------

echo "Running upid_to_taxid.py:"
echo "  Script                  : $UPID_TO_TAXID"
echo "  Input UPIDs             : $PROTEOME_LIST"
echo "  UPID-taxID output       : $UPID_TAXID_CSV"
echo "  NCBI taxID/species file : $NCBI_TXID_CSV"
echo "  Common taxonomy tree    : $COMMON_TREE_TXT"
echo "  Warnings/not-found file : $DIAGNOSTICS_TXT"
echo "  Batch size              : $BATCH_SIZE"
echo "  Sleep seconds           : $SLEEP_SECONDS"
echo "  Skip common tree        : $SKIP_COMMON_TREE"

cmd=(
    "$PYTHON_BIN" "$UPID_TO_TAXID"
    "$PROTEOME_LIST"
    "$UPID_TAXID_CSV"
    --batch-size "$BATCH_SIZE"
    --sleep "$SLEEP_SECONDS"
    --ncbi-output "$NCBI_TXID_CSV"
    --common-tree-output "$COMMON_TREE_TXT"
    --diagnostics-output "$DIAGNOSTICS_TXT"
    --ncbi-batch-size "$NCBI_BATCH_SIZE"
)

if [[ -n "$NCBI_EMAIL" ]]; then
    cmd+=(--ncbi-email "$NCBI_EMAIL")
fi

if [[ -n "$NCBI_API_KEY" ]]; then
    cmd+=(--ncbi-api-key "$NCBI_API_KEY")
fi

if [[ "$SKIP_COMMON_TREE" == true ]]; then
    cmd+=(--skip-common-tree)
fi

"${cmd[@]}"

if [[ ! -s "$UPID_TAXID_CSV" ]]; then
    echo "Error: UPID-to-taxID mapping was not created or is empty: $UPID_TAXID_CSV" >&2
    exit 1
fi

if [[ ! -s "$NCBI_TXID_CSV" ]]; then
    echo "Warning: NCBI_txID.csv was not created or is empty: $NCBI_TXID_CSV" >&2
fi

if [[ "$SKIP_COMMON_TREE" != true && ! -s "$COMMON_TREE_TXT" ]]; then
    echo "Warning: commontree.txt was not created or is empty: $COMMON_TREE_TXT" >&2
fi

if [[ ! -f "$DIAGNOSTICS_TXT" ]]; then
    echo "Warning: diagnostics file was not created: $DIAGNOSTICS_TXT" >&2
fi


# -------------------------
# Step 2: Build UniProt AC-to-taxID mapping
# -------------------------

echo
echo "Building protein-level UniProt accession to taxonomy mapping:"
echo "  Proteome FASTA root : $PROTEOME_FASTA_ROOT"
echo "  UPID-taxID mapping  : $UPID_TAXID_CSV"
echo "  Output file         : $UAC_TAXID_MAPPING"

"$PYTHON_BIN" - \
    "$PROTEOME_LIST" \
    "$UPID_TAXID_CSV" \
    "$PROTEOME_FASTA_ROOT" \
    "$UAC_TAXID_MAPPING" \
    "$MISSING_FASTA_REPORT" \
    "$MISSING_TAXID_REPORT" \
    "$DUPLICATE_UAC_REPORT" <<'PY'
import csv
import sys
from pathlib import Path
from collections import OrderedDict, defaultdict

(
    proteome_list_path,
    upid_taxid_csv_path,
    proteome_fasta_root,
    output_mapping_path,
    missing_fasta_report,
    missing_taxid_report,
    duplicate_uac_report,
) = sys.argv[1:8]

proteome_fasta_root = Path(proteome_fasta_root)
output_mapping_path = Path(output_mapping_path)
missing_fasta_report = Path(missing_fasta_report)
missing_taxid_report = Path(missing_taxid_report)
duplicate_uac_report = Path(duplicate_uac_report)


def read_proteome_ids(path):
    ids = []
    seen = set()
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip()
            if not value or value.startswith("#"):
                continue
            upid = value.split()[0].strip().upper()
            if upid and upid not in seen:
                ids.append(upid)
                seen.add(upid)
    return ids


def read_upid_taxid(path):
    mapping = {}
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for row_no, row in enumerate(reader, start=1):
            if not row or len(row) < 2:
                continue
            upid = row[0].strip().upper()
            taxid = row[1].strip()

            # Skip header from upid_to_taxid.py.
            if row_no == 1 and upid.lower() == "upid" and taxid.lower() == "taxid":
                continue

            mapping[upid] = taxid
    return mapping


def accession_from_fasta_header(header_line):
    """Extract UniProt accession from a FASTA header.

    Supports:
      >sp|P69905|HBA_HUMAN description
      >tr|A0A000|A0A000_9BACT description
      >P69905
      >P69905 description
    """
    header = header_line[1:].strip()
    if not header:
        return ""

    first_token = header.split()[0]
    parts = first_token.split("|")

    if len(parts) >= 3:
        return parts[1].strip()

    return first_token.strip()


def fasta_candidates(upid):
    """Return likely FASTA paths for one UPID.

    script 01 should create <UPID>.fasta. The fallbacks make this script
    robust if users keep only raw FASTA or manually downloaded FASTA files.
    """
    pdir = proteome_fasta_root / upid
    return [
        pdir / f"{upid}.fasta",
        pdir / f"{upid}.accession.fasta",
        pdir / f"{upid}.raw.fasta",
    ]


proteome_ids = read_proteome_ids(proteome_list_path)
upid_to_taxid = read_upid_taxid(upid_taxid_csv_path)

missing_fastas = []
missing_taxids = []
rows = []
uac_to_taxids = defaultdict(set)

for upid in proteome_ids:
    taxid = upid_to_taxid.get(upid, "")

    if not taxid or taxid in {"NOT_FOUND", "ERROR"}:
        missing_taxids.append((upid, taxid or "MISSING"))
        continue

    fasta_path = None
    for candidate in fasta_candidates(upid):
        if candidate.is_file() and candidate.stat().st_size > 0:
            fasta_path = candidate
            break

    if fasta_path is None:
        missing_fastas.append(upid)
        continue

    with open(fasta_path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.startswith(">"):
                continue
            accession = accession_from_fasta_header(line)
            if not accession:
                continue

            rows.append((accession, taxid))
            uac_to_taxids[accession].add(taxid)

# Preserve first-seen order, but remove exact duplicate rows.
unique_rows = OrderedDict()
for accession, taxid in rows:
    unique_rows[(accession, taxid)] = None

with open(output_mapping_path, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, lineterminator="\n")
    for accession, taxid in unique_rows:
        writer.writerow([accession, taxid])

with open(missing_fasta_report, "w", encoding="utf-8") as out:
    for upid in missing_fastas:
        out.write(f"{upid}\n")

with open(missing_taxid_report, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    writer.writerow(["upid", "taxid_status"])
    for upid, status in missing_taxids:
        writer.writerow([upid, status])

duplicate_assignments = {
    accession: sorted(taxids)
    for accession, taxids in uac_to_taxids.items()
    if len(taxids) > 1
}

with open(duplicate_uac_report, "w", encoding="utf-8", newline="") as out:
    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    writer.writerow(["uniprot_accession", "taxids"])
    for accession in sorted(duplicate_assignments):
        writer.writerow([accession, ";".join(duplicate_assignments[accession])])

print("SUMMARY")
print(f"  Proteomes in list:              {len(proteome_ids):,}")
print(f"  UPID-taxID entries:             {len(upid_to_taxid):,}")
print(f"  Raw protein-taxID rows:         {len(rows):,}")
print(f"  Unique protein-taxID rows:      {len(unique_rows):,}")
print(f"  Proteomes missing FASTA:        {len(missing_fastas):,}")
print(f"  Proteomes missing valid taxID:  {len(missing_taxids):,}")
print(f"  Accessions with >1 taxID:       {len(duplicate_assignments):,}")
print(f"  Output:                         {output_mapping_path}")
print(f"  Missing FASTA report:           {missing_fasta_report}")
print(f"  Missing taxID report:           {missing_taxid_report}")
print(f"  Duplicate accession report:     {duplicate_uac_report}")
PY

if [[ ! -s "$UAC_TAXID_MAPPING" ]]; then
    echo "Error: protein-level mapping was not created or is empty: $UAC_TAXID_MAPPING" >&2
    exit 1
fi

row_count=$(wc -l < "$UAC_TAXID_MAPPING" | tr -d ' ')

echo
echo "Script 07 finished."
echo "UPID-taxID mapping       : $UPID_TAXID_CSV"
echo "NCBI taxID/species file  : $NCBI_TXID_CSV"
echo "Common taxonomy tree     : $COMMON_TREE_TXT"
echo "Warnings/not-found file  : $DIAGNOSTICS_TXT"
echo "uAC-taxonomy mapping     : $UAC_TAXID_MAPPING"
echo "uAC-taxonomy row count   : $row_count"
echo "Reports folder           : $REPORT_DIR"
