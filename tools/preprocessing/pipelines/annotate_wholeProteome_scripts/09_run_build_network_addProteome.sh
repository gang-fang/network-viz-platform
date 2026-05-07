#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 09_run_build_network_addProteome.sh
#
# Purpose:
#   Final add-proteome network-building step.
#
# Workflow:
#   1. Copy all query-proteome signal files from:
#        $WORK_ROOT/$PROTEOME_ID/Signals.q
#      into:
#        $WORK_ROOT/$PROTEOME_ID/Signals.t
#
#      After this step, Signals.t contains both:
#        - signal files from the target-side/additional proteins
#        - signal files from the original query proteome
#
#   2. Run:
#        build_network_single_node.py --db Signals.t --output addproteome.csv
#
#      The output CSV has three columns:
#        AC1,AC2,SJI
#
#   3. Parse addproteome.csv using proteins listed in:
#        $WORK_ROOT/$PROTEOME_ID/lists/Pro_Q
#
#      For each protein in Pro_Q, create one output file named after
#      that protein. Each file contains only edges involving that protein.
#
#      In each output file:
#        column 1 = the Pro_Q protein matching the file name
#        column 2 = the linked node
#        column 3 = SJI
#
#      Edges are sorted by SJI in descending numerical order.
#
# Example:
#   bash 09_run_build_network_addProteome.sh UP000000556
#
# Main outputs:
#   $WORK_ROOT/$PROTEOME_ID/addProteome_Net/addproteome.csv
#   $WORK_ROOT/$PROTEOME_ID/addProteome_Net/Pro_Q_edges/<protein_id>
# ============================================================


# -------------------------
# Proteome ID
# -------------------------

PROTEOME_ID="${1:-UP000000556}"


# -------------------------
# Editable variables
# -------------------------

# Edit this variable to point to build_network_single_node.py.
SJI_NET="/protDC/Scripts/build_network_single_node.py"

# Optional Python environment activation.
# Leave VENV_ACT empty if no Python environment needs to be activated.
# Examples:
#   VENV_ACT="/protDC/venv/bin/activate"
#   VENV_ACT="/protDC/miniconda3/envs/protDC/bin/activate"
VENV_ACT=""

# Edit this variable to the main working folder.
WORK_ROOT="/protDC/data/work"

# If true, remove the previous Pro_Q edge files before rebuilding them.
OVERWRITE_EDGE_FILES=true

# Minimum SJI threshold for edges written to Pro_Q_edges/<protein_id>.
# Only edges with SJI strictly higher than this value are kept.
#
# Default:
#   SJI_MIN_THRESHOLD=0
#
# This preserves the current behavior if all reported SJI values are positive.
# Example:
#   SJI_MIN_THRESHOLD=0.3
SJI_MIN_THRESHOLD=0


# -------------------------
# Derived paths
# -------------------------

WORK_DIR="${WORK_ROOT%/}/${PROTEOME_ID}"

SIGNAL_Q_DIR="${WORK_DIR}/Signals.q"
SIGNAL_T_DIR="${WORK_DIR}/Signals.t"

LIST_DIR="${WORK_DIR}/lists"
PRO_Q="${LIST_DIR}/Pro_Q"

OUT_DIR="${WORK_DIR}/addProteome_Net"
ADDPROTEOME_CSV="${OUT_DIR}/addproteome.csv"

EDGE_DIR="${OUT_DIR}/Pro_Q_edges"
TMP_EDGE_DIR="${OUT_DIR}/Pro_Q_edges.tmp"

PARSE_REPORT="${OUT_DIR}/parse_addproteome_report.tsv"


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


# -------------------------
# Basic checks
# -------------------------

if [[ ! -f "$SJI_NET" ]]; then
    echo "Error: build_network_single_node.py was not found: $SJI_NET" >&2
    exit 1
fi

if [[ ! -d "$SIGNAL_Q_DIR" ]]; then
    echo "Error: Signals.q folder was not found: $SIGNAL_Q_DIR" >&2
    echo "Please run scripts 01-03 first." >&2
    exit 1
fi

if [[ ! -d "$SIGNAL_T_DIR" ]]; then
    echo "Error: Signals.t folder was not found: $SIGNAL_T_DIR" >&2
    echo "Please run scripts 04-08 first." >&2
    exit 1
fi

if [[ ! -s "$PRO_Q" ]]; then
    echo "Error: Pro_Q list was not found or is empty: $PRO_Q" >&2
    echo "Please run script 04 first." >&2
    exit 1
fi

mkdir -p "$OUT_DIR" "$EDGE_DIR" "$TMP_EDGE_DIR"


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
# Step 1: Copy Signals.q files into Signals.t
# -------------------------

echo "Copying query signal files into target signal folder:"
echo "  From: $SIGNAL_Q_DIR"
echo "  To  : $SIGNAL_T_DIR"

signal_q_count=$(find "$SIGNAL_Q_DIR" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')
signal_t_count_before=$(find "$SIGNAL_T_DIR" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')

if [[ "$signal_q_count" -eq 0 ]]; then
    echo "Error: no files were found in Signals.q: $SIGNAL_Q_DIR" >&2
    exit 1
fi

find "$SIGNAL_Q_DIR" -mindepth 1 -maxdepth 1 -type f | sort | \
while IFS= read -r f; do
    cp -p "$f" "${SIGNAL_T_DIR}/$(basename "$f")"
done

signal_t_count_after=$(find "$SIGNAL_T_DIR" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')

echo "Signals.q files copied: $signal_q_count"
echo "Signals.t file count before copy: $signal_t_count_before"
echo "Signals.t file count after copy : $signal_t_count_after"


# -------------------------
# Step 2: Run build_network_single_node.py
# -------------------------

echo
echo "Running build_network_single_node.py:"
echo "  Script : $SJI_NET"
echo "  DB     : $SIGNAL_T_DIR"
echo "  Output : $ADDPROTEOME_CSV"

python "$SJI_NET" \
    --db "$SIGNAL_T_DIR" \
    --output "$ADDPROTEOME_CSV"

if [[ ! -s "$ADDPROTEOME_CSV" ]]; then
    echo "Error: addproteome.csv was not created or is empty: $ADDPROTEOME_CSV" >&2
    exit 1
fi

edge_count_total=$(awk 'NF > 0 { count++ } END { print count + 0 }' "$ADDPROTEOME_CSV")

echo "Network CSV created: $ADDPROTEOME_CSV"
echo "Rows in addproteome.csv: $edge_count_total"


# -------------------------
# Step 3: Parse addproteome.csv for Pro_Q proteins
# -------------------------

echo
echo "Parsing addproteome.csv using Pro_Q:"
echo "  Pro_Q list         : $PRO_Q"
echo "  Edge output        : $EDGE_DIR"
echo "  SJI minimum cutoff : $SJI_MIN_THRESHOLD"

rm -rf "$TMP_EDGE_DIR"
mkdir -p "$TMP_EDGE_DIR"

if [[ "$OVERWRITE_EDGE_FILES" == true ]]; then
    rm -rf "$EDGE_DIR"
fi
mkdir -p "$EDGE_DIR"

python - "$ADDPROTEOME_CSV" "$PRO_Q" "$TMP_EDGE_DIR" "$PARSE_REPORT" "$SJI_MIN_THRESHOLD" <<'PY'
import csv
import os
import sys
from collections import OrderedDict

csv_path, pro_q_path, tmp_edge_dir, report_path, sji_min_threshold_text = sys.argv[1:6]

try:
    sji_min_threshold = float(sji_min_threshold_text)
except ValueError:
    raise SystemExit(f"SJI_MIN_THRESHOLD must be numeric, got: {sji_min_threshold_text!r}")

# Keep only this many edge files open at once.
# This prevents "OSError: [Errno 24] Too many open files" on clusters
# with conservative ulimit settings.
MAX_OPEN_HANDLES = 64

# Read Pro_Q while preserving order and removing duplicates.
pro_q = OrderedDict()
with open(pro_q_path, "r", encoding="utf-8") as handle:
    for line_no, line in enumerate(handle, start=1):
        protein = line.strip().split()[0] if line.strip() else ""
        if not protein:
            continue
        if "/" in protein or protein in {".", ".."}:
            raise SystemExit(
                f"Unsafe protein ID in Pro_Q at line {line_no}: {protein!r}"
            )
        pro_q[protein] = None

pro_q_set = set(pro_q)

if not pro_q_set:
    raise SystemExit(f"Pro_Q is empty: {pro_q_path}")

# Initialize counts and create empty temp files for all Pro_Q proteins.
counts = {protein: 0 for protein in pro_q}
for protein in pro_q:
    open(os.path.join(tmp_edge_dir, protein), "w", encoding="utf-8").close()

rows_seen = 0
rows_skipped_header = 0
rows_invalid = 0
rows_below_or_equal_threshold = 0
rows_written = 0

# Limited file-handle cache.
# Dict insertion order is used as a simple FIFO eviction policy.
handle_cache = OrderedDict()

def get_handle(protein):
    if protein in handle_cache:
        handle = handle_cache.pop(protein)
        handle_cache[protein] = handle
        return handle

    if len(handle_cache) >= MAX_OPEN_HANDLES:
        old_protein, old_handle = handle_cache.popitem(last=False)
        old_handle.close()

    handle = open(
        os.path.join(tmp_edge_dir, protein),
        "a",
        encoding="utf-8",
        newline="",
    )
    handle_cache[protein] = handle
    return handle

def write_edge(protein, linked_node, sji):
    global rows_written
    out = get_handle(protein)
    out.write(f"{protein},{linked_node},{sji:.10g}\n")
    counts[protein] += 1
    rows_written += 1

try:
    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        for row_no, row in enumerate(reader, start=1):
            if not row:
                continue

            # Expect AC1, AC2, SJI. Strip whitespace around each field.
            row = [field.strip() for field in row]

            if len(row) < 3:
                rows_invalid += 1
                continue

            ac1, ac2, sji_text = row[0], row[1], row[2]

            # Skip a header if present.
            if row_no == 1 and ac1.upper() == "AC1" and ac2.upper() == "AC2":
                rows_skipped_header += 1
                continue

            rows_seen += 1

            try:
                sji = float(sji_text)
            except ValueError:
                rows_invalid += 1
                continue

            # Keep only edges with SJI strictly higher than the threshold.
            # The default threshold is 0.
            if sji <= sji_min_threshold:
                rows_below_or_equal_threshold += 1
                continue

            # If AC1 is a Pro_Q protein, write the edge oriented as:
            #   Pro_Q protein, linked node, SJI
            if ac1 in pro_q_set:
                write_edge(ac1, ac2, sji)

            # If AC2 is a Pro_Q protein, write the reciprocal orientation.
            # For self-edges, avoid writing the same row twice.
            if ac2 in pro_q_set and ac2 != ac1:
                write_edge(ac2, ac1, sji)

finally:
    for out in handle_cache.values():
        out.close()

with open(report_path, "w", encoding="utf-8", newline="") as report:
    writer = csv.writer(report, delimiter="\t", lineterminator="\n")
    writer.writerow(["protein", "edge_count"])
    for protein in pro_q:
        writer.writerow([protein, counts[protein]])
    writer.writerow([])
    writer.writerow(["summary", "value"])
    writer.writerow(["pro_q_count", len(pro_q)])
    writer.writerow(["rows_seen_excluding_header", rows_seen])
    writer.writerow(["rows_skipped_header", rows_skipped_header])
    writer.writerow(["rows_invalid", rows_invalid])
    writer.writerow(["sji_min_threshold", sji_min_threshold])
    writer.writerow(["rows_below_or_equal_threshold", rows_below_or_equal_threshold])
    writer.writerow(["oriented_rows_written", rows_written])
    writer.writerow(["max_open_handles", MAX_OPEN_HANDLES])
PY


# -------------------------
# Step 4: Sort each Pro_Q edge file by SJI descending
# -------------------------

echo
echo "Sorting Pro_Q-specific edge files by SJI descending"

pro_q_count=$(awk 'NF > 0 { count++ } END { print count + 0 }' "$PRO_Q")
files_written=0

while IFS= read -r protein; do
    [[ -n "$protein" ]] || continue
    protein=$(printf '%s\n' "$protein" | awk '{print $1}')

    tmp_file="${TMP_EDGE_DIR}/${protein}"
    out_file="${EDGE_DIR}/${protein}"

    if [[ -s "$tmp_file" ]]; then
        LC_ALL=C sort -t, -k3,3gr "$tmp_file" > "$out_file"
    else
        : > "$out_file"
    fi

    files_written=$((files_written + 1))

done < "$PRO_Q"

rm -rf "$TMP_EDGE_DIR"

nonempty_edge_files=$(find "$EDGE_DIR" -mindepth 1 -maxdepth 1 -type f -size +0c | wc -l | tr -d ' ')
empty_edge_files=$(find "$EDGE_DIR" -mindepth 1 -maxdepth 1 -type f -size 0c | wc -l | tr -d ' ')


# -------------------------
# Final summary
# -------------------------

echo
echo "Script 09 finished successfully."
echo
echo "Main outputs:"
echo "  Combined signal DB      : $SIGNAL_T_DIR"
echo "  Network CSV             : $ADDPROTEOME_CSV"
echo "  Pro_Q edge folder        : $EDGE_DIR"
echo "  Parse report             : $PARSE_REPORT"
echo
echo "Counts:"
echo "  Pro_Q proteins           : $pro_q_count"
echo "  Pro_Q edge files written : $files_written"
echo "  Non-empty edge files      : $nonempty_edge_files"
echo "  Empty edge files          : $empty_edge_files"
echo "  SJI minimum cutoff        : $SJI_MIN_THRESHOLD"
echo
