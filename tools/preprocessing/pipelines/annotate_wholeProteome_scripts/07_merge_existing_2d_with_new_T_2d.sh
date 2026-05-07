#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 07_merge_existing_2d_with_new_T_2d.sh
#
# Purpose:
#   For each protein in the updated Pro_T list:
#     1. Find the existing package 2D file under 2D_HOME.
#     2. Find the newly generated 2D file under:
#          $WORK_ROOT/$PROTEOME_ID/2D_out.t
#     3. Concatenate the existing package 2D file with the newly
#        generated 2D file.
#     4. Replace the newly generated 2D file with the combined file.
#
# Important:
#   The script creates a backup of each newly generated 2D file:
#        <new_2D_file>.before_script07
#   If the script is rerun, it uses the backup as the original new
#   2D file, preventing repeated duplication of the package 2D file.
#
# Example:
#   bash 07_merge_existing_2d_with_new_T_2d.sh UP000000556
# ============================================================

PROTEOME_ID="${1:-UP000000556}"

# -------------------------
# Editable variables
# -------------------------

# Package 2D home folder.
# This folder should contain existing 2D files from the ProtDC package.
TWO_D_HOME="/protDC/data/Bacteria_2D/"

WORK_ROOT="/protDC/data/work"

# -------------------------
# Derived paths
# -------------------------

WORK_DIR="${WORK_ROOT%/}/${PROTEOME_ID}"
PRO_T="${WORK_DIR}/lists/Pro_T"
TWO_D_TARGET="${WORK_DIR}/2D_out.t"
REPORT_DIR="${WORK_DIR}/script07_merge_reports"

MERGED_REPORT="${REPORT_DIR}/merged.tsv"
MISSING_EXISTING="${REPORT_DIR}/missing_existing_2D_HOME.txt"
MISSING_NEW="${REPORT_DIR}/missing_new_2D_out.t.txt"
DUPLICATE_MATCHES="${REPORT_DIR}/duplicate_matches.tsv"

# -------------------------
# Basic checks
# -------------------------

if [[ ! -d "$TWO_D_HOME" ]]; then
    echo "Error: 2D_HOME folder was not found: $TWO_D_HOME" >&2
    exit 1
fi

if [[ ! -f "$PRO_T" ]]; then
    echo "Error: Pro_T list was not found: $PRO_T" >&2
    echo "Please run script 04 first." >&2
    exit 1
fi

if [[ ! -d "$TWO_D_TARGET" ]]; then
    echo "Error: target 2D_out.t folder was not found: $TWO_D_TARGET" >&2
    echo "Please run script 06 first." >&2
    exit 1
fi

if ! command -v python >/dev/null 2>&1; then
    echo "Error: python was not found in PATH." >&2
    exit 1
fi

mkdir -p "$REPORT_DIR"

# -------------------------
# Merge 2D files
# -------------------------

echo "Merging existing package 2D files with newly generated 2D files:"
echo "  Proteome ID      : $PROTEOME_ID"
echo "  Pro_T            : $PRO_T"
echo "  2D_HOME          : $TWO_D_HOME"
echo "  Target 2D folder : $TWO_D_TARGET"
echo "  Report folder    : $REPORT_DIR"

python - "$PRO_T" "$TWO_D_HOME" "$TWO_D_TARGET" "$MERGED_REPORT" "$MISSING_EXISTING" "$MISSING_NEW" "$DUPLICATE_MATCHES" <<'PY'
import os
import shutil
import sys
from collections import defaultdict
from pathlib import Path

(
    pro_t_path,
    two_d_home,
    two_d_target,
    merged_report,
    missing_existing,
    missing_new,
    duplicate_matches,
) = sys.argv[1:8]

with open(pro_t_path, "r", encoding="utf-8") as handle:
    protein_ids = [line.strip() for line in handle if line.strip()]

protein_set = set(protein_ids)


def candidate_ids_from_basename(path):
    """Return possible protein IDs represented by a file basename.

    The strict match is the whole basename. If the file has an extension,
    the first token before the first period is also considered. This supports
    both files named like P12345 and files named like P12345.csv.
    """
    base = os.path.basename(path)
    candidates = [base]

    if "." in base:
        candidates.append(base.split(".", 1)[0])

    return candidates


def index_files(root):
    index = defaultdict(list)

    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".before_script07"):
                continue
            if filename.endswith(".tmp_script07"):
                continue

            path = os.path.join(dirpath, filename)

            for cid in candidate_ids_from_basename(path):
                if cid in protein_set:
                    index[cid].append(path)

    for pid in list(index):
        index[pid] = sorted(set(index[pid]))

    return index


existing_index = index_files(two_d_home)
new_index = index_files(two_d_target)

Path(merged_report).parent.mkdir(parents=True, exist_ok=True)

merged = []
missing_existing_ids = []
missing_new_ids = []
duplicates = []

for pid in protein_ids:
    existing_files = existing_index.get(pid, [])
    new_files = new_index.get(pid, [])

    if len(existing_files) > 1:
        duplicates.append((pid, "2D_HOME", ";".join(existing_files)))

    if len(new_files) > 1:
        duplicates.append((pid, "2D_out.t", ";".join(new_files)))

    if not existing_files:
        missing_existing_ids.append(pid)
        continue

    if not new_files:
        missing_new_ids.append(pid)
        continue

    existing_file = existing_files[0]
    new_file = new_files[0]
    backup_file = new_file + ".before_script07"

    if not os.path.exists(backup_file):
        shutil.copy2(new_file, backup_file)

    # Always use the backup as the original newly generated file.
    # This prevents repeated duplication if script 07 is rerun.
    source_new_file = backup_file
    tmp_file = new_file + ".tmp_script07"

    with open(tmp_file, "wb") as out:
        with open(existing_file, "rb") as src:
            shutil.copyfileobj(src, out)

        # Separator newline between the package 2D file and the new 2D file.
        out.write(b"\n")

        with open(source_new_file, "rb") as src:
            shutil.copyfileobj(src, out)

    os.replace(tmp_file, new_file)
    merged.append((pid, existing_file, source_new_file, new_file))

with open(merged_report, "w", encoding="utf-8") as handle:
    handle.write("protein_id\texisting_2D_HOME_file\tnew_2D_backup_file\tcombined_2D_out_t_file\n")
    for row in merged:
        handle.write("\t".join(row) + "\n")

with open(missing_existing, "w", encoding="utf-8") as handle:
    for pid in missing_existing_ids:
        handle.write(pid + "\n")

with open(missing_new, "w", encoding="utf-8") as handle:
    for pid in missing_new_ids:
        handle.write(pid + "\n")

with open(duplicate_matches, "w", encoding="utf-8") as handle:
    handle.write("protein_id\tlocation\tmatched_files\n")
    for row in duplicates:
        handle.write("\t".join(row) + "\n")

print(f"proteins_in_Pro_T={len(protein_ids)}")
print(f"merged={len(merged)}")
print(f"missing_existing_2D_HOME={len(missing_existing_ids)}")
print(f"missing_new_2D_out_t={len(missing_new_ids)}")
print(f"duplicate_match_rows={len(duplicates)}")
PY

echo
echo "Script 07 finished."
echo "Merged report: $MERGED_REPORT"
echo "Missing existing 2D_HOME files: $MISSING_EXISTING"
echo "Missing new 2D_out.t files: $MISSING_NEW"
echo "Duplicate match report: $DUPLICATE_MATCHES"
