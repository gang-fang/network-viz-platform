#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 04_prepare_ProT_download_fastas.sh
#
# Purpose:
#   1. Read query-side signal output from:
#        $WORK_ROOT/$PROTEOME_ID/Signals.q
#
#   2. Save the file names in Signals.q as:
#        $WORK_ROOT/$PROTEOME_ID/lists/Pro_Q
#      These file names are treated as protein IDs already present
#      in the query proteome.
#
#   3. Concatenate all files in Signals.q to make a raw target list.
#      These entries are also treated as protein IDs.
#
#   4. Remove all IDs already present in Pro_Q from the target list.
#      The final target list is:
#        $WORK_ROOT/$PROTEOME_ID/lists/Pro_T
#
#   5. Split Pro_T into chunks of 5,000 IDs.
#
#   6. Use the UniProt REST API ID Mapping service to download
#      FASTA records for each chunk.
#
#   7. Save downloaded target FASTA chunks as:
#        $WORK_ROOT/$PROTEOME_ID/Pro_T_FASTA/T1
#        $WORK_ROOT/$PROTEOME_ID/Pro_T_FASTA/T2
#        ...
#
#   8. Report missing or unmapped IDs.
#
# Example:
#   bash 04_prepare_ProT_download_fastas.sh UP000000556
#
# Important implementation detail:
#   This version avoids parsing helper files such as .headers or .body
#   as JSON. It writes real JSON pages as:
#        chunk_1.json.page_1.json
#        chunk_1.json.page_2.json
#        ...
#   and stores the exact list of JSON page files to parse.
# ============================================================


# -------------------------
# Proteome ID
# -------------------------

PROTEOME_ID="${1:-UP000000556}"


# -------------------------
# Editable settings
# -------------------------

# Edit this variable to the main working folder.
WORK_ROOT="/protDC/data/work"

# Optional: edit this variable if a Python virtual environment must be activated.
#
# Leave it empty if no environment activation is needed:
#   VENV_ACT=""
#
# Example for a normal Python venv:
#   VENV_ACT="/protDC/venv/bin/activate"
#
# Example for a specific Conda environment by path:
#   VENV_ACT="/protDC/miniconda3/envs/protDC/bin/activate"
VENV_ACT=""

# Python is used only for small JSON/header parsing steps.
# Use python3 unless your system requires a different command.
PYTHON_BIN="python3"

# Number of accessions submitted per UniProt ID-mapping job.
CHUNK_SIZE=5000

# Polling settings for UniProt ID-mapping jobs.
MAX_POLLS=120
POLL_SLEEP_SECONDS=5

# Page size for the paginated UniProt results endpoint.
PAGE_SIZE=500

# Pause between paginated requests. This is intentionally small, but it
# makes the script gentler on UniProt and on cluster network proxies.
REQUEST_SLEEP_SECONDS=1

# UniProt ID-mapping source and target.
UNIPROT_FROM="UniProtKB_AC-ID"
UNIPROT_TO="UniProtKB"

# If true, remove previous T[0-9]* FASTA files and old chunk/log files
# produced by this script before rebuilding them.
OVERWRITE_OUTPUT=true


# -------------------------
# Derived paths
# -------------------------

WORK_DIR="${WORK_ROOT%/}/${PROTEOME_ID}"
SIGNAL_Q_DIR="${WORK_DIR}/Signals.q"

LIST_DIR="${WORK_DIR}/lists"
PRO_Q="${LIST_DIR}/Pro_Q"
PRO_T_RAW="${LIST_DIR}/Pro_T.raw"
PRO_T_RAW_UNIQ="${LIST_DIR}/Pro_T.raw.unique"
PRO_T="${LIST_DIR}/Pro_T"

PRO_T_FASTA_DIR="${WORK_DIR}/Pro_T_FASTA"
CHUNK_DIR="${PRO_T_FASTA_DIR}/chunks"
RAW_DIR="${PRO_T_FASTA_DIR}/raw"
LOG_DIR="${PRO_T_FASTA_DIR}/logs"

ALL_SUBMITTED="${LOG_DIR}/all_submitted_ids"
ALL_MAPPED_FROM="${LOG_DIR}/all_mapped_from_ids"
ALL_DOWNLOADED_ACCESSIONS="${LOG_DIR}/all_downloaded_accessions"
ALL_FAILED="${LOG_DIR}/all_failed_ids"
ALL_MISSING="${LOG_DIR}/all_missing_or_unmapped_ids"
ALL_MAPPING_TSV="${LOG_DIR}/all_id_mapping.tsv"


# -------------------------
# Activate Python environment, if needed
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

activate_python_env


# -------------------------
# Basic checks
# -------------------------

if [[ ! -d "$SIGNAL_Q_DIR" ]]; then
    echo "Error: Signals.q folder was not found: $SIGNAL_Q_DIR" >&2
    echo "Please run scripts 01, 02, and 03 first." >&2
    exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl was not found in PATH." >&2
    exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "Error: $PYTHON_BIN was not found in PATH." >&2
    echo "Please install Python 3 or edit PYTHON_BIN." >&2
    exit 1
fi

for cmd in sort comm split awk find paste wc grep; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Error: required command was not found in PATH: $cmd" >&2
        exit 1
    fi
done

mkdir -p "$LIST_DIR" "$PRO_T_FASTA_DIR" "$CHUNK_DIR" "$RAW_DIR" "$LOG_DIR"

if [[ "$OVERWRITE_OUTPUT" == true ]]; then
    rm -f "${PRO_T_FASTA_DIR}"/T[0-9]* "${PRO_T_FASTA_DIR}"/T_* 2>/dev/null || true
    rm -f "${CHUNK_DIR}"/Pro_T_chunk_* 2>/dev/null || true
    rm -f "${RAW_DIR}"/* 2>/dev/null || true
    rm -f "${LOG_DIR}"/* 2>/dev/null || true
fi


# -------------------------
# Step 1: Build Pro_Q from file names in Signals.q
# -------------------------

echo "Building Pro_Q from file names in: $SIGNAL_Q_DIR"

tmp_pro_q="${PRO_Q}.tmp"
: > "$tmp_pro_q"

find "$SIGNAL_Q_DIR" -mindepth 1 -maxdepth 1 -type f | sort | \
while IFS= read -r signal_file; do
    basename "$signal_file"
done | awk 'NF { gsub(/\r/, ""); print $1 }' | sort -u > "$tmp_pro_q"

mv "$tmp_pro_q" "$PRO_Q"

pro_q_count=$(wc -l < "$PRO_Q" | tr -d ' ')
if [[ "$pro_q_count" -eq 0 ]]; then
    echo "Error: Pro_Q is empty. No signal files were found in: $SIGNAL_Q_DIR" >&2
    exit 1
fi

echo "Pro_Q saved to: $PRO_Q"
echo "Number of proteins in Pro_Q: $pro_q_count"


# -------------------------
# Step 2: Build raw Pro_T by concatenating all Signals.q files
# -------------------------

echo
echo "Building raw Pro_T by concatenating files in: $SIGNAL_Q_DIR"

tmp_pro_t_raw="${PRO_T_RAW}.tmp"
: > "$tmp_pro_t_raw"

find "$SIGNAL_Q_DIR" -mindepth 1 -maxdepth 1 -type f | sort | \
while IFS= read -r signal_file; do
    cat "$signal_file"
done | awk '
    NF {
        gsub(/\r/, "")
        print $1
    }
' > "$tmp_pro_t_raw"

mv "$tmp_pro_t_raw" "$PRO_T_RAW"

pro_t_raw_count=$(wc -l < "$PRO_T_RAW" | tr -d ' ')
echo "Raw Pro_T saved to: $PRO_T_RAW"
echo "Number of raw Pro_T rows: $pro_t_raw_count"

if [[ "$pro_t_raw_count" -eq 0 ]]; then
    echo "Error: raw Pro_T is empty. The files in Signals.q may be empty." >&2
    exit 1
fi

sort -u "$PRO_T_RAW" > "$PRO_T_RAW_UNIQ"
pro_t_raw_uniq_count=$(wc -l < "$PRO_T_RAW_UNIQ" | tr -d ' ')
echo "Unique raw Pro_T saved to: $PRO_T_RAW_UNIQ"
echo "Number of unique raw Pro_T IDs: $pro_t_raw_uniq_count"


# -------------------------
# Step 3: Remove Pro_Q IDs from Pro_T
# -------------------------

echo
echo "Removing proteins already present in Pro_Q from Pro_T"

tmp_q_sorted="${LIST_DIR}/Pro_Q.sorted.tmp"
tmp_t_sorted="${LIST_DIR}/Pro_T.raw.unique.sorted.tmp"

sort -u "$PRO_Q" > "$tmp_q_sorted"
sort -u "$PRO_T_RAW_UNIQ" > "$tmp_t_sorted"

comm -23 "$tmp_t_sorted" "$tmp_q_sorted" > "$PRO_T"

rm -f "$tmp_q_sorted" "$tmp_t_sorted"

pro_t_count=$(wc -l < "$PRO_T" | tr -d ' ')
echo "Updated Pro_T saved to: $PRO_T"
echo "Number of proteins in updated Pro_T: $pro_t_count"

if [[ "$pro_t_count" -eq 0 ]]; then
    echo
    echo "No Pro_T proteins remain after removing Pro_Q."
    echo "Nothing to download."
    exit 0
fi


# -------------------------
# Step 4: Split Pro_T into chunks
# -------------------------

echo
echo "Splitting Pro_T into chunks of $CHUNK_SIZE IDs"

split -l "$CHUNK_SIZE" -d -a 4 "$PRO_T" "${CHUNK_DIR}/Pro_T_chunk_"

chunk_count=$(find "$CHUNK_DIR" -mindepth 1 -maxdepth 1 -type f -name 'Pro_T_chunk_*' | wc -l | tr -d ' ')

if [[ "$chunk_count" -eq 0 ]]; then
    echo "Error: no Pro_T chunks were created." >&2
    exit 1
fi

echo "Chunk folder: $CHUNK_DIR"
echo "Number of chunks: $chunk_count"


# -------------------------
# Helper functions for UniProt ID mapping
# -------------------------

extract_job_id() {
    local submit_json="$1"

    "$PYTHON_BIN" - "$submit_json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)

job_id = data.get("jobId", "")
if not job_id:
    raise SystemExit(f"Could not find jobId in {path}")

print(job_id)
PY
}

extract_job_status() {
    local status_json="$1"

    "$PYTHON_BIN" - "$status_json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    text = handle.read().strip()

if not text:
    print("EMPTY")
    raise SystemExit(0)

data = json.loads(text)

if "jobStatus" in data:
    print(data["jobStatus"])
elif "results" in data or "failedIds" in data:
    print("FINISHED")
else:
    print("UNKNOWN")
PY
}

extract_next_link() {
    local header_file="$1"

    "$PYTHON_BIN" - "$header_file" <<'PY'
import re
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8", errors="replace") as handle:
    text = handle.read()

# UniProt pagination uses an HTTP Link header containing rel="next".
match = re.search(r'<([^>]+)>;\s*rel="next"', text, flags=re.IGNORECASE)
print(match.group(1) if match else "")
PY
}

assert_valid_json() {
    local json_file="$1"

    "$PYTHON_BIN" - "$json_file" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    json.load(handle)
PY
}

parse_mapping_json_page() {
    local json_file="$1"
    local mapping_tsv="$2"
    local mapped_from_file="$3"
    local failed_file="$4"

    "$PYTHON_BIN" - "$json_file" "$mapping_tsv" "$mapped_from_file" "$failed_file" <<'PY'
import json
import sys

json_file, mapping_tsv, mapped_from_file, failed_file = sys.argv[1:5]

with open(json_file, "r", encoding="utf-8") as handle:
    data = json.load(handle)

results = data.get("results", [])
failed = data.get("failedIds", [])

with open(mapping_tsv, "a", encoding="utf-8") as map_handle, \
     open(mapped_from_file, "a", encoding="utf-8") as mapped_handle, \
     open(failed_file, "a", encoding="utf-8") as failed_handle:

    for item in results:
        source_id = item.get("from", "")
        target = item.get("to", "")

        if isinstance(target, dict):
            target_id = (
                target.get("primaryAccession")
                or target.get("uniProtkbId")
                or target.get("id")
                or ""
            )
        else:
            target_id = str(target)

        if source_id:
            mapped_handle.write(source_id + "\n")
            map_handle.write(f"{source_id}\t{target_id}\n")

    for source_id in failed:
        if source_id:
            failed_handle.write(str(source_id) + "\n")
PY
}

normalize_fasta_headers() {
    local raw_fasta="$1"
    local clean_fasta="$2"
    local downloaded_ids="$3"

    awk -v ids="$downloaded_ids" '
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
            print id >> ids
            next
        }
        {
            print
        }
    ' "$raw_fasta" > "$clean_fasta"
}


# -------------------------
# Helper function: download paginated JSON results
# -------------------------

download_json_pages() {
    local start_url="$1"
    local chunk_index="$2"
    local page_list_file="$3"

    local page=1
    local next_url="$start_url"
    local header_file
    local json_file
    local http_code

    : > "$page_list_file"

    while [[ -n "$next_url" ]]; do
        header_file="${RAW_DIR}/chunk_${chunk_index}.json.page_${page}.headers"
        json_file="${RAW_DIR}/chunk_${chunk_index}.json.page_${page}.json"

        echo "    Downloading json page ${page}"

        http_code=$(curl -L -sS \
            -w "%{http_code}" \
            -D "$header_file" \
            "$next_url" \
            -o "$json_file" || true)

        if [[ "$http_code" != "200" ]]; then
            echo "Error: JSON page download returned HTTP $http_code" >&2
            echo "URL: $next_url" >&2
            echo "Headers: $header_file" >&2
            echo "Body: $json_file" >&2
            head -40 "$json_file" >&2 || true
            exit 1
        fi

        if ! assert_valid_json "$json_file"; then
            echo "Error: downloaded JSON page is not valid JSON: $json_file" >&2
            echo "First lines of the file:" >&2
            head -40 "$json_file" >&2 || true
            exit 1
        fi

        printf '%s\n' "$json_file" >> "$page_list_file"

        next_url=$(extract_next_link "$header_file")

        page=$((page + 1))

        if [[ -n "$next_url" ]]; then
            sleep "$REQUEST_SLEEP_SECONDS"
        fi
    done
}


# -------------------------
# Helper function: download paginated FASTA results
# -------------------------

download_fasta_pages() {
    local start_url="$1"
    local output_fasta="$2"
    local chunk_index="$3"

    local page=1
    local next_url="$start_url"
    local header_file
    local page_fasta
    local http_code

    : > "$output_fasta"

    while [[ -n "$next_url" ]]; do
        header_file="${RAW_DIR}/chunk_${chunk_index}.fasta.page_${page}.headers"
        page_fasta="${RAW_DIR}/chunk_${chunk_index}.fasta.page_${page}.fasta"

        echo "    Downloading fasta page ${page}"

        http_code=$(curl -L -sS \
            -w "%{http_code}" \
            -D "$header_file" \
            "$next_url" \
            -o "$page_fasta" || true)

        if [[ "$http_code" != "200" ]]; then
            echo "Error: FASTA page download returned HTTP $http_code" >&2
            echo "URL: $next_url" >&2
            echo "Headers: $header_file" >&2
            echo "Body: $page_fasta" >&2
            head -40 "$page_fasta" >&2 || true
            exit 1
        fi

        cat "$page_fasta" >> "$output_fasta"

        next_url=$(extract_next_link "$header_file")

        page=$((page + 1))

        if [[ -n "$next_url" ]]; then
            sleep "$REQUEST_SLEEP_SECONDS"
        fi
    done
}


# -------------------------
# Step 5: Submit UniProt jobs and download FASTA chunks
# -------------------------

echo
echo "Downloading Pro_T proteins from UniProt"

: > "$ALL_SUBMITTED"
: > "$ALL_MAPPED_FROM"
: > "$ALL_DOWNLOADED_ACCESSIONS"
: > "$ALL_FAILED"
: > "$ALL_MISSING"
printf "from_id\tto_id\n" > "$ALL_MAPPING_TSV"

chunk_index=0

find "$CHUNK_DIR" -mindepth 1 -maxdepth 1 -type f -name 'Pro_T_chunk_*' | sort | \
while IFS= read -r chunk_file; do
    chunk_index=$((chunk_index + 1))

    chunk_id_count=$(wc -l < "$chunk_file" | tr -d ' ')
    output_fasta="${PRO_T_FASTA_DIR}/T${chunk_index}"
    raw_fasta="${RAW_DIR}/T${chunk_index}.raw.fasta"
    submit_json="${RAW_DIR}/chunk_${chunk_index}.submit.json"
    status_json="${RAW_DIR}/chunk_${chunk_index}.status.json"
    json_page_list="${RAW_DIR}/chunk_${chunk_index}.json_pages.list"
    mapped_from_file="${LOG_DIR}/chunk_${chunk_index}.mapped_from_ids"
    failed_file="${LOG_DIR}/chunk_${chunk_index}.failed_ids"
    downloaded_ids="${LOG_DIR}/chunk_${chunk_index}.downloaded_accessions"
    missing_file="${LOG_DIR}/chunk_${chunk_index}.missing_or_unmapped_ids"
    mapping_tsv="${LOG_DIR}/chunk_${chunk_index}.mapping.tsv"

    : > "$mapped_from_file"
    : > "$failed_file"
    : > "$downloaded_ids"
    : > "$missing_file"
    printf "from_id\tto_id\n" > "$mapping_tsv"

    echo
    echo "Processing chunk ${chunk_index}/${chunk_count}"
    echo "  Chunk file : $chunk_file"
    echo "  IDs        : $chunk_id_count"
    echo "  FASTA out  : $output_fasta"

    cat "$chunk_file" >> "$ALL_SUBMITTED"

    ids_csv=$(paste -sd, "$chunk_file")

    echo "  Submitting UniProt ID-mapping job"

    curl -sS --fail -X POST "https://rest.uniprot.org/idmapping/run" \
        --data-urlencode "from=${UNIPROT_FROM}" \
        --data-urlencode "to=${UNIPROT_TO}" \
        --data-urlencode "ids=${ids_csv}" \
        -o "$submit_json"

    job_id=$(extract_job_id "$submit_json")

    echo "  UniProt job ID: $job_id"

    finished=false

    for poll in $(seq 1 "$MAX_POLLS"); do
        http_code=$(curl -sS \
            -w "%{http_code}" \
            -o "$status_json" \
            "https://rest.uniprot.org/idmapping/status/${job_id}" || true)

        if [[ "$http_code" != "200" && "$http_code" != "303" ]]; then
            echo "  Warning: UniProt status request returned HTTP $http_code" >&2
            echo "  Status response was saved to: $status_json" >&2
            cat "$status_json" >&2 || true
            sleep "$POLL_SLEEP_SECONDS"
            continue
        fi

        job_status=$(extract_job_status "$status_json")

        if [[ "$job_status" == "FINISHED" ]]; then
            finished=true
            echo "  UniProt job finished."
            break
        fi

        if [[ "$job_status" == "FAILED" || "$job_status" == "ERROR" ]]; then
            echo "Error: UniProt job failed: $job_id" >&2
            cat "$status_json" >&2 || true
            exit 1
        fi

        echo "  Waiting for UniProt job... poll ${poll}/${MAX_POLLS}"
        sleep "$POLL_SLEEP_SECONDS"
    done

    if [[ "$finished" != true ]]; then
        echo "Error: UniProt job did not finish within polling limit: $job_id" >&2
        exit 1
    fi

    # The result endpoint confirmed from the cluster test:
    #   https://rest.uniprot.org/idmapping/uniprotkb/results/<job_id>
    result_base_url="https://rest.uniprot.org/idmapping/uniprotkb/results/${job_id}"

    echo "  Downloading JSON mapping results"
    json_url="${result_base_url}?format=json&compressed=false&size=${PAGE_SIZE}"
    download_json_pages "$json_url" "$chunk_index" "$json_page_list"

    # Parse only the exact JSON page files recorded by download_json_pages.
    while IFS= read -r json_page; do
        [[ -n "$json_page" ]] || continue
        parse_mapping_json_page "$json_page" "$mapping_tsv" "$mapped_from_file" "$failed_file"
    done < "$json_page_list"

    echo "  Downloading FASTA results"
    fasta_url="${result_base_url}?format=fasta&compressed=false&size=${PAGE_SIZE}"
    download_fasta_pages "$fasta_url" "$raw_fasta" "$chunk_index"

    if ! grep -q '^>' "$raw_fasta"; then
        echo "Error: downloaded FASTA for chunk $chunk_index contains no FASTA records: $raw_fasta" >&2
        echo "First lines of the file:" >&2
        head -40 "$raw_fasta" >&2 || true
        exit 1
    fi

    normalize_fasta_headers "$raw_fasta" "$output_fasta" "$downloaded_ids"

    downloaded_count=$(grep -c '^>' "$output_fasta" || true)

    sort -u "$mapped_from_file" -o "$mapped_from_file"
    sort -u "$failed_file" -o "$failed_file"
    sort -u "$downloaded_ids" -o "$downloaded_ids"

    # Missing/unmapped based on mapping source IDs:
    # submitted IDs minus successfully mapped source IDs.
    tmp_submitted_sorted="${RAW_DIR}/chunk_${chunk_index}.submitted.sorted"
    tmp_mapped_sorted="${RAW_DIR}/chunk_${chunk_index}.mapped.sorted"

    sort -u "$chunk_file" > "$tmp_submitted_sorted"
    sort -u "$mapped_from_file" > "$tmp_mapped_sorted"

    comm -23 "$tmp_submitted_sorted" "$tmp_mapped_sorted" > "$missing_file"

    missing_count=$(wc -l < "$missing_file" | tr -d ' ')
    failed_count=$(wc -l < "$failed_file" | tr -d ' ')

    tail -n +2 "$mapping_tsv" >> "$ALL_MAPPING_TSV"
    cat "$mapped_from_file" >> "$ALL_MAPPED_FROM"
    cat "$failed_file" >> "$ALL_FAILED"
    cat "$downloaded_ids" >> "$ALL_DOWNLOADED_ACCESSIONS"
    cat "$missing_file" >> "$ALL_MISSING"

    echo "  Downloaded FASTA records: $downloaded_count"
    echo "  Failed IDs reported by UniProt: $failed_count"
    echo "  Missing/unmapped IDs in this chunk: $missing_count"

    if [[ "$missing_count" -gt 0 ]]; then
        echo "  Missing/unmapped list: $missing_file"
    fi

done


# -------------------------
# Step 6: Final summary
# -------------------------

sort -u "$ALL_SUBMITTED" -o "$ALL_SUBMITTED"
sort -u "$ALL_MAPPED_FROM" -o "$ALL_MAPPED_FROM"
sort -u "$ALL_FAILED" -o "$ALL_FAILED"
sort -u "$ALL_DOWNLOADED_ACCESSIONS" -o "$ALL_DOWNLOADED_ACCESSIONS"
sort -u "$ALL_MISSING" -o "$ALL_MISSING"

total_fasta_records=$(grep -h '^>' "${PRO_T_FASTA_DIR}"/T[0-9]* 2>/dev/null | wc -l | tr -d ' ')
total_failed=$(wc -l < "$ALL_FAILED" | tr -d ' ')
total_missing=$(wc -l < "$ALL_MISSING" | tr -d ' ')

echo
echo "Script 04 finished successfully."
echo
echo "Main outputs:"
echo "  Pro_Q                          : $PRO_Q"
echo "  Pro_T                          : $PRO_T"
echo "  Pro_T FASTA directory           : $PRO_T_FASTA_DIR"
echo "  UniProt mapping TSV             : $ALL_MAPPING_TSV"
echo "  Failed IDs                      : $ALL_FAILED"
echo "  Missing/unmapped IDs            : $ALL_MISSING"
echo
echo "Counts:"
echo "  Pro_Q proteins                  : $pro_q_count"
echo "  Updated Pro_T proteins           : $pro_t_count"
echo "  FASTA records downloaded         : $total_fasta_records"
echo "  Failed IDs reported by UniProt   : $total_failed"
echo "  Missing/unmapped IDs             : $total_missing"
echo
echo "The FASTA chunks for script 05 are:"
find "$PRO_T_FASTA_DIR" -mindepth 1 -maxdepth 1 -type f -name 'T[0-9]*' | sort
