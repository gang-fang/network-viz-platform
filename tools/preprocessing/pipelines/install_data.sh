#!/usr/bin/env bash
set -euo pipefail

# Organize release data files into the expected data/ directory structure.
# By default, run this script from the folder that contains the
# downloaded/decompressed files. Use --source to point at that folder instead.
#
# Run with bash, not sh: `./install_data.sh ...` or `bash install_data.sh ...`.
# Invoking via `sh install_data.sh` is not supported; the script relies on
# bash-only features (arrays, `[[ ]]` and `=~` regex matching, `shopt -s
# nullglob`, `local`) that POSIX shells reject or that bash disables in POSIX
# mode.

usage() {
    cat <<'EOF'
Usage: ./install_data.sh [--source DIR] [--node-attr FILE] [--overwrite]

Options:
  -s, --source DIR       Directory containing downloaded/decompressed release files.
                         Defaults to the current directory.
      --node-attr FILE   Node-attribute file to install. Validated whenever
                         provided; required when multiple matches exist in
                         non-interactive runs. FILE may be a basename in
                         --source or a path resolving to one of the
                         node-attribute files in --source.
      --overwrite        Replace existing destination files.
  -h, --help             Show this help.
EOF
}

SOURCE_DIR="."
NODE_ATTR_CHOICE=""
OVERWRITE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--source)
            if [[ $# -lt 2 ]]; then
                echo "Error: $1 requires a directory." >&2
                exit 1
            fi
            SOURCE_DIR="$2"
            shift 2
            ;;
        --node-attr)
            if [[ $# -lt 2 ]]; then
                echo "Error: --node-attr requires a filename." >&2
                exit 1
            fi
            NODE_ATTR_CHOICE="$2"
            shift 2
            ;;
        --overwrite)
            OVERWRITE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ ! -d "$SOURCE_DIR" ]]; then
    echo "Error: source directory does not exist: $SOURCE_DIR" >&2
    exit 1
fi

SOURCE_DIR="$(cd "$SOURCE_DIR" && pwd)"
DATA_DIR="${SOURCE_DIR}/data"

shopt -s nullglob

release_markers=(
    "$SOURCE_DIR"/*.adj.*
    "$SOURCE_DIR"/*.nodes.attr
    "$SOURCE_DIR"/*.nodes.attr.gz
)

if [[ ${#release_markers[@]} -eq 0 ]]; then
    echo "Error: no release marker files found in $SOURCE_DIR." >&2
    echo "Expected at least one *.adj.*, *.nodes.attr, or *.nodes.attr.gz file." >&2
    exit 1
fi

move_file() {
    local src="$1"
    local dest_dir="$2"
    local dest="${dest_dir}/$(basename "$src")"

    if [[ -e "$dest" && "$OVERWRITE" -ne 1 ]]; then
        echo "Error: destination already exists: $dest" >&2
        echo "Use --overwrite to replace existing files." >&2
        exit 1
    fi

    mv "$src" "$dest_dir/"
}

print_file_tree() {
    find "$DATA_DIR" -maxdepth 2 -print | sort | while IFS= read -r p; do
        printf "%s\n" "data${p#"$DATA_DIR"}"
    done
}

# Resolve the node-attribute selection up front so we don't move other files
# and then fail validation halfway through.
node_attr_files=( "$SOURCE_DIR"/*.nodes.attr "$SOURCE_DIR"/*.nodes.attr.gz )
selected_node_attr=""

if [[ -n "$NODE_ATTR_CHOICE" ]]; then
    # An explicit choice is always validated, even when zero or one candidates
    # exist, so a typo never silently completes an install.
    if [[ ${#node_attr_files[@]} -eq 0 ]]; then
        echo "Error: --node-attr was given but no *.nodes.attr or *.nodes.attr.gz files exist in ${SOURCE_DIR}." >&2
        exit 1
    fi

    if [[ -f "${SOURCE_DIR}/${NODE_ATTR_CHOICE}" ]]; then
        candidate="${SOURCE_DIR}/${NODE_ATTR_CHOICE}"
    elif [[ -f "$NODE_ATTR_CHOICE" ]]; then
        candidate="$NODE_ATTR_CHOICE"
    else
        echo "Error: --node-attr file not found: $NODE_ATTR_CHOICE" >&2
        exit 1
    fi
    # Canonicalize so comparisons against node_attr_files entries (which
    # are already $SOURCE_DIR/<name>) match regardless of how the user
    # spelled the path (e.g. ./foo.nodes.attr).
    selected_node_attr="$(cd "$(dirname "$candidate")" && pwd)/$(basename "$candidate")"

    matched=0
    for f in "${node_attr_files[@]}"; do
        if [[ "$f" == "$selected_node_attr" ]]; then
            matched=1
            break
        fi
    done

    if [[ "$matched" -ne 1 ]]; then
        echo "Error: --node-attr must be one of the *.nodes.attr or *.nodes.attr.gz files in $SOURCE_DIR." >&2
        exit 1
    fi
elif [[ ${#node_attr_files[@]} -eq 1 ]]; then
    selected_node_attr="${node_attr_files[0]}"
elif [[ ${#node_attr_files[@]} -gt 1 ]]; then
    if [[ -t 0 ]]; then
        echo "Multiple node-attribute files found:"
        echo

        for i in "${!node_attr_files[@]}"; do
            printf "  [%d] %s\n" "$((i + 1))" "$(basename "${node_attr_files[$i]}")"
        done

        echo
        read -r -p "Enter the number of the node-attribute file to use: " choice

        if ! [[ "$choice" =~ ^[0-9]+$ ]]; then
            echo "Error: selection must be a number." >&2
            exit 1
        fi

        index=$((choice - 1))

        if (( index < 0 || index >= ${#node_attr_files[@]} )); then
            echo "Error: invalid selection." >&2
            exit 1
        fi

        selected_node_attr="${node_attr_files[$index]}"
    else
        echo "Error: multiple node-attribute files found, but stdin is not interactive." >&2
        echo "Pass --node-attr FILE to choose one." >&2
        exit 1
    fi
fi

# Preflight: check every planned destination for collisions before any
# filesystem changes (mkdir or mv), so a failed run leaves no trace.
# `-e` is false on paths whose parent dir doesn't exist yet, which is the
# correct behavior pre-mkdir: missing dir means no possible collision.
if [[ "$OVERWRITE" -ne 1 ]]; then
    collisions=()
    check_collision() {
        local dest_dir="$1"
        local src="$2"
        local dest="${dest_dir}/$(basename "$src")"
        if [[ -e "$dest" ]]; then
            collisions+=( "$dest" )
        fi
    }

    for f in "$SOURCE_DIR"/*.adj.*; do check_collision "${DATA_DIR}/indexes" "$f"; done
    for f in "$SOURCE_DIR"/*.node_ids.*; do check_collision "${DATA_DIR}/indexes" "$f"; done
    if [[ -f "${SOURCE_DIR}/NCBI_txID.csv" ]]; then
        check_collision "${DATA_DIR}/NCBI_txID" "${SOURCE_DIR}/NCBI_txID.csv"
    fi
    if [[ -f "${SOURCE_DIR}/commontree.txt" ]]; then
        check_collision "${DATA_DIR}/NCBI_txID" "${SOURCE_DIR}/commontree.txt"
    fi
    for f in "$SOURCE_DIR"/*.csv; do
        if [[ "$(basename "$f")" == "NCBI_txID.csv" ]]; then
            continue
        fi
        check_collision "${DATA_DIR}/networks" "$f"
    done
    if [[ -n "$selected_node_attr" ]]; then
        check_collision "${DATA_DIR}/nodes_attr" "$selected_node_attr"
    fi

    if [[ ${#collisions[@]} -gt 0 ]]; then
        echo "Error: ${#collisions[@]} destination file(s) already exist:" >&2
        for c in "${collisions[@]}"; do
            printf "  %s\n" "$c" >&2
        done
        echo "Use --overwrite to replace existing files." >&2
        exit 1
    fi
fi

echo "Creating data directory structure..."

mkdir -p "${DATA_DIR}/networks"
mkdir -p "${DATA_DIR}/nodes_attr"
mkdir -p "${DATA_DIR}/indexes"
mkdir -p "${DATA_DIR}/NCBI_txID"
# Runtime output directory for generated subnetwork CSV exports.
mkdir -p "${DATA_DIR}/exports"

echo "Moving index files..."

for f in "$SOURCE_DIR"/*.adj.*; do
    move_file "$f" "${DATA_DIR}/indexes"
done

for f in "$SOURCE_DIR"/*.node_ids.*; do
    move_file "$f" "${DATA_DIR}/indexes"
done

echo "Moving NCBI taxonomy files..."

if [[ -f "${SOURCE_DIR}/NCBI_txID.csv" ]]; then
    move_file "${SOURCE_DIR}/NCBI_txID.csv" "${DATA_DIR}/NCBI_txID"
fi

if [[ -f "${SOURCE_DIR}/commontree.txt" ]]; then
    move_file "${SOURCE_DIR}/commontree.txt" "${DATA_DIR}/NCBI_txID"
fi

echo "Moving network CSV files..."

for f in "$SOURCE_DIR"/*.csv; do
    # Every non-NCBI_txID.csv file matching *.csv in --source is treated as a
    # network file.
    # Keep this exclusion even though NCBI_txID.csv is moved above; it preserves
    # the taxonomy/network split if this block is reordered later.
    if [[ "$(basename "$f")" == "NCBI_txID.csv" ]]; then
        continue
    fi
    move_file "$f" "${DATA_DIR}/networks"
done

echo "Moving node-attribute file..."

if [[ ${#node_attr_files[@]} -eq 0 ]]; then
    echo "No .nodes.attr or .nodes.attr.gz file found in ${SOURCE_DIR}."
elif [[ ${#node_attr_files[@]} -eq 1 ]]; then
    echo "Found one node-attribute file: $(basename "$selected_node_attr")"
    move_file "$selected_node_attr" "${DATA_DIR}/nodes_attr"
    echo "Moved it to ${DATA_DIR}/nodes_attr/"
else
    move_file "$selected_node_attr" "${DATA_DIR}/nodes_attr"
    echo "Moved selected file to ${DATA_DIR}/nodes_attr/: $(basename "$selected_node_attr")"
    echo "The other node-attribute files were left in ${SOURCE_DIR}."
fi

echo
echo "Finished. Data directory contents:"
echo
print_file_tree

top_level_leftovers=()
for f in "$SOURCE_DIR"/*; do
    [[ -f "$f" ]] || continue
    top_level_leftovers+=( "$f" )
done

echo
if [[ ${#top_level_leftovers[@]} -eq 0 ]]; then
    echo "No files remain at the source top level."
else
    echo "Files remaining at the source top level:"
    for f in "${top_level_leftovers[@]}"; do
        printf "  %s\n" "$(basename "$f")"
    done
fi
