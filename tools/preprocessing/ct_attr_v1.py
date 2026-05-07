#!/usr/bin/env python3
"""
Create NH attribute CSV from Script 04 restored cluster output.

Input:
    clusters:  uniprotAC,cluster_id, with header
    mapping:   uniprotAC,NCBI_txID, no header

Output:
    node_id,NCBI_txID,NH_ID,NH_Size,,,,,

The output is a 9-field CSV. NH_ID is the original cluster_id with the
user-provided prefix prepended, separated by a period.
"""

import argparse
import csv
import sys
import time
from collections import Counter
from pathlib import Path


# Downstream tools expect a 9-field CSV with five reserved empty fields.
OUTPUT_HEADER = ["node_id", "NCBI_txID", "NH_ID", "NH_Size", "", "", "", "", ""]


def ensure_output_parent(path: str) -> None:
    parent = Path(path).expanduser().parent
    parent.mkdir(parents=True, exist_ok=True)


def validate_prefix(prefix: str) -> str:
    prefix = prefix.strip()
    if not prefix:
        raise ValueError("prefix cannot be empty")
    if len(prefix) > 3:
        raise ValueError("prefix must be at most three letters")
    if not prefix.isalpha():
        raise ValueError("prefix must contain letters only")
    return prefix


def prompt_prefix() -> str:
    while True:
        try:
            return validate_prefix(input("Enter NH_ID prefix (1-3 letters): "))
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
        except EOFError as e:
            raise ValueError("prefix is required when stdin is not interactive; pass --prefix") from e


def load_taxid_mapping(path: str) -> dict[str, str]:
    print(f"Loading UniProt AC to NCBI taxonomy ID mapping from {path}...")
    t0 = time.time()
    mapping: dict[str, str] = {}

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line_no, row in enumerate(reader, start=1):
            if not row or all(not field.strip() for field in row):
                continue
            if len(row) < 2:
                raise ValueError(f"mapping row {line_no:,} has fewer than 2 fields")

            uniprot_ac = row[0].strip()
            taxid = row[1].strip()
            if not uniprot_ac:
                raise ValueError(f"mapping row {line_no:,} has an empty UniProt accession")
            if uniprot_ac in mapping and mapping[uniprot_ac] != taxid:
                raise ValueError(
                    f"mapping contains conflicting NCBI taxonomy IDs for {uniprot_ac}: "
                    f"{mapping[uniprot_ac]} and {taxid}"
                )
            mapping[uniprot_ac] = taxid

    print(f"  Loaded {len(mapping):,} mapping rows in {time.time() - t0:.1f}s")
    return mapping


def load_clusters(path: str) -> tuple[list[tuple[str, str]], Counter[str]]:
    print(f"Loading clusters from {path}...")
    t0 = time.time()
    rows: list[tuple[str, str]] = []
    counts: Counter[str] = Counter()

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required_cols = {"uniprotAC", "cluster_id"}
        missing = required_cols - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"cluster file is missing required columns: {sorted(missing)}; "
                "expected header: uniprotAC,cluster_id"
            )

        for line_no, row in enumerate(reader, start=2):
            if not row or all(not (value or "").strip() for value in row.values()):
                continue
            node_id = (row.get("uniprotAC") or "").strip()
            cluster_id = (row.get("cluster_id") or "").strip()
            if not node_id:
                raise ValueError(f"cluster row {line_no:,} has an empty uniprotAC")
            if not cluster_id:
                raise ValueError(f"cluster row {line_no:,} has an empty cluster_id")
            rows.append((node_id, cluster_id))
            counts[cluster_id] += 1

    print(f"  Loaded {len(rows):,} nodes in {len(counts):,} clusters")
    print(f"  Finished in {time.time() - t0:.1f}s")
    return rows, counts


def write_attribute_csv(
    cluster_rows: list[tuple[str, str]],
    mapping: dict[str, str],
    cluster_sizes: Counter[str],
    prefix: str,
    out_path: str,
    allow_missing_taxid: bool,
) -> int:
    t0 = time.time()

    missing_nodes = [node_id for node_id, _cluster_id in cluster_rows if node_id not in mapping]
    missing_count = len(missing_nodes)
    if missing_count and not allow_missing_taxid:
        raise ValueError(
            "some UniProt accession codes are missing from the NCBI taxonomy mapping\n"
            f"  Missing count: {missing_count:,}\n"
            f"  First missing accessions: {', '.join(missing_nodes[:10])}"
        )

    print(f"Writing NH attributes to {out_path}...")
    ensure_output_parent(out_path)
    written = 0

    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(OUTPUT_HEADER)

        for node_id, cluster_id in cluster_rows:
            taxid = mapping.get(node_id)
            if taxid is None:
                taxid = ""

            writer.writerow(
                [
                    node_id,
                    taxid,
                    f"{prefix}.{cluster_id}",
                    cluster_sizes[cluster_id],
                    "",
                    "",
                    "",
                    "",
                    "",
                ]
            )
            written += 1

    print(f"  Wrote {written:,} rows in {time.time() - t0:.1f}s")
    if missing_count:
        print(
            f"  WARNING: {missing_count:,} rows were missing taxonomy IDs; first missing: "
            + ", ".join(missing_nodes[:10]),
            file=sys.stderr,
        )
    return missing_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create 9-field NH attribute CSV from Script 04 cluster output"
    )
    parser.add_argument(
        "--clusters",
        required=True,
        help="Script 04 output with header uniprotAC,cluster_id",
    )
    parser.add_argument(
        "--mapping",
        required=True,
        help="UniProt AC to NCBI taxonomy ID mapping file",
    )
    parser.add_argument(
        "--out",
        help="Output 9-field attribute CSV (default: <prefix>_attr.csv)",
    )
    parser.add_argument(
        "--prefix",
        help="NH_ID prefix, maximum three letters; prompts if omitted",
    )
    parser.add_argument(
        "--allow-missing-taxid",
        action="store_true",
        help="Write an empty NCBI_txID instead of failing when a UniProt AC is missing from mapping",
    )

    args = parser.parse_args()

    total_start = time.time()
    try:
        prefix = validate_prefix(args.prefix) if args.prefix is not None else prompt_prefix()
        out_path = args.out if args.out is not None else f"{prefix}_attr.csv"
        mapping = load_taxid_mapping(args.mapping)
        cluster_rows, cluster_sizes = load_clusters(args.clusters)
        missing_count = write_attribute_csv(
            cluster_rows,
            mapping,
            cluster_sizes,
            prefix,
            out_path,
            args.allow_missing_taxid,
        )
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nSUMMARY")
    print(f"  Clusters:   {args.clusters}")
    print(f"  Mapping:    {args.mapping}")
    print(f"  Prefix:     {prefix}")
    print(f"  Output:     {out_path}")
    print(f"  Missing taxIDs: {missing_count:,}")
    print(f"  Total time: {time.time() - total_start:.1f}s")


if __name__ == "__main__":
    main()
