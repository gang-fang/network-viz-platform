#!/usr/bin/env python3
"""
Restore integer protein IDs in partition.csv back to UniProt accession codes.

Inputs:
    partition.csv:           node_id,cluster_id, no header
    protein_id_mapping.csv:  int_id,protein_id, with header

Output:
    restored_partition.csv:  uniprotAC,cluster_id, with header

Rows are sorted so all UniProt accession codes in the same cluster_id are
grouped together.
"""

import argparse
import csv
import sys
import time
from pathlib import Path

import pandas as pd


def format_to_delimiter(file_format: str) -> str:
    if file_format == "tsv":
        return "\t"
    return ","


def ensure_output_parent(path: str) -> None:
    parent = Path(path).expanduser().parent
    if parent != Path('.'):
        parent.mkdir(parents=True, exist_ok=True)


def cluster_sort_key(cluster_id: str):
    """Sort hierarchical numeric cluster IDs naturally, e.g. 2 before 10."""
    parts = str(cluster_id).split(".")
    return tuple((0, int(part)) if part.isdigit() else (1, part) for part in parts)


def reject_partition_header(path: str, delimiter: str) -> None:
    with open(path, "r", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for parts in reader:
            if not parts or all(not part.strip() for part in parts):
                continue
            first = parts[0].strip().lower()
            second = parts[1].strip().lower() if len(parts) > 1 else ""
            if first in {"node_id", "int_id", "node"} or second in {"cluster_id", "community_id", "community"}:
                raise ValueError(
                    "FATAL: partition appears to contain a header row.\n"
                    "  Expected Script 02/03 partition format: node_id,cluster_id with no header."
                )
            return


def load_partition(path: str, delimiter: str) -> pd.DataFrame:
    print(f"Loading partition from {path}...")
    t0 = time.time()
    reject_partition_header(path, delimiter)
    try:
        partition = pd.read_csv(
            path,
            sep=delimiter,
            header=None,
            names=["int_id", "cluster_id"],
            dtype={"int_id": "int64", "cluster_id": "string"},
        )
    except ValueError as e:
        raise ValueError(
            "FATAL: failed to parse partition as headerless integer IDs.\n"
            "  Expected Script 02/03 partition format: node_id,cluster_id with no header."
        ) from e
    if partition["int_id"].duplicated().any():
        n_dup = int(partition["int_id"].duplicated().sum())
        raise ValueError(f"FATAL: partition contains {n_dup:,} duplicate integer node IDs")
    print(f"  Loaded {len(partition):,} partition rows in {time.time() - t0:.1f}s")
    return partition


def load_mapping(path: str, delimiter: str) -> pd.DataFrame:
    print(f"Loading protein ID mapping from {path}...")
    t0 = time.time()
    mapping = pd.read_csv(
        path,
        sep=delimiter,
        dtype={"int_id": "int64", "protein_id": "string"},
    )
    required_cols = {"int_id", "protein_id"}
    missing = required_cols - set(mapping.columns)
    if missing:
        raise ValueError(
            f"FATAL: mapping file is missing required columns: {sorted(missing)}\n"
            f"  Expected columns: int_id,protein_id"
        )
    mapping = mapping[["int_id", "protein_id"]]
    if mapping["int_id"].duplicated().any():
        n_dup = int(mapping["int_id"].duplicated().sum())
        raise ValueError(f"FATAL: mapping contains {n_dup:,} duplicate integer IDs")
    print(f"  Loaded {len(mapping):,} mapping rows in {time.time() - t0:.1f}s")
    return mapping


def restore_uniprot_ids(partition: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    print("Restoring UniProt accession codes...")
    t0 = time.time()

    restored = partition.merge(mapping, on="int_id", how="left", validate="one_to_one")
    missing_mapping = restored["protein_id"].isna()
    if missing_mapping.any():
        missing_ids = restored.loc[missing_mapping, "int_id"].head(10).tolist()
        raise ValueError(
            "FATAL: some partition node IDs are missing from protein_id_mapping.csv\n"
            f"  Missing count: {int(missing_mapping.sum()):,}\n"
            f"  First missing IDs: {missing_ids}"
        )

    extra_mapping = mapping.loc[~mapping["int_id"].isin(partition["int_id"])]
    if len(extra_mapping) > 0:
        print(f"  Note: mapping has {len(extra_mapping):,} IDs not present in partition; ignoring them")

    restored = restored.rename(columns={"protein_id": "uniprotAC"})
    restored = restored[["uniprotAC", "cluster_id"]]
    restored["__cluster_sort"] = restored["cluster_id"].map(cluster_sort_key)
    restored = restored.sort_values(
        ["__cluster_sort", "uniprotAC"],
        kind="mergesort",
    ).drop(columns=["__cluster_sort"]).reset_index(drop=True)

    print(f"  Restored {len(restored):,} rows in {time.time() - t0:.1f}s")
    print(f"  Clusters: {restored['cluster_id'].nunique():,}")
    return restored


def write_output(restored: pd.DataFrame, out: str, delimiter: str) -> None:
    print(f"Writing restored partition to {out}...")
    ensure_output_parent(out)
    restored.to_csv(out, sep=delimiter, index=False)
    print("  Written")


def main():
    parser = argparse.ArgumentParser(
        description="Restore partition integer node IDs to UniProt accession codes"
    )
    parser.add_argument("--partition", default="partition.csv",
                        help="Input integer partition file (default: partition.csv)")
    parser.add_argument("--mapping", default="protein_id_mapping.csv",
                        help="Input mapping file from 01_preprocess.py (default: protein_id_mapping.csv)")
    parser.add_argument("--out", default="restored_partition.csv",
                        help="Output restored partition file (default: restored_partition.csv)")
    parser.add_argument("--format", choices=["csv", "tsv"], default="csv",
                        help="Input/output file format (default: csv)")

    args = parser.parse_args()
    delimiter = format_to_delimiter(args.format)

    total_start = time.time()
    try:
        partition = load_partition(args.partition, delimiter)
        mapping = load_mapping(args.mapping, delimiter)
        restored = restore_uniprot_ids(partition, mapping)
        write_output(restored, args.out, delimiter)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nSUMMARY")
    print(f"  Partition: {args.partition}")
    print(f"  Mapping:   {args.mapping}")
    print(f"  Output:    {args.out}")
    print(f"  Total time: {time.time() - total_start:.1f}s")


if __name__ == "__main__":
    main()
