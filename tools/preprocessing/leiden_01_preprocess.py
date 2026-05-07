#!/usr/bin/env python3
"""
Preprocess a protein edge list for Leiden clustering.

Converts string protein IDs to dense 0-based integer IDs and writes the reverse
mapping needed to restore final partitions back to original protein IDs.

Input/output format, CSV by default:
    string1,string2,sji

Output files:
    edges.csv:   int_src,int_dst,sji, no header by default
    mapping.csv: int_id,protein_id, with header

The integer IDs are dense and 0-based, which is the format Script 02 passes to
cuGraph. Keep the mapping file so integer partitions can be restored to the
original protein IDs after Script 02/03 finish.
"""

import argparse
import csv
import os
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


def temp_output_path(path: str) -> Path:
    final_path = Path(path)
    return final_path.with_name(f".{final_path.name}.tmp.{os.getpid()}")


def read_edge_chunks(path: str, delimiter: str, has_header: bool, chunksize: int):
    read_kwargs = {
        "sep": delimiter,
        "usecols": [0, 1, 2],
        "chunksize": chunksize,
    }
    if has_header:
        read_kwargs["header"] = 0
    else:
        read_kwargs["header"] = None
        read_kwargs["names"] = ["string1", "string2", "sji"]

    for chunk in pd.read_csv(path, **read_kwargs):
        chunk.columns = ["string1", "string2", "sji"]
        yield chunk


def build_mapping(path: str, delimiter: str, has_header: bool, chunksize: int, sort_ids: bool):
    print("Pass 1: building protein ID mapping...")
    t0 = time.time()

    if sort_ids:
        unique_ids = set()
        for chunk in read_edge_chunks(path, delimiter, has_header, chunksize):
            unique_ids.update(chunk["string1"].astype(str))
            unique_ids.update(chunk["string2"].astype(str))
        id_to_int = {protein_id: idx for idx, protein_id in enumerate(sorted(unique_ids))}
    else:
        id_to_int = {}
        for chunk in read_edge_chunks(path, delimiter, has_header, chunksize):
            for protein_id in pd.concat([chunk["string1"], chunk["string2"]], ignore_index=True).astype(str):
                if protein_id not in id_to_int:
                    id_to_int[protein_id] = len(id_to_int)

    print(f"  Mapped {len(id_to_int):,} unique protein IDs in {time.time() - t0:.1f}s")
    return id_to_int


def write_mapping(id_to_int, mapping_path: str, delimiter: str) -> None:
    print(f"Writing mapping to {mapping_path}...")
    ensure_output_parent(mapping_path)
    final_path = Path(mapping_path)
    tmp_path = temp_output_path(mapping_path)
    try:
        with open(tmp_path, "w", newline="") as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(["int_id", "protein_id"])
            for protein_id, int_id in sorted(id_to_int.items(), key=lambda item: item[1]):
                writer.writerow([int_id, protein_id])
        tmp_path.replace(final_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    print("  Mapping written")


def write_integer_edges(path: str,
                        out_edges: str,
                        delimiter: str,
                        has_header: bool,
                        chunksize: int,
                        id_to_int,
                        output_header: bool) -> None:
    print(f"Pass 2: writing integer edge list to {out_edges}...")
    t0 = time.time()
    ensure_output_parent(out_edges)
    final_path = Path(out_edges)
    tmp_path = temp_output_path(out_edges)

    rows_written = 0
    first_chunk = True
    try:
        for chunk in read_edge_chunks(path, delimiter, has_header, chunksize):
            out = pd.DataFrame({
                "src": chunk["string1"].astype(str).map(id_to_int).astype("int64"),
                "dst": chunk["string2"].astype(str).map(id_to_int).astype("int64"),
                "weight": chunk["sji"].astype("float32"),
            })
            out.to_csv(
                tmp_path,
                sep=delimiter,
                index=False,
                header=output_header and first_chunk,
                mode="w" if first_chunk else "a",
            )
            rows_written += len(out)
            first_chunk = False
        tmp_path.replace(final_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise

    print(f"  Wrote {rows_written:,} edges in {time.time() - t0:.1f}s")


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess string protein IDs into dense 0-based integer IDs for Leiden clustering"
    )
    parser.add_argument("--input", required=True, help="Input edge file: string1,string2,sji")
    parser.add_argument("--out_edges", required=True, help="Output integer edge file for Script 02")
    parser.add_argument("--out_mapping", default="protein_id_mapping.csv",
                        help="Output int_id to protein_id mapping file (default: protein_id_mapping.csv)")
    parser.add_argument("--format", choices=["csv", "tsv"], default="csv",
                        help="Input/output file format (default: csv)")
    parser.add_argument("--has_header", action="store_true", help="Input file has a header row")
    parser.add_argument("--chunksize", type=int, default=5_000_000,
                        help="Rows per pandas chunk (default: 5,000,000)")
    parser.add_argument("--sort_ids", action="store_true",
                        help="Assign IDs by sorted protein ID instead of first-seen order; requires holding all unique IDs in memory")
    parser.add_argument("--output_header", action="store_true",
                        help="Write a header row to the integer edge file; Script 02 then needs --has_header")

    args = parser.parse_args()

    if args.chunksize <= 0:
        parser.error("--chunksize must be > 0")

    delimiter = format_to_delimiter(args.format)

    total_start = time.time()
    try:
        id_to_int = build_mapping(
            args.input,
            delimiter,
            args.has_header,
            args.chunksize,
            args.sort_ids,
        )
        write_mapping(id_to_int, args.out_mapping, delimiter)
        write_integer_edges(
            args.input,
            args.out_edges,
            delimiter,
            args.has_header,
            args.chunksize,
            id_to_int,
            args.output_header,
        )
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nSUMMARY")
    print(f"  Unique proteins: {len(id_to_int):,}")
    print(f"  Integer edges:   {args.out_edges}")
    print(f"  Mapping:         {args.out_mapping}")
    print(f"  Total time:      {time.time() - total_start:.1f}s")


if __name__ == "__main__":
    main()
