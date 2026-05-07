#!/usr/bin/env python3
"""
Build a protein neighborhood-Jaccard network from one directory of protein files.

This module provides the shared implementation used by the single-node, chunked,
and one-way entrypoints. It:
  * reads one file per protein from the reference DB folder
  * computes Jaccard scores between each query protein and its listed neighbors
  * uses worker-local LRU caching to reduce repeated small-file reads
  * distinguishes missing, empty, and unreadable neighbor files in run stats
  * prunes reciprocal query-query edges by default unless disabled

Default behavior remains conservative:
  * missing neighbor files can still be treated as singleton {protein}
  * self-accessions are kept in signal sets unless explicitly dropped
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from functools import lru_cache
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import FrozenSet, Iterable, List, Tuple

from tqdm import tqdm

# Worker globals, initialized by _init_worker()
DB_FOLDER: Path | None = None
READ_ONE = None
TREAT_MISSING_AS_SINGLETON = True
QUERY_PROTEIN_SET: FrozenSet[str] = frozenset()
CANONICAL_PAIRS = False
PRUNE_RECIPROCALS = False
MIN_JACCARD = 0.0


def add_common_args(
    parser: argparse.ArgumentParser,
    *,
    output_required: bool,
    output_default: str | None = None,
    add_include_hidden_flag: bool = False,
) -> argparse.ArgumentParser:
    parser.add_argument("--db", required=True, help="Folder containing one file per protein")
    parser.add_argument(
        "--output",
        required=output_required,
        default=output_default,
        help="Output CSV path",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=None,
        help="Worker process count (default: all visible CPUs)",
    )
    parser.add_argument(
        "--cache-size",
        type=int,
        default=5000,
        help="Per-worker LRU cache size for signal files (default: 5000)",
    )
    if add_include_hidden_flag:
        parser.add_argument(
            "--include-hidden",
            action="store_true",
            help="Include hidden files in the query listing",
        )
    parser.add_argument(
        "--drop-self-from-sets",
        action="store_true",
        help="Remove a protein's own accession from its signal set before scoring",
    )
    parser.add_argument(
        "--missing-as-empty",
        action="store_true",
        help="Treat missing neighbor files as empty instead of singleton {protein}",
    )
    parser.add_argument(
        "--canonical-pairs",
        action="store_true",
        help="Write each emitted pair as min(A,B), max(A,B)",
    )
    parser.add_argument(
        "--keep-reciprocal-edges",
        action="store_true",
        help="Keep both A-B and B-A when both directions exist",
    )
    parser.add_argument(
        "--min-jaccard",
        type=float,
        default=0.0,
        help="Only emit edges with Jaccard > this threshold (default: 0)",
    )
    return parser


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build protein neighborhood-Jaccard network on one node"
    )
    add_common_args(
        parser,
        output_required=False,
        output_default="protein_network.csv",
        add_include_hidden_flag=True,
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> tuple[Path, Path, int]:
    db_folder = Path(args.db).resolve()
    if not db_folder.exists():
        raise FileNotFoundError(f"Database folder does not exist: {db_folder}")
    if not db_folder.is_dir():
        raise NotADirectoryError(f"Database path is not a directory: {db_folder}")

    if args.processes is None:
        processes = cpu_count()
    else:
        if args.processes <= 0:
            raise ValueError("--processes must be a positive integer")
        processes = args.processes

    if args.cache_size < 0:
        raise ValueError("--cache-size must be >= 0")
    if not (0.0 <= args.min_jaccard <= 1.0):
        raise ValueError("--min-jaccard must be between 0 and 1")

    output_file = Path(args.output)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    return db_folder, output_file, processes


def list_protein_files(folder: Path, include_hidden: bool) -> List[str]:
    files: List[str] = []
    for path in folder.iterdir():
        if not path.is_file():
            continue
        name = path.name
        if not include_hidden and name.startswith("."):
            continue
        files.append(name)
    files.sort()
    return files


def validate_query_overlap(query_proteins: List[str], db_names: FrozenSet[str]) -> None:
    overlap = sum(1 for protein in query_proteins if protein in db_names)
    total = len(query_proteins)
    print(f"Query/DB overlap: {overlap:,}/{total:,}")
    if overlap == 0:
        raise ValueError("No query proteins were found in the database folder")
    if overlap < total:
        print(
            "Warning: some query proteins are missing from the database folder.",
            file=sys.stderr,
        )


def _read_signals_uncached(protein_ac: str, keep_self_in_sets: bool) -> tuple[str, FrozenSet[str]]:
    if DB_FOLDER is None:
        raise RuntimeError("Worker database folder was not initialized")
    filepath = DB_FOLDER / protein_ac
    try:
        with open(filepath, "r") as handle:
            values = {line.strip() for line in handle if line.strip()}
    except FileNotFoundError:
        return ("missing", frozenset())
    except Exception as exc:
        print(f"[worker] Error reading {filepath}: {exc}", file=sys.stderr)
        return ("error", frozenset())

    if not keep_self_in_sets:
        values.discard(protein_ac)

    if not values:
        return ("empty", frozenset())
    return ("ok", frozenset(values))


def _init_worker(
    db_folder: str,
    keep_self_in_sets: bool,
    missing_as_empty: bool,
    cache_size: int,
    query_protein_set: FrozenSet[str],
    canonical_pairs: bool,
    prune_reciprocals: bool,
    min_jaccard: float,
) -> None:
    global CANONICAL_PAIRS, DB_FOLDER, MIN_JACCARD, PRUNE_RECIPROCALS, QUERY_PROTEIN_SET, READ_ONE, TREAT_MISSING_AS_SINGLETON
    DB_FOLDER = Path(db_folder)
    TREAT_MISSING_AS_SINGLETON = not missing_as_empty
    QUERY_PROTEIN_SET = query_protein_set
    CANONICAL_PAIRS = canonical_pairs
    PRUNE_RECIPROCALS = prune_reciprocals
    MIN_JACCARD = min_jaccard

    def _reader(protein_ac: str) -> tuple[str, FrozenSet[str]]:
        return _read_signals_uncached(protein_ac, keep_self_in_sets)

    READ_ONE = lru_cache(maxsize=cache_size)(_reader)


def jaccard_index(set_a: FrozenSet[str], set_b: FrozenSet[str]) -> float:
    if not set_a and not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def process_protein(protein_ac: str) -> tuple[List[Tuple[str, str, float]], Counter]:
    if READ_ONE is None:
        raise RuntimeError("Worker reader was not initialized")

    stats = Counter()
    edges: List[Tuple[str, str, float]] = []

    status_a, signals_a = READ_ONE(protein_ac)
    stats[f"query_{status_a}"] += 1

    if status_a != "ok":
        return edges, stats

    for signal_ac in signals_a:
        if signal_ac == protein_ac:
            continue

        status_b, signals_b = READ_ONE(signal_ac)
        stats[f"neighbor_{status_b}"] += 1

        if status_b == "missing":
            if TREAT_MISSING_AS_SINGLETON:
                signals_b = frozenset([signal_ac])
            else:
                signals_b = frozenset()
        elif status_b in {"error", "empty"}:
            signals_b = frozenset()

        # Only prune when the reverse edge could also be emitted from the query set.
        if (
            PRUNE_RECIPROCALS
            and signal_ac in QUERY_PROTEIN_SET
            and protein_ac in signals_b
            and signal_ac < protein_ac
        ):
            stats["reciprocal_pruned"] += 1
            continue

        jaccard = jaccard_index(signals_a, signals_b)
        if jaccard <= MIN_JACCARD:
            continue

        a, b = protein_ac, signal_ac
        if CANONICAL_PAIRS and b < a:
            a, b = b, a
        edges.append((a, b, jaccard))

    return edges, stats


def write_edges_batch(edges: Iterable[Tuple[str, str, float]], output_file: Path, mode: str = "a") -> None:
    with open(output_file, mode, newline="") as handle:
        writer = csv.writer(handle)
        for protein_a, protein_b, jaccard in edges:
            writer.writerow((protein_a, protein_b, f"{jaccard:.4f}"))


def build_network(
    db_folder: Path,
    output_file: Path,
    query_proteins: List[str],
    processes: int,
    cache_size: int,
    keep_self_in_sets: bool,
    missing_as_empty: bool,
    canonical_pairs: bool,
    prune_reciprocals: bool,
    min_jaccard: float,
) -> None:
    query_protein_set = frozenset(query_proteins)
    processes = min(processes, len(query_protein_set))
    chunksize = max(1, min(1000, len(query_protein_set) // (processes * 4)))

    print(f"DB folder      : {db_folder}")
    print(f"Query proteins : {len(query_proteins):,}")
    print(f"Processes      : {processes}")
    print(f"Cache size     : {cache_size:,} per worker")
    if prune_reciprocals:
        print("Prune reciproc.: True (query-query only)")
    else:
        print("Prune reciproc.: False")
    print(f"Output         : {output_file}")

    output_file.write_text("")

    total_edges = 0
    totals = Counter()

    with Pool(
        processes=processes,
        initializer=_init_worker,
        initargs=(
            str(db_folder),
            keep_self_in_sets,
            missing_as_empty,
            cache_size,
            query_protein_set,
            canonical_pairs,
            prune_reciprocals,
            min_jaccard,
        ),
    ) as pool:
        with tqdm(total=len(query_proteins), desc="Processing proteins") as pbar:
            for i, (edges, stats) in enumerate(
                pool.imap_unordered(process_protein, query_proteins, chunksize=chunksize),
                start=1,
            ):
                if edges:
                    write_edges_batch(edges, output_file, mode="a")
                    total_edges += len(edges)
                totals.update(stats)
                pbar.update(1)
                if i % 1000 == 0:
                    pbar.set_description(f"Processing proteins (edges: {total_edges:,})")

    print("\nRun complete")
    print(f"Total edges written : {total_edges:,}")
    print("Status summary:")
    for key in sorted(totals):
        print(f"  {key:16s} {totals[key]:,}")


def main() -> None:
    args = parse_args()
    try:
        db_folder, output_file, processes = validate_args(args)
        query_proteins = list_protein_files(db_folder, include_hidden=args.include_hidden)
        build_network(
            db_folder=db_folder,
            output_file=output_file,
            query_proteins=query_proteins,
            processes=processes,
            cache_size=args.cache_size,
            keep_self_in_sets=not args.drop_self_from_sets,
            missing_as_empty=args.missing_as_empty,
            canonical_pairs=args.canonical_pairs,
            prune_reciprocals=not args.keep_reciprocal_edges,
            min_jaccard=args.min_jaccard,
        )
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
