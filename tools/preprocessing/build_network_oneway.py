#!/usr/bin/env python3
"""
Build a protein neighborhood-Jaccard network for a one-way query folder.

This is intended for cases where:
  * --query points to a folder containing the seed protein files
  * --db points to the full reference DB used for neighbor lookups
  * only the seed proteins are used as queries
  * each seed writes to its own CSV in the output folder
  * a per-seed status manifest is also written for auditing

Query proteins are discovered from filenames in the query folder, and their
signal sets are read from that query folder. Neighbor signal sets are read from
the reference DB folder. Query self-accessions are removed in memory before
scoring; DB files are read unchanged.

The optional `_seed_status.tsv` sidecar is written without a header row. Its
columns are: seed_protein, query_status, edges_written. `query_status` is one
of: ok, ok_no_edges, missing, empty, error.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from functools import lru_cache
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, FrozenSet, List, TextIO, Tuple

from tqdm import tqdm

# Worker globals, initialized by _init_worker()
DB_FOLDER: Path | None = None
QUERY_FOLDER: Path | None = None
READ_DB = None
READ_QUERY = None
TREAT_MISSING_AS_SINGLETON = True
MIN_JACCARD = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build neighborhood-Jaccard network for one-way query folder"
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Folder containing the seed protein files to use as queries",
    )
    parser.add_argument("--db", required=True, help="Folder containing one file per protein")
    parser.add_argument(
        "--output-folder",
        required=True,
        help="Folder where one CSV per seed/query protein will be written",
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
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include hidden files in the query listing",
    )
    parser.add_argument(
        "--drop-self-from-sets",
        action="store_true",
        help=(
            "Accepted for compatibility; query self-accessions are always "
            "removed in memory and DB signal sets are read unchanged"
        ),
    )
    parser.add_argument(
        "--missing-as-empty",
        action="store_true",
        help="Treat missing neighbor files as empty instead of singleton {protein}",
    )
    parser.add_argument(
        "--min-jaccard",
        type=float,
        default=0.0,
        help="Only emit edges with Jaccard > this threshold (default: 0)",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> tuple[Path, Path, Path, int]:
    db_folder = Path(args.db).resolve()
    if not db_folder.exists():
        raise FileNotFoundError(f"Database folder does not exist: {db_folder}")
    if not db_folder.is_dir():
        raise NotADirectoryError(f"Database path is not a directory: {db_folder}")

    query_folder = Path(args.query).resolve()
    if not query_folder.exists():
        raise FileNotFoundError(f"Query folder does not exist: {query_folder}")
    if not query_folder.is_dir():
        raise NotADirectoryError(f"Query path is not a directory: {query_folder}")

    output_folder = Path(args.output_folder).resolve()
    output_folder.mkdir(parents=True, exist_ok=True)

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

    return db_folder, query_folder, output_folder, processes


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


def write_seed_edges(output_folder: Path, seed_protein: str, edges: list[tuple[str, str, float]]) -> int:
    output_file = output_folder / f"{seed_protein}.csv"
    edges.sort(key=lambda edge: (-edge[2], edge[1]))
    with open(output_file, "w", newline="") as handle:
        writer = csv.writer(handle)
        for _, neighbor, jaccard in edges:
            writer.writerow((seed_protein, neighbor, f"{jaccard:.4f}"))
    return len(edges)


def open_seed_status_writer(output_folder: Path) -> tuple[TextIO, Any]:
    status_file = output_folder / "_seed_status.tsv"
    handle = open(status_file, "w", newline="")
    writer = csv.writer(handle, delimiter="\t")
    return handle, writer


def _read_signals_from_folder(
    folder: Path | None,
    protein_ac: str,
    drop_self: bool,
    folder_label: str,
) -> tuple[str, FrozenSet[str]]:
    if folder is None:
        raise RuntimeError(f"Worker {folder_label} folder was not initialized")
    filepath = folder / protein_ac
    try:
        with open(filepath, "r") as handle:
            values = {line.strip() for line in handle if line.strip()}
    except FileNotFoundError:
        return ("missing", frozenset())
    except Exception as exc:
        print(f"[worker] Error reading {filepath}: {exc}", file=sys.stderr)
        return ("error", frozenset())

    if drop_self:
        values.discard(protein_ac)

    if not values:
        return ("empty", frozenset())
    return ("ok", frozenset(values))


def _read_db_uncached(protein_ac: str) -> tuple[str, FrozenSet[str]]:
    return _read_signals_from_folder(DB_FOLDER, protein_ac, False, "database")


def _read_query_uncached(protein_ac: str) -> tuple[str, FrozenSet[str]]:
    return _read_signals_from_folder(QUERY_FOLDER, protein_ac, True, "query")


def _init_worker(
    db_folder: str,
    query_folder: str,
    missing_as_empty: bool,
    cache_size: int,
    min_jaccard: float,
) -> None:
    global DB_FOLDER, MIN_JACCARD, QUERY_FOLDER, READ_DB, READ_QUERY, TREAT_MISSING_AS_SINGLETON
    DB_FOLDER = Path(db_folder)
    QUERY_FOLDER = Path(query_folder)
    TREAT_MISSING_AS_SINGLETON = not missing_as_empty
    MIN_JACCARD = min_jaccard

    READ_DB = lru_cache(maxsize=cache_size)(_read_db_uncached)
    READ_QUERY = lru_cache(maxsize=cache_size)(_read_query_uncached)


def jaccard_index(set_a: FrozenSet[str], set_b: FrozenSet[str]) -> float:
    if not set_a and not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def process_protein(protein_ac: str) -> tuple[str, List[Tuple[str, str, float]], Counter]:
    if READ_DB is None or READ_QUERY is None:
        raise RuntimeError("Worker reader was not initialized")

    stats = Counter()
    edges: List[Tuple[str, str, float]] = []

    status_a, signals_a = READ_QUERY(protein_ac)
    stats[f"query_{status_a}"] += 1

    if status_a != "ok":
        return status_a, edges, stats

    for signal_ac in signals_a:
        status_b, signals_b = READ_DB(signal_ac)
        stats[f"neighbor_{status_b}"] += 1

        if status_b == "missing":
            if TREAT_MISSING_AS_SINGLETON:
                signals_b = frozenset([signal_ac])
            else:
                signals_b = frozenset()
        elif status_b in {"error", "empty"}:
            signals_b = frozenset()

        jaccard = jaccard_index(signals_a, signals_b)
        if jaccard <= MIN_JACCARD:
            continue

        edges.append((protein_ac, signal_ac, jaccard))

    return status_a, edges, stats


def build_network_oneway(
    db_folder: Path,
    query_folder: Path,
    output_folder: Path,
    query_proteins: list[str],
    processes: int,
    cache_size: int,
    missing_as_empty: bool,
    min_jaccard: float,
) -> None:
    processes = min(processes, len(query_proteins))
    chunksize = max(1, min(1000, len(query_proteins) // (processes * 4)))

    print(f"DB folder      : {db_folder}")
    print(f"Query folder   : {query_folder}")
    print(f"Query proteins : {len(query_proteins):,}")
    print(f"Processes      : {processes}")
    print(f"Cache size     : {cache_size:,} per worker")
    print(f"Output folder  : {output_folder}")

    total_edges = 0
    totals = Counter()

    with Pool(
        processes=processes,
        initializer=_init_worker,
        initargs=(
            str(db_folder),
            str(query_folder),
            missing_as_empty,
            cache_size,
            min_jaccard,
        ),
    ) as pool:
        status_handle, status_writer = open_seed_status_writer(output_folder)
        try:
            with tqdm(total=len(query_proteins), desc="Processing proteins") as pbar:
                for i, (seed_protein, result) in enumerate(
                    zip(
                        query_proteins,
                        pool.imap(process_protein, query_proteins, chunksize=chunksize),
                    ),
                    start=1,
                ):
                    query_status, edges, stats = result
                    edges_written = write_seed_edges(output_folder, seed_protein, edges)
                    total_edges += edges_written
                    totals.update(stats)
                    final_status = query_status if query_status != "ok" else ("ok" if edges_written else "ok_no_edges")
                    status_writer.writerow((seed_protein, final_status, edges_written))
                    if i % 1000 == 0:
                        status_handle.flush()
                        pbar.set_description(f"Processing proteins (edges: {total_edges:,})")
                    pbar.update(1)
        finally:
            status_handle.flush()
            status_handle.close()

    print("\nRun complete")
    print(f"Total edges written : {total_edges:,}")
    print(f"Seed CSV files      : {len(query_proteins):,}")
    print(f"Seed status file    : {output_folder / '_seed_status.tsv'}")
    print("Status summary:")
    for key in sorted(totals):
        print(f"  {key:16s} {totals[key]:,}")


def main() -> None:
    args = parse_args()
    try:
        db_folder, query_folder, output_folder, processes = validate_args(args)
        query_proteins = list_protein_files(query_folder, include_hidden=args.include_hidden)
        if not query_proteins:
            raise ValueError(f"Query folder is empty: {query_folder}")

        build_network_oneway(
            db_folder=db_folder,
            query_folder=query_folder,
            output_folder=output_folder,
            query_proteins=query_proteins,
            processes=processes,
            cache_size=args.cache_size,
            missing_as_empty=args.missing_as_empty,
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
