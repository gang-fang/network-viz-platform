#!/usr/bin/env python3
"""
make_2d.py

Input:
  A directory containing many files named like:
      q_t
  Example:
      UP002_UP000035

Behavior:
  1. Scan the directory and group files by q.
  2. For each q_t file:
       - parse as CSV
       - ignore the optional header:
           "Query ID","Query Length","Hit ID","Hit Score","Hit Length"
       - require exactly 5 columns per data row
       - validate types:
           col1: string
           col2: integer
           col3: string
           col4: float
           col5: integer
       - for each Query ID in that file, keep top N rows by Hit Score
  3. Merge selected rows across all t-files for the same q.
  4. Write one output file per query protein:
         output_dir/<q>/<query_id>

Output format (no header):
    Hit ID,Ratio,Hit Score

Where:
    Ratio = max(Query Length, Hit Length) / min(Query Length, Hit Length)

Allowed normal cases:
  - q == t
  - Query ID == Hit ID
"""

from __future__ import annotations

import argparse
import csv
import heapq
import os
import re
import sys
import tempfile
from collections import defaultdict, OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, Iterator, List, Optional, TextIO, Tuple


HEADER = ["Query ID", "Query Length", "Hit ID", "Hit Score", "Hit Length"]
FILENAME_RE = re.compile(r"^([^_]+)_(.+)$")


@dataclass(order=True)
class HitRecord:
    """
    Ordered by score first so it works naturally in a min-heap.
    """
    score: float
    hit_id: str
    ratio: float


class LimitedFilePool:
    """
    Keep only a bounded number of file handles open at once.
    Least-recently-used handles are closed automatically.
    """

    def __init__(
        self,
        max_open: int,
        mode: str,
        encoding: Optional[str] = "utf-8",
        newline: Optional[str] = None,
    ) -> None:
        if max_open <= 0:
            raise ValueError("max_open must be > 0")
        self.max_open = max_open
        self.mode = mode
        self.encoding = encoding
        self.newline = newline
        self._handles: "OrderedDict[str, TextIO]" = OrderedDict()

    def get(self, path: Path) -> TextIO:
        key = str(path)

        fh = self._handles.pop(key, None)
        if fh is None:
            path.parent.mkdir(parents=True, exist_ok=True)

            kwargs = {}
            if "b" not in self.mode:
                kwargs["encoding"] = self.encoding
                if self.newline is not None:
                    kwargs["newline"] = self.newline

            fh = open(path, self.mode, **kwargs)

        self._handles[key] = fh

        while len(self._handles) > self.max_open:
            _, old_fh = self._handles.popitem(last=False)
            old_fh.close()

        return fh

    def close(self) -> None:
        for fh in self._handles.values():
            fh.close()
        self._handles.clear()

    def __enter__(self) -> "LimitedFilePool":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Combine top-N hits per query protein across q_t files."
    )
    parser.add_argument(
        "input_dir",
        help="Directory containing input files named q_t"
    )
    parser.add_argument(
        "-n",
        "--topn",
        type=int,
        required=True,
        help="Top N rows to keep per query protein per file, ranked by Hit Score"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Output directory. If omitted, the script prompts for it."
    )
    parser.add_argument(
        "--tmp-dir",
        default=None,
        help="Temporary directory for bucket files"
    )
    parser.add_argument(
        "--num-buckets",
        type=int,
        default=4096,
        help="Number of bucket files used for q-grouping (default: 4096)"
    )
    parser.add_argument(
        "--max-open-files",
        type=int,
        default=128,
        help="Maximum number of simultaneously open files (default: 128)"
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Input text encoding (default: utf-8)"
    )
    parser.add_argument(
        "--errors-file",
        default=None,
        help="Optional log file for invalid filenames or invalid CSV files"
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively scan subdirectories under input_dir"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Abort on the first invalid file instead of logging and skipping"
    )
    return parser.parse_args()


def prompt_for_output_dir() -> Path:
    out = input("Enter output directory: ").strip()
    if not out:
        raise ValueError("Output directory cannot be empty.")
    path = Path(out)
    path.mkdir(parents=True, exist_ok=True)
    return path


def open_error_log(path: Optional[str]) -> Optional[TextIO]:
    if path is None:
        return None
    return open(path, "w", encoding="utf-8")


def log_error(err_fh: Optional[TextIO], message: str) -> None:
    if err_fh is not None:
        err_fh.write(message.rstrip() + "\n")


def safe_output_name(name: str) -> str:
    """
    Make output filenames safer across filesystems.
    Protein IDs are usually already safe, but this avoids path issues.
    """
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)


def ratio_from_lengths(query_len: int, hit_len: int) -> float:
    if query_len <= 0 or hit_len <= 0:
        raise ValueError("Lengths must be positive integers")
    return max(query_len, hit_len) / min(query_len, hit_len)


def iter_directory_files(input_dir: Path, recursive: bool) -> Iterator[Path]:
    """
    Iterate regular files under input_dir.
    """
    if recursive:
        for root, _, files in os.walk(input_dir):
            for name in files:
                path = Path(root) / name
                try:
                    if path.is_file():
                        yield path
                except OSError:
                    continue
    else:
        with os.scandir(input_dir) as it:
            for entry in it:
                try:
                    if entry.is_file(follow_symlinks=False):
                        yield Path(entry.path)
                except OSError:
                    continue


def parse_q_t_from_filename(filename: str) -> Optional[Tuple[str, str]]:
    """
    Split filename at the first underscore:
        UP002_UP000035 -> ("UP002", "UP000035")
    """
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    return m.group(1), m.group(2)


def bucket_index_for_q(q: str, num_buckets: int) -> int:
    """
    Deterministic hash for q, stable across runs.
    """
    h = 2166136261
    for b in q.encode("utf-8", errors="ignore"):
        h ^= b
        h = (h * 16777619) & 0xFFFFFFFF
    return h % num_buckets


def row_is_header(row: List[str]) -> bool:
    if len(row) != 5:
        return False
    normalized = [x.strip().strip('"') for x in row]
    return normalized == HEADER


def parse_csv_row_types(row: List[str]) -> Tuple[str, int, str, float, int]:
    """
    Validate one CSV data row and return typed fields.
    """
    if len(row) != 5:
        raise ValueError(f"Expected 5 columns, got {len(row)}")

    query_id = row[0]
    query_len = int(row[1])
    hit_id = row[2]
    hit_score = float(row[3])
    hit_len = int(row[4])

    if query_id == "":
        raise ValueError("Query ID is empty")
    if hit_id == "":
        raise ValueError("Hit ID is empty")

    return query_id, query_len, hit_id, hit_score, hit_len


def build_q_buckets(
    input_dir: Path,
    tmp_dir: Path,
    num_buckets: int,
    max_open_files: int,
    recursive: bool,
    err_fh: Optional[TextIO],
) -> List[Path]:
    """
    First pass:
      scan input files
      write lines:
          q<TAB>full_path
      into bucket files based on q

    This avoids building a huge q -> list_of_files dictionary in memory.
    """
    bucket_paths = [tmp_dir / f"bucket_{i:05d}.tsv" for i in range(num_buckets)]

    with LimitedFilePool(max_open=max_open_files, mode="a", encoding="utf-8") as pool:
        for path in iter_directory_files(input_dir, recursive=recursive):
            parsed = parse_q_t_from_filename(path.name)
            if parsed is None:
                log_error(err_fh, f"SKIP bad filename (missing underscore): {path}")
                continue

            q, _t = parsed
            idx = bucket_index_for_q(q, num_buckets)
            fh = pool.get(bucket_paths[idx])
            fh.write(f"{q}\t{path}\n")

    return bucket_paths


def topn_per_query_from_file(
    file_path: Path,
    topn: int,
    encoding: str,
) -> Dict[str, List[HitRecord]]:
    """
    Read one q_t file and return:
        query_id -> top-N HitRecord list from this file

    Top-N selection is done independently for each Query ID in the file.
    """
    per_query_heaps: DefaultDict[str, List[HitRecord]] = defaultdict(list)

    with open(file_path, "r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh)
        first_nonempty_seen = False

        for line_no, row in enumerate(reader, start=1):
            if not row:
                continue

            if not first_nonempty_seen:
                first_nonempty_seen = True
                if row_is_header(row):
                    continue

            try:
                query_id, query_len, hit_id, hit_score, hit_len = parse_csv_row_types(row)
                ratio = ratio_from_lengths(query_len, hit_len)
            except Exception as exc:
                raise ValueError(f"{file_path}:{line_no}: {exc}") from exc

            rec = HitRecord(score=hit_score, hit_id=hit_id, ratio=ratio)
            heap = per_query_heaps[query_id]

            if len(heap) < topn:
                heapq.heappush(heap, rec)
            elif rec.score > heap[0].score:
                heapq.heapreplace(heap, rec)

    result: Dict[str, List[HitRecord]] = {}
    for query_id, heap in per_query_heaps.items():
        result[query_id] = sorted(heap, key=lambda x: x.score, reverse=True)

    return result


def append_hits_to_query_outputs(
    q: str,
    q_files: Iterable[Path],
    output_dir: Path,
    topn: int,
    encoding: str,
    max_open_files: int,
    err_fh: Optional[TextIO],
    strict: bool,
) -> None:
    """
    Process all files belonging to one q and write outputs under:

        output_dir/<q>/<query_id>

    Each output line:
        Hit ID,Ratio,Hit Score

    No header is written.
    """
    q_dir = output_dir / safe_output_name(q)
    q_dir.mkdir(parents=True, exist_ok=True)

    with LimitedFilePool(
        max_open=max_open_files,
        mode="a",
        encoding="utf-8",
        newline="",
    ) as pool:
        for file_path in q_files:
            try:
                per_query = topn_per_query_from_file(
                    file_path=file_path,
                    topn=topn,
                    encoding=encoding,
                )
            except Exception as exc:
                msg = f"SKIP invalid file: {file_path} | {exc}"
                if strict:
                    raise RuntimeError(msg) from exc
                log_error(err_fh, msg)
                continue

            for query_id, hits in per_query.items():
                out_path = q_dir / safe_output_name(query_id)

                # Important:
                # do not cache csv.writer objects, because the underlying
                # file handle may have been closed by the LRU pool.
                fh = pool.get(out_path)
                writer = csv.writer(fh, lineterminator="\n")

                for rec in hits:
                    writer.writerow([
                        rec.hit_id,
                        f"{rec.ratio:.3f}",
                        f"{rec.score:.10g}",
                    ])


def process_bucket_file(
    bucket_path: Path,
    output_dir: Path,
    topn: int,
    encoding: str,
    max_open_files: int,
    err_fh: Optional[TextIO],
    strict: bool,
) -> None:
    """
    Read one bucket file, regroup in memory only for that bucket:
        q -> [file paths]
    then process each q independently.
    """
    if not bucket_path.exists():
        return

    q_to_files: DefaultDict[str, List[Path]] = defaultdict(list)

    with open(bucket_path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.rstrip("\n")
            if not line:
                continue

            try:
                q, path_str = line.split("\t", 1)
            except ValueError:
                msg = f"Malformed bucket line {bucket_path}:{line_no}"
                if strict:
                    raise RuntimeError(msg)
                log_error(err_fh, msg)
                continue

            q_to_files[q].append(Path(path_str))

    for q, q_files in q_to_files.items():
        append_hits_to_query_outputs(
            q=q,
            q_files=q_files,
            output_dir=output_dir,
            topn=topn,
            encoding=encoding,
            max_open_files=max_open_files,
            err_fh=err_fh,
            strict=strict,
        )


def main() -> int:
    args = parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"ERROR: input_dir is not a directory: {input_dir}", file=sys.stderr)
        return 1

    if args.topn <= 0:
        print("ERROR: --topn must be > 0", file=sys.stderr)
        return 1

    if args.output_dir is None:
        output_dir = prompt_for_output_dir()
    else:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

    tmp_parent = Path(args.tmp_dir) if args.tmp_dir else None
    err_fh = open_error_log(args.errors_file)

    try:
        with tempfile.TemporaryDirectory(
            prefix="q_buckets_",
            dir=str(tmp_parent) if tmp_parent else None,
        ) as td:
            tmp_dir = Path(td)

            print("[1/2] Scanning input directory and building q-buckets...")
            bucket_paths = build_q_buckets(
                input_dir=input_dir,
                tmp_dir=tmp_dir,
                num_buckets=args.num_buckets,
                max_open_files=args.max_open_files,
                recursive=args.recursive,
                err_fh=err_fh,
            )

            print("[2/2] Processing q-buckets...")
            total = len(bucket_paths)

            for i, bucket_path in enumerate(bucket_paths, start=1):
                if i % 100 == 0 or i == total:
                    print(f"  bucket {i}/{total}")

                process_bucket_file(
                    bucket_path=bucket_path,
                    output_dir=output_dir,
                    topn=args.topn,
                    encoding=args.encoding,
                    max_open_files=args.max_open_files,
                    err_fh=err_fh,
                    strict=args.strict,
                )

        print("Done.")
        return 0

    finally:
        if err_fh is not None:
            err_fh.close()


if __name__ == "__main__":
    raise SystemExit(main())
