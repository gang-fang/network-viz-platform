#!/usr/bin/env python3

import argparse
import os
import shutil
from pathlib import Path


def parse_q(filename: str):
    """
    Split at the first underscore.
    Returns q, or None if filename does not match q_t.
    """
    if "_" not in filename:
        return None
    q, _t = filename.split("_", 1)
    if not q:
        return None
    return q


def main():
    parser = argparse.ArgumentParser(
        description="Group files named q_t into destination/q/ subfolders."
    )
    parser.add_argument(
        "source_dir",
        help="Directory containing files named like q_t"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Destination directory. If omitted, you will be prompted."
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        help="Copy files instead of moving them"
    )
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    if not source_dir.is_dir():
        raise SystemExit(f"ERROR: source_dir is not a directory: {source_dir}")

    if args.output_dir is None:
        out = input("Enter destination directory: ").strip()
        if not out:
            raise SystemExit("ERROR: destination directory cannot be empty")
        output_dir = Path(out)
    else:
        output_dir = Path(args.output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    moved = 0
    skipped = 0

    for entry in os.scandir(source_dir):
        if not entry.is_file():
            continue

        name = entry.name
        q = parse_q(name)
        if q is None:
            skipped += 1
            continue

        q_dir = output_dir / q
        q_dir.mkdir(parents=True, exist_ok=True)

        src = Path(entry.path)
        dst = q_dir / name

        if dst.exists():
            print(f"SKIP already exists: {dst}")
            skipped += 1
            continue

        if args.copy:
            shutil.copy2(src, dst)
            print(f"COPY {src} -> {dst}")
        else:
            shutil.move(str(src), str(dst))
            print(f"MOVE {src} -> {dst}")

        moved += 1

    print(f"\nDone. Processed files: moved/copied={moved}, skipped={skipped}")


if __name__ == "__main__":
    main()
