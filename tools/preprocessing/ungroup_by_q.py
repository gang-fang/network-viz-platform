#!/usr/bin/env python3

import argparse
import shutil
from pathlib import Path


def is_empty_directory(path: Path) -> bool:
    try:
        next(path.iterdir())
        return False
    except StopIteration:
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Move files from input_dir/folder/files into one destination folder."
    )
    parser.add_argument(
        "input_dir",
        help="Input directory containing subfolders with files"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Destination folder. If omitted, you will be prompted."
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir).resolve()
    if not input_dir.is_dir():
        raise SystemExit(f"ERROR: input_dir is not a directory: {input_dir}")

    if args.output_dir is None:
        out = input("Enter destination folder: ").strip()
        if not out:
            raise SystemExit("ERROR: destination folder cannot be empty")
        output_dir = Path(out).resolve()
    else:
        output_dir = Path(args.output_dir).resolve()

    if output_dir == input_dir:
        raise SystemExit("ERROR: destination folder cannot be the same as input_dir")

    output_dir.mkdir(parents=True, exist_ok=True)

    moved_files = 0
    skipped_folders = 0
    deleted_folders = 0

    # Expected structure:
    # input_dir/
    #   folder/
    #     files
    for folder in sorted(input_dir.iterdir()):
        if not folder.is_dir():
            continue

        files = [p for p in sorted(folder.iterdir()) if p.is_file()]

        if not files:
            if is_empty_directory(folder):
                folder.rmdir()
                print(f"DELETED EMPTY FOLDER: {folder}")
                deleted_folders += 1
            continue

        # Check duplicates in destination first.
        collisions = [f.name for f in files if (output_dir / f.name).exists()]
        if collisions:
            print(f"ERROR: duplicate filename(s) found for folder {folder}")
            for name in collisions:
                print(f"  DUPLICATE: {name}")
            print("  -> skipping this folder; no files moved from it")
            skipped_folders += 1
            continue

        # Move all files from this folder.
        for src in files:
            dst = output_dir / src.name
            shutil.move(str(src), str(dst))
            print(f"MOVED: {src} -> {dst}")
            moved_files += 1

        # Delete folder only if now empty.
        if is_empty_directory(folder):
            folder.rmdir()
            print(f"DELETED EMPTY FOLDER: {folder}")
            deleted_folders += 1

    print("\nDone.")
    print(f"Files moved: {moved_files}")
    print(f"Folders skipped due to duplicates: {skipped_folders}")
    print(f"Empty folders deleted: {deleted_folders}")


if __name__ == "__main__":
    main()
