#!/usr/bin/env python3
"""
preprocess_graph_v3.py
======================
Vectorized edge-list preprocessor. Reads a 3-column edge list
(NodeA, NodeB, Weight) and writes a compact binary adjacency index.

Compared to v1 (two-pass Python) and v2 (chunked pandas + per-row Python),
v3 builds adjacency entirely with NumPy bulk operations:

  - pd.factorize(...)              vectorized name -> integer ID assignment
  - interleaved COO arrays         (src, dst, w), both edge directions
  - np.lexsort((-w, src))          stable sort: group by src asc, weight desc
  - np.bincount + cumsum           degrees and byte offsets
  - structured-dtype .tofile       single-call binary write

Output:
  <prefix>.node_ids.tsv    integer ID -> string node name (one per line)
  <prefix>.adj.bin         packed (uint32 neighbor_id, float32 weight) pairs
  <prefix>.adj.index.bin   header (uint32 num_nodes) +
                           per-node (uint64 byte_offset, uint32 degree)

Input format: 3 columns. By default the file has NO header (matches the
project's edge files). Use --has-header to skip the first line.
Delimiter is auto-detected (comma, tab, space).
"""

import argparse
import struct
import sys
import time

import numpy as np
import pandas as pd


def detect_delimiter(filepath, has_header):
    """Sniff delimiter from the first non-empty data line; validate 3 fields with a numeric weight."""
    skip = 1 if has_header else 0
    sample = []
    with open(filepath, "r") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            if i < skip:
                continue
            sample.append(line)
            if len(sample) >= 5:
                break
    if not sample:
        sys.exit("ERROR: file is empty (after skipping header).")

    for cand, name in [(",", "comma"), ("\t", "tab"), (" ", "space")]:
        parts = sample[0].split(cand)
        if len(parts) >= 3:
            try:
                float(parts[2])
                print(f"  Detected delimiter: {name}")
                return cand
            except ValueError:
                continue
    sys.exit(f"ERROR: cannot detect delimiter from line: {sample[0]!r}")


def main():
    ap = argparse.ArgumentParser(
        description="Vectorized edge-list preprocessor (NumPy-backed)"
    )
    ap.add_argument("input_file", help="3-column edge file: NodeA<delim>NodeB<delim>Weight")
    ap.add_argument("-o", "--output_prefix", default="graph_index",
                    help="Prefix for output files (default: graph_index)")
    ap.add_argument("--has-header", action="store_true",
                    help="Skip the first line as a header (default: no header)")
    ap.add_argument("--top_k", type=int, default=None,
                    help="Per-node, keep only top-K neighbors by weight (optional)")
    args = ap.parse_args()

    t0 = time.time()
    print(f"Validating input: {args.input_file}")
    delim = detect_delimiter(args.input_file, args.has_header)

    # ---- Read entire file ----
    print("Reading edges...")
    df = pd.read_csv(
        args.input_file,
        sep=delim,
        header=None,
        skiprows=1 if args.has_header else 0,
        names=["A", "B", "W"],
        usecols=[0, 1, 2],
        dtype={"A": str, "B": str, "W": np.float32},
        engine="c",
        na_filter=False,
    )
    n_edges = len(df)
    if n_edges == 0:
        sys.exit("ERROR: no edges parsed.")
    print(f"  {n_edges:,} edges  ({time.time()-t0:.1f}s)")

    # ---- Vectorized name -> integer ID ----
    t1 = time.time()
    print("Factorizing node names...")
    a = df["A"].to_numpy()
    b = df["B"].to_numpy()
    w = df["W"].to_numpy(dtype=np.float32)
    del df

    # sort=False: IDs assigned in order of first appearance (consistent with v1/v2)
    codes, uniques = pd.factorize(np.concatenate([a, b]), sort=False)
    aid = codes[:n_edges].astype(np.uint32, copy=False)
    bid = codes[n_edges:].astype(np.uint32, copy=False)
    del codes, a, b
    num_nodes = len(uniques)
    print(f"  {num_nodes:,} unique nodes  ({time.time()-t1:.1f}s)")

    if num_nodes > 2**32 - 1:
        sys.exit(f"ERROR: {num_nodes:,} nodes exceeds uint32 capacity.")

    # ---- Interleaved COO (both directions) ----
    # Interleaving (not concat) ensures that within a group of tied weights,
    # the stored order matches v1's file-order semantics.
    t2 = time.time()
    print("Building COO arrays (both directions, interleaved)...")
    m = 2 * n_edges
    src = np.empty(m, dtype=np.uint32)
    dst = np.empty(m, dtype=np.uint32)
    weights = np.empty(m, dtype=np.float32)
    src[0::2] = aid;    src[1::2] = bid
    dst[0::2] = bid;    dst[1::2] = aid
    weights[0::2] = w;  weights[1::2] = w
    del aid, bid, w
    print(f"  {m:,} directed entries  ({time.time()-t2:.1f}s)")

    # ---- Sort: primary src asc, secondary weight desc, stable ----
    t3 = time.time()
    print("Sorting by (src, -weight)...")
    order = np.lexsort((-weights, src))
    src = src[order]
    dst = dst[order]
    weights = weights[order]
    del order
    print(f"  Sort took {time.time()-t3:.1f}s")

    # ---- Degrees ----
    degrees = np.bincount(src, minlength=num_nodes).astype(np.uint32)

    # ---- Optional top-K (vectorized) ----
    if args.top_k is not None and args.top_k > 0:
        t4 = time.time()
        print(f"Applying top-K = {args.top_k}...")
        # rank-within-group = global_index - group_start[src]
        group_start = np.empty(num_nodes, dtype=np.int64)
        group_start[0] = 0
        np.cumsum(degrees[:-1].astype(np.int64), out=group_start[1:])
        within = np.arange(len(src), dtype=np.int64) - group_start[src]
        keep = within < args.top_k
        dst = dst[keep]
        weights = weights[keep]
        src = src[keep]
        degrees = np.bincount(src, minlength=num_nodes).astype(np.uint32)
        print(f"  Reduced to {len(dst):,} entries  ({time.time()-t4:.1f}s)")

    total_entries = len(dst)

    # ---- Byte offsets (uint64 to be safe on big graphs) ----
    deg64 = degrees.astype(np.uint64) * 8  # bytes per node (4+4 per entry)
    offsets = np.empty(num_nodes, dtype=np.uint64)
    offsets[0] = 0
    np.cumsum(deg64[:-1], out=offsets[1:])

    # ---- Write outputs ----
    t5 = time.time()
    id_file = f"{args.output_prefix}.node_ids.tsv"
    adj_file = f"{args.output_prefix}.adj.bin"
    idx_file = f"{args.output_prefix}.adj.index.bin"

    print(f"Writing {id_file}...")
    with open(id_file, "w", buffering=1 << 20) as f:
        for i, name in enumerate(uniques):
            f.write(f"{i}\t{name}\n")

    print(f"Writing {adj_file}...")
    adj_arr = np.empty(total_entries, dtype=[("nbr", "<u4"), ("w", "<f4")])
    adj_arr["nbr"] = dst
    adj_arr["w"] = weights
    adj_arr.tofile(adj_file)
    del adj_arr

    print(f"Writing {idx_file}...")
    idx_arr = np.empty(num_nodes, dtype=[("offset", "<u8"), ("degree", "<u4")])
    idx_arr["offset"] = offsets
    idx_arr["degree"] = degrees
    with open(idx_file, "wb") as f:
        f.write(struct.pack("<I", num_nodes))
        idx_arr.tofile(f)

    file_size_adj = total_entries * 8
    file_size_idx = 4 + num_nodes * 12
    print(f"  adj.bin:       {file_size_adj/1e9:.2f} GB")
    print(f"  adj.index.bin: {file_size_idx/1e6:.1f} MB")
    print(f"  Write took {time.time()-t5:.1f}s")
    print(f"\nTotal: {time.time()-t0:.1f}s   ({n_edges:,} edges, {num_nodes:,} nodes, {total_entries:,} entries)")
    print("Done.")


if __name__ == "__main__":
    main()
