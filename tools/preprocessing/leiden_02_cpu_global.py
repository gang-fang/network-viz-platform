#!/usr/bin/env python3
"""
CPU-based global Leiden clustering using python-igraph and leidenalg.

This mirrors 02_gpu_leiden_global.py for CPU-only environments:
1. Loads a preprocessed integer edge list (CSV by default: src, dst, weight)
2. Canonicalizes an undirected graph by default
3. Builds an undirected igraph graph
4. Runs global Leiden clustering
5. Writes integer-ID partition.csv, metrics.csv, and optional subnetworks/subnet.* files for Script 03

For large production graphs, the GPU version is strongly preferred. A graph with
hundreds of millions of edges can require hundreds of GB of CPU RAM with pandas,
igraph, Leiden working memory, and optional subnetwork generation.
"""

import argparse
import csv
import gc
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import igraph as ig
    import leidenalg
except ImportError:
    print("ERROR: igraph or leidenalg not found.")
    print("Install with: pip install python-igraph leidenalg")
    sys.exit(1)


def normalize_delimiter(delimiter: str) -> str:
    if delimiter in ("tab", "\\t", "tsv"):
        return "\t"
    if delimiter in ("comma", "csv"):
        return ","
    if len(delimiter) != 1:
        raise ValueError("--delimiter must be a single character, 'comma', 'csv', 'tab', 'tsv', or '\\t'")
    return delimiter


def ensure_output_parent(path: str) -> None:
    parent = Path(path).expanduser().parent
    if parent != Path('.'):
        parent.mkdir(parents=True, exist_ok=True)


def temp_output_path(path) -> Path:
    final_path = Path(path)
    return final_path.with_name(f".{final_path.name}.tmp.{os.getpid()}.{time.time_ns()}")


def atomic_to_csv(df: pd.DataFrame, path, sep: str, index: bool = False, header: bool = True) -> None:
    final_path = Path(path)
    tmp_path = temp_output_path(final_path)
    try:
        df.to_csv(tmp_path, sep=sep, index=index, header=header)
        tmp_path.replace(final_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def validate_edge_rows(path: str, has_header: bool, validation_rows: int, delimiter: str) -> None:
    with open(path, 'r', newline='') as f:
        reader = csv.reader(f, delimiter=delimiter)
        lines_checked = 0
        for line_num, parts in enumerate(reader, 1):
            if not parts or all(not str(part).strip() for part in parts):
                continue
            if has_header and line_num == 1:
                if len(parts) > 3:
                    print(f"  Note: Header has {len(parts)} columns, using first 3", file=sys.stderr)
                continue
            if len(parts) != 3:
                raise ValueError(
                    f"FATAL: Data line {line_num} has {len(parts)} columns (expected exactly 3)\n"
                    f"  Expected format: src{delimiter}dst{delimiter}weight\n"
                    "  Did you run 01_preprocess.py?"
                )
            lines_checked += 1
            if validation_rows > 0 and lines_checked >= validation_rows:
                break

    if validation_rows > 0:
        print(f"  Sample-validated {lines_checked} data rows have exactly 3 columns")
    else:
        print(f"  Validated all {lines_checked:,} data rows have exactly 3 columns")


def load_edgelist(path: str, has_header: bool, validation_rows: int, delimiter: str) -> pd.DataFrame:
    print(f"Loading edge list from {path}...")
    t0 = time.time()
    validate_edge_rows(path, has_header, validation_rows, delimiter)

    read_kwargs = {
        "sep": delimiter,
        "usecols": [0, 1, 2],
    }
    if has_header:
        read_kwargs["header"] = 0
    else:
        read_kwargs["header"] = None
        read_kwargs["names"] = ["src", "dst", "weight"]

    try:
        if has_header:
            df = pd.read_csv(path, **read_kwargs)
        else:
            df = pd.read_csv(
                path,
                dtype={"src": "int64", "dst": "int64", "weight": "float32"},
                **read_kwargs,
            )
    except Exception as e:
        raise ValueError(
            f"FATAL: Failed to load integer edge file: {path}\n"
            f"  Error: {e}\n"
            "  Expected dense integer IDs from 01_preprocess.py: src,dst,weight"
        )

    df.columns = ["src", "dst", "weight"]
    df["src"] = df["src"].astype("int64")
    df["dst"] = df["dst"].astype("int64")
    df["weight"] = df["weight"].astype("float32")
    print(f"  Loaded {len(df):,} edges in {time.time() - t0:.1f}s")
    print(f"  Edge DataFrame memory: {df.memory_usage(deep=True).sum() / 1e9:.2f} GB")
    return df


def canonicalize_edges(df: pd.DataFrame, weight_agg: str) -> pd.DataFrame:
    print("Canonicalizing undirected edges...")
    t0 = time.time()
    original_count = len(df)
    if original_count == 0:
        raise ValueError("FATAL: Edge list is empty")

    src = df["src"].to_numpy(copy=False)
    dst = df["dst"].to_numpy(copy=False)
    lo = np.minimum(src, dst)
    hi = np.maximum(src, dst)
    df = pd.DataFrame({
        "src": lo.astype("int64", copy=False),
        "dst": hi.astype("int64", copy=False),
        "weight": df["weight"].to_numpy(dtype="float32", copy=False),
    })

    self_loop_mask = df["src"] == df["dst"]
    n_self_loops = int(self_loop_mask.sum())
    if n_self_loops > 0:
        print(f"  Removing {n_self_loops:,} self-loops")
        df = df.loc[~self_loop_mask]

    if weight_agg == "max":
        df = df.groupby(["src", "dst"], as_index=False, sort=False)["weight"].max()
    elif weight_agg == "mean":
        df = df.groupby(["src", "dst"], as_index=False, sort=False)["weight"].mean()
    elif weight_agg == "sum":
        df = df.groupby(["src", "dst"], as_index=False, sort=False)["weight"].sum()
    else:
        df = df.drop_duplicates(subset=["src", "dst"], keep="first")

    n_removed = original_count - len(df)
    print(f"  Canonicalized in {time.time() - t0:.1f}s")
    print(f"  Removed {n_removed:,} edges ({100*n_removed/original_count:.1f}%)")
    print(f"  Final edge count: {len(df):,}")
    return df


def prepare_node_ids(df: pd.DataFrame):
    print("Preparing node IDs for igraph...")
    t0 = time.time()
    all_nodes = pd.Index(np.concatenate([
        df["src"].to_numpy(dtype="int64", copy=False),
        df["dst"].to_numpy(dtype="int64", copy=False),
    ])).unique()
    n_nodes = len(all_nodes)
    if n_nodes == 0:
        raise ValueError("FATAL: Edge list contains no usable nodes after preprocessing")

    min_id = int(all_nodes.min())
    max_id = int(all_nodes.max())
    if min_id != 0 or max_id != n_nodes - 1:
        raise ValueError(
            "FATAL: Script 02 requires dense zero-based integer node IDs from 01_preprocess.py.\n"
            f"  Found min={min_id:,}, max={max_id:,}, unique={n_nodes:,}.\n"
            "  Refusing to silently remap IDs because protein_id_mapping.csv must remain authoritative."
        )

    print("  Node IDs are dense zero-based integers")
    df["src_int"] = df["src"].astype("int64", copy=False)
    df["dst_int"] = df["dst"].astype("int64", copy=False)

    print(f"  Validated {n_nodes:,} unique nodes in {time.time() - t0:.1f}s")
    return df, n_nodes


def build_graph(df: pd.DataFrame, n_nodes: int):
    print("Building igraph graph object...")
    t0 = time.time()
    edges = df[["src_int", "dst_int"]].to_numpy(dtype=np.int64, copy=False)
    graph = ig.Graph(n=n_nodes, edges=edges, directed=False)
    weights = df["weight"].to_numpy(dtype=np.float64, copy=False)
    print(f"  Graph built in {time.time() - t0:.1f}s")
    print(f"  Nodes: {graph.vcount():,}")
    print(f"  Edges: {graph.ecount():,}")
    return graph, weights


def find_connected_components(graph):
    print("Finding connected components...")
    t0 = time.time()
    components = graph.connected_components(mode="weak")
    sizes = pd.DataFrame({
        "component": np.arange(len(components.sizes()), dtype=np.int64),
        "size": np.array(components.sizes(), dtype=np.int64),
    }).sort_values("size", ascending=False).reset_index(drop=True)
    print(f"  Found {len(sizes):,} connected components in {time.time() - t0:.1f}s")
    if len(sizes) > 0:
        print(f"  Largest component: {int(sizes['size'].iloc[0]):,} nodes")
    if len(sizes) > 1:
        print(f"  Second largest: {int(sizes['size'].iloc[1]):,} nodes")
    return sizes


def run_leiden(graph, weights, resolution: float, max_iter: int, seed: int) -> pd.DataFrame:
    print(f"Running Leiden (resolution={resolution}, max_iter={max_iter}, seed={seed})...")
    t0 = time.time()
    partition = leidenalg.find_partition(
        graph,
        leidenalg.RBConfigurationVertexPartition,
        weights=weights,
        resolution_parameter=resolution,
        n_iterations=max_iter,
        seed=seed,
    )
    membership = np.asarray(partition.membership, dtype=np.int64)
    partition_df = pd.DataFrame({
        "node": np.arange(graph.vcount(), dtype=np.int64),
        "community": membership,
    })
    modularity = graph.modularity(membership.tolist(), weights=weights)
    print(f"  Leiden completed in {time.time() - t0:.1f}s")
    print(f"  Found {partition_df['community'].nunique():,} communities")
    print(f"  Modularity: {modularity:.4f}")
    return partition_df, modularity


def compute_community_metrics(df_edges: pd.DataFrame, partition: pd.DataFrame) -> pd.DataFrame:
    print("Computing community metrics...")
    t0 = time.time()

    sizes = partition.groupby("community", sort=False).size().reset_index(name="size")
    sizes = sizes.rename(columns={"community": "cluster_id"})

    node_to_comm = partition.sort_values("node")["community"].to_numpy(dtype=np.int64, copy=False)
    src_comm = node_to_comm[df_edges["src_int"].to_numpy(dtype=np.int64, copy=False)]
    dst_comm = node_to_comm[df_edges["dst_int"].to_numpy(dtype=np.int64, copy=False)]
    internal_mask = src_comm == dst_comm

    internal = pd.DataFrame({
        "cluster_id": src_comm[internal_mask],
        "weight": df_edges["weight"].to_numpy(dtype=np.float32, copy=False)[internal_mask],
    })
    internal_stats = internal.groupby("cluster_id", sort=False)["weight"].agg(
        internal_edges="count",
        internal_weight_sum="sum",
        avg_internal_weight="mean",
    ).reset_index()
    del internal

    metrics = sizes.merge(internal_stats, on="cluster_id", how="left").fillna(0)
    metrics["internal_edges"] = metrics["internal_edges"].astype("int64")
    metrics["size"] = metrics["size"].astype("int64")

    raw_max_possible = (metrics["size"].to_numpy(dtype=np.int64) * (metrics["size"].to_numpy(dtype=np.int64) - 1)) // 2
    density_denominator = raw_max_possible.copy()
    density_denominator[density_denominator < 1] = 1
    metrics["max_internal_edges"] = raw_max_possible.astype("int64")
    metrics["internal_density"] = metrics["internal_edges"].to_numpy(dtype=np.float64) / density_denominator.astype(np.float64)
    metrics["weighted_density"] = metrics["internal_weight_sum"].to_numpy(dtype=np.float64) / density_denominator.astype(np.float64)

    metrics = metrics.sort_values("size", ascending=False).reset_index(drop=True)
    print(f"  Computed metrics for {len(metrics):,} communities in {time.time() - t0:.1f}s")
    return metrics


def write_cluster_subnetworks(df_edges: pd.DataFrame,
                              partition: pd.DataFrame,
                              output_dir: str,
                              subnetwork_dir_name: str,
                              subnetwork_prefix: str,
                              delimiter: str):
    print("\nWriting cluster subnetworks...")
    t0 = time.time()
    subnetwork_dir = Path(output_dir) / subnetwork_dir_name
    subnetwork_dir.mkdir(parents=True, exist_ok=True)
    print(f"  Subnetwork directory: {subnetwork_dir}")

    node_to_comm = partition.sort_values("node")["community"].to_numpy(dtype=np.int64, copy=False)
    src_comm = node_to_comm[df_edges["src_int"].to_numpy(dtype=np.int64, copy=False)]
    dst_comm = node_to_comm[df_edges["dst_int"].to_numpy(dtype=np.int64, copy=False)]
    internal_mask = src_comm == dst_comm

    internal_edges = pd.DataFrame({
        "cluster_id": src_comm[internal_mask],
        "src": df_edges["src_int"].to_numpy(dtype=np.int64, copy=False)[internal_mask],
        "dst": df_edges["dst_int"].to_numpy(dtype=np.int64, copy=False)[internal_mask],
        "weight": df_edges["weight"].to_numpy(dtype=np.float32, copy=False)[internal_mask],
    })

    written_files = []
    for cluster_id, cluster_edges in internal_edges.groupby("cluster_id", sort=False):
        filepath = subnetwork_dir / f"{subnetwork_prefix}.{int(cluster_id)}"
        atomic_to_csv(cluster_edges[["src", "dst", "weight"]], filepath, sep=delimiter, index=False, header=False)
        written_files.append(str(filepath))

    print(f"  Wrote {len(written_files):,} subnetwork files in {time.time() - t0:.1f}s")
    print(f"  Files written to: {subnetwork_dir}/")
    return subnetwork_dir, written_files


def write_outputs(partition: pd.DataFrame,
                  metrics: pd.DataFrame,
                  out_partition: str,
                  out_metrics: str,
                  delimiter: str) -> pd.DataFrame:
    print("\nPreparing integer-ID partition output...")
    partition_out = partition[["node", "community"]].copy()
    partition_out.columns = ["node_id", "community_id"]
    partition_out["community_id"] = partition_out["community_id"].astype(str)

    metrics = metrics.copy()
    metrics["cluster_id"] = metrics["cluster_id"].astype(str)
    for col in ["size", "internal_edges", "max_internal_edges"]:
        if col in metrics.columns:
            metrics[col] = metrics[col].astype("int64")
    for col in ["internal_weight_sum", "avg_internal_weight", "internal_density", "weighted_density"]:
        if col in metrics.columns:
            metrics[col] = metrics[col].astype("float64")

    ensure_output_parent(out_partition)
    ensure_output_parent(out_metrics)
    print(f"Saving partition to {out_partition}...")
    atomic_to_csv(partition_out, out_partition, sep=delimiter, index=False, header=False)
    print(f"Saving metrics to {out_metrics}...")
    atomic_to_csv(metrics, out_metrics, sep=delimiter, index=False)
    return metrics


def main():
    parser = argparse.ArgumentParser(description="CPU Leiden clustering with igraph/leidenalg")
    parser.add_argument("--edges", required=True, help="Input integer edge list from 01_preprocess.py")
    parser.add_argument("--output_dir", default="networks",
                        help="Output directory for Script 02 outputs (default: networks)")
    parser.add_argument("--out_partition", default=None, help="Output partition file (default: <output_dir>/partition.csv)")
    parser.add_argument("--out_metrics", default=None, help="Output community metrics file (default: <output_dir>/metrics.csv)")
    parser.add_argument("--delimiter", default=",", help="Input/output delimiter: default ','; use 'tab' or '\\t' for TSV")
    parser.add_argument("--subnetwork_dir", default="subnetworks",
                        help="Directory name under output_dir for subnetwork files (default: subnetworks)")
    parser.add_argument("--subnetwork_prefix", default="subnet",
                        help="Filename prefix for subnetwork files (default: subnet)")
    parser.add_argument("--resolution", type=float, default=1.0, help="Leiden resolution parameter")
    parser.add_argument("--max_iter", type=int, default=100, help="Maximum Leiden iterations")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for Leiden")
    parser.add_argument("--has_header", action="store_true", help="Input file has header row")
    parser.add_argument("--validation_rows", type=int, default=10,
                        help="Number of data rows to validate; 0 validates all rows")
    parser.add_argument("--no_canonicalize", action="store_true",
                        help="Skip undirected canonicalization if input is already canonical and deduplicated")
    parser.add_argument("--weight_agg", choices=["max", "mean", "sum", "first"], default="max",
                        help="Weight aggregation for duplicate undirected edges")
    parser.add_argument("--skip_subnetworks", action="store_true",
                        help="Skip writing subnetwork files for Script 03")
    parser.add_argument("--verbose_cc", action="store_true",
                        help="Compute and print connected-component size summary (can be expensive)")
    args = parser.parse_args()

    if args.validation_rows < 0:
        parser.error("--validation_rows must be >= 0")
    try:
        args.delimiter = normalize_delimiter(args.delimiter)
    except ValueError as e:
        parser.error(str(e))

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(sep in args.subnetwork_prefix for sep in ('/', '\\')) or '.' in args.subnetwork_prefix:
        parser.error("--subnetwork_prefix must not contain path separators or dots")
    if args.out_partition is None:
        args.out_partition = str(output_dir / "partition.csv")
    if args.out_metrics is None:
        args.out_metrics = str(output_dir / "metrics.csv")

    total_start = time.time()
    df = load_edgelist(args.edges, args.has_header, args.validation_rows, args.delimiter)
    if not args.no_canonicalize:
        df = canonicalize_edges(df, args.weight_agg)
    else:
        print("Skipping edge canonicalization (--no_canonicalize specified)")

    if len(df) == 0:
        raise ValueError("FATAL: Edge list is empty after canonicalization/self-loop removal")

    df, n_nodes = prepare_node_ids(df)
    graph, weights = build_graph(df, n_nodes)

    if graph.vcount() != n_nodes:
        raise ValueError("FATAL: Graph vertex count does not match dense input node count")

    cc_count = None
    if args.verbose_cc:
        cc_sizes = find_connected_components(graph)
        cc_count = len(cc_sizes)
    else:
        print("Skipping connected-component summary (use --verbose_cc to enable)")

    partition, modularity = run_leiden(graph, weights, args.resolution, args.max_iter, args.seed)
    if len(partition) != n_nodes:
        raise ValueError("FATAL: Leiden partition size does not match dense input node count")

    metrics = compute_community_metrics(df, partition)

    subnetwork_dir = None
    subnetwork_files = []
    if not args.skip_subnetworks:
        subnetwork_dir, subnetwork_files = write_cluster_subnetworks(
            df, partition, args.output_dir, args.subnetwork_dir, args.subnetwork_prefix, args.delimiter
        )

    edge_memory_gb = df.memory_usage(deep=True).sum() / 1e9
    print(f"\nEdge DataFrame still resident: {edge_memory_gb:.2f} GB")
    gc.collect()

    metrics = write_outputs(partition, metrics, args.out_partition, args.out_metrics, args.delimiter)

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total nodes:          {graph.vcount():,}")
    print(f"Total edges:          {graph.ecount():,}")
    if cc_count is not None:
        print(f"Connected components: {cc_count:,}")
    else:
        print("Connected components: skipped")
    print(f"Communities found:    {len(metrics):,}")
    print(f"Global modularity:    {modularity:.4f}")
    print(f"Resolution used:      {args.resolution}")
    print(f"Total time:           {time.time() - total_start:.1f}s")
    print("\nOutputs written to:")
    print(f"  Partition: {args.out_partition}")
    print(f"  Metrics:   {args.out_metrics}")
    if subnetwork_dir:
        print(f"  Subnetworks: {subnetwork_dir}/ ({len(subnetwork_files)} files)")

    if subnetwork_dir and subnetwork_files:
        print("\nNext steps for hierarchical refinement (Script 03):")
        print(f"  python 03_leiden_refine.py \\")
        print(f"    --edges_dir {subnetwork_dir} \\")
        print(f"    --partition {args.out_partition} \\")
        print(f"    --metrics {args.out_metrics} \\")
        print(f"    --output_dir {args.output_dir} \\")
        print(f"    --base_network_name {args.subnetwork_prefix}")


if __name__ == "__main__":
    main()
