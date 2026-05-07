#!/usr/bin/env python3
"""
CPU-based parallel Leiden refinement with recursive execution support.

This script enables hierarchical community detection through iterative refinement:

Key Features:
1. Loads partition and metrics from previous pass (GPU or CPU)
2. Selects communities for refinement based on size and density thresholds
3. Refines selected communities in parallel using Leiden algorithm
4. Outputs:
   - Refined partition with hierarchical cluster IDs (e.g., 9.0.3 → 9.0.3.0, 9.0.3.1, ...)
   - Recalculated metrics for all clusters
   - Subnetwork files for each refined cluster (enables recursive execution)
5. Manages file versions (.rf suffix for processed files)
6. Preserves non-consecutive protein IDs (no reindexing)

Refinement criteria (both must be met):
- size >= min_nodes (default: 3)
- density <= density_threshold (default: 0.1) - loose communities
  - By default uses weighted_density / SJI (weight-based: weight_sum/max_possible)
  - With --ignore_SJI flag: uses internal_density (count-based: edges/max_possible)
  - SJI-based density is useful for filtering weak connections (e.g., 2-node cluster with SJI=0.1 has weighted_density=0.1)

Directory Structure:
  Initial run:
    input: network.csv, partition.csv, metrics.csv
    output:
      - subnetworks/ (directory for subnetworks)
      - subnetworks/subnet.0.1 (refined subnetwork files)
      - subnetworks/subnet.0.2
      - partition.csv (updated with hierarchical cluster IDs, ALL proteins)
      - partition.csv.rf (previous version)
      - metrics.csv (recalculated for ALL clusters)
      - metrics.csv.rf (previous version)

  Recursive run (refining cluster 0.1):
    input: subnetworks/subnet.0.1, partition.csv, metrics.csv
    output:
      - subnetworks/subnet.0.1.rf (renamed)
      - subnetworks/subnet.0.1/ (new subdirectory)
      - subnetworks/subnet.0.1/subnet.0.1.0 (further refined subnetworks)
      - subnetworks/subnet.0.1/subnet.0.1.1
      - partition.csv (updated, still contains ALL proteins)
      - partition.csv.rf (previous version)
      - metrics.csv (updated for all clusters)
      - metrics.csv.rf (previous version)

Important:
  - partition.csv always contains ALL proteins (e.g., 0 to 971157)
  - Each run only updates cluster IDs for proteins in refined communities
  - metrics.csv contains metrics for ALL clusters across entire hierarchy
  - Only subnetwork edge files follow hierarchical directory structure

Requirements:
    - igraph
    - leidenalg
    - joblib (for parallelism)
    - pandas

Usage (first refinement):
    python 03_leiden_refine.py \
        --output_dir ./networks \
        --density_threshold 0.25 \
        --jobs 8

Usage (recursive refinement):
    python 03_leiden_refine.py \
        --output_dir ./networks \
        --density_threshold 0.5 \
        --prune_weak_pairs \
        --resolution 1.5 \
        --auto_recursive \
        --jobs 8

Parameters:
    --edges_dir:    Directory of subnetwork files (default: <output_dir>/subnetworks)
    --partition:    Input partition (default: <output_dir>/partition.csv)
    --metrics:      Input metrics (default: <output_dir>/metrics.csv)
    --output_dir:   Output directory (default: current directory)
    --min_nodes:    Minimum community size to refine (default: 3)
    --density_threshold:  Maximum density to refine (default: 0.1)
                    (high density = tight community, likely doesn't need refinement)
    --ignore_SJI:           Use internal_density (count-based) instead of SJI-based weighted_density
    --prune_weak_pairs:     Prune weak 2-node clusters each iteration
                            Splits them into singleton child clusters
    --resolution:           Leiden resolution for refinement (default: 1.0)
    --jobs:                 Number of parallel jobs (default: 0 = all cores)
    --auto_recursive:       Keep rerunning until fully converged (no flag = one pass then exit)
                            Useful for first runs where you want to control --jobs manually
                            (e.g. --jobs 4 for early memory-heavy passes, --jobs 0 later)
"""

import argparse
import csv
import hashlib
import shutil
import time
import sys
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, Tuple, Optional, List, Union

import pandas as pd
import numpy as np

try:
    import igraph as ig
    import leidenalg
except ImportError:
    print("ERROR: igraph or leidenalg not found.")
    print("Install with: pip install python-igraph leidenalg")
    sys.exit(1)

try:
    from joblib import Parallel, delayed
except ImportError:
    print("ERROR: joblib not found. Install with: pip install joblib")
    sys.exit(1)


# Optional RAPIDS (CUDA/GPU) support for Leiden only
RAPIDS_AVAILABLE = False
try:
    import cudf  # type: ignore
    import cugraph  # type: ignore
    RAPIDS_AVAILABLE = True
except Exception:
    # Keep CPU mode fully functional if RAPIDS is not installed
    RAPIDS_AVAILABLE = False


NodeId = Union[int, str]
FLOAT_DECIMALS = 5
FLOAT_FORMAT = f"%.{FLOAT_DECIMALS}f"
METRIC_FLOAT_COLUMNS = [
    'internal_weight_sum',
    'avg_internal_weight',
    'internal_density',
    'weighted_density',
]


def round_float_columns(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Round floating-point columns used by this script to a consistent precision."""
    rounded = df.copy()
    target_columns = columns if columns is not None else METRIC_FLOAT_COLUMNS
    for col in target_columns:
        if col in rounded.columns:
            rounded[col] = rounded[col].astype('float64').round(FLOAT_DECIMALS)
    return rounded


def normalize_delimiter(delimiter: str) -> str:
    if delimiter in ("tab", "\\t", "tsv"):
        return "\t"
    if delimiter in ("comma", "csv"):
        return ","
    if len(delimiter) != 1:
        raise ValueError("--delimiter must be a single character, 'comma', 'csv', 'tab', 'tsv', or '\\t'")
    return delimiter


def parse_node_id(value) -> NodeId:
    """Preserve non-integer IDs while keeping the existing compact int path."""
    if hasattr(value, "item"):
        value = value.item()
    text = str(value).strip()
    try:
        return int(text)
    except ValueError:
        return text


def node_sort_key(value: NodeId):
    """Sort numeric IDs numerically and string IDs lexicographically."""
    if isinstance(value, int):
        return (0, value)
    return (1, str(value))


def stable_cluster_seed(base_seed: int, cluster_id: str) -> int:
    """Derive a reproducible per-cluster seed independent of filesystem order."""
    key = f"{base_seed}:{cluster_id}".encode("utf-8")
    digest = hashlib.blake2b(key, digest_size=4).digest()
    return int.from_bytes(digest, "little") % (2**31 - 1)


def load_edges(path: str, has_header: bool = False, verbose: bool = True, delimiter: str = ',') -> pd.DataFrame:
    """
    Load edge list into pandas DataFrame.

    Subnetwork files (Script 02/03 output): CSV by default, no header, 3 columns (src, dst, weight)

    Keeps IDs as int32 when possible and falls back to string IDs when needed.
    Weights are kept as float32 for memory efficiency.

    Note: has_header parameter exists for future compatibility but is not currently used
    in strict directory mode (all subnetwork files are headerless).
    """
    if verbose:
        print(f"Loading edges from {path}...")
    t0 = time.time()

    try:
        if has_header:
            df = pd.read_csv(path, sep=delimiter, header=0, usecols=[0, 1, 2])
            df.columns = ['src', 'dst', 'weight']
            df['src'] = df['src'].astype('int32')
            df['dst'] = df['dst'].astype('int32')
            df['weight'] = df['weight'].astype('float32')
        else:
            df = pd.read_csv(
                path,
                sep=delimiter,
                header=None,
                names=['src', 'dst', 'weight'],
                dtype={'src': 'int32', 'dst': 'int32', 'weight': 'float32'}
            )
    except Exception as e:
        try:
            if has_header:
                df = pd.read_csv(path, sep=delimiter, header=0, usecols=[0, 1, 2])
                df.columns = ['src', 'dst', 'weight']
            else:
                df = pd.read_csv(
                    path,
                    sep=delimiter,
                    header=None,
                    names=['src', 'dst', 'weight'],
                    dtype={'src': 'string', 'dst': 'string'}
                )
            df['src'] = df['src'].map(parse_node_id)
            df['dst'] = df['dst'].map(parse_node_id)
            df['weight'] = df['weight'].astype('float32')
            if verbose:
                print("  Node IDs are non-integer; keeping string IDs where needed")
        except Exception as fallback_error:
            raise ValueError(
                f"FATAL: Failed to load edge file: {path}\n"
                f"  Integer-ID error: {e}\n"
                f"  String-ID fallback error: {fallback_error}\n"
                f"  Expected format: src{delimiter}dst{delimiter}weight"
            )

    # Verify exactly 3 columns
    if len(df.columns) != 3:
        raise ValueError(f"FATAL: DataFrame has {len(df.columns)} columns (expected 3)")

    df['weight'] = df['weight'].round(FLOAT_DECIMALS)

    if verbose:
        print(f"  ✓ Loaded {len(df):,} edges")
        print(f"  Memory usage: {df.memory_usage(deep=True).sum() / 1e9:.5f} GB")
        print(f"  Time: {time.time() - t0:.5f}s")
    return df


def load_partition(path: str, delimiter: str = ',') -> Dict[NodeId, str]:
    """
    Load partition as dict: node_id -> community_id.
    Node IDs may be integers or strings; community IDs are strings for hierarchical labels.
    """
    print(f"Loading partition from {path}...")
    t0 = time.time()

    partition = {}
    seen_nodes = set()
    checked_first_data_row = False
    with open(path, 'r', newline='') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for line_num, parts in enumerate(reader, 1):
            if len(parts) >= 2 and parts[0]:
                first = str(parts[0]).strip().lower()
                second = str(parts[1]).strip().lower()
                if not checked_first_data_row and (
                    first in {"node_id", "int_id", "node"} or
                    second in {"cluster_id", "community_id", "community"}
                ):
                    raise ValueError(
                        "FATAL: partition appears to contain a header row.\n"
                        "  Expected Script 02/03 partition format: node_id,cluster_id with no header."
                    )
                checked_first_data_row = True
                node_id = parse_node_id(parts[0])
                if node_id in seen_nodes:
                    raise ValueError(
                        f"FATAL: duplicate node ID in partition at line {line_num}: {node_id}"
                    )
                seen_nodes.add(node_id)
                # Cluster ID stays string to support hierarchical labels like "0.1.2".
                partition[node_id] = str(parts[1])

    print(f"  Loaded {len(partition):,} node assignments in {time.time() - t0:.5f}s")
    return partition


def load_metrics(path: str, delimiter: str = ',') -> pd.DataFrame:
    """
    Load community metrics.

    Accepts either 'community' (from Script 02 old format) or 'cluster_id' (new format)
    and normalizes to 'cluster_id' for consistent downstream processing.
    """
    print(f"Loading metrics from {path}...")
    # IMPORTANT: dtype={'cluster_id': str, 'community': str} prevents cluster IDs like "9.0"
    # from being parsed as floats and re-serialized inconsistently
    df = pd.read_csv(path, sep=delimiter, dtype={'cluster_id': str, 'community': str})

    # Schema compatibility: accept both 'community' (Script 02) and 'cluster_id' (Script 03)
    if 'community' in df.columns and 'cluster_id' not in df.columns:
        # Old format from Script 02 - rename to cluster_id
        df = df.rename(columns={'community': 'cluster_id'})
        print(f"  Note: Renamed 'community' column to 'cluster_id' for compatibility")
    elif 'cluster_id' not in df.columns:
        raise ValueError(
            f"FATAL: Metrics file missing 'cluster_id' or 'community' column\n"
            f"  Found columns: {list(df.columns)}\n"
            f"  Expected metrics from Script 02 or Script 03"
        )

    # Ensure cluster ID is string (for hierarchical IDs like "0.1")
    df['cluster_id'] = df['cluster_id'].astype(str)

    # Validate required columns for refinement selection
    required_cols = ['cluster_id', 'size', 'internal_density']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"FATAL: Metrics file missing required columns: {missing}\n"
            f"  Found columns: {list(df.columns)}\n"
            f"  Required for refinement: {required_cols}\n"
            f"  Ensure metrics file was generated by Script 02 or Script 03"
        )

    # CRITICAL: Enforce consistent data types for numeric columns
    # This prevents mixed int/float formatting in output (e.g., "100" vs "100.0")

    # Fill NaNs before casting (prevents errors if file has missing values)
    numeric_cols = ['size', 'internal_edges', 'max_internal_edges',
                    'internal_weight_sum', 'avg_internal_weight', 'internal_density', 'weighted_density']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # These columns should always be integers (counts)
    int_columns = ['size', 'internal_edges', 'max_internal_edges']
    for col in int_columns:
        if col in df.columns:
            df[col] = df[col].astype('int64')

    # These columns should be floats (computed metrics)
    for col in METRIC_FLOAT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype('float64')

    # Backward compatibility: if weighted_density missing, create it from existing data
    if 'weighted_density' not in df.columns and 'internal_weight_sum' in df.columns:
        print(f"  Note: Computing weighted_density from internal_weight_sum (backward compatibility)")
        max_possible = (df['size'] * (df['size'] - 1)) // 2
        density_denominator = max_possible.where(max_possible >= 1, 1)
        df['weighted_density'] = df['internal_weight_sum'] / density_denominator

    df = round_float_columns(df)

    print(f"  Loaded metrics for {len(df)} clusters")
    return df


def backup_with_suffix(filepath: Path, suffix: str = '.rf') -> Optional[Path]:
    """
    Copy a file to a rolling backup path without removing the current file.

    This is used for final partition/metrics saves so a failed atomic write
    cannot leave only the backup behind.
    """
    if not filepath.exists():
        return None

    backup_path = filepath.parent / f"{filepath.name}{suffix}"
    shutil.copy2(filepath, backup_path)
    print(f"  Backed up: {filepath.name} -> {backup_path.name}")
    return backup_path


def temp_output_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp.{os.getpid()}.{time.time_ns()}")


def replace_with_csv_writer(path: Path, rows, delimiter: str) -> None:
    tmp_path = temp_output_path(path)
    try:
        with open(tmp_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter)
            for row in rows:
                writer.writerow(row)
        tmp_path.replace(path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def replace_with_dataframe_csv(df: pd.DataFrame,
                               path: Path,
                               delimiter: str,
                               header: bool = True) -> None:
    tmp_path = temp_output_path(path)
    try:
        df.to_csv(tmp_path, sep=delimiter, index=False, header=header, float_format=FLOAT_FORMAT)
        tmp_path.replace(path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def save_partition_and_metrics(partition: Dict[NodeId, str],
                               metrics: pd.DataFrame,
                               partition_output: Path,
                               metrics_output: Path,
                               backup: bool = True,
                               delimiter: str = ',') -> None:
    """
    Save partition and metrics to disk with optional backup.

    Args:
        partition: Partition dict (node_id -> cluster_id)
        metrics: Metrics DataFrame
        partition_output: Output path for partition file
        metrics_output: Output path for metrics file
        backup: If True, rename existing files to .rf before writing
    """
    # Backup old files if they exist and backup is requested
    if backup:
        if partition_output.exists():
            backup_with_suffix(partition_output, '.rf')
        if metrics_output.exists():
            backup_with_suffix(metrics_output, '.rf')

    # Write partition
    print(f"  Writing partition to {partition_output}...")
    print(f"    Total nodes: {len(partition):,}")
    replace_with_csv_writer(
        partition_output,
        ([node_id, partition[node_id]] for node_id in sorted(partition.keys(), key=node_sort_key)),
        delimiter
    )
    print(f"    ✓ Written")

    # Write metrics
    print(f"  Writing metrics to {metrics_output}...")
    print(f"    Total clusters: {len(metrics)}")

    # Enforce consistent data types
    metrics_copy = metrics.copy()
    metrics_copy['cluster_id'] = metrics_copy['cluster_id'].astype(str)
    int_columns = ['size', 'internal_edges', 'max_internal_edges']
    for col in int_columns:
        if col in metrics_copy.columns:
            metrics_copy[col] = metrics_copy[col].astype('int64')
    for col in METRIC_FLOAT_COLUMNS:
        if col in metrics_copy.columns:
            metrics_copy[col] = metrics_copy[col].astype('float64')
    metrics_copy = round_float_columns(metrics_copy)

    replace_with_dataframe_csv(metrics_copy, metrics_output, delimiter)
    print(f"    ✓ Written")


def filter_weak_2node_clusters(edges_dir: Path,
                               partition: Dict[NodeId, str],
                               metrics: pd.DataFrame,
                               weighted_density_threshold: float,
                               base_network_name: str) -> Tuple[Dict[NodeId, str], pd.DataFrame]:
    """
    Find and filter weak 2-node clusters by:
    1. Renaming subnetwork files to .rf (marks them as processed/skipped)
    2. Splitting each weak pair into singleton child cluster IDs (.0 and .1)
    3. Updating metrics to represent the singleton child clusters

    A 2-node cluster with low weighted_density represents a weak connection that
    cannot be split further (Leiden needs 3+ nodes), so it is split into two
    singleton clusters without rerunning Leiden.

    Args:
        edges_dir: Directory containing subnetwork files
        partition: Global partition dict (node_id -> cluster_id)
        metrics: Global metrics DataFrame
        weighted_density_threshold: Maximum weighted_density for pruning
        base_network_name: Subnetwork filename prefix (e.g., 'subnet', 'network' for older runs)

    Returns:
        (updated_partition, updated_metrics) with weak pairs split into singleton child clusters
    """
    # Find 2-node clusters with low weighted_density
    weak_mask = (metrics['size'] == 2) & (metrics['weighted_density'] < weighted_density_threshold)
    weak_clusters = metrics.loc[weak_mask, 'cluster_id'].tolist()

    if not weak_clusters:
        # Identity return is intentional: main() uses object identity to skip
        # checkpoint writes when pruning made no changes.
        return partition, metrics

    print(f"\nPreprocessing: Found {len(weak_clusters)} weak 2-node clusters to prune")
    print(f"  Criteria: size=2 AND weighted_density < {weighted_density_threshold:.5f}")

    # Validate against the partition before renaming any subnetwork files.
    # Otherwise an inconsistent metrics row could orphan a still-valid subnetwork.
    weak_clusters_set = set(weak_clusters)
    if len(partition) <= 10_000_000:
        partition_series = pd.Series(partition, dtype='object')
        weak_assignments = partition_series[partition_series.isin(weak_clusters_set)]
        weak_cluster_nodes = {
            cid: list(node_ids)
            for cid, node_ids in weak_assignments.groupby(weak_assignments, sort=False).groups.items()
        }
    else:
        print("  Large partition detected; using lower-memory pruning validation path")
        weak_cluster_nodes = defaultdict(list)
        for node_id, cid in partition.items():
            if cid in weak_clusters_set:
                weak_cluster_nodes[cid].append(node_id)

    valid_weak_clusters = []
    valid_cluster_nodes = {}
    invalid_count = 0
    for cid in weak_clusters:
        nodes = sorted(weak_cluster_nodes.get(cid, []), key=node_sort_key)
        if len(nodes) != 2:
            print(f"  WARNING: Expected 2 nodes for weak cluster {cid}, found {len(nodes)}; leaving unchanged")
            invalid_count += 1
            continue
        valid_weak_clusters.append(cid)
        valid_cluster_nodes[cid] = nodes

    if not valid_weak_clusters:
        print("  No weak 2-node clusters passed partition validation; leaving files unchanged")
        # Identity return is intentional: main() uses object identity to skip
        # checkpoint writes when pruning made no changes.
        return partition, metrics

    if invalid_count:
        print(f"  Skipping {invalid_count} weak clusters with inconsistent partition/metrics state")

    # Rename only validated weak cluster files to .rf (skip them in future processing)
    # OPTIMIZED: Single recursive scan instead of repeated globs.
    print(f"  Scanning directory for subnetwork files...")
    t0 = time.time()

    prefix = base_network_name + "."
    all_subnetwork_files = {}
    for dirpath, _dirnames, filenames in os.walk(edges_dir):
        for filename in filenames:
            if filename.startswith(prefix) and not filename.endswith('.rf'):
                all_subnetwork_files[filename] = Path(dirpath) / filename

    print(f"  Found {len(all_subnetwork_files)} total subnetwork files in {time.time() - t0:.5f}s")

    print(f"  Renaming {len(valid_weak_clusters)} validated weak cluster files to .rf...")
    renamed_count = 0
    progress_interval = max(1, len(valid_weak_clusters) // 20)

    for i, cluster_id in enumerate(valid_weak_clusters):
        filename = f"{base_network_name}.{cluster_id}"

        if filename in all_subnetwork_files:
            filepath = all_subnetwork_files[filename]
            new_path = filepath.parent / f"{filepath.name}.rf"
            if new_path.exists():
                timestamp = time.time_ns()
                new_path = filepath.parent / f"{filepath.name}.rf.{timestamp}"

            filepath.rename(new_path)
            renamed_count += 1

        if (i + 1) % progress_interval == 0 or (i + 1) == len(valid_weak_clusters):
            percent = 100 * (i + 1) / len(valid_weak_clusters)
            print(f"    Progress: {i + 1}/{len(valid_weak_clusters)} ({percent:.5f}%)", end='\r')

    print(f"\n  ✓ Renamed {renamed_count} subnetwork files to .rf in {time.time() - t0:.5f}s")

    sys.stdout.flush()
    if hasattr(os, 'sync'):
        os.sync()

    # Update partition: split each weak pair into two singleton child clusters.
    # This avoids leaving both proteins with the same cluster_id after pruning.
    print(f"  Splitting weak pairs into singleton child clusters...", end='', flush=True)

    updated_partition = partition.copy()
    singleton_metric_rows = []
    nodes_updated = 0
    clusters_split = 0

    for cid in valid_weak_clusters:
        nodes = valid_cluster_nodes[cid]
        for child_idx, node_id in enumerate(nodes):
            child_cluster_id = f"{cid}.{child_idx}"
            updated_partition[node_id] = child_cluster_id
            singleton_metric_rows.append({
                'cluster_id': child_cluster_id,
                'size': 1,
                'internal_edges': 0,
                'internal_weight_sum': 0.0,
                'avg_internal_weight': 0.0,
                'internal_density': 0.0,
                'weighted_density': 0.0,
                'max_internal_edges': 0,
            })
            nodes_updated += 1
        clusters_split += 1

    print(f" Done!")
    print(f"  Split {nodes_updated} nodes from {clusters_split} weak pairs")

    # Update metrics: remove weak parent rows and add singleton child rows.
    print(f"  Updating metrics ({len(metrics):,} clusters)...", end='', flush=True)

    valid_weak_clusters_set = set(valid_weak_clusters)
    updated_metrics = metrics[~metrics['cluster_id'].isin(valid_weak_clusters_set)].copy()
    if singleton_metric_rows:
        singleton_metrics = pd.DataFrame(singleton_metric_rows)
        updated_metrics = pd.concat([updated_metrics, singleton_metrics], ignore_index=True)
        updated_metrics = updated_metrics.sort_values('size', ascending=False).reset_index(drop=True)
        updated_metrics = round_float_columns(updated_metrics)

    print(f" Done!")
    print(f"  Replaced {clusters_split} weak pair metric rows with {len(singleton_metric_rows)} singleton rows")

    return updated_partition, updated_metrics




def compute_cluster_metrics(edges: pd.DataFrame,
                            partition: Dict[NodeId, str],
                            verbose: bool = True) -> pd.DataFrame:
    """
    Compute metrics for all clusters in a partition.

    Returns DataFrame with columns:
        - cluster_id
        - size (number of nodes)
        - internal_edges
        - internal_weight_sum
        - avg_internal_weight
        - internal_density (count-based: edges / max_possible)
        - weighted_density (weight-based: weight_sum / max_possible)
        - max_internal_edges
    """
    if verbose:
        print("Computing cluster metrics...")
    t0 = time.time()

    # Count nodes per cluster
    cluster_sizes = pd.Series(partition, dtype='object').value_counts(sort=False)

    # Map cluster assignments without copying the full edge DataFrame.
    src_cluster = edges['src'].map(partition)
    dst_cluster = edges['dst'].map(partition)
    internal_mask = src_cluster == dst_cluster
    internal = pd.DataFrame({
        'src_cluster': src_cluster[internal_mask],
        'weight': edges.loc[internal_mask, 'weight'],
    })

    # Aggregate by cluster
    cluster_stats = internal.groupby('src_cluster').agg({
        'weight': ['count', 'sum', 'mean']
    }).reset_index()
    cluster_stats.columns = ['cluster_id', 'internal_edges', 'internal_weight_sum', 'avg_internal_weight']

    # Build metrics DataFrame
    metrics = cluster_sizes.rename_axis('cluster_id').reset_index(name='size')

    # Merge with edge stats
    metrics = metrics.merge(cluster_stats, on='cluster_id', how='left')
    metrics = metrics.fillna(0)

    # Compute internal density
    metrics['internal_edges'] = metrics['internal_edges'].astype('int64')
    metrics['size'] = metrics['size'].astype('int64')

    # Max possible edges: size * (size - 1) / 2. Keep the reported count true
    # for singletons (0), but use denominator 1 for density to avoid division by zero.
    max_possible = (metrics['size'] * (metrics['size'] - 1)) // 2
    density_denominator = max_possible.where(max_possible >= 1, 1)

    metrics['max_internal_edges'] = max_possible.astype('int64')

    # Count-based density (traditional): edges / max_possible
    metrics['internal_density'] = metrics['internal_edges'] / density_denominator

    # Weight-based density: sum_of_weights / max_possible
    # This considers edge strength, not just structure
    # For a 2-node cluster with weak edge (weight=0.1): weighted_density = 0.1/1 = 0.1
    # For a 2-node cluster with strong edge (weight=0.9): weighted_density = 0.9/1 = 0.9
    metrics['weighted_density'] = metrics['internal_weight_sum'] / density_denominator

    # Ensure cluster_id is string (critical for hierarchical IDs like "0.1")
    # Prevents pandas from converting "9.0" to float 9.0
    metrics['cluster_id'] = metrics['cluster_id'].astype(str)
    metrics = round_float_columns(metrics)

    # Sort by size descending
    metrics = metrics.sort_values('size', ascending=False).reset_index(drop=True)

    if verbose:
        print(f"  Computed metrics for {len(metrics)} clusters in {time.time() - t0:.5f}s")

    return metrics




def run_leiden_on_subgraph(subgraph_edges: pd.DataFrame,
                           resolution: float,
                           seed: int = 42,
                           use_gpu: bool = False) -> Dict[NodeId, int]:
    """Run Leiden on a subgraph and return node -> subcommunity mapping.

    Low-complexity CUDA enablement: when use_gpu=True and RAPIDS is available,
    only the Leiden step is executed on GPU (cuGraph). All other logic remains CPU.

    Expects integer node IDs in columns: src, dst, weight.
    """
    if len(subgraph_edges) == 0:
        return {}

    if use_gpu:
        if not RAPIDS_AVAILABLE:
            raise RuntimeError(
                "--use_gpu_leiden was requested, but RAPIDS (cudf/cugraph) is not available. "
                "Install RAPIDS matching your CUDA/driver, or rerun without --use_gpu_leiden."
            )
        return run_leiden_on_subgraph_gpu(subgraph_edges, resolution, seed)

    return run_leiden_on_subgraph_cpu(subgraph_edges, resolution, seed)



def run_leiden_on_subgraph_cpu(subgraph_edges: pd.DataFrame,
                               resolution: float,
                               seed: int = 42) -> Dict[NodeId, int]:
    """CPU backend (igraph + leidenalg). This is the original implementation."""
    if len(subgraph_edges) == 0:
        return {}

    # Get unique nodes in sorted order so local vertex IDs are reproducible
    # regardless of subnetwork edge-file order.
    all_nodes = pd.concat([subgraph_edges['src'], subgraph_edges['dst']])
    uniques = pd.Index(sorted(pd.unique(all_nodes), key=node_sort_key))
    node_to_code = pd.Series(np.arange(len(uniques), dtype=np.int64), index=uniques)
    src_codes = subgraph_edges['src'].map(node_to_code).to_numpy(dtype=np.int64)
    dst_codes = subgraph_edges['dst'].map(node_to_code).to_numpy(dtype=np.int64)

    # Build edge array without a Python list of tuples.
    edges = np.column_stack((src_codes, dst_codes))
    weights = subgraph_edges['weight'].to_numpy(dtype=np.float64, copy=False)

    # Create graph
    g = ig.Graph(n=len(uniques), edges=edges, directed=False)
    g.es['weight'] = weights

    # Run Leiden
    partition = leidenalg.find_partition(
        g,
        leidenalg.RBConfigurationVertexPartition,
        weights=g.es['weight'],
        resolution_parameter=resolution,
        seed=seed
    )

    # Map back to original node IDs
    return {
        parse_node_id(uniques[idx]): int(membership)
        for idx, membership in enumerate(partition.membership)
    }



def run_leiden_on_subgraph_gpu(subgraph_edges: pd.DataFrame,
                               resolution: float,
                               seed: int = 42) -> Dict[NodeId, int]:
    """GPU backend (cuGraph Leiden). Only used when --use_gpu_leiden is set."""
    if len(subgraph_edges) == 0:
        return {}

    # Create a reproducible consecutive vertex ID mapping (cuGraph prefers int32/int64 vertex IDs)
    all_nodes = pd.concat([subgraph_edges['src'], subgraph_edges['dst']])
    uniques = pd.Index(sorted(pd.unique(all_nodes), key=node_sort_key))
    node_to_code = pd.Series(np.arange(len(uniques), dtype=np.int64), index=uniques)
    src_codes = subgraph_edges['src'].map(node_to_code).to_numpy(dtype=np.int64)
    dst_codes = subgraph_edges['dst'].map(node_to_code).to_numpy(dtype=np.int64)

    # Move edge list to GPU
    gdf = cudf.DataFrame({
        'src': src_codes.astype('int32'),
        'dst': dst_codes.astype('int32'),
        'weight': cudf.Series(subgraph_edges['weight'].to_numpy()).astype('float32'),
    })

    G = cugraph.Graph(directed=False)
    # We already renumbered, so keep renumber=False
    G.from_cudf_edgelist(gdf, source='src', destination='dst', edge_attr='weight', renumber=False)

    # cuGraph reads weights from the graph's edge_attr. Some RAPIDS versions do
    # not accept a separate weight= keyword on leiden().
    out = cugraph.leiden(G, resolution=resolution, random_state=seed)
    parts = out[0] if isinstance(out, tuple) else out

    parts_pd = parts.to_pandas()
    if 'vertex' not in parts_pd.columns or 'partition' not in parts_pd.columns:
        raise RuntimeError(f"Unexpected cuGraph leiden output columns: {list(parts_pd.columns)}")

    # Map back to original node IDs
    membership = parts_pd.set_index('vertex')['partition'].to_dict()
    return {parse_node_id(uniques[int(v)]): int(comm) for v, comm in membership.items()}





def find_all_subnetworks(edges_dir: Path, base_network_name: str = "subnet") -> List[Tuple[Path, str]]:
    """
    Recursively find all subnetwork files in a directory tree.

    Returns list of (filepath, cluster_id) tuples.

    Finds both:
    - Initial subnetworks from Script 02: subnet.0, subnet.1, ... (cluster IDs: "0", "1", ...)
    - Recursive subnetworks from Script 03: subnet.0.1, subnet.0.1.2, ... (hierarchical cluster IDs)

    Subnetwork files are identified by pattern: {base_network_name}.{cluster_id}
    """
    print(f"Scanning for subnetwork files in {edges_dir}...")
    subnetworks = []
    prefix = base_network_name + "."

    # os.walk() is significantly faster than Path.rglob() on large directory trees
    # (especially on network/HPC filesystems) because it batches directory reads
    # via os.scandir() internally, avoiding a stat() call per entry.
    filtered_non_numeric = 0
    for dirpath, _dirnames, filenames in os.walk(edges_dir):
        for filename in filenames:
            if not filename.startswith(prefix):
                continue
            if filename.endswith('.rf') or filename.endswith('.nosplit') or filename.startswith('.'):
                continue
            cluster_id = filename[len(prefix):]
            if cluster_id and all(part.isdigit() for part in cluster_id.split('.')):
                subnetworks.append((Path(dirpath) / filename, cluster_id))
            else:
                filtered_non_numeric += 1

    print(f"  Found {len(subnetworks)} subnetwork files")
    if filtered_non_numeric:
        print(f"  Ignored {filtered_non_numeric} subnetwork files with non-numeric cluster IDs")
    return subnetworks


def is_already_processed(subnetwork_path: Path) -> bool:
    """
    Check if a subnetwork has already been refined.

    A subnetwork is considered processed if:
    1. A .nosplit marker exists (Leiden ran but couldn't split — never retry)
    2. Its child directory exists AND contains a .done marker file

    Returns True if already processed, False otherwise.
    """
    nosplit_marker = subnetwork_path.parent / (subnetwork_path.name + '.nosplit')
    if nosplit_marker.exists():
        return True

    expected_dir = subnetwork_path.parent / subnetwork_path.name
    if not expected_dir.exists() or not expected_dir.is_dir():
        return False

    done_marker = expected_dir / ".done"
    return done_marker.exists()


def build_processed_set(edges_dir: Path) -> set:
    """
    Scan the directory tree once and return a set of absolute path strings
    for all subnetwork files that are already processed.

    This avoids calling is_already_processed() per file (which does 2 stat()
    calls each), replacing O(N) stat calls with a single os.walk() pass.

    A file is processed if:
    - A sibling .nosplit marker exists, OR
    - A same-named subdirectory exists with a .done marker inside
    """
    nosplit_files = set()   # absolute paths of .nosplit markers
    done_dirs = set()       # absolute paths of directories containing .done

    for dirpath, dirnames, filenames in os.walk(edges_dir):
        dirpath_p = Path(dirpath)
        for filename in filenames:
            if filename.endswith('.nosplit'):
                # e.g. subnet.0.nosplit -> marks subnet.0 as processed
                nosplit_files.add(str(dirpath_p / filename[:-len('.nosplit')]))
            elif filename == '.done':
                # This directory has a .done marker — its parent file is processed
                done_dirs.add(dirpath)

    processed = nosplit_files | done_dirs
    return processed


def write_temp_partition(partition_updates: Dict[NodeId, str], output_dir: Path, delimiter: str = ',') -> Path:
    """
    Write partition updates to temporary file in subdirectory.

    Format: node_id,cluster_id by default
    Returns path to temporary file.
    """
    temp_file = output_dir / ".partition.tmp"
    replace_with_csv_writer(
        temp_file,
        ([node_id, partition_updates[node_id]] for node_id in sorted(partition_updates.keys(), key=node_sort_key)),
        delimiter
    )
    return temp_file


def write_temp_metrics(metrics: pd.DataFrame, output_dir: Path, delimiter: str = ',') -> Path:
    """
    Write metrics to temporary file in subdirectory.

    Returns path to temporary file.
    """
    temp_file = output_dir / ".metrics.tmp"
    metrics = round_float_columns(metrics)
    replace_with_dataframe_csv(metrics, temp_file, delimiter)
    return temp_file


def load_temp_partition(temp_file: Path, delimiter: str = ',') -> Dict[NodeId, str]:
    """Load partition updates from temporary file."""
    partition = {}
    with open(temp_file, 'r', newline='') as f:
        reader = csv.reader(f, delimiter=delimiter)
        for parts in reader:
            if len(parts) >= 2 and parts[0]:
                partition[parse_node_id(parts[0])] = str(parts[1])
    return partition


def load_temp_metrics(temp_file: Path, delimiter: str = ',') -> pd.DataFrame:
    """Load metrics from temporary file."""
    metrics = pd.read_csv(temp_file, sep=delimiter, dtype={'cluster_id': str})
    return round_float_columns(metrics)


def merge_temp_results(edges_dir: Path,
                       global_partition: Dict[NodeId, str],
                       global_metrics: pd.DataFrame,
                       delimiter: str = ',') -> Tuple[Dict[NodeId, str], pd.DataFrame]:
    """
    Merge all temporary partition/metrics files into global versions.

    Recursively finds all .partition.tmp and .metrics.tmp files,
    merges them, and returns updated global partition and metrics.
    """
    print("\nMerging temporary results...")

    # Find all temporary files via os.walk (much faster than rglob on large trees)
    partition_tmp_files = []
    metrics_tmp_files = []
    for dirpath, _dirnames, filenames in os.walk(edges_dir):
        for filename in filenames:
            if filename == ".partition.tmp":
                partition_tmp_files.append(Path(dirpath) / filename)
            elif filename == ".metrics.tmp":
                metrics_tmp_files.append(Path(dirpath) / filename)

    print(f"  Found {len(partition_tmp_files)} temporary partition files")
    print(f"  Found {len(metrics_tmp_files)} temporary metrics files")

    # Start with global versions
    merged_partition = global_partition.copy()

    # Merge partition updates
    total_updates = 0
    for tmp_file in partition_tmp_files:
        updates = load_temp_partition(tmp_file, delimiter)
        for node_id, cluster_id in updates.items():
            merged_partition[node_id] = cluster_id
            total_updates += 1

    print(f"  Merged {total_updates} partition updates")

    # Merge metrics
    # Strategy: collect all new cluster IDs, remove old parents, concat new metrics
    all_new_metrics = []
    for tmp_file in metrics_tmp_files:
        metrics = load_temp_metrics(tmp_file, delimiter)
        all_new_metrics.append(metrics)

    if all_new_metrics:
        new_metrics_combined = pd.concat(all_new_metrics, ignore_index=True)

        # Remove duplicate cluster IDs (keep first occurrence)
        new_metrics_combined = new_metrics_combined.drop_duplicates(subset=['cluster_id'], keep='first')

        # Identify parent clusters that were split
        new_cluster_ids = set(new_metrics_combined['cluster_id'])
        split_parents = set()
        for cluster_id in new_cluster_ids:
            if '.' in cluster_id:
                parent = cluster_id.rsplit('.', 1)[0]
                split_parents.add(parent)

        existing_parents = set(global_metrics['cluster_id']).intersection(split_parents)
        missing_parents = split_parents - existing_parents
        if missing_parents:
            print(f"  Note: {len(missing_parents)} parent metric rows were already absent")

        # Remove parent clusters from global metrics
        merged_metrics = global_metrics[~global_metrics['cluster_id'].isin(split_parents)].copy()

        # Also remove any clusters that appear in new metrics (to avoid duplicates)
        merged_metrics = merged_metrics[~merged_metrics['cluster_id'].isin(new_cluster_ids)]

        # Concatenate with new metrics
        merged_metrics = pd.concat([merged_metrics, new_metrics_combined], ignore_index=True)
        merged_metrics = merged_metrics.sort_values('size', ascending=False).reset_index(drop=True)

        print(f"  Merged {len(new_metrics_combined)} new metric rows")
    else:
        merged_metrics = global_metrics

    return merged_partition, merged_metrics


def cleanup_temp_files(edges_dir: Path):
    """
    Remove all temporary partition/metrics files after successful merge.
    """
    print("\nCleaning up temporary files...")

    temp_files = []
    for dirpath, _dirnames, filenames in os.walk(edges_dir):
        for filename in filenames:
            if filename in (".partition.tmp", ".metrics.tmp"):
                temp_files.append(Path(dirpath) / filename)

    for tmp_file in temp_files:
        tmp_file.unlink()

    print(f"  Cleaned up {len(temp_files)} temporary files")


def refine_single_subnetwork(subnetwork_path: Path,
                             cluster_id: str,
                             partition_global: Dict[NodeId, str],
                             resolution: float,
                             base_network_name: str,
                             seed: int = 42,
                             use_gpu_leiden: bool = False,
                             verbose: bool = False,
                             delimiter: str = ',') -> Tuple[str, bool]:
    """
    Refine a single subnetwork and write temporary results.

    Returns (cluster_id, success_flag).

    Writes to subdirectory:
    - .partition.tmp: updated partition for nodes in this subnetwork
    - .metrics.tmp: metrics for new child clusters
    - {base_network_name}.{cluster_id}.{i}: child subnetwork files

    Exception-safe: Catches all errors and returns (cluster_id, False) instead of crashing.
    """
    try:
        # Load edges (silent mode)
        edges = load_edges(str(subnetwork_path), has_header=False, verbose=verbose, delimiter=delimiter)

        # Get nodes in this subnetwork
        edge_nodes = set(edges['src'].unique().tolist() + edges['dst'].unique().tolist())

        # Filter partition to these nodes
        # CRITICAL: Iterate over edge_nodes (small) not partition_global (huge)
        # O(N_subnetwork) instead of O(N_total) - ~100x faster for typical subnetworks
        partition_local = {node: partition_global[node] for node in edge_nodes if node in partition_global}
        missing_nodes = edge_nodes - set(partition_local.keys())
        if missing_nodes:
            sample = sorted(missing_nodes, key=node_sort_key)[:10]
            raise ValueError(
                f"FATAL: Subnetwork {subnetwork_path} contains {len(missing_nodes)} nodes "
                f"missing from global partition. First missing nodes: {sample}"
            )

        if not partition_local:
            if verbose:
                print(f"    WARNING: No nodes found in partition for cluster {cluster_id}")
            nosplit_marker = subnetwork_path.parent / (subnetwork_path.name + '.nosplit')
            nosplit_marker.touch()
            return cluster_id, False

        # All nodes should belong to the same cluster (the one being refined)
        # But we'll work with whatever clusters are present
        clusters_present = set(partition_local.values())
        if clusters_present != {cluster_id}:
            raise ValueError(
                f"FATAL: Subnetwork {subnetwork_path} is stale for cluster {cluster_id}; "
                f"found partition assignments {sorted(clusters_present)}"
            )

        # Run Leiden on entire subnetwork
        subcommunities = run_leiden_on_subgraph(edges, resolution, seed, use_gpu=use_gpu_leiden)

        n_subcommunities = len(set(subcommunities.values())) if subcommunities else 0

        if n_subcommunities <= 1:
            # Mark as unsplittable so future runs/iterations skip it without re-running Leiden
            nosplit_marker = subnetwork_path.parent / (subnetwork_path.name + '.nosplit')
            nosplit_marker.touch()
            return cluster_id, False

        # Create hierarchical labels
        refined_partition = {node: f"{cluster_id}.{sub}" for node, sub in subcommunities.items()}
        if set(refined_partition.keys()) != edge_nodes:
            missing_refined = edge_nodes - set(refined_partition.keys())
            raise RuntimeError(
                f"FATAL: Leiden result for {cluster_id} omitted {len(missing_refined)} edge nodes"
            )

        # Create output directory for child subnetworks
        output_dir = subnetwork_path.parent / subnetwork_path.name

        # CRASH RECOVERY: Track renamed file for rollback if processing fails
        renamed_file_path = None
        original_file_path = None

        try:
            # FIX #3: Check for incomplete previous runs (directory exists but no .done marker)
            done_marker = output_dir / ".done"
            if output_dir.exists():
                if output_dir.is_dir() and not done_marker.exists():
                    # Incomplete from previous crashed run - clean up
                    if verbose:
                        print(f"    Cleaning incomplete directory from previous run: {output_dir}")
                    shutil.rmtree(output_dir)
                elif output_dir.is_dir() and done_marker.exists():
                    # Already completed successfully - shouldn't reach here, but handle gracefully
                    if verbose:
                        print(f"    Cluster {cluster_id} already processed (found .done marker)")
                    return cluster_id, False
                elif output_dir.is_file():
                    # FIX #2: Rename original file if it collides with output directory
                    # Track paths for rollback in case of crash
                    original_file_path = output_dir
                    renamed_file_path = output_dir.parent / f"{output_dir.name}.rf"

                    # Handle collision with existing .rf file
                    if renamed_file_path.exists():
                        timestamp = time.time_ns()
                        renamed_file_path = output_dir.parent / f"{output_dir.name}.rf.{timestamp}"

                    original_file_path.rename(renamed_file_path)

            # Create output directory
            output_dir.mkdir(parents=True, exist_ok=True)

            # Write child subnetwork files
            src_subcluster = edges['src'].map(subcommunities)
            dst_subcluster = edges['dst'].map(subcommunities)

            child_cluster_ids = []
            for subcluster_id in sorted(set(subcommunities.values())):
                # Only internal edges
                mask = (src_subcluster == subcluster_id) & (dst_subcluster == subcluster_id)
                subcluster_edges = edges[mask][['src', 'dst', 'weight']]

                if len(subcluster_edges) > 0:
                    hierarchical_id = f"{cluster_id}.{subcluster_id}"
                    child_cluster_ids.append(hierarchical_id)

                    # Write subnetwork file
                    filename = f"{base_network_name}.{hierarchical_id}"
                    filepath = output_dir / filename
                    subcluster_edges = round_float_columns(subcluster_edges, ['weight'])
                    replace_with_dataframe_csv(
                        subcluster_edges,
                        filepath,
                        delimiter,
                        header=False
                    )

            # Compute metrics for new child clusters (silent mode)
            child_metrics = compute_cluster_metrics(edges, refined_partition, verbose=verbose)

            # Write temporary files
            write_temp_partition(refined_partition, output_dir, delimiter)
            write_temp_metrics(child_metrics, output_dir, delimiter)

            # Write .done marker to indicate successful completion
            done_marker.touch()

            return cluster_id, True

        except Exception as e:
            # FIX #2: ROLLBACK - Restore original file if we renamed it
            if renamed_file_path and renamed_file_path.exists() and original_file_path:
                try:
                    # Remove incomplete directory if created
                    if output_dir.exists() and output_dir.is_dir():
                        shutil.rmtree(output_dir)

                    # Restore original file name
                    renamed_file_path.rename(original_file_path)

                    if verbose:
                        print(f"    Rolled back: restored {original_file_path.name} after error")
                except Exception as rollback_error:
                    # Log rollback failure but don't hide original error
                    print(f"    WARNING: Rollback failed for {cluster_id}: {rollback_error}")

            # Re-raise original exception to be caught by outer handler
            raise

    except Exception as e:
        if str(e).startswith("FATAL:"):
            raise
        # Graceful degradation: log error but don't crash entire parallel job
        # ALWAYS print errors regardless of verbose flag
        print(f"ERROR processing cluster {cluster_id}: {type(e).__name__}: {e}")
        return cluster_id, False


def main():
    parser = argparse.ArgumentParser(
        description="Automatic recursive CPU Leiden refinement with parallel processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
WORKFLOW:
  Script 02 → Script 03 (recursive until no refinements)

  Step 1: Run Script 02 (GPU Leiden clustering)
    Generates:
      - partition.csv (node→cluster mapping)
      - metrics.csv (cluster metrics)
      - subnetworks/ directory with subnetworks (subnet.0, subnet.1, ...)

  Step 2: Run Script 03 (this script) - First Pass
    - Scans subnetworks/ directory for subnetwork files
    - Auto-selects clusters meeting criteria (size ≥ min_nodes, density ≤ density_threshold)
    - Refines selected clusters in parallel
    - Creates hierarchical subnetworks (subnet.0.1, subnet.0.2, ...)
    - Updates global partition.csv and metrics.csv
    - Writes .done markers to prevent re-processing

  Step 3: Run Script 03 again - Recursive Passes
    - Scans for new subnetworks (including nested ones)
    - Skips already-processed clusters (checks .done markers)
    - Refines newly created clusters
    - Repeats until no more clusters meet refinement criteria

DIRECTORY STRUCTURE (after multiple passes):
  networks/
    partition.csv            # Global partition (ALL proteins)
    metrics.csv              # Global metrics (ALL clusters)
    subnetworks/             # Initial subnetworks from Script 02
      subnet.0               # Cluster 0 edges
      subnet.0/              # Refined children of cluster 0
        subnet.0.0
        subnet.0.1
        subnet.0.1/          # Further refined
          subnet.0.1.0
          subnet.0.1.1
          .done              # Marker: processing complete
        .done
      subnet.1
      subnet.1/
        ...

USAGE:
  # First refinement pass (on Script 02 output)
  python 03_leiden_refine.py \\
    --output_dir ./networks \\
    --density_threshold 0.25 \\
    --jobs 16

  # Subsequent pass with weak-pair pruning enabled
  python 03_leiden_refine.py \\
    --output_dir ./networks \\
    --density_threshold 0.5 \\
    --prune_weak_pairs \\
    --resolution 1.5 \\
    --auto_recursive \\
    --jobs 16
        """
    )

    # Output directory and input files
    parser.add_argument("--output_dir", default=".", help="Output directory (default: current directory)")
    parser.add_argument("--edges_dir", default=None,
                        help="Directory containing subnetwork files (default: <output_dir>/subnetworks)")
    parser.add_argument("--partition", default=None,
                        help="Input partition file (default: <output_dir>/partition.csv)")
    parser.add_argument("--metrics", default=None,
                        help="Input community metrics file (default: <output_dir>/metrics.csv)")
    parser.add_argument("--delimiter", default=",",
                        help="Input/output delimiter: default ','; use 'tab' or '\\t' for TSV")

    # Refinement criteria
    parser.add_argument("--min_nodes", type=int, default=3,
                        help="Minimum community size to refine (default: 3)")
    parser.add_argument("--density_threshold", type=float, default=0.1,
                        help="Maximum internal density to refine (default: 0.1)")
    parser.add_argument("--ignore_SJI", action="store_true",
                        help="Use internal_density (count-based) instead of SJI-based weighted_density for "
                             "cluster selection. By default, weighted_density (SJI) is used, which considers "
                             "edge strengths rather than just edge counts.")
    parser.add_argument("--prune_weak_pairs", action="store_true",
                        help="Prune weak 2-node clusters each iteration. "
                             "Renames subnetwork files to .rf and splits nodes into singleton child clusters. "
                             "Uses --density_threshold as the SJI threshold.")

    # Leiden parameters
    parser.add_argument("--resolution", type=float, default=1.0,
                        help="Leiden resolution for refinement (default: 1.0)")
    parser.add_argument("--use_gpu_leiden", action="store_true",
                        help="Use CUDA/GPU (cuGraph) for run_leiden_on_subgraph() only. \n                             Requires RAPIDS (cudf, cugraph). All other steps remain CPU.")

    # Execution parameters
    parser.add_argument("--jobs", type=int, default=0,
                        help="Number of parallel jobs (default: 0 = all available cores)")
    parser.add_argument("--prefer_threads", action="store_true",
                        help="Use threading instead of processes (reduces memory for large partitions)")
    parser.add_argument("--base_network_name", default="subnet",
                        help="Subnetwork filename prefix (default: 'subnet')")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for Leiden algorithm (default: 42)")
    parser.add_argument("--auto_recursive", action="store_true",
                        help="Automatically rerun refinement until no clusters can be processed further. "
                             "Without this flag, the script runs one pass and exits. "
                             "Useful when you want to control --jobs per run (e.g. first run with --jobs 4 "
                             "to avoid memory pressure, later runs with --jobs 0 for all cores).")

    args = parser.parse_args()

    try:
        args.delimiter = normalize_delimiter(args.delimiter)
    except ValueError as e:
        parser.error(str(e))

    if args.use_gpu_leiden and not RAPIDS_AVAILABLE:
        print("\nERROR: --use_gpu_leiden requested but RAPIDS (cudf/cugraph) is not installed.")
        print("  Install RAPIDS matching your CUDA/driver, or rerun without --use_gpu_leiden.")
        return

    if args.use_gpu_leiden and args.jobs not in (0, 1):
        print("\nWARNING: --use_gpu_leiden is enabled on a single GPU; using --jobs > 1 can cause GPU contention.")
        print("  Recommended: set --jobs 1 for GPU Leiden mode.")

    # Determine number of cores
    if args.jobs == 0:
        import multiprocessing
        args.jobs = multiprocessing.cpu_count()
        print(f"Auto-detected {args.jobs} CPU cores")

    total_start = time.time()

    print("="*70)
    print("AUTOMATIC RECURSIVE HIERARCHICAL LEIDEN REFINEMENT")
    print("="*70)

    # Setup paths
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.edges_dir is None:
        args.edges_dir = str(output_dir / "subnetworks")
    edges_dir = Path(args.edges_dir)
    if args.partition is None:
        args.partition = str(output_dir / "partition.csv")
    if args.metrics is None:
        args.metrics = str(output_dir / "metrics.csv")

    if not edges_dir.exists():
        raise ValueError(f"Edges directory does not exist: {edges_dir}")

    print(f"\nConfiguration:")
    print(f"  Edges directory: {edges_dir}")
    print(f"  Partition file: {args.partition}")
    print(f"  Metrics file: {args.metrics}")
    print(f"  Output directory: {output_dir}")
    print(f"  Delimiter: {repr(args.delimiter)}")
    print(f"  Base network name: {args.base_network_name}")
    print(f"  Min nodes: {args.min_nodes}")
    print(f"  Density threshold: {args.density_threshold:.5f}")
    density_metric = "internal_density (count-based)" if args.ignore_SJI else "weighted_density / SJI (weight-based)"
    print(f"  Density metric: {density_metric}")
    if args.prune_weak_pairs:
        print(f"  Prune weak pairs: ENABLED (SJI < {args.density_threshold:.5f})")
    print(f"  Resolution: {args.resolution}")
    print(f"  Parallel jobs: {args.jobs}")

    # Load global partition and metrics
    print("\n" + "="*70)
    print("STEP 1: Loading global partition and metrics")
    print("="*70)
    partition_global = load_partition(args.partition, args.delimiter)
    metrics_global = load_metrics(args.metrics, args.delimiter)
    print(f"  Global partition: {len(partition_global):,} nodes")
    print(f"  Global metrics: {len(metrics_global)} clusters")
    if not args.prefer_threads and len(partition_global) >= 1_000_000 and args.jobs != 1:
        print("  Note: process-based parallelism copies the global partition into workers.")
        print("        For large partitions, --prefer_threads can reduce memory pressure.")

    if not args.ignore_SJI and 'weighted_density' not in metrics_global.columns:
        raise ValueError(
            "FATAL: Metrics file is missing 'weighted_density', but Script 03 uses "
            "weighted_density/SJI by default. Regenerate metrics with Script 02/03 "
            "or rerun with --ignore_SJI to use internal_density instead."
        )

    if args.prune_weak_pairs and args.ignore_SJI:
        print("\nERROR: --prune_weak_pairs cannot be used with --ignore_SJI")
        print("  Pruning is based on SJI (weighted_density) threshold")
        return

    iteration = 0
    while True:
        iteration += 1
        print(f"\n{'='*70}")
        print(f"ITERATION {iteration}")
        print(f"{'='*70}")
        prune_checkpoint_saved = False

        # Optional: Prune weak 2-node clusters before discovering candidates
        # Runs each iteration so newly formed weak 2-node child clusters are also split
        if args.prune_weak_pairs:
            print("\n" + "="*70)
            print("STEP 1b: Pruning weak 2-node clusters")
            print("="*70)

            partition_before_prune = partition_global
            metrics_before_prune = metrics_global
            partition_global, metrics_global = filter_weak_2node_clusters(
                edges_dir=edges_dir,
                partition=partition_global,
                metrics=metrics_global,
                weighted_density_threshold=args.density_threshold,
                base_network_name=args.base_network_name
            )

            print(f"  Updated partition: {len(partition_global):,} nodes")
            print(f"  Updated metrics: {len(metrics_global)} clusters")

            pruning_changed = (
                partition_global is not partition_before_prune or
                metrics_global is not metrics_before_prune
            )
            if pruning_changed:
                print("\n  Checkpoint: Saving updated partition and metrics to disk...")
                partition_output = Path(args.partition)
                metrics_output = Path(args.metrics)
                save_partition_and_metrics(
                    partition=partition_global,
                    metrics=metrics_global,
                    partition_output=partition_output,
                    metrics_output=metrics_output,
                    backup=True,
                    delimiter=args.delimiter
                )
                prune_checkpoint_saved = True
                print(f"  ✓ Checkpoint saved (crash-safe)")
            else:
                print("  No pruning changes to checkpoint")

        # Find all subnetwork files
        print("\n" + "="*70)
        print("STEP 2: Discovering subnetwork files")
        print("="*70)
        subnetworks = find_all_subnetworks(edges_dir, args.base_network_name)

        if not subnetworks:
            print("  No subnetwork files found. Exiting.")
            break

        # Select subnetworks based on metrics and already-processed status
        print("\n" + "="*70)
        print("STEP 3: Selecting subnetworks for refinement")
        print("="*70)

        # Build processed set in one os.walk pass (avoids 2 stat() calls per file)
        print("  Building processed-subnetwork index...")
        t_scan = time.time()
        processed_set = build_processed_set(edges_dir)
        print(f"  Done in {time.time() - t_scan:.5f}s")

        # Create metrics lookup dict for fast access
        metrics_dict = metrics_global.set_index('cluster_id').to_dict('index')

        # Filter subnetworks based on criteria
        candidates = []
        already_processed = []
        no_metrics = []
        too_small = []
        above_threshold = []

        for filepath, cluster_id in subnetworks:
            # Check if already processed (in-memory lookup, no stat calls)
            if str(filepath) in processed_set:
                already_processed.append(cluster_id)
                continue

            # Check if cluster has metrics
            if cluster_id not in metrics_dict:
                no_metrics.append(cluster_id)
                continue

            metrics_row = metrics_dict[cluster_id]

            # Check refinement criteria
            # Use SJI (weighted_density) by default; fall back to internal_density if --ignore_SJI
            density_value = metrics_row['internal_density'] if args.ignore_SJI else metrics_row['weighted_density']

            if metrics_row['size'] < args.min_nodes:
                too_small.append(cluster_id)
            elif density_value > args.density_threshold:
                above_threshold.append(cluster_id)
            else:
                candidates.append((filepath, cluster_id, metrics_row['size'], density_value))

        print(f"  Total subnetworks found: {len(subnetworks)}")
        print(f"  Already processed (skipped): {len(already_processed)}")
        print(f"  No metrics available (skipped): {len(no_metrics)}")
        print(f"  Too small (skipped): {len(too_small)}")
        print(f"  Above density threshold (skipped): {len(above_threshold)}")
        print(f"  Selected for refinement: {len(candidates)}")

        if len(candidates) == 0:
            print("\n  No subnetworks meet refinement criteria. Converged.")
            break

        # Show top candidates
        print(f"\n  Top 10 candidates by size:")
        sorted_candidates = sorted(candidates, key=lambda x: x[2], reverse=True)
        for filepath, cluster_id, size, density in sorted_candidates[:10]:
            print(f"    {cluster_id}: size={size:,}, density={density:.5f}")

        # Process subnetworks in parallel
        print("\n" + "="*70)
        print(f"STEP 4: Processing {len(candidates)} subnetworks in parallel")
        print("="*70)

        # Extract just filepath and cluster_id for processing
        tasks = [(filepath, cluster_id) for filepath, cluster_id, _, _ in candidates]

        # Run parallel refinement
        backend_type = "threads" if args.prefer_threads else "processes"
        print(f"  Starting parallel processing with {args.jobs} workers ({backend_type})...")

        # Memory consideration:
        # - Processes: Each worker gets copy of partition_global (~200MB per 1M nodes × N workers)
        # - Threads: Shared memory, but Python GIL overhead (mitigated by leidenalg C++ code)
        # Use --prefer_threads for large partitions (>5M nodes) or many workers (>32)

        parallel_kwargs = {'n_jobs': args.jobs, 'verbose': 10}
        if args.prefer_threads:
            parallel_kwargs['prefer'] = 'threads'

        results = Parallel(**parallel_kwargs)(
            delayed(refine_single_subnetwork)(
                filepath,
                cluster_id,
                partition_global,
                args.resolution,
                args.base_network_name,
                stable_cluster_seed(args.seed, cluster_id),
                args.use_gpu_leiden,
                verbose=False,  # Suppress per-subnetwork output
                delimiter=args.delimiter
            )
            for i, (filepath, cluster_id) in enumerate(tasks)
        )

        # Count successes
        successful = sum(1 for _, success in results if success)
        failed = len(results) - successful

        print(f"\n  Processing complete:")
        print(f"    Successful refinements: {successful}")
        print(f"    No split (unchanged): {failed}")

        if successful == 0:
            print("\n  No subnetworks were split. Converged.")
            break

        # Merge temporary results
        print("\n" + "="*70)
        print("STEP 5: Merging temporary results into global files")
        print("="*70)

        merged_partition, merged_metrics = merge_temp_results(
            edges_dir,
            partition_global,
            metrics_global,
            args.delimiter
        )

        # Write updated global files
        print("\n" + "="*70)
        print("STEP 6: Writing updated partition and metrics files")
        print("="*70)

        partition_output = Path(args.partition)
        metrics_output = Path(args.metrics)

        print("")  # Blank line for formatting
        save_partition_and_metrics(
            partition=merged_partition,
            metrics=merged_metrics,
            partition_output=partition_output,
            metrics_output=metrics_output,
            backup=not prune_checkpoint_saved,
            delimiter=args.delimiter
        )

        # Cleanup temporary files
        cleanup_temp_files(edges_dir)

        # Iteration summary
        print("\n" + "="*70)
        print(f"ITERATION {iteration} SUMMARY")
        print("="*70)
        print(f"  Subnetworks discovered: {len(subnetworks)}")
        print(f"  Subnetworks processed: {len(candidates)}")
        print(f"  Successful refinements: {successful}")
        print(f"  Total nodes in partition: {len(merged_partition):,}")
        print(f"  Total clusters in metrics: {len(merged_metrics)}")
        print(f"  Elapsed time: {time.time() - total_start:.5f}s")
        print(f"\n  Output files:")
        print(f"    Partition: {partition_output}")
        print(f"    Metrics: {metrics_output}")
        print("="*70)

        # Update in-memory state for next iteration
        partition_global = merged_partition
        metrics_global = merged_metrics

        if not args.auto_recursive:
            print("\n  --auto_recursive not set: exiting after one pass.")
            print("  Rerun the same command to continue refinement.")
            print("  Previously refined subnetworks and subnetworks marked")
            print("  unsplittable will be skipped; newly formed child clusters")
            print("  will be discovered and processed.")
            break

    print(f"\nAll done. Total time: {time.time() - total_start:.5f}s")


if __name__ == "__main__":
    main()
