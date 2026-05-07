#!/usr/bin/env python3
"""
GPU-based global Leiden clustering using RAPIDS cuGraph.

This script:
1. Loads a preprocessed integer edge list (CSV by default: src, dst, weight)
2. Identifies connected components (handles disconnected graphs)
3. Runs Leiden clustering on the full graph
4. Computes community-level metrics (size, internal edge count, internal density)
5. Outputs partition, metrics, and subnetwork files for hierarchical refinement

Output Files:
- partition.csv: Complete integer node→cluster mapping
- metrics.csv: Metrics for all clusters (size, density, etc.)
- subnetworks/: Directory containing subnetwork files
  - subnet.0: Edges for cluster 0
  - subnet.1: Edges for cluster 1
  - ... (one file per cluster)

These outputs feed directly into Script 03 for hierarchical refinement.

Requirements:
    - RAPIDS cuGraph (conda install -c rapidsai cugraph)
    - CUDA-capable GPU with sufficient VRAM (24GB recommended for ~259M edges)

Usage:
    python 02_gpu_leiden_global.py \
        --edges network.csv \
        --output_dir networks \
        --resolution 1.0 \
        --max_iter 100

    # Skip subnetwork writing (faster, but can't use Script 03):
    python 02_gpu_leiden_global.py \
        --edges network.csv \
        --skip_subnetworks

Input format from 01_preprocess.py (CSV by default, no header):
    src_id,dst_id,weight

Output format:
    networks/partition.csv: node_id,cluster_id
    networks/metrics.csv:   cluster_id,size,internal_edges,...
    networks/subnetworks/subnet.0: src_id,dst_id,weight (subnetwork for cluster 0)
"""

import argparse
import csv
import os
import time
import sys
from pathlib import Path

cudf = None
cugraph = None


def import_rapids():
    """
    Import RAPIDS lazily so argparse help and static checks work without a GPU env.
    """
    global cudf, cugraph

    if cudf is not None and cugraph is not None:
        return

    try:
        import cudf as _cudf
        import cugraph as _cugraph
    except ImportError:
        print("ERROR: RAPIDS cuGraph not found.")
        print("Install with: conda install -c rapidsai -c conda-forge -c nvidia cugraph cuda-version=12.0")
        sys.exit(1)

    cudf = _cudf
    cugraph = _cugraph


def normalize_delimiter(delimiter: str) -> str:
    """
    Accept readable delimiter names on the CLI while defaulting to CSV commas.
    """
    if delimiter in ("tab", "\\t", "tsv"):
        return "\t"
    if delimiter in ("comma", "csv"):
        return ","
    if len(delimiter) != 1:
        raise ValueError("--delimiter must be a single character, 'comma', 'csv', 'tab', 'tsv', or '\\t'")
    return delimiter


def load_edgelist(path: str, has_header: bool = False, validation_rows: int = 10, delimiter: str = ','):
    """
    Load edge list into cuDF DataFrame.
    Requires integer node IDs produced by 01_preprocess.py.

    Input data rows must have exactly 3 delimited columns. By default this
    validates a prefix of the file to avoid a second full pass over very large
    edge lists. Pass validation_rows=0 to validate the entire file before load.
    """
    print(f"Loading edge list from {path}...")
    t0 = time.time()

    # VALIDATION: Check data rows for format issues
    # Note: Header row (if present) is allowed to have ≥3 columns
    with open(path, 'r') as f:
        lines_checked = 0

        for line_num, line in enumerate(f, 1):
            raw_line = line.rstrip('\r\n')

            # Skip blank/whitespace-only lines
            if not raw_line.strip():
                continue

            # Skip header line
            if has_header and line_num == 1:
                header_parts = next(csv.reader([raw_line], delimiter=delimiter))
                if len(header_parts) > 3:
                    print(f"  Note: Header has {len(header_parts)} columns, using first 3", file=sys.stderr)
                continue

            # Validate data rows
            parts = next(csv.reader([raw_line], delimiter=delimiter))
            if len(parts) != 3:
                raise ValueError(
                    f"FATAL: Data line {line_num} has {len(parts)} columns (expected exactly 3)\n"
                    f"  Content: {raw_line[:100]}\n"
                    f"  This file format is invalid for Script 02.\n"
                    f"  Expected format: src{delimiter}dst{delimiter}weight\n"
                    f"  Did you run 01_preprocess.py to create integer IDs?"
                )

            lines_checked += 1
            if validation_rows > 0 and lines_checked >= validation_rows:
                break

        if validation_rows > 0:
            print(f"  ✓ Sample-validated {lines_checked} data rows have exactly 3 columns")
        else:
            print(f"  ✓ Validated all {lines_checked:,} data rows have exactly 3 columns")

    # SIMPLIFIED HEADER HANDLING: Treat header as just a row to skip
    # Don't rely on header names (too fragile - users may have different names)
    if has_header:
        # Skip first row (header), read by position only
        # Don't specify dtype at read time (header names unknown)
        df = cudf.read_csv(
            path,
            sep=delimiter,
            header=0,           # First row is header (will be used as temp column names)
            usecols=[0, 1, 2]   # Read first 3 columns by position
        )
        # Rename to canonical names (whatever the header said, we use these)
        df.columns = ['src', 'dst', 'weight']

        # Apply dtypes AFTER renaming (avoid header name dependency).
        df['src'] = df['src'].astype('int64')
        df['dst'] = df['dst'].astype('int64')
        df['weight'] = df['weight'].astype('float32')
    else:
        # No header - read all columns and validate
        df = cudf.read_csv(
            path,
            sep=delimiter,
            header=None,
            names=['src', 'dst', 'weight'],
            dtype={'src': 'int64', 'dst': 'int64', 'weight': 'float32'}
        )

        # Verify exactly 3 columns (data rows must have exactly 3)
        if len(df.columns) != 3:
            raise ValueError(
                f"FATAL: DataFrame has {len(df.columns)} columns (expected 3)\n"
                f"  This indicates data rows with ≠3 columns.\n"
                f"  Did you run 01_preprocess.py to create integer IDs?"
            )

    print(f"  ✓ Loaded format: 3 columns (src, dst, weight)")
    print(f"  Loaded {len(df):,} edges in {time.time() - t0:.1f}s")
    return df


def canonicalize_edges(df, weight_agg: str = 'max'):
    """
    Canonicalize undirected edges: ensure (u,v) where u < v.
    Removes duplicates and self-loops.
    
    Args:
        df: DataFrame with src, dst, weight columns
        weight_agg: How to aggregate weights for duplicate edges
                    'max', 'mean', 'sum', or 'first'
    
    Returns:
        Deduplicated DataFrame with canonical edge ordering
    """
    print("Canonicalizing undirected edges...")
    t0 = time.time()
    original_count = len(df)

    if original_count == 0:
        raise ValueError("FATAL: Edge list is empty")
    
    # Create canonical ordering: (min, max)
    df = df.copy()
    mask = df['src'] > df['dst']
    
    # Swap where src > dst
    src_temp = df.loc[mask, 'src'].copy()
    df.loc[mask, 'src'] = df.loc[mask, 'dst']
    df.loc[mask, 'dst'] = src_temp
    
    # Remove self-loops
    self_loops = df['src'] == df['dst']
    n_self_loops = int(to_host_scalar(self_loops.sum()))
    if n_self_loops > 0:
        print(f"  Removing {n_self_loops:,} self-loops")
        df = df[~self_loops]
    
    # Aggregate duplicates
    if weight_agg == 'max':
        df = df.groupby(['src', 'dst'], as_index=False)['weight'].max()
    elif weight_agg == 'mean':
        df = df.groupby(['src', 'dst'], as_index=False)['weight'].mean()
    elif weight_agg == 'sum':
        df = df.groupby(['src', 'dst'], as_index=False)['weight'].sum()
    else:  # 'first'
        df = df.drop_duplicates(subset=['src', 'dst'], keep='first')
    
    n_removed = original_count - len(df)
    print(f"  Canonicalized in {time.time() - t0:.1f}s")
    print(f"  Removed {n_removed:,} edges ({100*n_removed/original_count:.1f}%)")
    print(f"  Final edge count: {len(df):,}")
    
    return df


def to_host_scalar(value):
    """
    Convert cuDF/CuPy/NumPy scalar-like values to plain Python scalars.
    Keeps logging and control-flow comparisons robust across RAPIDS versions.
    """
    if hasattr(value, "item"):
        return value.item()
    return value


def prepare_node_ids(df):
    """
    Prepare graph vertex IDs for cuGraph.

    Script 02 now requires integer IDs from 01_preprocess.py. They must be dense
    and zero-based because cuGraph is built with renumber=False.
    """
    print("Validating node IDs for cuGraph...")
    t0 = time.time()

    try:
        df['src'] = df['src'].astype('int64')
        df['dst'] = df['dst'].astype('int64')
    except (ValueError, TypeError):
        raise ValueError(
            "FATAL: Script 02 requires integer node IDs from 01_preprocess.py. "
            "String IDs are no longer remapped inside Script 02."
        )

    all_nodes = cudf.concat([df['src'], df['dst']], ignore_index=True).drop_duplicates()
    n_nodes = len(all_nodes)

    if n_nodes == 0:
        raise ValueError("FATAL: Edge list contains no usable nodes after preprocessing")

    min_id = int(to_host_scalar(all_nodes.min()))
    max_id = int(to_host_scalar(all_nodes.max()))

    if min_id != 0 or max_id != n_nodes - 1:
        raise ValueError(
            "FATAL: Script 02 requires dense zero-based integer node IDs from 01_preprocess.py.\n"
            f"  Found min={min_id:,}, max={max_id:,}, unique={n_nodes:,}.\n"
            "  Refusing to silently remap IDs because protein_id_mapping.csv must remain authoritative."
        )

    print("  Node IDs are dense zero-based integers")
    df['src_int'] = df['src']
    df['dst_int'] = df['dst']

    print(f"  Validated {n_nodes:,} unique nodes in {time.time() - t0:.1f}s")
    return df, n_nodes


def find_connected_components(G):
    """
    Find connected components in the graph.
    """
    print("Finding connected components...")
    t0 = time.time()
    
    cc = cugraph.connected_components(G)
    n_components = int(to_host_scalar(cc['labels'].nunique()))
    
    # Component size distribution
    sizes = cc.groupby('labels').size().reset_index()
    sizes.columns = ['component', 'size']
    sizes = sizes.sort_values('size', ascending=False).reset_index(drop=True)
    
    print(f"  Found {n_components} connected components in {time.time() - t0:.1f}s")
    print(f"  Largest component: {int(to_host_scalar(sizes['size'].iloc[0])):,} nodes")
    if len(sizes) > 1:
        print(f"  Second largest: {int(to_host_scalar(sizes['size'].iloc[1])):,} nodes")
    
    return cc, sizes


def run_leiden(G, resolution: float, max_iter: int, seed: int):
    """
    Run Leiden clustering.
    """
    print(f"Running Leiden (resolution={resolution}, max_iter={max_iter}, seed={seed})...")
    t0 = time.time()
    
    partition, modularity = cugraph.leiden(
        G,
        resolution=resolution,
        max_iter=max_iter,
        random_state=seed
    )
    
    partition.columns = ['node', 'community']
    n_communities = int(to_host_scalar(partition['community'].nunique()))
    
    print(f"  Leiden completed in {time.time() - t0:.1f}s")
    print(f"  Found {n_communities:,} communities")
    print(f"  Modularity: {modularity:.4f}")
    
    return partition, modularity


def compute_community_metrics(df_edges, partition):
    """
    Compute metrics for each community:
    - size (number of nodes)
    - internal_edges (edges within community)
    - internal_weight_sum (sum of weights within community)
    - avg_internal_weight (average weight of internal edges)

    These metrics help decide which communities need further refinement.

    NOTE: Uses 'cluster_id' column name for compatibility with Script 03.

    Args:
        df_edges: cuDF DataFrame with columns [src_int, dst_int, weight]
        partition: cuDF DataFrame with columns [node, community]
    """
    print("Computing community metrics...")
    t0 = time.time()

    # Community sizes
    sizes = partition.groupby('community').size().reset_index()
    sizes.columns = ['cluster_id', 'size']  # Use cluster_id for Script 03 compatibility

    # OPTIMIZED GPU-NATIVE APPROACH: Use merges with minimal intermediate DataFrames
    # Avoid .copy() and Python dict conversions (which spike host RAM)
    # Strategy: Work on views/slices, merge only needed columns, clean up intermediates

    # Add community labels to edges via merge (GPU-native)
    # Use minimal column set to reduce VRAM
    partition_src = partition[['node', 'community']].rename(
        columns={'node': 'src_int', 'community': 'src_comm'}
    )
    partition_dst = partition[['node', 'community']].rename(
        columns={'node': 'dst_int', 'community': 'dst_comm'}
    )

    # Select only needed columns from edges (avoid copying weight unnecessarily early)
    edges_minimal = df_edges[['src_int', 'dst_int', 'weight']]

    # Merge 1: Add src community
    edges_labeled = edges_minimal.merge(partition_src, on='src_int', how='left')
    del partition_src  # Free immediately

    # Merge 2: Add dst community
    edges_labeled = edges_labeled.merge(partition_dst, on='dst_int', how='left')
    del partition_dst  # Free immediately

    # Filter to internal edges (both endpoints in same community)
    internal = edges_labeled[edges_labeled['src_comm'] == edges_labeled['dst_comm']]
    del edges_labeled  # Free immediately

    # Aggregate by community
    internal_stats = internal.groupby('src_comm').agg({
        'weight': ['count', 'sum', 'mean']
    }).reset_index()
    del internal  # Free immediately

    internal_stats.columns = ['cluster_id', 'internal_edges', 'internal_weight_sum', 'avg_internal_weight']

    # Merge with sizes
    metrics = sizes.merge(internal_stats, on='cluster_id', how='left')
    metrics = metrics.fillna(0)

    # Sort by size descending
    metrics = metrics.sort_values('size', ascending=False).reset_index(drop=True)

    print(f"  Computed metrics for {len(metrics):,} communities in {time.time() - t0:.1f}s")

    return metrics


def compute_internal_density(metrics):
    """
    Compute internal density metric for each community.

    internal_density = internal_edges / max_possible_internal_edges
    where max_possible = size * (size - 1) / 2

    This helps identify "loose" communities that might benefit from refinement.
    High density (~0.1+) = tight community, likely doesn't need refinement.
    Low density (~0.001) = loose community, may contain substructure.
    """
    print("Computing internal density metrics...")

    # CRITICAL: Cast to int64 to prevent overflow for large communities
    # For a community with 100k nodes, size*(size-1) reaches 10^10, exceeding int32 max.
    # Keep these as cuDF Series to avoid fragile CuPy array assignment across RAPIDS versions.
    sizes = metrics['size'].astype('int64')
    internal_edges = metrics['internal_edges'].astype('int64')

    # Max possible edges in each community.
    # Use int64 arithmetic to prevent overflow and integer division to avoid
    # count columns briefly becoming floats.
    max_possible = (sizes * (sizes - 1)) // 2

    # Keep reported max_internal_edges true for singletons (0), but use
    # denominator 1 for density to avoid division by zero.
    density_denominator = max_possible.copy()
    density_denominator[density_denominator < 1] = 1

    # Internal density (float64)
    density = internal_edges.astype('float64') / density_denominator.astype('float64')
    weighted_density = (
        metrics['internal_weight_sum'].astype('float64') /
        density_denominator.astype('float64')
    )

    metrics['internal_density'] = density
    metrics['weighted_density'] = weighted_density
    metrics['max_internal_edges'] = max_possible

    return metrics


def ensure_output_parent(path: str):
    """
    Create an output file's parent directory when a parent was provided.
    """
    parent = Path(path).expanduser().parent
    if parent != Path('.'):
        parent.mkdir(parents=True, exist_ok=True)


def temp_output_path(path) -> Path:
    final_path = Path(path)
    return final_path.with_name(f".{final_path.name}.tmp.{os.getpid()}.{time.time_ns()}")


def atomic_to_csv(df, path, sep: str, index: bool = False, header: bool = True) -> None:
    final_path = Path(path)
    tmp_path = temp_output_path(final_path)
    try:
        df.to_csv(tmp_path, sep=sep, index=index, header=header)
        tmp_path.replace(final_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise


def write_cluster_subnetworks(df_edges, partition, output_dir, subnetwork_dir_name, subnetwork_prefix, delimiter: str = ','):
    """
    Write subnetwork files for each cluster.

    Creates directory structure:
        {output_dir}/{subnetwork_dir_name}/
        {output_dir}/{subnetwork_dir_name}/{subnetwork_prefix}.0
        {output_dir}/{subnetwork_dir_name}/{subnetwork_prefix}.1
        ...

    This enables Script 03 to refine specific clusters.
    """
    print("\nWriting cluster subnetworks...")
    t0 = time.time()

    # Create subnetwork directory
    output_path = Path(output_dir)
    subnetwork_dir = output_path / subnetwork_dir_name
    subnetwork_dir.mkdir(parents=True, exist_ok=True)

    print(f"  Subnetwork directory: {subnetwork_dir}")

    # Get unique clusters
    unique_clusters = partition['community'].unique()
    n_clusters = len(unique_clusters)

    print(f"  Writing {n_clusters:,} subnetwork files...")

    # OPTIMIZED GPU-NATIVE APPROACH: Use merges with immediate cleanup
    # Avoids host RAM spike from Python dict conversions

    # Prepare minimal lookup DataFrames (only needed columns)
    partition_src = partition[['node', 'community']].rename(
        columns={'node': 'src_int', 'community': 'src_comm'}
    )
    partition_dst = partition[['node', 'community']].rename(
        columns={'node': 'dst_int', 'community': 'dst_comm'}
    )
    # Start with minimal edge columns
    edges_work = df_edges[['src_int', 'dst_int', 'weight']]

    # Add community labels (2 merges)
    edges_work = edges_work.merge(partition_src, on='src_int', how='left')
    del partition_src
    edges_work = edges_work.merge(partition_dst, on='dst_int', how='left')
    del partition_dst

    # Filter to internal edges. Script 02 outputs the same integer IDs it received.
    internal_edges = edges_work[edges_work['src_comm'] == edges_work['dst_comm']]
    del edges_work

    # Final DataFrame has: src_comm, src_int, dst_int, weight
    internal_edges = internal_edges[['src_comm', 'src_int', 'dst_int', 'weight']]

    # PERFORMANCE OPTIMIZATION: Use groupby instead of repeated filtering
    # Old approach: N filter operations → N GPU kernel launches (slow for many clusters)
    # New approach: 1 groupby operation → iterate through pre-partitioned groups
    print(f"  Grouping edges by cluster (single GPU operation)...")
    t_group = time.time()
    grouped = internal_edges.groupby('src_comm')
    print(f"    Groupby completed in {time.time() - t_group:.1f}s")

    # Write one file per cluster
    # Iterate through the grouped object directly (clusters already partitioned on GPU)
    written_files = []
    print(f"  Writing files for {n_clusters:,} clusters...")
    t_write = time.time()

    for cluster_id, cluster_edges in grouped:
        if len(cluster_edges) > 0:
            # GPU-native CSV writing (avoids host memory blowup from to_pandas())
            # For large clusters (50M+ edges), to_pandas() could allocate 5-10 GB host RAM
            # cuDF's to_csv() writes directly from GPU memory
            filename = f"{subnetwork_prefix}.{int(cluster_id)}"
            filepath = subnetwork_dir / filename

            # Select columns and write directly from GPU (no pandas conversion)
            atomic_to_csv(cluster_edges[['src_int', 'dst_int', 'weight']], filepath, sep=delimiter, index=False, header=False)

            written_files.append(str(filepath))

    print(f"    File writing completed in {time.time() - t_write:.1f}s")

    print(f"  Wrote {len(written_files):,} subnetwork files in {time.time() - t0:.1f}s")
    print(f"  Files written to: {subnetwork_dir}/")

    return subnetwork_dir, written_files


def main():
    parser = argparse.ArgumentParser(description="GPU Leiden clustering with cuGraph")
    parser.add_argument("--edges", required=True, help="Input integer edge list from 01_preprocess.py")
    parser.add_argument("--output_dir", default="networks",
                        help="Output directory for Script 02 outputs (default: networks)")
    parser.add_argument("--out_partition", default=None,
                        help="Output partition file (default: <output_dir>/partition.csv)")
    parser.add_argument("--out_metrics", default=None,
                        help="Output community metrics file (default: <output_dir>/metrics.csv)")
    parser.add_argument("--delimiter", default=",",
                        help="Input/output delimiter: default ','; use 'tab' or '\\t' for TSV")
    parser.add_argument("--subnetwork_dir", default="subnetworks",
                        help="Directory name under output_dir for subnetwork files (default: subnetworks)")
    parser.add_argument("--subnetwork_prefix", default="subnet",
                        help="Filename prefix for subnetwork files (default: subnet)")
    parser.add_argument("--resolution", type=float, default=1.0,
                        help="Leiden resolution parameter (default: 1.0)")
    parser.add_argument("--max_iter", type=int, default=100,
                        help="Maximum iterations (default: 100)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for Leiden (default: 42)")
    parser.add_argument("--has_header", action="store_true",
                        help="Input file has header row")
    parser.add_argument("--validation_rows", type=int, default=10,
                        help="Number of data rows to validate before cuDF load; 0 validates all rows (default: 10)")
    parser.add_argument("--no_canonicalize", action="store_true",
                        help="Skip edge canonicalization (use if edges are already deduplicated)")
    parser.add_argument("--weight_agg", choices=['max', 'mean', 'sum', 'first'],
                        default='max',
                        help="Weight aggregation for duplicate edges (default: max)")
    parser.add_argument("--skip_subnetworks", action="store_true",
                        help="Skip writing subnetwork files (saves time/space)")
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

    import_rapids()
    
    total_start = time.time()
    
    # Load edges
    df = load_edgelist(args.edges, args.has_header, args.validation_rows, args.delimiter)
    
    # Canonicalize undirected edges (default behavior)
    # This ensures each edge appears once as (min, max) and removes self-loops
    if not args.no_canonicalize:
        df = canonicalize_edges(df, args.weight_agg)
    else:
        print("Skipping edge canonicalization (--no_canonicalize specified)")

    if len(df) == 0:
        raise ValueError("FATAL: Edge list is empty after canonicalization/self-loop removal")

    df, n_nodes = prepare_node_ids(df)
    
    # Build graph
    print("Building cuGraph graph object...")
    t0 = time.time()

    # Validate before building graph
    print(f"  Edge DataFrame has {len(df):,} edges")
    print(f"  Validated node count: {n_nodes:,}")
    edge_min = min(
        int(to_host_scalar(df['src_int'].min())),
        int(to_host_scalar(df['dst_int'].min()))
    )
    edge_max = max(
        int(to_host_scalar(df['src_int'].max())),
        int(to_host_scalar(df['dst_int'].max()))
    )
    print(f"  Node ID range in edges: {edge_min:,} to {edge_max:,}")

    # Estimate memory before graph construction (best-effort across cuDF versions)
    try:
        df_memory_gb = df.memory_usage(deep=True).sum() / 1e9
    except (TypeError, AttributeError):
        # Fallback: some cuDF versions don't support deep=True
        try:
            df_memory_gb = df.memory_usage().sum() / 1e9
        except:
            # Last resort: rough estimate (3 columns × 8 bytes × row count)
            df_memory_gb = (len(df) * 3 * 8) / 1e9
    print(f"  Edge DataFrame memory: ~{df_memory_gb:.2f} GB")

    G = cugraph.Graph(directed=False)
    G.from_cudf_edgelist(
        df[['src_int', 'dst_int', 'weight']],
        source='src_int',
        destination='dst_int',
        edge_attr='weight',
        renumber=False  # IDs were prepared above; keep them stable for joins
    )
    print(f"  Graph built in {time.time() - t0:.1f}s")
    print(f"  Nodes: {G.number_of_vertices():,}")
    print(f"  Edges: {G.number_of_edges():,}")

    # CRITICAL: Check that cuGraph did not add/drop vertices during construction
    if G.number_of_vertices() != n_nodes:
        raise ValueError(
            f"FATAL: Graph has {G.number_of_vertices():,} nodes but input has "
            f"{n_nodes:,} dense integer IDs. Refusing to write a corrupt partition."
        )
    
    # Optional connected-component summary
    cc_count = None
    if args.verbose_cc:
        _cc, cc_sizes = find_connected_components(G)
        cc_count = len(cc_sizes)
    else:
        print("Skipping connected-component summary (use --verbose_cc to enable)")
    
    # Run Leiden
    partition, modularity = run_leiden(G, args.resolution, args.max_iter, args.seed)

    # Validate that every Leiden output node is one of the input integer IDs
    print("\nValidating partition completeness...")

    # OPTIMIZATION: Quick check before expensive set operations
    n_partition = len(partition)
    expected_count = n_nodes

    print(f"  Partition has {n_partition:,} nodes")
    print(f"  Expected dense node count: {expected_count:,}")

    if n_partition == expected_count:
        # Fast path: counts match, likely no missing nodes
        # Still verify, but expect success
        print(f"  Counts match - running quick validation...")

    node_min = int(to_host_scalar(partition['node'].min()))
    node_max = int(to_host_scalar(partition['node'].max()))
    n_unique_partition_nodes = int(to_host_scalar(partition['node'].nunique()))

    if n_partition != expected_count or n_unique_partition_nodes != expected_count or node_min != 0 or node_max != expected_count - 1:
        raise ValueError(
            "FATAL: Leiden partition is not complete over the dense input node IDs.\n"
            f"  rows={n_partition:,}, unique={n_unique_partition_nodes:,}, "
            f"min={node_min:,}, max={node_max:,}, expected=0..{expected_count - 1:,}"
        )
    else:
        print(f"  ✓ Partition covers all dense input node IDs")

    # Compute community metrics
    metrics = compute_community_metrics(df, partition)
    metrics = compute_internal_density(metrics)

    # Write subnetwork files for each cluster (before freeing df)
    # This enables Script 03 to refine specific clusters
    subnetwork_dir = None
    subnetwork_files = []
    if not args.skip_subnetworks:
        subnetwork_dir, subnetwork_files = write_cluster_subnetworks(
            df, partition, args.output_dir, args.subnetwork_dir, args.subnetwork_prefix, args.delimiter
        )

    # CRITICAL: Free edge DataFrame to reclaim VRAM
    # After metrics and subnetwork writing, we only need partition
    # For large networks (259M edges), this frees ~3-4 GB of GPU memory
    print(f"\nFreeing edge DataFrame ({df_memory_gb:.2f} GB)...")
    del df
    import gc
    gc.collect()
    print(f"  VRAM reclaimed for final processing")

    # Final output keeps the integer node IDs produced by 01_preprocess.py.
    print("\nPreparing integer-ID partition output...")
    partition_out = partition[['node', 'community']].copy()
    partition_out.columns = ['node_id', 'community_id']

    # IMPORTANT: Ensure community_id is string (prevents "9.0" float serialization issues)
    partition_out['community_id'] = partition_out['community_id'].astype(str)

    # Verify no data loss
    final_count = len(partition_out)
    expected_count = len(partition)
    if final_count != expected_count:
        print(f"  ERROR: Output has {final_count:,} rows but expected {expected_count:,}!")
    else:
        print(f"  Validation passed: {final_count:,} nodes in output")

    # Save outputs
    ensure_output_parent(args.out_partition)
    ensure_output_parent(args.out_metrics)

    print(f"\nSaving partition to {args.out_partition}...")
    atomic_to_csv(partition_out, args.out_partition, sep=args.delimiter, index=False, header=False)

    print(f"Saving metrics to {args.out_metrics}...")

    # CRITICAL: Enforce consistent data types before writing
    # This prevents mixed int/float formatting in output (e.g., "100" vs "100.0")
    metrics['cluster_id'] = metrics['cluster_id'].astype(str)

    # Integer columns (counts) - always write as integers without decimals
    int_columns = ['size', 'internal_edges', 'max_internal_edges']
    for col in int_columns:
        if col in metrics.columns:
            metrics[col] = metrics[col].astype('int64')

    # Float columns (computed metrics) - always write with consistent precision
    float_columns = ['internal_weight_sum', 'avg_internal_weight', 'internal_density', 'weighted_density']
    for col in float_columns:
        if col in metrics.columns:
            metrics[col] = metrics[col].astype('float64')

    atomic_to_csv(metrics, args.out_metrics, sep=args.delimiter, index=False)
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total nodes:        {G.number_of_vertices():,}")
    print(f"Total edges:        {G.number_of_edges():,}")
    if cc_count is not None:
        print(f"Connected components: {cc_count:,}")
    else:
        print("Connected components: skipped")
    print(f"Communities found:  {len(metrics):,}")
    print(f"Global modularity:  {modularity:.4f}")
    print(f"Resolution used:    {args.resolution}")
    print(f"Total time:         {time.time() - total_start:.1f}s")
    
    # Refinement candidates preview
    print("\nCommunities potentially needing refinement (size >= 10000):")
    large = metrics[metrics['size'] >= 10000].to_pandas()
    if len(large) > 0:
        for _, row in large.head(10).iterrows():
            cluster_id = row['cluster_id']
            # Convert to int if it's a simple number, otherwise keep as string
            try:
                cluster_id = int(cluster_id)
            except (ValueError, TypeError):
                pass
            print(f"  Cluster {cluster_id}: "
                  f"{int(row['size']):,} nodes, "
                  f"{int(row['internal_edges']):,} internal edges, "
                  f"density={row['internal_density']:.4f}")
        if len(large) > 10:
            print(f"  ... and {len(large) - 10} more large communities")
    else:
        print("  None (all communities < 10000 nodes)")
    
    print(f"\nOutputs written to:")
    print(f"  Partition: {args.out_partition}")
    print(f"  Metrics:   {args.out_metrics}")
    if subnetwork_dir:
        print(f"  Subnetworks: {subnetwork_dir}/ ({len(subnetwork_files)} files)")

    # Suggest next steps for Script 03
    if subnetwork_dir and len(subnetwork_files) > 0:
        print(f"\nNext steps for hierarchical refinement (Script 03):")
        print(f"  1. Review cluster metrics in: {args.out_metrics}")
        print(f"  2. Identify large, low-density clusters for refinement")
        print(f"  3. Run Script 03 on selected clusters:")
        print(f"     Example (refine cluster 0):")
        print(f"       python 03_leiden_refine.py \\")
        print(f"         --edges_dir {subnetwork_dir} \\")
        print(f"         --partition {args.out_partition} \\")
        print(f"         --metrics {args.out_metrics} \\")
        print(f"         --output_dir {args.output_dir} \\")
        print(f"         --base_network_name {args.subnetwork_prefix} \\")
        print(f"         --min_nodes 10000 \\")
        print(f"         --density_threshold 0.01")


if __name__ == "__main__":
    main()
