#!/usr/bin/env python3
"""
extract_subnetwork.py
=====================
Fast subnetwork extraction from a preprocessed binary graph index.

Strategy (two-stage seeded expansion with balanced coverage):

  Stage 0: Include all seed nodes.
  Stage 1: Per-seed local expansion (guaranteed minimum neighborhood).
           Each seed independently expands by raw edge weight up to a quota.
           Ensures sparse-region seeds get adequate representation.
  Stage 2: Global merit-based expansion (remaining budget).
           All frontier candidates compete by aggregate score.
           Nodes reachable from multiple seeds get boosted (sum of best
           edge weights per seed), naturally promoting bridge nodes.
  Stage 3: (Optional) Component stitching via shortest weighted paths.

Usage:
  python extract_subnetwork.py graph_index A0A2I3TW31 G3R0Y9 Q6ZQA0 \\
      --max_nodes 2000 --local_fraction 0.3 --output abc.csv
"""

import argparse
import csv
import heapq
import json
import mmap
import re
import struct
import sys
import time
from collections import defaultdict, deque

import numpy as np


# ===================================================================
# Binary index reader (mmap-backed for speed)
# ===================================================================

class GraphIndex:
    """Memory-mapped reader for the binary adjacency index."""

    ENTRY_SIZE = 8  # uint32 + float32
    ENTRY_DTYPE = np.dtype([("nbr", "<u4"), ("w", "<f4")])
    EMPTY_NEIGHBORS = ()

    def __init__(self, prefix: str, cache_neighbors: bool = True):
        self.prefix = prefix
        self._idx_fh = None
        self._idx_mm = None
        self._adj_fh = None
        self._adj_mm = None

        # Load node name mappings
        self._load_node_names()

        try:
            # Memory-map the index file
            idx_path = f"{prefix}.adj.index.bin"
            self._idx_fh = open(idx_path, "rb")
            self._idx_mm = mmap.mmap(self._idx_fh.fileno(), 0, access=mmap.ACCESS_READ)

            # Read header
            self.num_nodes = struct.unpack_from("<I", self._idx_mm, 0)[0]
            self._idx_header_size = 4
            self._idx_entry_size = 12  # uint64 offset + uint32 degree

            # Memory-map the adjacency file
            adj_path = f"{prefix}.adj.bin"
            self._adj_fh = open(adj_path, "rb")
            self._adj_mm = mmap.mmap(self._adj_fh.fileno(), 0, access=mmap.ACCESS_READ)
        except Exception:
            self.close()
            raise

        # The extraction algorithm fetches the same node's neighbors several
        # times (Stage 1 seeds, Stage 2 rescan, _finalize, _stitch_components).
        # Cache Python-native pairs so downstream dict/set-heavy loops avoid
        # repeated NumPy scalar boxing on every iteration.
        self._neighbor_cache = {} if cache_neighbors else None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _load_node_names(self):
        """Load the bidirectional node ID <-> name mapping."""
        self.id_to_name = {}
        self.name_to_id = {}
        tsv_path = f"{self.prefix}.node_ids.tsv"
        with open(tsv_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t", 1)
                nid = int(parts[0])
                name = parts[1]
                self.id_to_name[nid] = name
                self.name_to_id[name] = nid

    def resolve_seed(self, name: str) -> int:
        """Resolve a UniProt accession to its integer ID. Raises KeyError if not found."""
        if name in self.name_to_id:
            return self.name_to_id[name]
        raise KeyError(f"Seed accession not found in graph: {name}")

    def get_neighbors(self, node_id: int):
        """
        Return a cached list of Python (neighbor_id, weight) pairs for a node,
        already sorted by descending weight from preprocessing.
        Cached on first access if cache_neighbors=True.
        """
        if self._neighbor_cache is not None:
            cached = self._neighbor_cache.get(node_id)
            if cached is not None:
                return cached

        pos = self._idx_header_size + node_id * self._idx_entry_size
        offset, degree = struct.unpack_from("<QI", self._idx_mm, pos)

        if degree == 0:
            result = self.EMPTY_NEIGHBORS
        else:
            arr = np.frombuffer(
                self._adj_mm,
                dtype=self.ENTRY_DTYPE,
                count=degree,
                offset=offset,
            )
            result = list(zip(arr["nbr"].tolist(), arr["w"].tolist()))

        if self._neighbor_cache is not None:
            self._neighbor_cache[node_id] = result
        return result

    def get_degree(self, node_id: int) -> int:
        """Return the degree of a node without reading neighbors."""
        pos = self._idx_header_size + node_id * self._idx_entry_size
        _, degree = struct.unpack_from("<QI", self._idx_mm, pos)
        return degree

    def clear_cache(self):
        if self._neighbor_cache is not None:
            self._neighbor_cache.clear()

    def close(self):
        self.clear_cache()
        if self._adj_mm is not None:
            self._adj_mm.close()
            self._adj_mm = None
        if self._adj_fh is not None:
            self._adj_fh.close()
            self._adj_fh = None
        if self._idx_mm is not None:
            self._idx_mm.close()
            self._idx_mm = None
        if self._idx_fh is not None:
            self._idx_fh.close()
            self._idx_fh = None


# ===================================================================
# Subnetwork extraction
# ===================================================================

def extract_subnetwork(
    graph: GraphIndex,
    seed_ids: list,
    max_nodes: int = 2000,
    local_fraction: float = 0.3,
    stitch: bool = False,
):
    """
    Two-stage subnetwork extraction.

    Returns:
        selected: set of node IDs in the subnetwork
        edges: list of (node_a, node_b, weight) edges within the subnetwork
        seed_membership: dict mapping node_id -> set of seed_ids that reached it
    """

    selected = set(seed_ids)
    seed_membership = defaultdict(set)  # node_id -> set of seeds that reached it
    for s in seed_ids:
        seed_membership[s].add(s)

    budget = max_nodes - len(selected)
    if budget <= 0:
        return _finalize(graph, selected, seed_membership)

    n_seeds = len(seed_ids)

    # ------------------------------------------------------------------
    # Stage 1: Per-seed guaranteed local expansion
    # ------------------------------------------------------------------
    quota_per_seed = max(5, int(local_fraction * max_nodes / n_seeds))

    # Per-seed priority queues and frontier tracking
    # We track per-seed scores for Stage 2
    per_seed_scores = defaultdict(lambda: defaultdict(float))  # node -> seed -> best_weight

    local_selected = {seed: 0 for seed in seed_ids}
    local_heaps = {seed: [] for seed in seed_ids}
    active_stage1_seeds = set(seed_ids)
    for seed in seed_ids:
        for nbr, w in graph.get_neighbors(seed):
            if nbr not in selected:
                heapq.heappush(local_heaps[seed], (-w, nbr))
            per_seed_scores[nbr][seed] = max(per_seed_scores[nbr].get(seed, 0.0), w)

    while budget > 0 and active_stage1_seeds:
        added_this_round = False
        for seed in seed_ids:
            if seed not in active_stage1_seeds:
                continue

            local_heap = local_heaps[seed]
            while local_heap:
                _neg_w, node = heapq.heappop(local_heap)
                if node in selected:
                    continue

                selected.add(node)
                seed_membership[node].add(seed)
                local_selected[seed] += 1
                budget -= 1
                added_this_round = True

                # Expand this node's neighbors into the local frontier.
                # Record this seed's reach to *every* neighbor (selected or not),
                # so end-of-stage-1 sync can credit shared-bridge nodes.
                for nbr, w in graph.get_neighbors(node):
                    per_seed_scores[nbr][seed] = max(
                        per_seed_scores[nbr].get(seed, 0.0), w
                    )
                    if nbr not in selected:
                        heapq.heappush(local_heap, (-w, nbr))
                if local_selected[seed] >= quota_per_seed:
                    active_stage1_seeds.discard(seed)
                break
            else:
                # while-else: ran only because the heap drained without a
                # successful add from this seed's frontier.
                active_stage1_seeds.discard(seed)

            if budget <= 0:
                break

        if not added_this_round:
            break

    # Stage 1 only writes to seed_membership at the moment a node is added.
    # A later seed that *reaches* a node owned by an earlier seed (its heap
    # pop hits an already-selected node, or its BFS frontier brushes one)
    # would otherwise be missed — affecting both reached_by_seeds output and
    # Stage 2 propagation through that shared node. per_seed_scores has the
    # full reachability picture; fold it back into seed_membership now.
    stage1_membership_grew = set()
    for node, ss in per_seed_scores.items():
        if node in selected:
            before = len(seed_membership[node])
            seed_membership[node].update(ss.keys())
            if len(seed_membership[node]) > before:
                stage1_membership_grew.add(node)

    if budget <= 0:
        return _finalize(graph, selected, seed_membership)

    # ------------------------------------------------------------------
    # Stage 2: Global merit-based expansion
    # ------------------------------------------------------------------
    # Score per candidate = sum over seeds of (best weight on any path from
    # that seed). Multi-seed candidates accumulate, naturally promoting
    # bridge nodes. Aggregates are maintained incrementally as edges are
    # discovered, avoiding repeated O(n_seeds) sum() calls.

    candidate_seed_scores = defaultdict(dict)  # nbr -> {seed: best_w}
    agg_score = {}                              # nbr -> sum of values above

    def _bump(nbr, seeds_iter, w):
        """Update per-seed best weights for nbr; return delta to agg_score."""
        ss = candidate_seed_scores[nbr]
        delta = 0.0
        for s in seeds_iter:
            old = ss.get(s, 0.0)
            if w > old:
                delta += w - old
                ss[s] = w
        return delta

    # Seed initial candidate pool from Stage 1's frontier scores.
    for nbr, seed_scores in per_seed_scores.items():
        if nbr in selected:
            continue
        score = sum(seed_scores.values())
        if score <= 0.0:
            continue
        candidate_seed_scores[nbr] = seed_scores
        agg_score[nbr] = score
    del per_seed_scores

    # Augment only the selected nodes whose seed memberships grew during the
    # post-Stage-1 sync; other selected nodes already exposed their frontiers.
    for node in stage1_membership_grew:
        node_seeds = seed_membership[node]
        if not node_seeds:
            continue
        for nbr, w in graph.get_neighbors(node):
            if nbr in selected:
                continue
            delta = _bump(nbr, node_seeds, w)
            if delta > 0:
                agg_score[nbr] = agg_score.get(nbr, 0.0) + delta

    global_heap = [(-s, n) for n, s in agg_score.items()]
    heapq.heapify(global_heap)

    while global_heap and budget > 0:
        neg_score, node = heapq.heappop(global_heap)
        if node in selected:
            continue
        # Stale-entry guard: skip if a higher score has been pushed since.
        if -neg_score < agg_score.get(node, 0.0):
            continue

        selected.add(node)
        budget -= 1

        # Inherit seed memberships from the score-tracking dict
        seed_membership[node].update(candidate_seed_scores.get(node, {}).keys())

        node_seeds = seed_membership[node]
        for nbr, w in graph.get_neighbors(node):
            if nbr in selected:
                continue
            delta = _bump(nbr, node_seeds, w)
            if delta > 0:
                agg_score[nbr] = agg_score.get(nbr, 0.0) + delta
                heapq.heappush(global_heap, (-agg_score[nbr], nbr))

    # ------------------------------------------------------------------
    # Stage 3: Optional stitching
    # ------------------------------------------------------------------
    if stitch and budget > 0:
        _stage3_added, budget = _stitch_components(
            graph, selected, seed_membership, budget
        )

    return _finalize(graph, selected, seed_membership)


def _stitch_components(graph, selected, seed_membership, budget):
    """
    If the selected subnetwork has multiple connected components,
    attempt to connect them via shortest paths in the full graph.
    Note: this is an UNWEIGHTED hop-count BFS; edge weights are ignored when
    choosing the bridge. (TODO: switch to Dijkstra on a distance derived from
    the SJI weight if that matters for downstream analysis.)
    """
    # Find connected components within selected
    adj_local = defaultdict(set)
    for node in selected:
        for nbr, _w in graph.get_neighbors(node):
            if nbr in selected:
                adj_local[node].add(nbr)
                adj_local[nbr].add(node)

    visited = set()
    components = []
    for node in selected:
        if node in visited:
            continue
        comp = set()
        stack = [node]
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            comp.add(n)
            for nbr in adj_local[n]:
                if nbr not in visited:
                    stack.append(nbr)
        components.append(comp)

    if len(components) <= 1:
        return 0, budget  # already connected

    # Try to connect components pairwise via BFS in the full graph
    # Connect each remaining component to the current largest component.
    components.sort(key=len, reverse=True)
    main_comp = components[0]
    main_comp_seeds = set().union(
        *(seed_membership.get(node, set()) for node in main_comp)
    )
    stage3_added = 0

    for comp in components[1:]:
        if budget <= 0:
            break
        comp_seeds = set().union(*(seed_membership.get(node, set()) for node in comp))
        bridge_seeds = main_comp_seeds | comp_seeds
        # BFS from comp nodes toward main_comp
        path = _bfs_path(graph, comp, main_comp, max_explore=budget * 50)
        if path:
            for node in path:
                if node not in selected and budget > 0:
                    selected.add(node)
                    # Bridge nodes are synthetic connectors, so they inherit
                    # both components' seed labels by convention.
                    seed_membership[node].update(bridge_seeds)
                    budget -= 1
                    stage3_added += 1
            main_comp = main_comp | comp | set(path)
            main_comp_seeds.update(comp_seeds)

    return stage3_added, budget


def _bfs_path(graph, source_set, target_set, max_explore=10000):
    """BFS from source_set to find shortest path to target_set."""
    queue = deque()
    parent = {}
    for s in source_set:
        queue.append(s)
        parent[s] = None

    explored = 0
    while queue and explored < max_explore:
        node = queue.popleft()
        explored += 1
        if node in target_set and node not in source_set:
            # Reconstruct path
            path = []
            curr = node
            while curr is not None and curr not in source_set:
                path.append(curr)
                curr = parent[curr]
            return path

        for nbr, _w in graph.get_neighbors(node):
            if nbr not in parent:
                parent[nbr] = node
                queue.append(nbr)

    return None  # no path found within budget


def _finalize(graph, selected, seed_membership):
    """Extract edges within the selected subnetwork.

    Dedup strategy:
      - The adjacency is stored symmetrically; emitting only when nbr > node
        kills the (node, nbr) vs (nbr, node) double.
      - The binary index also contains within-list duplicates when the source
        edge file lists the same pair multiple times (eu_80.pairs has ~46%
        duplicates), so we also dedup per-node with a small local set.
        TODO: deduplicate at preprocessing time so this set is unnecessary.
    """
    edges = []
    for node in selected:
        seen = set()
        for nbr, w in graph.get_neighbors(node):
            if nbr <= node or nbr not in selected or nbr in seen:
                continue
            seen.add(nbr)
            edges.append((node, nbr, w))
    return selected, edges, seed_membership


# ===================================================================
# Output
# ===================================================================

def write_output(
    graph,
    edges,
    output_path,
):
    """Write edge CSV directly to output_path, without a header row."""
    with open(output_path, "w", newline="") as f:
        w_csv = csv.writer(f)
        for a, b, w in edges:
            na = graph.id_to_name.get(a, str(a))
            nb = graph.id_to_name.get(b, str(b))
            w_csv.writerow([na, nb, f"{w:.6f}"])


SEED_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")


def validate_seed_names(seed_names):
    invalid = [name for name in seed_names if not SEED_NAME_PATTERN.match(name)]
    if invalid:
        invalid_str = ", ".join(invalid)
        raise ValueError(
            "Seed identifiers must start with a letter or digit and may only "
            f"contain letters, digits, '.', '_', ':', and '-': {invalid_str}"
        )


def emit_json(payload):
    print(f"__SUBNET_JSON__ {json.dumps(payload, separators=(',', ':'))}")


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Extract a subnetwork from a preprocessed graph index",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic extraction with 3 UniProt accession seeds
  python extract_subnetwork.py index_prefix A0A2I3TW31 G3R0Y9 Q6ZQA0 -o abc.csv

  # Custom budget and local fraction
  python extract_subnetwork.py index_prefix A0A2I3TW31 -o abc.csv --max_nodes 500 --local_fraction 0.4
        """
    )
    parser.add_argument("index_prefix", help="Prefix of preprocessed index files")
    parser.add_argument("seeds", nargs="+",
                        help="Seed UniProt accession numbers")
    parser.add_argument("--max_nodes", "-n", type=int, default=2000,
                        help="Maximum nodes in subnetwork (default: 2000)")
    parser.add_argument("--local_fraction", "-lf", type=float, default=0.3,
                        help="Fraction of budget reserved for per-seed local expansion (default: 0.3)")
    parser.add_argument("--stitch", action="store_true",
                        help="Attempt to connect disconnected components (optional)")
    parser.add_argument("--output", "-o", required=True,
                        help="Full output CSV file path, e.g. abc.csv")
    parser.add_argument("--no_cache", action="store_true",
                        help="Disable the GraphIndex neighbor cache. Trades "
                             "speed for memory; worth considering on very "
                             "large --stitch runs where BFS may explore many "
                             "distinct hub nodes.")
    parser.add_argument("--json", action="store_true",
                        help="Emit a single JSON summary on stdout for programmatic callers")
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Validate numeric args
    # ------------------------------------------------------------------
    if args.max_nodes < 1:
        sys.exit(f"Error: --max_nodes must be >= 1 (got {args.max_nodes})")
    if not (0.0 <= args.local_fraction <= 1.0):
        sys.exit(f"Error: --local_fraction must be in [0.0, 1.0] "
                 f"(got {args.local_fraction})")

    seed_names = list(args.seeds)
    start_time = time.perf_counter()

    try:
        validate_seed_names(seed_names)
    except ValueError as exc:
        message = str(exc)
        print(f"Error: {message}", file=sys.stderr)
        if args.json:
            emit_json({
                "ok": False,
                "error": message,
                "missingSeeds": [],
                "edgeCount": 0,
                "resolvedSeedCount": 0,
                "inputSeedCount": len(seed_names),
                "emptySubnetwork": False,
                "elapsedMs": int((time.perf_counter() - start_time) * 1000),
            })
        sys.exit(1)

    # Load graph index
    with GraphIndex(args.index_prefix, cache_neighbors=not args.no_cache) as graph:
        # Resolve seed names to IDs
        seed_ids = []
        missing_seed_names = []
        for name in seed_names:
            try:
                sid = graph.resolve_seed(name)
                seed_ids.append(sid)
            except KeyError:
                missing_seed_names.append(name)

        if missing_seed_names:
            missing_str = ", ".join(missing_seed_names)
            print(
                f"WARNING: {len(missing_seed_names)} input seed(s) were not found "
                f"in the network: {missing_str}",
                file=sys.stderr,
            )

        if not seed_ids:
            print("Error: No valid seeds found.", file=sys.stderr)
            if args.json:
                emit_json({
                    "ok": False,
                    "error": "No valid seeds found.",
                    "missingSeeds": missing_seed_names,
                    "edgeCount": 0,
                    "resolvedSeedCount": 0,
                    "inputSeedCount": len(seed_names),
                    "emptySubnetwork": True,
                    "elapsedMs": int((time.perf_counter() - start_time) * 1000),
                })
            sys.exit(1)

        # Deduplicate seeds
        seed_ids = list(dict.fromkeys(seed_ids))

        # Extract subnetwork
        selected, edges, seed_membership = extract_subnetwork(
            graph, seed_ids,
            max_nodes=args.max_nodes,
            local_fraction=args.local_fraction,
            stitch=args.stitch,
        )
        if not edges:
            print("WARNING: Generated subnetwork is empty.", file=sys.stderr)

        # Write output
        write_output(
            graph,
            edges,
            args.output,
        )

        elapsed_ms = int((time.perf_counter() - start_time) * 1000)
        if args.json:
            emit_json({
                "ok": True,
                "missingSeeds": missing_seed_names,
                "edgeCount": len(edges),
                "resolvedSeedCount": len(seed_ids),
                "inputSeedCount": len(seed_names),
                "selectedNodeCount": len(selected),
                "emptySubnetwork": len(edges) == 0,
                "elapsedMs": elapsed_ms,
            })
        else:
            print(f"Wrote {len(edges)} edges to {args.output} in {elapsed_ms} ms")


if __name__ == "__main__":
    main()
