#!/usr/bin/env python3
"""
Spectral signal-over-noise caller, final.

Strategy
--------
Spectral clustering with automatic k-selection, followed by a small
domain-specific decision layer. The generated inputs guarantee that a
self-hit exists near (x=1, y=100) and that the largest cluster is noise.

  1. Build a self-tuning Gaussian affinity graph on robustly z-scored
     coordinates (median + MAD), so that extreme outliers do not
     compress the rest of the data when standardizing.
  2. Compute the top n_eigs eigenpairs of the symmetric normalized
     affinity. Choose k by the largest leave-one-out eigengap z-score
     (>0); if no gap stands out, declare k=1 and run the fallback.
  3. Cluster the row-normalized top-k eigenvectors with k-means
     (Ng-Jordan-Weiss embedding).
  4. If k=1, run the y-axis MAD fallback and force-include the self-hit.
  5. If k>=2, label by the self-hit's relationship to the largest cluster:

     - self-hit outside largest cluster, normal case: report clusters whose
       centroids are strictly upper-left of the largest cluster centroid
       (x lower AND y higher).
     - self-hit outside largest cluster, X case: the self-hit cluster's
       centroid is lower than or right of the largest cluster centroid.
       Ignore cluster labels, draw a convex hull around the largest noise
       cluster, and report points outside the hull whose y is above the
       hull's upper boundary at the same x.
     - self-hit inside largest cluster: draw a convex hull around the
       largest cluster, ignore first-round labels, keep only points inside
       that hull as candidates, and apply the MAD y-threshold there.

The self-hit is always force-included in the final signal mask unless the
run is marked unreliable.

When k=1 is selected (the spectrum reports no cluster structure), a
robust y-axis outlier fallback runs: a point is signal iff
y > median(y) + N * 1.4826 * MAD(y) with N=5 by default. This catches
isolated extreme outliers (e.g. a y=100 point in an otherwise tight
band) that no clustering method could isolate.

Retry logic
-----------
After a round is finalized, the signal count is compared with a fixed
cutoff of 50% of the original input size. If the signal count is too high,
the next round removes the lowest-y 25% of the current working set and
reruns the full spectral pipeline. The cutoff is never recomputed from the
shrinking working set. The number of rounds is configurable with
`--max-rounds`. If the last allowed round still exceeds the fixed cutoff,
the file is marked unreliable and no signals are emitted.

Why robust z-score: the data shape is "dense cloud of background plus a
few extreme outliers." Standard z-score (mean + std) lets the outliers
inflate the std, which compresses the dense band into a tiny range and
destroys the spectral structure. Median + MAD ignores the outliers when
estimating scale, so the dense band keeps its internal structure and
the outliers stay genuinely far away in scaled coordinates.

Why strict upper-left: in the normal k>=2 case, a cluster is signal only
when its centroid is both left of and above the largest noise cluster's
centroid. Ambiguous cases are handled by hull geometry rather than by
enumerating the older Case A/B/C variants.

Constants used
--------------
  n_eigs                       : how deep into the spectrum to look
                                 (default 20). Bounds maximum k.
  k1_fallback_mad_multiplier   : N in the k=1 fallback (default 5).
                                 Catches moderate-to-extreme isolated
                                 outliers when no cluster structure
                                 exists.
  max_rounds                   : maximum number of total rounds
                                 (default 10; set lower to reduce
                                 retries, higher to allow more).
  seed                         : RNG seed for k-means reproducibility.

Assumptions and limitations
---------------------------
1. Largest cluster = noise. This is a domain assumption, not a
   general property of spectral clustering. It is correct for this
   data because the noise injection process produces files where
   noise points dominate (typically ~95%+ of points). If the
   assumption is ever violated — e.g. a file where signal points
   outnumber noise points — the algorithm will INVERT LABELS: the
   signal cluster will be called "noise" and noise sub-clusters
   will be called "signal." There is no automatic detection for
   this inversion; it has to be caught by domain inspection of
   the per-file output.

2. Dense O(n^2) memory and time. The script builds a full pairwise
   distance matrix and a dense affinity matrix. At n=2550 this
   consumes ~50 MB and ~10 seconds per file. At n=10000 it would
   be ~800 MB and substantially slower; at n>~25000 it would exceed
   typical memory limits. For larger inputs, the affinity
   construction would need to be rewritten to use a k-nearest-
   neighbor sparse graph with scipy.sparse.linalg.eigsh on the
   sparse normalized Laplacian. Not implemented here because the
   intended batch has n~2500 points per file.

3. k-means with random restarts is non-deterministic without a
   seed. A fixed seed (default 42) is used so reruns produce
   identical results, but the partition is sensitive to k-means
   initialization. Different seeds may produce slightly different
   cluster boundaries on borderline cases.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from dataclasses import dataclass, field
from functools import partial
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any

import numpy as np
from scipy.sparse.linalg import eigsh
from scipy.spatial import ConvexHull, QhullError
from scipy.spatial.distance import pdist, squareform
from sklearn.cluster import KMeans

NUMERIC_EPS = 1e-12
HULL_TOL = 1e-9
SELF_HIT_X = 1.0
SELF_HIT_Y = 100.0
SELF_HIT_MAX_DISTANCE = 1.0
SIGNAL_FRACTION_CUTOFF = 0.5
RETRY_TRIM_FRACTION = 0.25
DEFAULT_K1_MAD_MULTIPLIER = 5.0
DEFAULT_LARGEST_HULL_MAD_MULTIPLIER = 5.0


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

@dataclass
class SpectralDiagnostics:
    n_total: int
    n_signal: int
    n_noise: int
    selected_k: int = -1
    eigenvalues_top: list[float] = field(default_factory=list)
    gap_zscores: list[float] = field(default_factory=list)
    argmax_gap_z: float = math.nan
    noise_cluster_label: int = -1
    noise_cluster_size: int = 0
    noise_centroid_x: float = math.nan
    noise_centroid_y: float = math.nan
    cluster_summary: str = ""   # "label:size:centroid:decision; ..."
    k1_fallback_threshold_y: float = math.nan  # only set when k=1 path runs
    n_rounds_run: int = 1
    round_history: str = ""     # "r1:n=...,k=...,trigger=case_X,n_dropped=...; r2:..."
    notes: str = ""


@dataclass
class RoundOutput:
    n_in: int
    signal_mask: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=bool))
    cluster_labels: np.ndarray | None = None
    k: int = -1
    noise_info: dict[str, Any] | None = None
    eigs_top: list[float] = field(default_factory=list)
    gap_z_top: list[float] = field(default_factory=list)
    argmax_z: float = math.nan
    threshold_y: float = math.nan
    k1_fallback_used: bool = False
    notes: str = ""


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_input_data(filepath: str | Path) -> tuple[np.ndarray, np.ndarray]:
    protein_ids: list[str] = []
    coords: list[tuple[float, float]] = []
    skipped_header = False

    with open(filepath, "r", encoding="utf-8") as handle:
        for line_num, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue

            if "\t" in line:
                parts = [p.strip() for p in line.split("\t")]
            elif "," in line:
                parts = [p.strip() for p in line.split(",")]
            else:
                parts = line.split()

            if len(parts) < 3:
                raise ValueError(
                    f"Line {line_num} has fewer than 3 columns: {raw_line.rstrip()}"
                )

            protein_id = parts[0]
            try:
                x = float(parts[1])
                y = float(parts[2])
            except ValueError:
                if line_num == 1 and not skipped_header:
                    skipped_header = True
                    continue
                raise ValueError(
                    f"Line {line_num} has non-numeric coordinates: {raw_line.rstrip()}"
                )

            protein_ids.append(protein_id)
            coords.append((x, y))

    if not protein_ids:
        raise ValueError("Input file is empty or contains only a header")

    return np.asarray(protein_ids, dtype=str), np.asarray(coords, dtype=float)


# ---------------------------------------------------------------------------
# Coordinate scaling
# ---------------------------------------------------------------------------

def robust_zscore(coords: np.ndarray) -> np.ndarray:
    """
    Robust z-score using median and MAD instead of mean and std. This is
    the default standardization for the final version because the data shape (dense
    cloud + extreme outliers) breaks standard z-score: a single y=100
    outlier in an otherwise-32-to-35 file inflates the std enough to
    compress the rest of the y-axis to near-zero range, destroying the
    affinity graph structure.

    MAD-based scaling estimates the spread of the bulk distribution
    while ignoring the outliers, so the dense band keeps its internal
    distances and the outliers stay genuinely far away in scaled space.
    """
    med = np.median(coords, axis=0)
    mad = np.median(np.abs(coords - med), axis=0)
    scale = 1.4826 * np.where(mad < NUMERIC_EPS, 1.0, mad)
    return (coords - med) / scale


# ---------------------------------------------------------------------------
# Affinity graph and spectrum
# ---------------------------------------------------------------------------

def compute_local_sigma(dist_matrix: np.ndarray) -> np.ndarray:
    """
    Per-point bandwidth for the self-tuning Gaussian kernel. Each point's
    sigma is the median of its nonzero pairwise distances.

    Note: canonical self-tuning spectral clustering (Zelnik-Manor & Perona
    2004) uses distance to the kth nearest neighbor (typically k=7). This
    implementation uses the median over all distances instead, which is
    smoother but less "local." Both are O(n^2) given a full distance matrix.
    The choice has not caused issues on the datasets tested.
    """
    masked = np.where(dist_matrix > 0, dist_matrix, np.nan)
    sigmas = np.nanmedian(masked, axis=1)
    sigmas = np.where(np.isnan(sigmas), 1.0, sigmas)
    return np.maximum(sigmas, NUMERIC_EPS)


def build_self_tuning_affinity(coords: np.ndarray) -> np.ndarray:
    """
    Self-tuning Gaussian affinity: each point picks its own bandwidth
    sigma_i from its neighborhood (median nonzero distance), so the
    kernel adapts to local density variations. Returns S, the symmetric
    normalized affinity used for spectral decomposition. The raw
    affinity W is intentionally not returned — it would be ~n^2 floats
    of dead weight; the final version uses only S.
    """
    dist_matrix = squareform(pdist(coords))
    local_sigma = compute_local_sigma(dist_matrix)
    sigma_product = np.outer(local_sigma, local_sigma)
    W = np.exp(-(dist_matrix ** 2) / np.maximum(sigma_product, NUMERIC_EPS))
    np.fill_diagonal(W, 0.0)
    degree = W.sum(axis=1)
    inv_sqrt_degree = 1.0 / np.sqrt(np.maximum(degree, NUMERIC_EPS))
    S = (inv_sqrt_degree[:, None] * W) * inv_sqrt_degree[None, :]
    # W is intentionally not returned; let it go out of scope and be GC'd.
    return S


def top_eigenpairs(S: np.ndarray, n_eigs: int) -> tuple[np.ndarray, np.ndarray]:
    n = S.shape[0]
    k = max(2, min(n_eigs, n - 1))

    if n <= 64 or k >= n - 1:
        vals, vecs = np.linalg.eigh(S)
        order = np.argsort(vals)[::-1][:k]
    else:
        vals, vecs = eigsh(S, k=k, which="LA", tol=1e-6)
        order = np.argsort(vals)[::-1]
    vals = vals[order]
    vecs = vecs[:, order]

    # Sign-orient eigenvectors for reproducibility
    for j in range(vecs.shape[1]):
        idx = int(np.argmax(np.abs(vecs[:, j])))
        if vecs[idx, j] < 0:
            vecs[:, j] *= -1.0

    return vals, vecs


# ---------------------------------------------------------------------------
# k-selection
# ---------------------------------------------------------------------------

def select_k_by_gap_zscore(eigenvalues: np.ndarray) -> tuple[int, np.ndarray]:
    """
    Pick k by the largest leave-one-out eigengap z-score. Each consecutive
    gap g_i = lambda_i - lambda_{i+1} suggests a k = i + 1 split. For each
    gap, compute its robust z-score against the other gaps in the spectrum
    (median and MAD computed leave-one-out so the focal gap is excluded).

    If the maximum z-score is <= 0, no gap stands out from the spectrum's
    own typical decay and we declare k = 1 (no clusters).

    Returns (k, gap_z_scores). gap_z_scores has length n_eigs - 1.
    """
    gaps = -np.diff(eigenvalues)  # gap_i = lambda_i - lambda_{i+1}
    if gaps.size == 0:
        # No gaps at all (single eigenvalue). Return empty array; this is
        # correctly typed as "zero z-scores" since there are zero gaps.
        return 1, np.array([], dtype=float)
    # Leave-one-out z-score requires at least 2 gaps (so "others" is non-empty
    # when we delete one). With a single gap there is nothing to compare it
    # against, so we cannot decide it stands out — declare k=1. Return NaN
    # in the z-score slot rather than the raw gap value, so the diagnostics
    # column ("gap_zscores") is never silently populated with non-z-scores.
    if gaps.size < 2:
        return 1, np.full(gaps.shape, np.nan)

    gap_z = np.empty_like(gaps)
    for i in range(gaps.size):
        others = np.delete(gaps, i)
        med = float(np.median(others))
        mad = float(np.median(np.abs(others - med)))
        scale = 1.4826 * max(mad, NUMERIC_EPS)
        gap_z[i] = (gaps[i] - med) / scale

    if gap_z.max() > 0:
        return int(np.argmax(gap_z)) + 1, gap_z
    return 1, gap_z


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------

def kmeans_on_embedding(
    eigenvectors: np.ndarray,
    k: int,
    *,
    seed: int,
) -> np.ndarray:
    """
    Ng-Jordan-Weiss spectral clustering: take the top-k eigenvectors,
    row-normalize, and run k-means.
    """
    embedding = eigenvectors[:, :k]
    norms = np.linalg.norm(embedding, axis=1, keepdims=True)
    embedding = embedding / np.maximum(norms, NUMERIC_EPS)
    km = KMeans(n_clusters=k, random_state=seed, n_init=10)
    return km.fit_predict(embedding)


# ---------------------------------------------------------------------------
# Signal labeling
# ---------------------------------------------------------------------------

def identify_noise_cluster(
    coords_raw: np.ndarray,
    cluster_labels: np.ndarray,
) -> dict[str, Any]:
    """
    Identifies which cluster is "noise" (the largest by count) and
    computes its centroid. Per-cluster signal decisions are made later
    by the simplified finalization logic.

    Returns a dict with: noise_label, noise_size, noise_centroid_x,
    noise_centroid_y. If there is only one cluster, returns sentinel
    values (noise_label=-1, NaN centroid) and the caller is expected
    to handle the no-signal-vs-noise case.
    """
    n = len(coords_raw)
    x = coords_raw[:, 0]
    y = coords_raw[:, 1]

    unique, counts = np.unique(cluster_labels, return_counts=True)

    if unique.size <= 1:
        return {
            "noise_label": -1,
            "noise_size": int(n),
            "noise_centroid_x": math.nan,
            "noise_centroid_y": math.nan,
        }

    noise_label = int(unique[np.argmax(counts)])
    noise_mask = cluster_labels == noise_label
    return {
        "noise_label": noise_label,
        "noise_size": int(noise_mask.sum()),
        "noise_centroid_x": float(np.mean(x[noise_mask])),
        "noise_centroid_y": float(np.mean(y[noise_mask])),
    }


def k1_fallback_y_threshold(
    coords_raw: np.ndarray,
    *,
    mad_multiplier: float,
) -> tuple[np.ndarray, float]:
    """
    Fallback used when spectral clustering selects k=1 (no cluster
    structure detected). Identifies extreme outliers on the y-axis using
    a robust threshold:

        threshold_y = median(y) + mad_multiplier * 1.4826 * MAD(y)

    A point is signal iff its y exceeds this threshold. The default
    multiplier in this script is 5 (catches moderate-to-extreme isolated
    outliers). Set higher (e.g. 10) for a more stringent rule that only
    catches truly extreme outliers; set very high to effectively disable
    the fallback.

    Returns (signal_mask, threshold_y).
    """
    y = coords_raw[:, 1]
    y_med = float(np.median(y))
    y_mad = float(np.median(np.abs(y - y_med)))
    y_scale = 1.4826 * max(y_mad, NUMERIC_EPS)
    threshold = y_med + mad_multiplier * y_scale
    signal_mask = y > threshold
    return signal_mask, threshold


def force_include_self_hit(signal_mask: np.ndarray, coords_raw: np.ndarray) -> None:
    self_idx = find_self_hit_index(coords_raw)
    if self_idx >= 0:
        signal_mask[self_idx] = True


def cleanup_upper_left_dominance(
    signal_mask: np.ndarray,
    coords_raw: np.ndarray,
) -> int:
    """
    Promote any non-signal point that lies strictly upper-left of at least
    one identified signal point.
    """
    signal_idx = np.flatnonzero(signal_mask)
    if signal_idx.size == 0:
        return 0

    nonsignal_idx = np.flatnonzero(~signal_mask)
    if nonsignal_idx.size == 0:
        return 0

    nonsignal = coords_raw[nonsignal_idx]
    signal = coords_raw[signal_idx]
    upper_left_of_signal = (
        (nonsignal[:, None, 0] < signal[None, :, 0]) &
        (nonsignal[:, None, 1] > signal[None, :, 1])
    )
    promoted = np.any(upper_left_of_signal, axis=1)
    if not np.any(promoted):
        return 0

    signal_mask[nonsignal_idx[promoted]] = True
    return int(np.sum(promoted))


def apply_round_cleanup(
    signal_mask: np.ndarray,
    coords_raw: np.ndarray,
    summary: str,
    notes: str,
) -> tuple[np.ndarray, str, str]:
    force_include_self_hit(signal_mask, coords_raw)
    n_added = cleanup_upper_left_dominance(signal_mask, coords_raw)
    if n_added > 0:
        summary = f"{summary}; cleanup_upper_left_added={n_added}"
        notes = f"{notes};cleanup_upper_left_added_{n_added}"
    return signal_mask, summary, notes


def cluster_centroids(
    coords_raw: np.ndarray,
    cluster_labels: np.ndarray,
) -> dict[int, tuple[int, float, float]]:
    out: dict[int, tuple[int, float, float]] = {}
    for lbl in np.unique(cluster_labels):
        m = cluster_labels == lbl
        out[int(lbl)] = (
            int(m.sum()),
            float(np.mean(coords_raw[m, 0])),
            float(np.mean(coords_raw[m, 1])),
        )
    return out


def summarize_clusters(
    centroids: dict[int, tuple[int, float, float]],
    *,
    noise_label: int,
    decisions: dict[int, str],
) -> str:
    parts: list[str] = []
    for lbl in sorted(centroids):
        sz, cx, cy = centroids[lbl]
        decision = "NOISE_largest" if lbl == noise_label else decisions.get(lbl, "noise")
        parts.append(f"{lbl}:n={sz}:c=({cx:.3f},{cy:.3f}):{decision}")
    return "; ".join(parts)


def strict_upper_left_cluster_signals(
    coords_raw: np.ndarray,
    cluster_labels: np.ndarray,
    noise_info: dict[str, Any],
) -> tuple[np.ndarray, str]:
    n = len(coords_raw)
    signal_mask = np.zeros(n, dtype=bool)
    noise_label = int(noise_info["noise_label"])
    noise_cx = float(noise_info["noise_centroid_x"])
    noise_cy = float(noise_info["noise_centroid_y"])
    centroids = cluster_centroids(coords_raw, cluster_labels)
    decisions: dict[int, str] = {}

    for lbl, (_, cx, cy) in centroids.items():
        if lbl == noise_label:
            continue
        if cx < noise_cx and cy > noise_cy:
            signal_mask |= cluster_labels == lbl
            decisions[lbl] = "signal:centroid_upper_left_AND"
        else:
            decisions[lbl] = "noise:not_centroid_upper_left_AND"

    return signal_mask, summarize_clusters(
        centroids, noise_label=noise_label, decisions=decisions
    )


def _convex_hull_geometry(noise_points: np.ndarray) -> tuple[ConvexHull | None, np.ndarray]:
    unique_points = np.unique(noise_points, axis=0)
    if unique_points.shape[0] < 3:
        return None, unique_points
    try:
        return ConvexHull(unique_points), unique_points
    except QhullError:
        return None, unique_points


def points_inside_noise_hull(
    coords_raw: np.ndarray,
    noise_points: np.ndarray,
    *,
    tol: float = HULL_TOL,
) -> tuple[np.ndarray, ConvexHull | None, np.ndarray]:
    hull, hull_points = _convex_hull_geometry(noise_points)
    if hull is None:
        if hull_points.size == 0:
            return np.zeros(len(coords_raw), dtype=bool), None, hull_points
        min_xy = np.min(hull_points, axis=0)
        max_xy = np.max(hull_points, axis=0)
        inside = np.all((coords_raw >= min_xy - tol) & (coords_raw <= max_xy + tol), axis=1)
        return inside, None, hull_points

    equations = hull.equations
    inside = np.all(
        coords_raw @ equations[:, :2].T + equations[:, 2] <= tol,
        axis=1,
    )
    return inside, hull, hull_points


def hull_upper_y_at_x(
    x_values: np.ndarray,
    hull: ConvexHull | None,
    hull_points: np.ndarray,
) -> np.ndarray:
    if hull_points.size == 0:
        return np.full_like(x_values, math.inf, dtype=float)
    if hull is None:
        return np.full_like(x_values, float(np.max(hull_points[:, 1])), dtype=float)

    vertices = hull_points[hull.vertices]
    upper = np.full_like(x_values, -math.inf, dtype=float)
    min_x = float(np.min(vertices[:, 0]))
    max_x = float(np.max(vertices[:, 0]))

    for simplex in hull.simplices:
        p1 = hull_points[simplex[0]]
        p2 = hull_points[simplex[1]]
        x1, y1 = float(p1[0]), float(p1[1])
        x2, y2 = float(p2[0]), float(p2[1])
        lo = min(x1, x2) - HULL_TOL
        hi = max(x1, x2) + HULL_TOL
        m = (x_values >= lo) & (x_values <= hi)
        if not np.any(m):
            continue
        if abs(x2 - x1) < NUMERIC_EPS:
            y_edge = max(y1, y2)
            upper[m] = np.maximum(upper[m], y_edge)
        else:
            t = (x_values[m] - x1) / (x2 - x1)
            y_edge = y1 + t * (y2 - y1)
            upper[m] = np.maximum(upper[m], y_edge)

    left = x_values < min_x
    right = x_values > max_x
    if np.any(left):
        upper[left] = float(np.max(vertices[vertices[:, 0] == min_x, 1]))
    if np.any(right):
        upper[right] = float(np.max(vertices[vertices[:, 0] == max_x, 1]))
    unresolved = ~np.isfinite(upper)
    if np.any(unresolved):
        upper[unresolved] = float(np.max(vertices[:, 1]))
    return upper


def hull_upper_boundary_signals(
    coords_raw: np.ndarray,
    cluster_labels: np.ndarray,
    noise_info: dict[str, Any],
) -> tuple[np.ndarray, str]:
    noise_label = int(noise_info["noise_label"])
    noise_mask = cluster_labels == noise_label
    noise_points = coords_raw[noise_mask]
    inside_hull, hull, hull_points = points_inside_noise_hull(coords_raw, noise_points)
    upper_y = hull_upper_y_at_x(coords_raw[:, 0], hull, hull_points)

    signal_mask = (~inside_hull) & (coords_raw[:, 1] > upper_y)

    centroids = cluster_centroids(coords_raw, cluster_labels)
    summary = summarize_clusters(
        centroids,
        noise_label=noise_label,
        decisions={lbl: "ignored:hull_boundary_x_case" for lbl in centroids if lbl != noise_label},
    )
    n_hull_vertices = 0 if hull is None else int(len(hull.vertices))
    summary += (
        f"; hull_boundary:n_noise={int(noise_mask.sum())}:"
        f"n_vertices={n_hull_vertices}:n_signal={int(signal_mask.sum())}"
    )
    return signal_mask, summary


def mad_within_noise_hull_signals(
    coords_raw: np.ndarray,
    cluster_labels: np.ndarray,
    noise_info: dict[str, Any],
    *,
    mad_multiplier: float,
) -> tuple[np.ndarray, float, str]:
    noise_label = int(noise_info["noise_label"])
    noise_mask = cluster_labels == noise_label
    inside_hull, hull, _ = points_inside_noise_hull(coords_raw, coords_raw[noise_mask])

    signal_mask = np.zeros(len(coords_raw), dtype=bool)
    if np.any(inside_hull):
        local_mask, threshold_y = k1_fallback_y_threshold(
            coords_raw[inside_hull],
            mad_multiplier=mad_multiplier,
        )
        signal_mask[np.flatnonzero(inside_hull)[local_mask]] = True
    else:
        threshold_y = math.nan

    centroids = cluster_centroids(coords_raw, cluster_labels)
    summary = summarize_clusters(
        centroids,
        noise_label=noise_label,
        decisions={lbl: "ignored:mad_inside_largest_hull" for lbl in centroids if lbl != noise_label},
    )
    n_hull_vertices = 0 if hull is None else int(len(hull.vertices))
    summary += (
        f"; largest_hull_mad:n_candidates={int(inside_hull.sum())}:"
        f"n_vertices={n_hull_vertices}:threshold_y={threshold_y:.6g}:"
        f"n_signal={int(signal_mask.sum())}"
    )
    return signal_mask, threshold_y, summary


# ---------------------------------------------------------------------------
# Main classifier (initial round plus bounded signal-count retries)
# ---------------------------------------------------------------------------

def find_self_hit_index(
    coords_raw: np.ndarray,
    max_distance: float = SELF_HIT_MAX_DISTANCE,
) -> int:
    """
    Identify the self-hit point: the protein that, when matched against
    itself, sits at (x ~ 1, y ~ 100). Returns the index of the point
    closest to (1.0, 100.0) under Euclidean distance.

    If no point is within max_distance of (1.0, 100.0), returns -1. This
    guards against files that lack a true self-hit: without this check,
    the algorithm would silently promote some unrelated point to "self-hit"
    and use it for workflow decisions.

    The self-hit is exact at (1.0, 100.0) in the data we have (distance 0),
    so the default tolerance of 1.0 is generous. Any point further than 1.0
    from (1, 100) is definitely not a self-hit.
    """
    if len(coords_raw) == 0:
        return -1
    target = np.array([SELF_HIT_X, SELF_HIT_Y])
    distances = np.linalg.norm(coords_raw - target, axis=1)
    best = int(np.argmin(distances))
    if distances[best] > max_distance:
        return -1
    return best


def _classify_one_round(
    coords_raw: np.ndarray,
    *,
    n_eigs: int,
    seed: int,
    k1_fallback_mad_multiplier: float,
) -> RoundOutput:
    """
    Run one round of the spectral pipeline on coords_raw.
    Returns a dict with everything the caller needs to finalize labels.

    A "round" includes: robust z-score, self-tuning affinity, top
    eigenpairs, auto-k selection, k-means clustering, and noise-cluster
    identification (centroid + label). Signal labels are assigned by
    finalize_round_simple().

    For the k=1 path, the MAD fallback runs immediately and produces
    the round's signal_mask.
    """
    n = len(coords_raw)
    out = RoundOutput(
        n_in=n,
        signal_mask=np.zeros(n, dtype=bool),
    )

    if n < 3:
        out.k = 1
        out.notes = "n_lt_3"
        return out

    coords_scaled = robust_zscore(coords_raw)
    S = build_self_tuning_affinity(coords_scaled)
    eigenvalues, eigenvectors = top_eigenpairs(S, n_eigs=n_eigs)
    k, gap_z = select_k_by_gap_zscore(eigenvalues)

    out.eigs_top = [float(v) for v in eigenvalues[: min(10, eigenvalues.size)]]
    out.gap_z_top = [float(z) for z in gap_z[: min(10, gap_z.size)]]
    out.argmax_z = (
        float(np.nanmax(gap_z))
        if gap_z.size and np.any(np.isfinite(gap_z))
        else math.nan
    )
    out.k = int(k)

    if k == 1:
        # k=1 has no clusters, so the MAD fallback is the only option and
        # its output is the finalized answer for this round.
        signal_mask, threshold_y = k1_fallback_y_threshold(
            coords_raw, mad_multiplier=k1_fallback_mad_multiplier
        )
        n_sig = int(signal_mask.sum())
        out.signal_mask = signal_mask
        out.threshold_y = threshold_y
        out.k1_fallback_used = True
        out.notes = (
            f"k=1_fallback_caught_{n_sig}_outliers"
            if n_sig > 0 else "k=1_fallback_no_outliers"
        )
        return out

    # k >= 2: cluster and identify the largest cluster as noise. The
    # simplified finalizer decides how to label signals from these outputs.
    cluster_labels = kmeans_on_embedding(eigenvectors, k, seed=seed)
    noise_info = identify_noise_cluster(coords_raw, cluster_labels)

    out.cluster_labels = cluster_labels
    out.noise_info = noise_info
    out.notes = "ok"
    return out


def finalize_round_simple(
    coords_raw: np.ndarray,
    round_out: RoundOutput,
    *,
    largest_hull_mad_multiplier: float,
) -> tuple[np.ndarray, str, str, float]:
    if round_out.k1_fallback_used:
        signal_mask = round_out.signal_mask.copy()
        notes = f"k1_mad_caught_{int(signal_mask.sum())}_before_cleanup"
        signal_mask, summary, notes = apply_round_cleanup(
            signal_mask,
            coords_raw,
            "k1_mad_fallback",
            notes,
        )
        return signal_mask, summary, notes, float(round_out.threshold_y)

    cluster_labels = round_out.cluster_labels
    noise_info = round_out.noise_info
    if cluster_labels is None or noise_info is None or noise_info["noise_label"] < 0:
        signal_mask = np.zeros(len(coords_raw), dtype=bool)
        signal_mask, summary, notes = apply_round_cleanup(
            signal_mask,
            coords_raw,
            "no_clusters",
            "no_clusters_before_cleanup",
        )
        return signal_mask, summary, notes, math.nan

    self_idx = find_self_hit_index(coords_raw)
    if self_idx < 0:
        return (
            np.zeros(len(coords_raw), dtype=bool),
            "self_hit_not_found",
            "self_hit_not_found",
            math.nan,
        )

    noise_label = int(noise_info["noise_label"])
    self_cluster = int(cluster_labels[self_idx])

    if self_cluster == noise_label:
        signal_mask, threshold_y, summary = mad_within_noise_hull_signals(
            coords_raw,
            cluster_labels,
            noise_info,
            mad_multiplier=largest_hull_mad_multiplier,
        )
        signal_mask, summary, notes = apply_round_cleanup(
            signal_mask,
            coords_raw,
            summary,
            "self_hit_in_largest_hull_mad",
        )
        return signal_mask, summary, notes, threshold_y

    centroids = cluster_centroids(coords_raw, cluster_labels)
    _, self_cx, self_cy = centroids[self_cluster]
    noise_cx = float(noise_info["noise_centroid_x"])
    noise_cy = float(noise_info["noise_centroid_y"])
    is_x_case = self_cy < noise_cy or self_cx > noise_cx

    if is_x_case:
        signal_mask, summary = hull_upper_boundary_signals(
            coords_raw,
            cluster_labels,
            noise_info,
        )
        signal_mask, summary, notes = apply_round_cleanup(
            signal_mask,
            coords_raw,
            summary,
            "self_hit_not_largest_x_case_hull_boundary",
        )
        return signal_mask, summary, notes, math.nan

    signal_mask, summary = strict_upper_left_cluster_signals(
        coords_raw,
        cluster_labels,
        noise_info,
    )
    signal_mask, summary, notes = apply_round_cleanup(
        signal_mask,
        coords_raw,
        summary,
        "self_hit_not_largest_normal_upper_left_clusters",
    )
    return signal_mask, summary, notes, math.nan


def classify(
    coords_raw: np.ndarray,
    *,
    n_eigs: int,
    seed: int,
    k1_fallback_mad_multiplier: float,
    largest_hull_mad_multiplier: float,
    signal_fraction_cutoff: float = SIGNAL_FRACTION_CUTOFF,
    retry_trim_fraction: float = RETRY_TRIM_FRACTION,
    max_rounds: int = 10,
) -> tuple[np.ndarray, SpectralDiagnostics]:
    """
    Main entry point. Runs one spectral/MAD classification round, then
    retries only if the reported signal set is too large to be credible.

    The retry threshold is fixed at 50% of the original input size. If a
    round exceeds that threshold, the next round drops the lowest-y 25% of
    the current working set and reruns the full spectral pipeline. The
    number of rounds is controlled by `max_rounds` (default 10).
    """
    n = len(coords_raw)
    if n < 3:
        return (
            np.zeros(n, dtype=bool),
            SpectralDiagnostics(n_total=n, n_signal=0, n_noise=n,
                                notes="n_lt_3"),
        )

    max_rounds = max(1, max_rounds)
    signal_cutoff = int(math.floor(signal_fraction_cutoff * n))
    current_indices = np.arange(n)
    history: list[str] = []
    final_round_out: RoundOutput | None = None
    final_round_coords: np.ndarray | None = None
    final_signal_local: np.ndarray | None = None
    final_cluster_summary = ""
    final_notes = ""
    final_threshold_y = math.nan
    unreliable = False

    for round_num in range(1, max_rounds + 1):
        sub_coords = coords_raw[current_indices]
        round_out = _classify_one_round(
            sub_coords,
            n_eigs=n_eigs,
            seed=seed,
            k1_fallback_mad_multiplier=k1_fallback_mad_multiplier,
        )
        final_round_out = round_out
        final_round_coords = sub_coords

        signal_local, cluster_summary, notes, threshold_y = finalize_round_simple(
            sub_coords,
            round_out,
            largest_hull_mad_multiplier=largest_hull_mad_multiplier,
        )
        final_signal_local = signal_local
        final_cluster_summary = cluster_summary
        final_notes = notes
        final_threshold_y = threshold_y
        n_signal_local = int(signal_local.sum())
        history_entry = (
            f"r{round_num}:n_in={round_out.n_in},k={round_out.k},"
            f"path={notes},n_signal={n_signal_local},cutoff={signal_cutoff}"
        )

        if n_signal_local <= signal_cutoff:
            history.append(history_entry)
            break

        if round_num == max_rounds:
            unreliable = True
            history.append(history_entry + ",unreliable_signal_count_exceeds_cutoff")
            final_signal_local = np.zeros(len(sub_coords), dtype=bool)
            final_notes = "unreliable_signal_count_exceeds_fixed_cutoff"
            break

        n_drop = max(1, int(math.floor(retry_trim_fraction * len(current_indices))))
        if len(current_indices) - n_drop < 3:
            unreliable = True
            history.append(history_entry + ",cannot_trim_without_too_few_points")
            final_signal_local = np.zeros(len(sub_coords), dtype=bool)
            final_notes = "unreliable_trim_would_leave_too_few_points"
            break

        order = np.argsort(sub_coords[:, 1], kind="mergesort")
        keep_local = np.ones(len(sub_coords), dtype=bool)
        keep_local[order[:n_drop]] = False
        current_indices = current_indices[keep_local]
        history.append(history_entry + f",dropped_lowest_y={n_drop}")

    assert final_round_out is not None
    assert final_round_coords is not None
    assert final_signal_local is not None

    # Lift the final round's signal_mask back to the original index space.
    final_signal_full = np.zeros(n, dtype=bool)
    final_signal_full[current_indices] = final_signal_local

    # Pull diagnostic fields from the final round.
    noise_info = final_round_out.noise_info
    diag = SpectralDiagnostics(
        n_total=n,
        n_signal=int(final_signal_full.sum()),
        n_noise=int(n - final_signal_full.sum()),
        selected_k=final_round_out.k,
        eigenvalues_top=final_round_out.eigs_top,
        gap_zscores=final_round_out.gap_z_top,
        argmax_gap_z=final_round_out.argmax_z,
        noise_cluster_label=noise_info["noise_label"] if noise_info else -1,
        noise_cluster_size=noise_info["noise_size"] if noise_info else 0,
        noise_centroid_x=noise_info["noise_centroid_x"] if noise_info else math.nan,
        noise_centroid_y=noise_info["noise_centroid_y"] if noise_info else math.nan,
        cluster_summary=final_cluster_summary,
        k1_fallback_threshold_y=final_threshold_y,
        n_rounds_run=len(history),
        round_history="; ".join(history),
        notes=final_notes + (";unreliable" if unreliable else ""),
    )
    return final_signal_full, diag


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_outputs(
    input_path: Path,
    protein_ids: np.ndarray,
    coords_raw: np.ndarray,
    signal_mask: np.ndarray,
    output_dir: Path,
    *,
    verbose: bool = False,
) -> None:
    stem = input_path.stem
    output_dir.mkdir(parents=True, exist_ok=True)

    signal_path = output_dir / stem
    with open(signal_path, "w", encoding="utf-8") as handle:
        for protein_id in protein_ids[signal_mask]:
            handle.write(f"{protein_id}\n")

    if not verbose:
        return

    labels_path = output_dir / f"{stem}.labels.tsv"
    with open(labels_path, "w", encoding="utf-8") as handle:
        handle.write("protein_id\tx\ty\tlabel\n")
        for protein_id, (x, y), is_signal in zip(protein_ids, coords_raw, signal_mask):
            label = "signal" if is_signal else "noise"
            handle.write(f"{protein_id}\t{x:.6f}\t{y:.6f}\t{label}\n")


def write_summary(results: list[tuple[str, dict[str, Any]]], output_dir: Path) -> None:
    summary_path = output_dir / "summary.tsv"
    with open(summary_path, "w", encoding="utf-8") as handle:
        handle.write(
            "file\tn_total\tn_signal\tn_noise\tselected_k\targmax_gap_z\t"
            "noise_cluster_label\tnoise_cluster_size\t"
            "noise_centroid_x\tnoise_centroid_y\t"
            "k1_fallback_threshold_y\t"
            "n_rounds_run\tround_history\t"
            "eigenvalues_top10\tgap_zscores_top10\tcluster_summary\t"
            "notes\ttime_sec\tsuccess\n"
        )
        for _, r in sorted(results, key=lambda item: item[0]):
            eigs_str = ",".join(f"{v:.4f}" for v in r["eigenvalues_top"])
            gz_str = ",".join(f"{v:.3f}" for v in r["gap_zscores"])
            handle.write(
                f"{r['file']}\t{r['n_total']}\t{r['n_signal']}\t{r['n_noise']}\t"
                f"{r['selected_k']}\t{r['argmax_gap_z']:.4g}\t"
                f"{r['noise_cluster_label']}\t{r['noise_cluster_size']}\t"
                f"{r['noise_centroid_x']:.6g}\t{r['noise_centroid_y']:.6g}\t"
                f"{r['k1_fallback_threshold_y']:.6g}\t"
                f"{r['n_rounds_run']}\t{r['round_history']}\t"
                f"{eigs_str}\t{gz_str}\t{r['cluster_summary']}\t"
                f"{r['notes']}\t{r['time_sec']:.4f}\t{r['success']}\n"
            )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def process_file(
    filepath: str | Path,
    *,
    output_dir: str | Path | None,
    n_eigs: int,
    seed: int,
    k1_fallback_mad_multiplier: float,
    largest_hull_mad_multiplier: float,
    max_rounds: int,
    verbose: bool,
) -> tuple[str, dict[str, Any]]:
    start_time = time.perf_counter()
    input_path = Path(filepath)

    try:
        protein_ids, coords_raw = load_input_data(input_path)
        signal_mask, diag = classify(
            coords_raw,
            n_eigs=n_eigs,
            seed=seed,
            k1_fallback_mad_multiplier=k1_fallback_mad_multiplier,
            largest_hull_mad_multiplier=largest_hull_mad_multiplier,
            max_rounds=max_rounds,
        )

        if output_dir is not None:
            write_outputs(
                input_path=input_path,
                protein_ids=protein_ids,
                coords_raw=coords_raw,
                signal_mask=signal_mask,
                output_dir=Path(output_dir),
                verbose=verbose,
            )

        elapsed = time.perf_counter() - start_time
        result = {
            "file": str(input_path),
            "n_total": diag.n_total,
            "n_signal": diag.n_signal,
            "n_noise": diag.n_noise,
            "selected_k": diag.selected_k,
            "argmax_gap_z": diag.argmax_gap_z,
            "noise_cluster_label": diag.noise_cluster_label,
            "noise_cluster_size": diag.noise_cluster_size,
            "noise_centroid_x": diag.noise_centroid_x,
            "noise_centroid_y": diag.noise_centroid_y,
            "eigenvalues_top": diag.eigenvalues_top,
            "gap_zscores": diag.gap_zscores,
            "cluster_summary": diag.cluster_summary,
            "k1_fallback_threshold_y": diag.k1_fallback_threshold_y,
            "n_rounds_run": diag.n_rounds_run,
            "round_history": diag.round_history,
            "notes": diag.notes,
            "time_sec": elapsed,
            "success": True,
        }
        return str(input_path), result

    except Exception as exc:
        elapsed = time.perf_counter() - start_time
        print(f"ERROR processing {input_path}: {exc}", file=sys.stderr)
        return str(input_path), {
            "file": str(input_path),
            "n_total": 0, "n_signal": 0, "n_noise": 0,
            "selected_k": -1, "argmax_gap_z": math.nan,
            "noise_cluster_label": -1, "noise_cluster_size": 0,
            "noise_centroid_x": math.nan, "noise_centroid_y": math.nan,
            "eigenvalues_top": [], "gap_zscores": [],
            "cluster_summary": "",
            "k1_fallback_threshold_y": math.nan,
            "n_rounds_run": 0,
            "round_history": "",
            "notes": str(exc), "time_sec": elapsed, "success": False,
        }


def process_batch(file_list: list[Path], **kwargs: Any) -> list[tuple[str, dict[str, Any]]]:
    output_dir = kwargs.get("output_dir")
    if output_dir is not None:
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    worker_kwargs = dict(kwargs)
    n_workers = worker_kwargs.pop("n_workers")
    worker = partial(process_file, **worker_kwargs)

    if n_workers == 1:
        return [worker(path) for path in file_list]

    chunksize = max(1, math.ceil(len(file_list) / max(1, n_workers * 4)))
    with Pool(n_workers) as pool:
        return list(pool.imap_unordered(worker, file_list, chunksize=chunksize))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Spectral signal-over-noise caller (final). Auto-k spectral "
            "clustering on robustly z-scored coordinates. The largest "
            "cluster is noise; k>=2 files are labeled by strict "
            "upper-left centroid logic, convex-hull boundary logic for "
            "X cases, or MAD thresholding inside the largest-cluster "
            "hull when the self-hit is in the largest cluster."
        )
    )
    parser.add_argument("input", help="Input file or directory")
    parser.add_argument("-o", "--output",
                        help=(
                            "Output directory. Signal files are named by input "
                            "file stem (e.g. Q12851 from /path/to/Q12851). "
                            "NOTE: if processing files from different source "
                            "directories that share the same stem, use distinct "
                            "output directories to avoid overwriting."
                        ))
    parser.add_argument("-j", "--jobs", type=int, default=None,
                        help=(
                            "Number of parallel jobs (default: CPU count). "
                            "Each file holds a dense n×n affinity matrix in "
                            "memory (~50 MB at n=2550). With many parallel "
                            "workers, total memory is roughly 50 MB × jobs. "
                            "On memory-constrained machines, reduce -j to "
                            "avoid OOM failures that may look like they are "
                            "caused by specific input files."
                        ))
    parser.add_argument("-n", "--limit", type=int, help="Limit number of files")
    parser.add_argument("--pattern", default="*", help="File pattern for directory input")

    parser.add_argument("--n-eigs", type=int, default=20,
                        help=(
                            "Number of top eigenpairs to inspect for k-selection "
                            "(default: 20). Bounds the maximum k the algorithm can "
                            "choose to n_eigs - 1. Set higher if you expect more "
                            "than ~19 distinct clusters in your data."
                        ))
    parser.add_argument("--k1-fallback-mad-multiplier", type=float, default=DEFAULT_K1_MAD_MULTIPLIER,
                        help=(
                            "When k=1 is selected (no spectral cluster structure), "
                            "fall back to a y-axis outlier rule: a point is signal "
                            "iff y > median(y) + N * 1.4826 * MAD(y), where N is "
                            "this multiplier. Default 5 catches moderate-to-extreme "
                            "isolated outliers. Increase (e.g. to 10) to be more "
                            "stringent; set very large to effectively disable the "
                            "fallback."
                        ))
    parser.add_argument("--largest-hull-mad-multiplier", type=float,
                        default=DEFAULT_LARGEST_HULL_MAD_MULTIPLIER,
                        help=(
                            "When k>=2 and the self-hit is in the largest cluster, "
                            "apply a MAD y-threshold inside the largest-cluster hull "
                            "using this multiplier. Default 5 matches the k=1 fallback, "
                            "but this control is tuned independently."
                        ))
    parser.add_argument("--max-rounds", type=int, default=10,
                        help=(
                            "Maximum number of total classification rounds "
                            "(default 10). "
                            "A retry occurs only when the reported signal count "
                            "exceeds 50%% of the original input size; each retry "
                            "drops the lowest-y 25%% of the current working set. "
                            "Set to 1 to disable retries. Larger values allow "
                            "additional trim-and-rerun rounds."
                        ))
    parser.add_argument("--seed", type=int, default=42,
                        help="RNG seed for k-means")

    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Also write *.labels.tsv and summary.tsv")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)

    if input_path.is_file():
        file_list = [input_path]
    elif input_path.is_dir():
        file_list = sorted(f for f in input_path.glob(args.pattern) if f.is_file())
    else:
        print(f"ERROR: {args.input} not found", file=sys.stderr)
        return 1

    if not file_list:
        print(f"ERROR: No files found matching pattern '{args.pattern}'", file=sys.stderr)
        return 1

    if args.limit is not None:
        file_list = file_list[: args.limit]

    n_workers = args.jobs if args.jobs is not None else cpu_count()
    n_workers = max(1, n_workers)

    results = process_batch(
        file_list,
        output_dir=args.output,
        n_eigs=args.n_eigs,
        seed=args.seed,
        k1_fallback_mad_multiplier=args.k1_fallback_mad_multiplier,
        largest_hull_mad_multiplier=args.largest_hull_mad_multiplier,
        max_rounds=args.max_rounds,
        verbose=args.verbose,
        n_workers=n_workers,
    )

    if args.output and args.verbose:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        write_summary(results, output_dir)
    elif len(file_list) == 1:
        _, r = results[0]
        print(
            f"file={r['file']} n_signal={r['n_signal']} k={r['selected_k']} "
            f"argmax_gap_z={r['argmax_gap_z']:.2f} notes={r['notes']}",
            file=sys.stderr,
        )

    return 0 if all(r["success"] for _, r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
