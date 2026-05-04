"""Clustering + flag emission.

Two-tier output by design:

1. **Clusters** (`clusters.csv`) -- groups of views that are
   STRUCTURALLY IDENTICAL: same all_tables AND same all_joins.
   Each cluster represents a duplicate-report finding; members are
   interchangeable from a shape standpoint. Only clusters with >=
   `min_members` views are emitted.

2. **Cross-cluster pairs** (`cross_pairs.csv`) -- pair-level flags
   for everything weaker than full structural identity:
     - `fact_subset` / `fact_superset` -- one view's fact tables ⊊ the other's
     - `fact_overlap` -- facts intersect; neither subset of the other
     - `same_facts_different_joins` -- same fact tables, different fact joins
     - `dim_extension` -- same fact tables and fact joins, different dim tables
     - `join_subset` -- same fact tables, one's fact joins ⊊ the other's
     - `same_driver` -- same FROM driver but otherwise unrelated

A view that's identical to none and has no fact-table neighbors gets
no row anywhere. Its features.csv entry is the only trace. That's
correct: it has no shape-related governance finding to surface.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

from .features import ViewShape


# Cluster-level (members are pair-wise equivalent)
FLAG_TABLE_IDENTICAL = "table_identical"

# Cross-cluster pair flags (asymmetric or weaker relationships)
FLAG_FACT_IDENTICAL = "fact_identical"
FLAG_FACT_SUBSET = "fact_subset"
FLAG_FACT_SUPERSET = "fact_superset"
FLAG_FACT_OVERLAP = "fact_overlap"
FLAG_SAME_FACTS_DIFFERENT_JOINS = "same_facts_different_joins"
FLAG_DIM_EXTENSION = "dim_extension"
FLAG_JOIN_SUBSET = "join_subset"
FLAG_JOIN_TOPOLOGY_DIFFERS = "join_topology_differs"
FLAG_SAME_DRIVER = "same_driver"
FLAG_JOIN_IDENTICAL = "join_identical"      # alias used in tests / casual reading


@dataclass
class Cluster:
    """One group of views that are STRUCTURALLY IDENTICAL (same all_tables
    AND same all_joins). Members are interchangeable shape-wise."""
    cluster_key: tuple[tuple[str, ...], tuple[tuple[str, str, str], ...]]
    members: list[ViewShape]
    flags: set[str] = field(default_factory=lambda: {FLAG_TABLE_IDENTICAL})


@dataclass
class CrossClusterPair:
    """An asymmetric or weaker relationship between two views (typically
    in different primary clusters)."""
    a: str
    b: str
    flag: str
    detail: str


@dataclass
class ClusterReport:
    clusters: list[Cluster]
    cross_pairs: list[CrossClusterPair]


def _pair_flags(a: ViewShape, b: ViewShape) -> list[tuple[str, str]]:
    """Return [(flag, detail), ...] for one ordered pair (a, b).

    ORDER MATTERS for asymmetric flags (subset/superset). Caller is
    responsible for not double-emitting the same pair in both orders.
    """
    out: list[tuple[str, str]] = []

    # No fact tables -> nothing to compare on the fact axis.
    if not a.fact_tables or not b.fact_tables:
        return out

    inter = a.fact_tables & b.fact_tables
    if not inter:
        # No fact overlap at all; we still emit same_driver as the only
        # weak signal worth surfacing.
        if a.driver_table and a.driver_table == b.driver_table:
            out.append((
                FLAG_SAME_DRIVER,
                f"both views FROM-driven by {a.driver_table}",
            ))
        return out

    # ---- Fact-table relationships ----
    if a.fact_tables == b.fact_tables:
        # Same fact tables: drill into joins next.
        if a.fact_joins_sig == b.fact_joins_sig:
            # Same facts AND same fact joins. Difference (if any) is
            # purely dim-table presentation.
            if a.all_tables == b.all_tables and a.all_joins_sig == b.all_joins_sig:
                # Fully identical -- handled at the cluster level, not here.
                pass
            else:
                added_b = sorted(b.all_tables - a.all_tables)
                added_a = sorted(a.all_tables - b.all_tables)
                bits = []
                if added_b:
                    bits.append(f"{b.view_name} adds dims: {{{', '.join(added_b)}}}")
                if added_a:
                    bits.append(f"{a.view_name} adds dims: {{{', '.join(added_a)}}}")
                out.append((
                    FLAG_DIM_EXTENSION,
                    "; ".join(bits) or "differ in dim joins only",
                ))
        else:
            # Same facts, different fact joins.
            a_joins = set(a.fact_joins)
            b_joins = set(b.fact_joins)
            if a_joins < b_joins:
                out.append((
                    FLAG_JOIN_SUBSET,
                    f"{a.view_name}'s fact joins ⊊ {b.view_name}'s "
                    f"(B adds: {sorted(b_joins - a_joins)})",
                ))
            elif b_joins < a_joins:
                out.append((
                    FLAG_JOIN_SUBSET,
                    f"{b.view_name}'s fact joins ⊊ {a.view_name}'s "
                    f"(A adds: {sorted(a_joins - b_joins)})",
                ))
            else:
                out.append((
                    FLAG_SAME_FACTS_DIFFERENT_JOINS,
                    f"both have facts {{{', '.join(sorted(a.fact_tables))}}}; "
                    f"{a.view_name} joins {sorted(a_joins)}; "
                    f"{b.view_name} joins {sorted(b_joins)}",
                ))
    elif a.fact_tables < b.fact_tables:
        out.append((
            FLAG_FACT_SUBSET,
            f"{a.view_name}.facts ⊊ {b.view_name}.facts "
            f"(B adds: {sorted(b.fact_tables - a.fact_tables)})",
        ))
    elif b.fact_tables < a.fact_tables:
        out.append((
            FLAG_FACT_SUPERSET,
            f"{a.view_name}.facts ⊋ {b.view_name}.facts "
            f"(A adds: {sorted(a.fact_tables - b.fact_tables)})",
        ))
    else:
        # True overlap (intersect, but each has unique facts too).
        out.append((
            FLAG_FACT_OVERLAP,
            f"shared: {sorted(inter)}; "
            f"{a.view_name} only: {sorted(a.fact_tables - b.fact_tables)}; "
            f"{b.view_name} only: {sorted(b.fact_tables - a.fact_tables)}",
        ))

    # ---- Same driver as a secondary signal ----
    if a.driver_table and a.driver_table == b.driver_table:
        # Only emit if it's not already implied by a stronger flag above.
        if not out:
            out.append((
                FLAG_SAME_DRIVER,
                f"both views FROM-driven by {a.driver_table}",
            ))

    return out


def build_clusters(shapes: Iterable[ViewShape], *, min_members: int = 2) -> ClusterReport:
    """Cluster views by strict structural identity (all_tables AND
    all_joins). Emit per-pair flags for weaker relationships.

    Members of the same cluster are NOT also emitted as cross_pairs --
    their relationship is fully described by the cluster row.
    """
    shapes = list(shapes)

    # Strict cluster key: (sorted all_tables, sorted all_joins).
    by_key: dict[
        tuple[tuple[str, ...], tuple[tuple[str, str, str], ...]],
        list[ViewShape],
    ] = defaultdict(list)
    for s in shapes:
        if not s.fact_tables and not s.all_tables:
            continue
        key = (s.all_tables_sig, s.all_joins_sig)
        by_key[key].append(s)

    clusters: list[Cluster] = []
    in_cluster: set[str] = set()
    for key, members in sorted(
        by_key.items(),
        key=lambda kv: (-len(kv[1]), kv[0]),
    ):
        if len(members) < min_members:
            continue
        clusters.append(Cluster(cluster_key=key, members=members))
        for m in members:
            in_cluster.add(m.view_name)

    # Cross-cluster pairs: every UNORDERED pair of views, where at
    # least one isn't in the same cluster as the other. Quadratic; fine
    # for hundreds of views.
    cluster_membership: dict[str, int] = {}
    for ci, cluster in enumerate(clusters):
        for m in cluster.members:
            cluster_membership[m.view_name] = ci

    cross_pairs: list[CrossClusterPair] = []
    for i, a in enumerate(shapes):
        for b in shapes[i + 1:]:
            ca = cluster_membership.get(a.view_name)
            cb = cluster_membership.get(b.view_name)
            if ca is not None and ca == cb:
                continue   # already in the same cluster
            for flag, detail in _pair_flags(a, b):
                cross_pairs.append(CrossClusterPair(
                    a=a.view_name, b=b.view_name, flag=flag, detail=detail,
                ))

    return ClusterReport(clusters=clusters, cross_pairs=cross_pairs)
