"""Clustering signatures into 4 hierarchical levels.

L1 is non-transitive (one-way driver containment), so we build it
with union-find. L2-L4 are strict equality on tuples, so they're
straightforward groupby's WITHIN each L1 cluster.

The hierarchy is strict: every L4 cluster lives inside an L3 cluster,
which lives inside an L2 cluster, which lives inside an L1 cluster.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

from .signatures import ViewSignature


# ---------- union-find for the L1 (asymmetric driver) layer --------------

class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self.parent:
            self.parent[x] = x
            return x
        # Path compression
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        cur = x
        while self.parent[cur] != root:
            nxt = self.parent[cur]
            self.parent[cur] = root
            cur = nxt
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _l1_match(a: ViewSignature, b: ViewSignature) -> bool:
    """One-way driver containment: A.driver in B.all_tables OR vice versa.

    Both views must have a non-empty driver (otherwise we can't say
    they share a subject).
    """
    if not a.driver or not b.driver:
        return False
    return (a.driver in b.all_tables) or (b.driver in a.all_tables)


# ---------- cluster output structures ------------------------------------

@dataclass(frozen=True)
class Cluster:
    """One equivalence class of views at a given level.

    `next_level_split` shows how this cluster's members partition at
    the next-stricter level -- empty for L4. The split count tells the
    reviewer "this L2 of 8 actually splits into 5 L3 sub-clusters,
    meaning the projections vary."
    """
    cluster_id: str             # e.g., "L2-3"
    level: str                  # "L1" | "L2" | "L3" | "L4"
    members: tuple[str, ...]    # view names
    signature: dict             # axes that pass at this level
    join_type_consistency: str  # "consistent" | "mixed" | "n/a" (L1)
    join_types_per_member: dict[str, list[str]]  # for mixed clusters
    next_level_split: list[dict] = field(default_factory=list)


@dataclass(frozen=True)
class SimilarityReport:
    by_level: dict[str, list[Cluster]]   # {"L1": [...], "L2": [...], ...}
    signatures: tuple[ViewSignature, ...]


# ---------- core clustering ----------------------------------------------

def _build_l1_clusters(sigs: list[ViewSignature]) -> list[list[ViewSignature]]:
    """Group views into L1 equivalence classes via union-find on the
    asymmetric driver-containment relation. Returns lists of
    ViewSignature, NOT yet wrapped in Cluster objects."""
    uf = _UnionFind()
    by_name = {s.view_name: s for s in sigs if s.driver}
    for s in by_name.values():
        uf.find(s.view_name)
    names = list(by_name.keys())
    for i, na in enumerate(names):
        sa = by_name[na]
        for j in range(i + 1, len(names)):
            nb = names[j]
            sb = by_name[nb]
            if _l1_match(sa, sb):
                uf.union(na, nb)
    groups: dict[str, list[ViewSignature]] = defaultdict(list)
    for name, sig in by_name.items():
        groups[uf.find(name)].append(sig)
    return [g for g in groups.values()]


def _join_type_consistency(members: list[ViewSignature]) -> tuple[str, dict[str, list[str]]]:
    """For a cluster, return (consistency_label, per_member_join_types).
    "consistent" if every member's joined-table-to-type map is identical.
    "mixed" otherwise."""
    if len(members) <= 1:
        return ("n/a", {m.view_name: [f"{t}:{jt}" for t, jt in m.join_types]
                          for m in members})
    types_per_member: dict[str, list[str]] = {}
    canon_per_member: list[tuple[tuple[str, str], ...]] = []
    for m in members:
        items = m.join_types
        canon_per_member.append(items)
        types_per_member[m.view_name] = [f"{t}:{jt}" for t, jt in items]
    consistent = all(c == canon_per_member[0] for c in canon_per_member)
    return ("consistent" if consistent else "mixed", types_per_member)


def _l1_signature_dict(members: list[ViewSignature]) -> dict:
    """L1 cluster signature: drivers and shared subject tables."""
    drivers = sorted({m.driver for m in members if m.driver})
    # Subject tables = intersection of all_tables across members.
    if members:
        intersect = set(members[0].all_tables)
        for m in members[1:]:
            intersect &= m.all_tables
        subject_tables = sorted(intersect)
    else:
        subject_tables = []
    return {"drivers": drivers, "subject_tables": subject_tables}


def _ln_signature_dict(level: str, sample: ViewSignature) -> dict:
    """L2/L3/L4 cluster signature -- since members are equivalent at
    this level, we can read the signature off the sample member."""
    out: dict = {"driver": sample.driver}
    if level in ("L2", "L3", "L4"):
        out["joined_set"] = sorted(sample.joined_set)
    if level in ("L3", "L4"):
        out["projections"] = sorted(sample.projections)
    if level == "L4":
        out["filters"] = sorted(sample.filters)
    return out


def _split_at_next_level(
    members: list[ViewSignature],
    current_level: str,
) -> list[dict]:
    """Return the sub-clusters of `members` at the next-stricter level."""
    if current_level == "L4":
        return []
    next_level = {"L1": "L2", "L2": "L3", "L3": "L4"}[current_level]

    def key(s: ViewSignature):
        if next_level == "L2":
            return (s.driver, tuple(sorted(s.joined_set)))
        if next_level == "L3":
            return (s.driver, tuple(sorted(s.joined_set)),
                    tuple(sorted(s.projections)))
        # L4
        return (s.driver, tuple(sorted(s.joined_set)),
                tuple(sorted(s.projections)),
                tuple(sorted(s.filters)))

    groups: dict = defaultdict(list)
    for m in members:
        groups[key(m)].append(m.view_name)
    return [
        {"key_axis": next_level, "members": sorted(names)}
        for names in groups.values()
    ]


def build_clusters(
    signatures: Iterable[ViewSignature],
    *,
    min_members: int = 2,
) -> SimilarityReport:
    """Cluster signatures at all four levels.

    `min_members` filters out singleton clusters. Only clusters with
    >= min_members survive in the output.
    """
    sigs = list(signatures)

    # L1: union-find. Then L2-L4 are within-L1 group-bys.
    l1_groups: list[list[ViewSignature]] = _build_l1_clusters(sigs)

    by_level: dict[str, list[Cluster]] = {
        "L1": [], "L2": [], "L3": [], "L4": [],
    }

    for l1_idx, l1_members in enumerate(l1_groups, 1):
        if len(l1_members) >= min_members:
            consistency, types_per_member = _join_type_consistency(l1_members)
            by_level["L1"].append(Cluster(
                cluster_id=f"L1-{l1_idx}",
                level="L1",
                members=tuple(sorted(m.view_name for m in l1_members)),
                signature=_l1_signature_dict(l1_members),
                join_type_consistency=consistency,
                join_types_per_member=types_per_member,
                next_level_split=_split_at_next_level(l1_members, "L1"),
            ))

        # L2 sub-clusters: group by (driver, joined_set)
        l2_groups: dict = defaultdict(list)
        for m in l1_members:
            l2_groups[(m.driver, tuple(sorted(m.joined_set)))].append(m)
        l2_idx_local = 0
        for l2_key, l2_members in l2_groups.items():
            if len(l2_members) < min_members:
                continue
            l2_idx_local += 1
            consistency, types_per_member = _join_type_consistency(l2_members)
            by_level["L2"].append(Cluster(
                cluster_id=f"L2-{l1_idx}.{l2_idx_local}",
                level="L2",
                members=tuple(sorted(m.view_name for m in l2_members)),
                signature=_ln_signature_dict("L2", l2_members[0]),
                join_type_consistency=consistency,
                join_types_per_member=types_per_member,
                next_level_split=_split_at_next_level(l2_members, "L2"),
            ))

            # L3 sub-clusters: + projections
            l3_groups: dict = defaultdict(list)
            for m in l2_members:
                l3_groups[tuple(sorted(m.projections))].append(m)
            l3_idx_local = 0
            for l3_key, l3_members in l3_groups.items():
                if len(l3_members) < min_members:
                    continue
                l3_idx_local += 1
                consistency, types_per_member = _join_type_consistency(l3_members)
                by_level["L3"].append(Cluster(
                    cluster_id=f"L3-{l1_idx}.{l2_idx_local}.{l3_idx_local}",
                    level="L3",
                    members=tuple(sorted(m.view_name for m in l3_members)),
                    signature=_ln_signature_dict("L3", l3_members[0]),
                    join_type_consistency=consistency,
                    join_types_per_member=types_per_member,
                    next_level_split=_split_at_next_level(l3_members, "L3"),
                ))

                # L4 sub-clusters: + filters
                l4_groups: dict = defaultdict(list)
                for m in l3_members:
                    l4_groups[tuple(sorted(m.filters))].append(m)
                l4_idx_local = 0
                for l4_key, l4_members in l4_groups.items():
                    if len(l4_members) < min_members:
                        continue
                    l4_idx_local += 1
                    consistency, types_per_member = _join_type_consistency(l4_members)
                    by_level["L4"].append(Cluster(
                        cluster_id=f"L4-{l1_idx}.{l2_idx_local}.{l3_idx_local}.{l4_idx_local}",
                        level="L4",
                        members=tuple(sorted(m.view_name for m in l4_members)),
                        signature=_ln_signature_dict("L4", l4_members[0]),
                        join_type_consistency=consistency,
                        join_types_per_member=types_per_member,
                        next_level_split=[],
                    ))

    return SimilarityReport(by_level=by_level, signatures=tuple(sigs))
