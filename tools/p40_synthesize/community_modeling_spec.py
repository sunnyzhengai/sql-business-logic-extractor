"""Per-community modeling spec generator -- the data-modeling-team handoff.

Lean version (Phase 3e-iv rewrite). The earlier version had 7 sections
including column-variance, filter-patterns, and member-view splits.
User feedback: "all I care about is tables, joins. Just write the
model code already." So this version is just:

  1. Tables          -- core / bridges / lookups (one-line list each)
  2. Joins           -- the common spine (sorted by view-count)
  3. Starter SQL     -- a CREATE VIEW that joins the spine
  4. Replaced views  -- the silo'd views this model would consolidate

The richer governance findings (reconciliation candidates, common
filters, weak members) stay COMPUTED in p30_analyze -- they're useful
for other artifacts (term_disagreements, exec deck) and for the
communities.md cross-community report -- but they no longer crowd the
modeling-team handoff.

One spec per community at:
    <output_dir>/modeling_specs/community_<NN>_<top_table>.md
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path


def write_community_modeling_spec(
    community_index: int,
    top_table: str,
    analysis: dict,
    join_paths: list[dict],
    bridge_table_labels: list[str],
    bridge_to_neighbor_communities: dict[str, list[int]],
    output_path: str | Path,
    spine_threshold_fraction: float = 0.5,
) -> str:
    """Write a lean per-community modeling spec.

    Parameters
    ----------
    community_index : int
    top_table       : str   -- community's highest-degree table (used as name)
    analysis        : dict  -- from p30_analyze.community_analysis.analyze_community
    join_paths      : list  -- from p30_analyze.join_paths.analyze_join_paths (per-community)
    bridge_table_labels             : list of bridge labels (corpus-wide)
    bridge_to_neighbor_communities  : bridge label -> communities it connects
    output_path                     : where to write the markdown
    spine_threshold_fraction        : edges used by >= this fraction of
                                       primary views go in the spine.
    """
    primary_views = analysis["primary_views"]
    n_primary = len(primary_views)
    n_tables = analysis["n_tables"]

    # Identify which bridges connect to THIS community (for the
    # conformed-dimensions list).
    bridges_here = sorted(
        label for label in bridge_table_labels
        if community_index in bridge_to_neighbor_communities.get(label, [])
    )

    # Threshold: an edge is "spine" if used by >= this many views.
    threshold_count = max(1, int(spine_threshold_fraction * n_primary)) if n_primary else 1
    spine = [j for j in join_paths if j["n_views"] >= threshold_count]
    peripheral = [j for j in join_paths if j["n_views"] < threshold_count]

    lines: list[str] = []
    lines.append(f"# Community {community_index} -- {top_table}")
    lines.append("")
    lines.append(f"{n_tables} tables, {n_primary} primary views.")
    lines.append("")

    # ---- Tables -----------------------------------------------------------
    lines.append("## Tables")
    lines.append("")
    if analysis["core_tables"]:
        lines.append(
            "**Core** (cohort-shaping): "
            + ", ".join(f"`{t}`" for t in analysis["core_tables"])
        )
        lines.append("")
    if bridges_here:
        lines.append(
            "**Conformed dimensions** (bridges, shared across communities): "
            + ", ".join(f"`{t}`" for t in bridges_here)
        )
        lines.append("")
    if analysis["leaf_tables"]:
        lines.append(
            "**Lookups** (decorative -- ZC, codes, etc.): "
            + ", ".join(f"`{t}`" for t in analysis["leaf_tables"])
        )
        lines.append("")

    # ---- Joins ------------------------------------------------------------
    lines.append("## Joins")
    lines.append("")
    if spine:
        lines.append(
            f"Spine (used by >= {threshold_count}/{n_primary} = "
            f"{int(spine_threshold_fraction * 100)}% of views):"
        )
        lines.append("")
        lines.append("| from | to | join | views |")
        lines.append("|---|---|---|---|")
        for j in spine[:20]:
            jt = j["join_type"]
            if j["n_distinct_join_types"] > 1:
                jt = f"{jt} _(+{j['n_distinct_join_types'] - 1} other)_"
            lines.append(
                f"| `{j['from_table']}` | `{j['to_table']}` | {jt} | "
                f"{j['n_views']}/{n_primary} |"
            )
        if len(spine) > 20:
            lines.append(f"")
            lines.append(f"... and {len(spine) - 20} more spine edges.")
        lines.append("")
    else:
        lines.append("_(no JOIN edges meet the spine threshold)_")
        lines.append("")

    if peripheral:
        lines.append(f"Peripheral edges (< spine threshold): {len(peripheral)}.")
        lines.append("")

    # ---- Starter SQL ------------------------------------------------------
    lines.append("## Starter SQL")
    lines.append("")
    sql = _generate_starter_sql(
        community_index=community_index,
        top_table=top_table,
        spine=spine,
        n_primary=n_primary,
    )
    lines.append("```sql")
    lines.append(sql)
    lines.append("```")
    lines.append("")

    # ---- Replaced views ---------------------------------------------------
    lines.append(f"## Replaces these views ({n_primary})")
    lines.append("")
    if primary_views:
        for v in primary_views:
            lines.append(f"- `{v}`")
    else:
        lines.append("_(no primary member views)_")
    lines.append("")

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")
    return str(out)


def _safe_sql_name(s: str) -> str:
    """Convert an arbitrary table name to a safe SQL identifier fragment."""
    return "".join(c.lower() if c.isalnum() else "_" for c in s)[:60]


def _generate_starter_sql(
    community_index: int,
    top_table: str,
    spine: list[dict],
    n_primary: int,
) -> str:
    """Generate a CREATE VIEW statement based on the spine edges.

    Heuristic: the "root" of the FROM clause is the most-common
    `from_table` across spine edges. Edges starting from the root
    become joins in the SQL; edges starting elsewhere are noted as
    comments (the modeler may want to chain them differently).
    """
    if not spine:
        return (
            "-- (no JOIN spine identified -- this community's views may use\n"
            "--  single-table reads or scope-internal joins. Manual modeling.)"
        )

    # Pick the natural FROM-clause root: the table that appears as the
    # LEFT side of the most spine edges.
    from_counts: Counter = Counter()
    for j in spine:
        from_counts[j["from_table"]] += 1
    root_table, _ = from_counts.most_common(1)[0]

    # Sort joins for output: those FROM the root first (in view-count desc),
    # then any joins from non-root tables (also view-count desc).
    spine_from_root = sorted(
        (j for j in spine if j["from_table"] == root_table),
        key=lambda j: (-j["n_views"], j["to_table"]),
    )
    spine_other = sorted(
        (j for j in spine if j["from_table"] != root_table),
        key=lambda j: (-j["n_views"], j["from_table"], j["to_table"]),
    )

    view_name = _safe_sql_name(top_table)
    out: list[str] = []
    out.append(f"-- Starter data model for community {community_index} -- {top_table}.")
    out.append(f"-- Based on the JOIN spine used by primary views in this community.")
    out.append("-- ")
    out.append("-- TODO: replace `SELECT *` with the columns you want to expose.")
    out.append("-- TODO: verify the ON clauses below (extracted from the corpus;")
    out.append("--       table aliases inside them may not match your CREATE VIEW context).")
    out.append("")
    out.append(f"CREATE VIEW dbo.model_{view_name} AS")
    out.append("SELECT *")
    out.append(f"FROM {root_table}")

    for j in spine_from_root:
        on_clause = j["on_expression"] or "/* TODO: verify ON clause */ 1 = 1"
        out.append(
            f"{j['join_type']} {j['to_table']} ON {on_clause}  "
            f"-- {j['n_views']}/{n_primary} views"
        )

    if spine_other:
        out.append("")
        out.append(
            "-- The following spine edges start from non-root tables;"
        )
        out.append(
            "-- the modeler must decide where to insert them in the FROM clause."
        )
        for j in spine_other:
            on_clause = j["on_expression"] or "/* TODO */ ?"
            out.append(
                f"-- {j['from_table']} -> {j['to_table']} ({j['join_type']}, "
                f"{j['n_views']}/{n_primary} views): ON {on_clause}"
            )

    # The CREATE VIEW statement needs a terminator.
    out.append(";")
    return "\n".join(out)
