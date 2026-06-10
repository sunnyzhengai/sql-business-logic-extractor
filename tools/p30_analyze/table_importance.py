"""Table importance ranking within communities.

Computes per-table importance scores that reflect the "gravity model"
Sunny described: each domain cluster has a center table (the primary
fact table everything else joins to), secondary tables (one hop from
center, moderate usage), and peripheral tables (ZC lookups, one-off
references).

The algorithm combines three signals:

  1. **Non-ZC JOIN PageRank** -- PageRank on the directed JOIN subgraph
     with ZC tables excluded entirely.  PageRank finds tables that other
     important tables point to (via JOINs).  This naturally elevates
     fact tables like REFERRAL, PAT_ENC, ORDER_PROC that sit at the
     hub of many joins, while ZC lookup tables (which are always
     leaves) contribute no authority.

  2. **View frequency** -- how many distinct views/procs reference
     each table across the corpus.  A table appearing in 73 views is
     more central than one appearing in 2.  ZC tables are counted but
     capped at a low weight so they don't inflate past center tables.

  3. **FK ontology role** (optional) -- when clarity_schema.yaml is
     available, tables that are the PK side of many FK relationships
     (i.e., other tables point to them) get a bonus.  This captures
     structural importance even when corpus coverage is sparse.

Each table receives a composite score (0-1 normalized within its
community) and a role classification:

  - **center**     -- the top-scoring non-ZC table in the community
  - **secondary**  -- non-ZC tables scoring above the community median
  - **peripheral** -- everything else (ZC tables, low-scoring tables)

Entry points
------------
  rank_tables_in_community(g, community_tables)
      -> list of (table_label, score, role) sorted by score descending

  rank_all_communities(g, communities)
      -> list of lists, index-aligned with communities

  build_corpus_table_frequency(g)
      -> dict mapping table node ID to number of distinct views
"""

from __future__ import annotations

from typing import Optional


def build_corpus_table_frequency(g) -> dict[str, int]:
    """Count how many distinct views reference each table.

    Walks READS_FROM_TABLE edges in the full graph to count distinct
    view provenance per table node.

    Returns
    -------
    dict mapping table node ID -> number of distinct views that read it.
    """
    freq: dict[str, set[str]] = {}
    for u, v, attrs in g.edges(data=True):
        if attrs.get("relation") != "READS_FROM_TABLE":
            continue
        # u = scope node, v = table node; edge carries `view` provenance
        view = attrs.get("view", "")
        if v not in freq:
            freq[v] = set()
        freq[v].add(view)
    return {table_id: len(views) for table_id, views in freq.items()}


def _build_join_digraph(g, community_tables: set[str], exclude_zc: bool = True):
    """Extract a reversed directed graph of JOIN edges among community tables.

    The graph builder creates JOIN edges as FROM_table -> right_table
    (the driver/center table has outgoing edges).  For PageRank we
    REVERSE this: an edge from right_table -> FROM_table means "the
    joined table depends on the center table."  This way PageRank
    authority flows to the table that everything else joins FROM --
    exactly the center/driver table we want to identify.

    Parameters
    ----------
    g                : the full MultiDiGraph
    community_tables : set of table node IDs in this community
    exclude_zc       : if True, drop ZC tables from the subgraph so they
                       don't dilute PageRank authority

    Returns
    -------
    A networkx DiGraph (not Multi -- we collapse parallel edges into
    weight) with only table nodes from the community.  Edges are
    reversed relative to the original JOIN direction.
    """
    import networkx as nx
    dg = nx.DiGraph()

    for table_id in community_tables:
        if exclude_zc and g.nodes[table_id].get("is_zc"):
            continue
        dg.add_node(table_id)

    for u, v, attrs in g.edges(data=True):
        if attrs.get("relation") != "JOIN":
            continue
        if u not in dg or v not in dg:
            continue
        # REVERSE the edge: v -> u (joined table points back to driver)
        if dg.has_edge(v, u):
            dg[v][u]["weight"] += 1
        else:
            dg.add_edge(v, u, weight=1)

    return dg


def _load_fk_ontology(schema_path: str | None) -> dict[str, int]:
    """Load FK relationships and count inbound FKs per table.

    Returns a dict mapping table name (bare, upper) -> number of columns
    across all tables that have a foreign_key pointing to it.  Tables
    that are the PK side of many relationships score higher.

    Returns empty dict if schema_path is None or file doesn't exist.
    """
    if not schema_path:
        return {}
    from pathlib import Path
    p = Path(schema_path)
    if not p.exists():
        return {}
    try:
        import yaml
        with open(p, encoding="utf-8") as f:
            schema = yaml.safe_load(f)
    except Exception:
        return {}

    fk_targets: dict[str, int] = {}
    for table in schema.get("tables") or []:
        for col in table.get("columns") or []:
            fk = col.get("foreign_key")
            if not fk:
                continue
            target = (fk.get("table") or "").upper()
            if target:
                fk_targets[target] = fk_targets.get(target, 0) + 1
    return fk_targets


def rank_tables_in_community(
    g,
    community_tables: set[str],
    corpus_freq: dict[str, int] | None = None,
    fk_ontology: dict[str, int] | None = None,
    *,
    pagerank_weight: float = 0.50,
    frequency_weight: float = 0.35,
    fk_weight: float = 0.15,
) -> list[tuple[str, float, str]]:
    """Rank tables within one community by composite importance.

    Parameters
    ----------
    g                : full MultiDiGraph from graph_builder
    community_tables : set of table node IDs in this community
    corpus_freq      : pre-computed table -> view count (from
                       build_corpus_table_frequency).  If None, computed
                       on the fly (slower for repeated calls).
    fk_ontology      : table name (bare upper) -> inbound FK count.
                       If None, FK signal is zeroed out and its weight
                       is redistributed to PageRank and frequency.
    pagerank_weight  : weight for PageRank signal (default 0.50)
    frequency_weight : weight for view-frequency signal (default 0.35)
    fk_weight        : weight for FK-ontology signal (default 0.15)

    Returns
    -------
    List of (table_label, score, role) tuples sorted by score descending.
    Score is normalized 0-1 within this community.
    Role is one of: "center", "secondary", "peripheral".
    """
    import networkx as nx

    if not community_tables:
        return []

    # If no FK ontology, redistribute its weight
    if not fk_ontology:
        total = pagerank_weight + frequency_weight
        pagerank_weight = pagerank_weight / total
        frequency_weight = frequency_weight / total
        fk_weight = 0.0

    if corpus_freq is None:
        corpus_freq = build_corpus_table_frequency(g)

    def label(table_id: str) -> str:
        return g.nodes[table_id].get("label", table_id)

    # --- Signal 1: PageRank on non-ZC JOIN subgraph ---
    join_dg = _build_join_digraph(g, community_tables, exclude_zc=True)

    pagerank_scores: dict[str, float] = {}
    if len(join_dg) > 0:
        try:
            pr = nx.pagerank(join_dg, weight="weight", alpha=0.85)
            # Normalize to 0-1
            max_pr = max(pr.values()) if pr else 1.0
            if max_pr > 0:
                pagerank_scores = {k: v / max_pr for k, v in pr.items()}
            else:
                pagerank_scores = {k: 0.0 for k in pr}
        except nx.PowerIterationFailedConvergence:
            # Fallback: use in-degree as proxy
            for node in join_dg:
                in_deg = join_dg.in_degree(node, weight="weight")
                pagerank_scores[node] = float(in_deg)
            max_val = max(pagerank_scores.values()) if pagerank_scores else 1.0
            if max_val > 0:
                pagerank_scores = {k: v / max_val for k, v in pagerank_scores.items()}

    # --- Signal 2: View frequency (corpus-wide) ---
    freq_scores: dict[str, float] = {}
    max_freq = 0
    for t in community_tables:
        f = corpus_freq.get(t, 0)
        # Cap ZC frequency contribution: a ZC table gets at most 20%
        # of its raw frequency so it doesn't outrank center tables
        if g.nodes[t].get("is_zc"):
            f = f * 0.2
        freq_scores[t] = float(f)
        if f > max_freq:
            max_freq = f
    if max_freq > 0:
        freq_scores = {k: v / max_freq for k, v in freq_scores.items()}

    # --- Signal 3: FK ontology (inbound FK count) ---
    fk_scores: dict[str, float] = {}
    if fk_ontology:
        max_fk = 0
        for t in community_tables:
            bare = label(t).upper()
            fk_count = fk_ontology.get(bare, 0)
            fk_scores[t] = float(fk_count)
            if fk_count > max_fk:
                max_fk = fk_count
        if max_fk > 0:
            fk_scores = {k: v / max_fk for k, v in fk_scores.items()}
        else:
            fk_scores = {t: 0.0 for t in community_tables}

    # --- Composite score ---
    raw_scores: dict[str, float] = {}
    for t in community_tables:
        pr = pagerank_scores.get(t, 0.0)
        freq = freq_scores.get(t, 0.0)
        fk = fk_scores.get(t, 0.0)
        raw_scores[t] = (pr * pagerank_weight
                         + freq * frequency_weight
                         + fk * fk_weight)

    # Normalize composite to 0-1
    max_score = max(raw_scores.values()) if raw_scores else 1.0
    if max_score > 0:
        scores = {k: v / max_score for k, v in raw_scores.items()}
    else:
        scores = {k: 0.0 for k in raw_scores}

    # --- Role classification ---
    non_zc_scores = [s for t, s in scores.items()
                     if not g.nodes[t].get("is_zc")]
    if non_zc_scores:
        median_score = sorted(non_zc_scores)[len(non_zc_scores) // 2]
    else:
        median_score = 0.0

    # Find the top-scoring non-ZC table = center
    center_table = None
    center_score = -1.0
    for t, s in scores.items():
        if not g.nodes[t].get("is_zc") and s > center_score:
            center_table = t
            center_score = s

    result: list[tuple[str, float, str]] = []
    for t in community_tables:
        s = scores[t]
        if g.nodes[t].get("is_zc"):
            role = "peripheral"
        elif t == center_table:
            role = "center"
        elif s >= median_score:
            role = "secondary"
        else:
            role = "peripheral"
        result.append((label(t), s, role))

    result.sort(key=lambda x: x[1], reverse=True)
    return result


def rank_all_communities(
    g,
    communities: list[set],
    schema_path: str | None = None,
) -> list[list[tuple[str, float, str]]]:
    """Rank tables in every community. Returns list aligned with communities.

    Parameters
    ----------
    g           : full MultiDiGraph from graph_builder
    communities : list of sets of table node IDs (from Louvain)
    schema_path : path to clarity_schema.yaml (optional; enables FK signal)

    Returns
    -------
    List of lists, each inner list = rank_tables_in_community() output.
    """
    corpus_freq = build_corpus_table_frequency(g)
    fk_ontology = _load_fk_ontology(schema_path)

    return [
        rank_tables_in_community(g, community, corpus_freq, fk_ontology)
        for community in communities
    ]
