"""Interactive query graph builder for Streamlit.

Manages the state of an interactive table graph where users:
- See tables as nodes with live row counts
- Click nodes to view/toggle filters
- Add new tables from FK neighbors
- Watch counts update as filters change

The graph state lives in st.session_state and drives SQL generation
behind the scenes.
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class GraphTable:
    """A table node in the query graph."""
    name: str
    join_from: Optional[str]       # table this joins from (None for root)
    join_column: str               # FK column for the join
    pk_column: str                 # PK column on the other side
    join_direction: str            # "child_to_parent" or "parent_to_child"
    filters: list[dict] = field(default_factory=list)
    # Each filter: {"expression": str, "english": str, "active": bool}
    row_count: Optional[int] = None
    is_root: bool = False
    join_type: str = "JOIN"        # "JOIN" (inner), "LEFT JOIN", "RIGHT JOIN"


def get_fk_neighbors(table_name: str, fk_graph, exclude: set[str] | None = None) -> list[dict]:
    """Get tables that can be joined from the given table via FK relationships.

    Returns list of {"table": str, "fk_column": str, "pk_column": str, "direction": str, "description": str}
    """
    if fk_graph is None:
        return []

    exclude = exclude or set()
    table_upper = table_name.upper()
    neighbors = []

    # Outgoing edges (this table has FK to parent)
    if table_upper in fk_graph:
        for _, target in fk_graph.out_edges(table_upper):
            if target not in exclude:
                edge = fk_graph.edges[table_upper, target]
                desc = fk_graph.nodes[target].get("description", "")
                neighbors.append({
                    "table": target,
                    "fk_column": edge.get("fk_column", ""),
                    "pk_column": edge.get("pk_column", ""),
                    "direction": "child_to_parent",
                    "description": desc,
                })

    # Incoming edges (child tables that have FK to this table)
    if table_upper in fk_graph:
        for source, _ in fk_graph.in_edges(table_upper):
            if source not in exclude:
                edge = fk_graph.edges[source, table_upper]
                desc = fk_graph.nodes[source].get("description", "")
                neighbors.append({
                    "table": source,
                    "fk_column": edge.get("fk_column", ""),
                    "pk_column": edge.get("pk_column", ""),
                    "direction": "parent_to_child",
                    "description": desc,
                })

    return neighbors


def build_graph_sql(tables: list[GraphTable]) -> str:
    """Generate SQL from the current graph state.

    Builds a query that joins all tables in order, applying active filters.
    Returns a COUNT(*) query.
    """
    if not tables:
        return "SELECT 0"

    root = tables[0]
    from_clause = root.name
    where_clauses = []
    join_clauses = []

    # Root table filters
    for f in root.filters:
        if f.get("active", True):
            where_clauses.append(f["expression"])

    # Subsequent tables
    for t in tables[1:]:
        if t.join_from and t.join_column:
            if t.join_direction == "child_to_parent":
                on_clause = f"{t.join_from}.{t.join_column} = {t.name}.{t.pk_column}"
            else:
                on_clause = f"{t.name}.{t.join_column} = {t.join_from}.{t.pk_column}"
            jtype = t.join_type or "JOIN"
            join_clauses.append(f"{jtype} {t.name} ON {on_clause}")

        for f in t.filters:
            if f.get("active", True):
                where_clauses.append(f["expression"])

    sql = f"SELECT COUNT(*) FROM {from_clause}"
    for jc in join_clauses:
        sql += f"\n{jc}"
    if where_clauses:
        sql += "\nWHERE " + "\n  AND ".join(where_clauses)

    return sql


def build_count_up_to(tables: list[GraphTable], up_to_index: int) -> str:
    """Build COUNT SQL including tables up to the given index."""
    return build_graph_sql(tables[:up_to_index + 1])


def execute_graph_counts(tables: list[GraphTable], conn: sqlite3.Connection) -> list[GraphTable]:
    """Re-run counts for each table in the graph (cumulative).

    Updates row_count on each GraphTable in-place and returns the list.
    """
    # Run the FULL query (all tables + all active filters) once
    full_sql = build_graph_sql(tables)
    full_count = None
    try:
        cur = conn.cursor()
        cur.execute(full_sql)
        full_count = cur.fetchone()[0]
    except Exception as e:
        # Log but don't crash
        full_count = None

    # Compute per-step cumulative for the funnel
    for i, t in enumerate(tables):
        sql = build_count_up_to(tables, i)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            t.row_count = cur.fetchone()[0]
        except Exception as e:
            # If the cumulative query fails, try just this table alone
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT COUNT(*) FROM {t.name}")
                t.row_count = cur.fetchone()[0]
            except Exception:
                t.row_count = None

    return tables


def compute_neighbor_preview(
    tables: list[GraphTable],
    neighbor_table: str,
    neighbor_join_from: str,
    neighbor_fk_col: str,
    neighbor_pk_col: str,
    neighbor_direction: str,
    conn: sqlite3.Connection,
) -> Optional[int]:
    """Preview what the count would be if we added a neighbor table.

    Returns the row count after adding this join, or None on error.
    """
    # Build a temporary table list with the neighbor added
    preview_tables = list(tables) + [GraphTable(
        name=neighbor_table,
        join_from=neighbor_join_from,
        join_column=neighbor_fk_col,
        pk_column=neighbor_pk_col,
        join_direction=neighbor_direction,
        filters=[],
        is_root=False,
    )]
    sql = build_graph_sql(preview_tables)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchone()[0]
    except Exception:
        return None


def build_join_popularity(glossary_dir: str | Path = None) -> dict[tuple[str, str], int]:
    """Count how often each table pair co-occurs in reports.

    Returns dict mapping (table_a, table_b) (sorted) → count.
    """
    if glossary_dir is None:
        glossary_dir = Path(__file__).resolve().parents[1] / "data" / "report_glossary"
    else:
        glossary_dir = Path(glossary_dir)

    pair_counts: dict[tuple[str, str], int] = Counter()

    for yf in sorted(glossary_dir.glob("*.yaml")):
        with open(yf) as f:
            r = yaml.safe_load(f)
        report_tables = [t.upper() for t in r.get("tables_used", [])]
        for i, t1 in enumerate(report_tables):
            for t2 in report_tables[i + 1:]:
                pair = tuple(sorted([t1, t2]))
                pair_counts[pair] += 1

    return dict(pair_counts)


def get_pair_popularity(t1: str, t2: str,
                        popularity: dict[tuple[str, str], int]) -> int:
    """Get the join popularity between two tables."""
    pair = tuple(sorted([t1.upper(), t2.upper()]))
    return popularity.get(pair, 0)


def score_neighbor_relevance(
    neighbor: dict,
    question_tokens: set[str],
    popularity: dict[tuple[str, str], int],
    join_from: str,
    learned_terms: dict | None = None,
    definitions: list[dict] | None = None,
    existing_tables: set[str] | None = None,
) -> float:
    """Score how relevant a neighbor table is to the current question.

    Combines:
    - Token overlap between question and table name/description
    - Join popularity (how often this join appears in existing reports)
    - Learned term match (if the table is associated with a known term)
    - Business definition match (if a certified definition uses this table
      alongside existing tables AND matches the question)

    Returns a score from 0.0 to 1.0.
    """
    score = 0.0
    table_name = neighbor["table"]
    desc = neighbor.get("description", "").lower()
    name_tokens = set(table_name.lower().replace("_", " ").split())

    # Token overlap with question — check both full tokens and substrings
    desc_tokens = set(desc.split())
    all_table_tokens = name_tokens | desc_tokens
    overlap = question_tokens & all_table_tokens
    if overlap:
        score += len(overlap) * 0.15

    # Substring match — "dx" in "pat_enc_dx", "diag" in description, etc.
    table_lower = table_name.lower()
    for qt in question_tokens:
        if len(qt) >= 2 and qt in table_lower:
            score += 0.15
            break
        if len(qt) >= 3 and qt in desc:
            score += 0.1
            break

    # Join popularity
    pop = get_pair_popularity(join_from, table_name, popularity)
    score += pop * 0.1

    # Learned term association
    if learned_terms:
        for key, term in learned_terms.items():
            term_tables = [t.upper() for t in term.get("tables", [])]
            if table_name.upper() in term_tables:
                aliases = set(a.lower() for a in term.get("aliases", []))
                aliases.add(term.get("term", "").lower())
                if question_tokens & aliases:
                    score += 0.3
                    break

    # Business definition match — the strongest signal
    # If a certified definition uses this table AND its label/description
    # matches the question, this is almost certainly the right join
    if definitions and existing_tables:
        table_upper = table_name.upper()
        existing_upper = {t.upper() for t in existing_tables}

        for defn in definitions:
            bb = defn.get("backbone", {})
            defn_tables = set(t.upper() for t in bb.get("tables", []))

            # Does this definition use the candidate table?
            if table_upper not in defn_tables:
                continue

            # Does it also use at least one table already in the graph?
            if not (defn_tables & existing_upper):
                continue

            # Does the definition's label/description match the question?
            label = defn.get("label", "").lower()
            desc_defn = defn.get("description", "").lower()
            label_tokens = set(label.split())
            desc_tokens_d = set(desc_defn.split())
            defn_tokens = label_tokens | desc_tokens_d

            # Also check filter english
            for f in bb.get("characteristic_filters", []):
                defn_tokens.update(f.get("english", "").lower().split())

            defn_overlap = question_tokens & defn_tokens
            if defn_overlap:
                # Strong boost — a certified definition matches
                score += 0.4 + len(defn_overlap) * 0.05
                break

    return min(score, 1.0)
