"""Render scope tree as a chain of datasets (pure functions, no IO)."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetColumn:
    name: str
    english: str               # business_description, falling back to technical
    column_type: str           # passthrough / calculated / aggregate / case / ...


@dataclass(frozen=True)
class DatasetFilter:
    expression: str
    english: str
    kind: str                  # where | join_on | having | exists | ...


@dataclass(frozen=True)
class Dataset:
    """One scope rendered as a dataset for human / governance review."""
    scope_id: str              # canonical scope id ("main" | "cte:Foo" | ...)
    name: str                  # humanized name shown to users
    kind: str                  # scope kind
    base_datasets: tuple[str, ...]            # upstream scope ids (humanized)
    base_tables: tuple[str, ...]
    data_columns: tuple[DatasetColumn, ...]
    filters: tuple[DatasetFilter, ...]


def humanize_scope_id(scope_id: str) -> str:
    """Turn a scope id like `cte:ActivePatients` into a readable label.

    `main` becomes "Main query (view output)". Any `<kind>:<NAME>` form
    splits NAME on snake/camel boundaries and title-cases. Pure
    cosmetic -- no semantic interpretation."""
    if scope_id == "main":
        return "Main query (view output)"
    if ":" not in scope_id:
        return scope_id
    _kind, raw = scope_id.split(":", 1)
    parts = re.findall(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|\d+", raw)
    if not parts:
        return raw
    return " ".join(p[0].upper() + p[1:].lower() if p.isalpha() else p
                     for p in parts)


def _column_to_dataset(col: dict) -> DatasetColumn:
    eng = (col.get("business_description") or "").strip()
    if not eng:
        eng = (col.get("technical_description") or "").strip()
    return DatasetColumn(
        name=col.get("column_name") or "",
        english=eng,
        column_type=col.get("column_type") or "unknown",
    )


def _filter_to_dataset(f: dict) -> DatasetFilter:
    return DatasetFilter(
        expression=f.get("expression") or "",
        english=(f.get("english") or "").strip()
                  or f.get("expression") or "",
        kind=f.get("kind") or "where",
    )


def scope_to_dataset(scope: dict) -> Dataset:
    """Map one ScopeV1 dict (from corpus.jsonl) to a Dataset."""
    sid = scope.get("id") or ""
    return Dataset(
        scope_id=sid,
        name=humanize_scope_id(sid),
        kind=scope.get("kind") or "",
        base_datasets=tuple(humanize_scope_id(s)
                              for s in scope.get("reads_from_scopes") or []),
        base_tables=tuple(scope.get("reads_from_tables") or []),
        data_columns=tuple(_column_to_dataset(c)
                             for c in scope.get("columns") or []),
        filters=tuple(_filter_to_dataset(f)
                        for f in scope.get("filters") or []),
    )


def view_to_datasets(view: dict) -> tuple[Dataset, ...]:
    """Render every scope of one view as a Dataset.

    Order: scopes are kept in the corpus's declaration order (CTEs in
    the order they were declared, then `main`). For typical views
    (CTE1, CTE2 uses CTE1, main) this matches the dataflow order. For
    odd patterns where a scope references a later-declared one, the
    order may not be strictly topological; consumers can re-sort by
    walking `base_datasets` if needed.
    """
    scopes = view.get("scopes") or []
    return tuple(scope_to_dataset(s) for s in scopes)


# -- JSON / Markdown converters -------------------------------------------

def datasets_to_json_dict(view_name: str, datasets: tuple[Dataset, ...]) -> dict:
    return {
        "view_name": view_name,
        "datasets": [
            {
                "scope_id": d.scope_id,
                "name": d.name,
                "kind": d.kind,
                "base_datasets": list(d.base_datasets),
                "base_tables": list(d.base_tables),
                "data_columns": [
                    {"name": c.name, "english": c.english,
                     "column_type": c.column_type}
                    for c in d.data_columns
                ],
                "filters": [
                    {"expression": f.expression, "english": f.english,
                     "kind": f.kind}
                    for f in d.filters
                ],
            }
            for d in datasets
        ],
    }


def datasets_to_markdown(view_name: str, datasets: tuple[Dataset, ...]) -> str:
    """Render one view's datasets as a markdown section.

    Format mirrors the user's stated dataset description format:
      ### dataset name (scope id)
      - Base dataset: <upstream>
      - Reads tables: <table list>
      - Data columns:
          - col: english
      - Filters:
          - [where] english
    """
    lines: list[str] = [f"## {view_name}\n"]
    for d in datasets:
        header = f"### {d.name}  *({d.scope_id})*"
        lines.append(header)
        if d.base_datasets:
            lines.append(f"- **Base dataset:** {', '.join(d.base_datasets)}")
        if d.base_tables:
            lines.append(f"- **Reads tables:** {', '.join(d.base_tables)}")
        if d.data_columns:
            lines.append(f"- **Data columns:**")
            for c in d.data_columns:
                eng = f": {c.english}" if c.english else ""
                lines.append(f"    - `{c.name}`{eng}")
        if d.filters:
            lines.append(f"- **Filters:**")
            for f in d.filters:
                lines.append(f"    - *[{f.kind}]* {f.english}")
        lines.append("")  # blank line between datasets
    return "\n".join(lines)
