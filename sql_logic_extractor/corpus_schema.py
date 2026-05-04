"""Canonical corpus schema (v3) -- one structured artifact per view,
shaped as a tree that maps trivially onto a graph database.

A view is a tree:

    ViewV1
      ├── report (ReportV1)              -- view-level human-readable summary
      ├── view_level_notes               -- author top-of-file comments
      ├── inventory                      -- raw (database, schema, table, column) refs
      └── scopes (list[ScopeV1])         -- one per structural unit
            ├── id                       -- "main" | "cte:NAME" | "derived:ALIAS" | ...
            ├── kind                     -- AST-derived, free-form (NOT a closed enum)
            ├── filters (list[FilterV1])
            ├── columns (list[ColumnV1]) -- this scope's output columns
            ├── reads_from_scopes        -- IDs of upstream scopes
            └── reads_from_tables        -- base table names this scope reads

Cross-scope dataflow is captured exclusively through scope-qualified
`base_columns` strings on each column:

  - "table:Clarity.dbo.PATIENT.PAT_ID"   -- terminal base column
  - "cte:CTE1.PAT_ID"                     -- upstream CTE column
  - "derived:t0.x"                        -- derived-table column

A graph loader walks these as edges. There is no flat / compact form,
no inheritance flags, no expand_view() -- the tree IS the canonical
representation.

Filters NEVER propagate across scope boundaries. Each scope owns only
the predicates declared inside it. A column's "real" constraints are
the union of its own scope's filters plus the filters of every scope it
transitively reads from -- the consumer composes that on demand by
walking `reads_from_scopes`.

`scope.kind` strings come from the SQL parser, NOT a closed enum we
maintain. Common values: "main", "cte", "derived", "subquery", "exists",
"in", "union:N", "intersect:N", "except:N", "lateral", "pivot". Unknown
kinds flow through verbatim; consumers handle the recognized subset
with named logic and treat the rest as "structural scope, opaque kind".

Bump `SCHEMA_VERSION` for breaking changes. v3 introduced the tree shape
and scope-qualified base_columns; v1/v2 corpus.jsonl files are NOT
backward-readable.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Iterator


SCHEMA_VERSION: int = 3


# ============================================================
# Filters and columns (per-scope, no inheritance)
# ============================================================

@dataclass(frozen=True)
class JoinV1:
    """One JOIN declared in a scope, surfaced for view-shape comparison.

    `right_table` is the right-hand side (base table or scope id, e.g.
    a CTE reference). `join_type` preserves the parser's wording
    ('INNER JOIN' / 'LEFT JOIN' / 'CROSS JOIN' / ...). The left side
    is implicit -- determined by the scope's FROM driver and prior
    joins -- and is NOT stored, since shape comparison normalizes a
    view's joins as a multiset rather than an ordered chain.

    Additive field on ScopeV1 (added post-v3, no schema bump). Old
    corpus.jsonl files without a `joins` field are still readable.
    """
    right_table: str = ""
    join_type: str = "JOIN"
    on_expression: str = ""
    right_alias: str = ""


@dataclass(frozen=True)
class FilterV1:
    """A predicate declared in one scope.

    `kind` distinguishes where/having/qualify/join_on/exists/in. English
    translation lives alongside the raw SQL so consumers can stitch
    column-English with filter-English without a second translation pass.

    `subquery_scope_ids` lists scope IDs of any subqueries referenced
    inside this predicate; the subquery scope itself is emitted as a
    sibling under the same view.
    """
    expression: str = ""
    english: str = ""
    kind: str = "where"
    subquery_scope_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class TermV1:
    """The governance comparison unit for one column. Empty
    `name_tokens` + `name_is_structural=True` means "this column did
    not qualify as a Term and the consumer should skip it for bucketing
    purposes" (kept in the corpus for completeness)."""
    name_tokens: tuple[str, ...] = ()
    is_passthrough: bool = False
    name_is_structural: bool = False
    has_filters: bool = False


@dataclass(frozen=True)
class ColumnV1:
    """One output column of one scope.

    `technical_description` is the column's SQL expression as resolved
    inside this scope (no view-level filter inlining). `business_description`
    is the plain-English translation produced by the engineered translator.

    `base_columns` is scope-qualified -- entries name the immediate
    upstream column (in another scope or in a base table). Walking these
    is how a graph consumer reaches base tables.

    Filters are NOT carried here -- they live on the owning ScopeV1.
    """
    column_name: str = ""
    column_type: str = "unknown"
    technical_description: str = ""
    business_description: str = ""
    business_domain: str = ""

    # Lineage (scope-qualified)
    base_columns: tuple[str, ...] = ()
    base_tables: tuple[str, ...] = ()

    # Comment-as-data attachments
    author_notes: tuple[str, ...] = ()

    # Governance
    term: TermV1 = field(default_factory=TermV1)
    fingerprint: str | None = None


# ============================================================
# Scopes (the tree's interior nodes)
# ============================================================

@dataclass(frozen=True)
class ScopeV1:
    """One structural unit of the view's SQL: main SELECT, CTE,
    derived table, subquery, set-op branch, lateral, pivot, etc.

    `kind` is sourced from the parser AST and is intentionally NOT a
    closed enum -- new SQL constructs flow through verbatim. Consumers
    should switch on canonical names they recognize and treat anything
    else as opaque structural data.
    """
    id: str = ""
    kind: str = ""

    filters: tuple[FilterV1, ...] = ()
    columns: tuple[ColumnV1, ...] = ()

    reads_from_scopes: tuple[str, ...] = ()
    reads_from_tables: tuple[str, ...] = ()

    # Structured joins for view-shape comparison. Additive; default empty.
    joins: tuple[JoinV1, ...] = ()


# ============================================================
# View-level summary (one per view)
# ============================================================

@dataclass(frozen=True)
class ReportV1:
    """View-level human-readable description.

    `technical_description` and `business_description` are bullet-form
    summaries assembled per scope. Each scope contributes its own bullet
    section (output columns, filters, joins, group by, etc.).
    """
    technical_description: str = ""
    business_description: str = ""
    primary_purpose: str = ""
    key_metrics: tuple[str, ...] = ()
    column_count: int = 0
    use_llm: bool = False


@dataclass(frozen=True)
class InventoryRefV1:
    """One entry from Tool 1's per-view manifest. A view may reference
    a table/column via WHERE / JOIN / EXISTS subquery WITHOUT exposing
    it as an output column; the inventory is the flat catalogue of all
    such references for governance reporting."""
    table: str = ""
    column: str = ""
    database: str = ""
    schema: str = ""
    reference_type: str = "column"   # "column" | "table"
    confidence: str = "medium"        # "high" | "medium" | "low"


# ============================================================
# View + Corpus
# ============================================================

@dataclass(frozen=True)
class ViewV1:
    """A view as a self-contained graph fragment.

    `view_outputs` lists the scope IDs whose columns are user-visible:
    typically `["main"]`, but for top-level UNION views it's the branch
    scopes (positional alignment is implicit in SQL set semantics).
    """
    view_name: str = ""
    report: ReportV1 = field(default_factory=ReportV1)
    view_level_notes: tuple[str, ...] = ()
    scopes: tuple[ScopeV1, ...] = ()
    view_outputs: tuple[str, ...] = ()
    inventory: tuple[InventoryRefV1, ...] = ()
    use_llm: bool = False


@dataclass(frozen=True)
class CorpusV1:
    """One CorpusV1 = one extractor run over one folder of views.
    `views` ordering matches the input folder for stable cross-run diffs."""
    schema_version: int = SCHEMA_VERSION
    views: tuple[ViewV1, ...] = ()


# ============================================================
# Serialization
# ============================================================

def corpus_to_dict(corpus: CorpusV1) -> dict:
    """Plain dict for json.dump. Tuples are converted to lists."""
    return _to_jsonable(asdict(corpus))


def _to_jsonable(obj):
    """Recursively convert tuples to lists; other containers traversed."""
    if isinstance(obj, tuple):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, list):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    return obj


def corpus_from_dict(d: dict) -> CorpusV1:
    """Inverse of corpus_to_dict, with version validation."""
    validate_corpus_dict(d)
    views_raw = d.get("views", [])
    views = tuple(_view_from_dict(v) for v in views_raw)
    return CorpusV1(schema_version=d["schema_version"], views=views)


def _view_from_dict(d: dict) -> ViewV1:
    rep_d = d.get("report", {}) or {}
    return ViewV1(
        view_name=d.get("view_name", ""),
        report=ReportV1(
            technical_description=rep_d.get("technical_description", ""),
            business_description=rep_d.get("business_description", ""),
            primary_purpose=rep_d.get("primary_purpose", ""),
            key_metrics=tuple(rep_d.get("key_metrics", []) or []),
            column_count=int(rep_d.get("column_count", 0) or 0),
            use_llm=bool(rep_d.get("use_llm", False)),
        ),
        view_level_notes=tuple(d.get("view_level_notes", []) or []),
        scopes=tuple(_scope_from_dict(s) for s in d.get("scopes", []) or []),
        view_outputs=tuple(d.get("view_outputs", []) or []),
        inventory=tuple(
            InventoryRefV1(
                table=r.get("table", ""),
                column=r.get("column", ""),
                database=r.get("database", ""),
                schema=r.get("schema", ""),
                reference_type=r.get("reference_type", "column"),
                confidence=r.get("confidence", "medium"),
            )
            for r in d.get("inventory", []) or []
        ),
        use_llm=bool(d.get("use_llm", False)),
    )


def _scope_from_dict(d: dict) -> ScopeV1:
    return ScopeV1(
        id=d.get("id", ""),
        kind=d.get("kind", ""),
        filters=tuple(_filter_from_dict(f) for f in d.get("filters", []) or []),
        columns=tuple(_column_from_dict(c) for c in d.get("columns", []) or []),
        reads_from_scopes=tuple(d.get("reads_from_scopes", []) or []),
        reads_from_tables=tuple(d.get("reads_from_tables", []) or []),
        joins=tuple(_join_from_dict(j) for j in d.get("joins", []) or []),
    )


def _join_from_dict(d: dict) -> JoinV1:
    return JoinV1(
        right_table=d.get("right_table", ""),
        join_type=d.get("join_type", "JOIN"),
        on_expression=d.get("on_expression", ""),
        right_alias=d.get("right_alias", ""),
    )


def _filter_from_dict(d: dict) -> FilterV1:
    return FilterV1(
        expression=d.get("expression", ""),
        english=d.get("english", ""),
        kind=d.get("kind", "where"),
        subquery_scope_ids=tuple(d.get("subquery_scope_ids", []) or []),
    )


def _column_from_dict(d: dict) -> ColumnV1:
    term_d = d.get("term", {}) or {}
    return ColumnV1(
        column_name=d.get("column_name", ""),
        column_type=d.get("column_type", "unknown"),
        technical_description=d.get("technical_description", ""),
        business_description=d.get("business_description", ""),
        business_domain=d.get("business_domain", ""),
        base_columns=tuple(d.get("base_columns", []) or []),
        base_tables=tuple(d.get("base_tables", []) or []),
        author_notes=tuple(d.get("author_notes", []) or []),
        term=TermV1(
            name_tokens=tuple(term_d.get("name_tokens", []) or []),
            is_passthrough=bool(term_d.get("is_passthrough", False)),
            name_is_structural=bool(term_d.get("name_is_structural", False)),
            has_filters=bool(term_d.get("has_filters", False)),
        ),
        fingerprint=d.get("fingerprint"),
    )


def validate_corpus_dict(d: dict) -> None:
    """Raise ValueError if `d` doesn't conform to schema v3.

    Required: schema_version present and equal to SCHEMA_VERSION.
    Field-level validation is deliberately permissive -- missing optional
    fields default per the dataclasses; unknown fields are ignored
    (additive evolution). Bump SCHEMA_VERSION for breaking changes.
    """
    if not isinstance(d, dict):
        raise ValueError("corpus must be a dict")
    if "schema_version" not in d:
        raise ValueError("corpus dict missing required field: schema_version")
    if d["schema_version"] != SCHEMA_VERSION:
        raise ValueError(
            f"unsupported schema_version {d['schema_version']!r}; "
            f"this build supports only {SCHEMA_VERSION}"
        )
    if not isinstance(d.get("views", []), (list, tuple)):
        raise ValueError("'views' must be a list or tuple")


# ============================================================
# Streaming JSONL helpers
# ============================================================

def corpus_to_jsonl_lines(corpus: CorpusV1) -> Iterator[str]:
    """Yield a header line + one JSON object per view.

    Format:
        line 1:    {"schema_version": 3, "n_views": 130}
        line 2..N: one ViewV1 dict per line

    Consumers stream with bounded memory:
        with open('corpus.jsonl') as f:
            header = json.loads(next(f))
            assert header['schema_version'] == 3
            for line in f:
                view = json.loads(line)
                for scope in view['scopes']:
                    ...
    """
    yield json.dumps({
        "schema_version": corpus.schema_version,
        "n_views": len(corpus.views),
    })
    for v in corpus.views:
        yield json.dumps(_to_jsonable(asdict(v)))


def corpus_from_jsonl_lines(lines: Iterator[str]) -> CorpusV1:
    """Inverse of corpus_to_jsonl_lines."""
    header = json.loads(next(lines))
    if header.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"unsupported schema_version {header.get('schema_version')!r} "
            f"in JSONL header; this build supports only {SCHEMA_VERSION}"
        )
    views = tuple(_view_from_dict(json.loads(line)) for line in lines)
    return CorpusV1(schema_version=SCHEMA_VERSION, views=views)
