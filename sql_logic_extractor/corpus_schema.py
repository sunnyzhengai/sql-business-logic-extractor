"""Canonical corpus schema -- the single source of truth for everything
the pipeline produces about a view.

Today's pipeline emits 5+ overlapping CSVs (`view_name` and
`column_name` repeated in every row of every file; `base_tables`,
`filters`, `author_notes` duplicated across Tools 2/3 and term
extraction). That redundancy is fine for ad-hoc Excel review but bad
for programmatic consumption and bad for scale.

The fix: ONE canonical structured artifact per corpus, with audience-
specific CSVs DERIVED from it. This module defines that artifact.

Design choices, all reversible behind the `schema_version` constant:

  - Compact at write, expand at read. Filters and base-table lists that
    every column inherits from its parent view are stored ONCE on the
    view (`view_level.filters`, `view_level.tables_referenced`) and
    referenced from columns by index or boolean flag. The
    `expand_view()` helper re-inflates a view into the fully-redundant
    form on demand, so humans never have to deal with indices.

  - Streaming-friendly. `Corpus.to_jsonl_lines()` emits one JSON
    object per view per line. Consumers iterate with constant memory
    no matter how big the corpus is.

  - Versioned. `SCHEMA_VERSION = 1`. Every artifact embeds it. Mismatch
    on read fails fast. Additive changes (new optional fields) keep
    version 1; restructuring bumps to 2 with a migration helper.

  - Pydantic-free. Dataclass tree + explicit to_dict/from_dict so the
    project picks up no new heavy dependency. Validation is explicit:
    `validate_corpus_dict()` raises on missing required fields or
    wrong version.

THIS MODULE is Phase A only -- the data model, validation, and helpers.
It deliberately has NO knowledge of how a corpus is BUILT (Phase B's
extractor) or how CSVs are derived from it (Phase C's rematerializer).
That separation lets us ship and review the model on paper before any
runtime change.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Iterator


SCHEMA_VERSION: int = 1


# ============================================================
# View-level shared content (one copy per view)
# ============================================================

@dataclass(frozen=True)
class ReportV1:
    """Tool 4 output -- one per view, never per column."""
    technical_description: str = ""
    business_description: str = ""
    primary_purpose: str = ""
    key_metrics: tuple[str, ...] = ()
    column_count: int = 0
    use_llm: bool = False


@dataclass(frozen=True)
class ViewLevelV1:
    """All content shared across every column of a view -- stored ONCE.

    `filters` are the WHERE / JOIN-ON predicates that constrain the
    row population for the whole view. `tables_referenced` is the
    deduped, ordered list of base tables the view touches; columns
    reference into it by index. `view_level_notes` are author comments
    (top-of-file, between-CTE) attached to the view itself rather
    than any specific column.
    """
    filters: tuple[str, ...] = ()
    tables_referenced: tuple[str, ...] = ()
    view_level_notes: tuple[str, ...] = ()
    report: ReportV1 = field(default_factory=ReportV1)


# ============================================================
# Per-column governance fields
# ============================================================

@dataclass(frozen=True)
class TermV1:
    """The governance comparison unit for one column. Empty
    `name_tokens` + `name_is_structural=True` means "this column
    didn't qualify as a Term and the consumer should skip it for
    bucketing purposes" (kept in the corpus for completeness)."""
    name_tokens: tuple[str, ...] = ()
    is_passthrough: bool = False
    name_is_structural: bool = False
    has_filters: bool = False


@dataclass(frozen=True)
class ColumnV1:
    """One output column.

    Compact representation:
      - `base_tables_idx` indexes into the parent view's
        `view_level.tables_referenced`
      - `filters_inherited=True` means all view-level filters apply;
        `filters_extra` lists ONLY column-specific additions
      - The `expand_view()` helper denormalizes both into the flat
        forms a downstream consumer typically wants.
    """
    column_name: str = ""
    column_type: str = "unknown"
    resolved_expression: str = ""

    # Lineage
    base_tables_idx: tuple[int, ...] = ()
    base_columns: tuple[str, ...] = ()

    # Filter context
    filters_inherited: bool = True
    filters_extra: tuple[str, ...] = ()

    # English (Tool 3)
    english_definition: str = ""
    english_definition_with_filters: str = ""
    business_domain: str = ""

    # Comment-as-data attachments
    author_notes: tuple[str, ...] = ()

    # Governance
    term: TermV1 = field(default_factory=TermV1)

    # Cross-view similarity
    fingerprint: str | None = None


@dataclass(frozen=True)
class InventoryRefV1:
    """One entry from Tool 1's per-view manifest. Stored separately
    from the per-column data because a single view may reference a
    table/column via WHERE, JOIN, or EXISTS subquery WITHOUT exposing
    it as an output column."""
    table: str = ""
    column: str = ""
    database: str = ""
    schema: str = ""
    reference_type: str = "column"   # column | table
    confidence: str = "medium"        # high | medium | low


# ============================================================
# View + Corpus
# ============================================================

@dataclass(frozen=True)
class ViewV1:
    """Everything we know about one view.

    Two top-level lists:
      - `columns` -- per-output-column data (Tools 2 + 3 + governance)
      - `inventory` -- per-(table, column) reference (Tool 1)
    """
    view_name: str = ""
    view_level: ViewLevelV1 = field(default_factory=ViewLevelV1)
    columns: tuple[ColumnV1, ...] = ()
    inventory: tuple[InventoryRefV1, ...] = ()
    use_llm: bool = False


@dataclass(frozen=True)
class CorpusV1:
    """The whole artifact.

    One CorpusV1 corresponds to one run of the extractor over one
    folder of views. `views` is ordered the same as the input
    folder for stable diffs across runs.
    """
    schema_version: int = SCHEMA_VERSION
    views: tuple[ViewV1, ...] = ()


# ============================================================
# Serialization
# ============================================================

def corpus_to_dict(corpus: CorpusV1) -> dict:
    """Plain dict for json.dump.

    Note: dataclasses.asdict() preserves tuple-typed fields as tuples
    (per Python docs); we walk and convert to lists explicitly so the
    output is unambiguously JSON-friendly and downstream consumers
    don't have to special-case tuple vs list isinstance checks.
    """
    return _to_jsonable(asdict(corpus))


def _to_jsonable(obj):
    """Convert any dataclass-asdict output to a strictly-JSON-friendly
    structure: tuples -> lists, recursively into containers."""
    if isinstance(obj, tuple):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, list):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    return obj


def corpus_from_dict(d: dict) -> CorpusV1:
    """Inverse of corpus_to_dict, with version validation. Raises
    ValueError on version mismatch or structural problems."""
    validate_corpus_dict(d)
    views_raw = d.get("views", [])
    views = tuple(_view_from_dict(v) for v in views_raw)
    return CorpusV1(schema_version=d["schema_version"], views=views)


def _view_from_dict(d: dict) -> ViewV1:
    vl = d.get("view_level", {}) or {}
    rep = vl.get("report", {}) or {}
    view_level = ViewLevelV1(
        filters=tuple(vl.get("filters", []) or []),
        tables_referenced=tuple(vl.get("tables_referenced", []) or []),
        view_level_notes=tuple(vl.get("view_level_notes", []) or []),
        report=ReportV1(
            technical_description=rep.get("technical_description", ""),
            business_description=rep.get("business_description", ""),
            primary_purpose=rep.get("primary_purpose", ""),
            key_metrics=tuple(rep.get("key_metrics", []) or []),
            column_count=int(rep.get("column_count", 0) or 0),
            use_llm=bool(rep.get("use_llm", False)),
        ),
    )
    columns = tuple(_column_from_dict(c) for c in d.get("columns", []) or [])
    inventory = tuple(
        InventoryRefV1(
            table=r.get("table", ""),
            column=r.get("column", ""),
            database=r.get("database", ""),
            schema=r.get("schema", ""),
            reference_type=r.get("reference_type", "column"),
            confidence=r.get("confidence", "medium"),
        )
        for r in d.get("inventory", []) or []
    )
    return ViewV1(
        view_name=d.get("view_name", ""),
        view_level=view_level,
        columns=columns,
        inventory=inventory,
        use_llm=bool(d.get("use_llm", False)),
    )


def _column_from_dict(d: dict) -> ColumnV1:
    term_d = d.get("term", {}) or {}
    return ColumnV1(
        column_name=d.get("column_name", ""),
        column_type=d.get("column_type", "unknown"),
        resolved_expression=d.get("resolved_expression", ""),
        base_tables_idx=tuple(d.get("base_tables_idx", []) or []),
        base_columns=tuple(d.get("base_columns", []) or []),
        filters_inherited=bool(d.get("filters_inherited", True)),
        filters_extra=tuple(d.get("filters_extra", []) or []),
        english_definition=d.get("english_definition", ""),
        english_definition_with_filters=d.get("english_definition_with_filters", ""),
        business_domain=d.get("business_domain", ""),
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
    """Raise ValueError if `d` doesn't conform to schema v1.

    Checks: schema_version present and equal to 1; views is a list.
    Field-level validation is intentionally permissive -- missing
    optional fields default per the dataclasses, unknown fields are
    ignored (additive evolution). Use bump SCHEMA_VERSION for
    breaking changes.
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
    # Accept list or tuple -- asdict() preserves tuples for tuple-typed
    # dataclass fields, and JSON loads always returns lists, so both
    # are valid in-the-wild representations.
    if not isinstance(d.get("views", []), (list, tuple)):
        raise ValueError("'views' must be a list or tuple")


# ============================================================
# Streaming JSONL helpers
# ============================================================

def corpus_to_jsonl_lines(corpus: CorpusV1) -> Iterator[str]:
    """Yield one JSON line per view, plus a header line with metadata.

    Format:
        line 1: {"schema_version": 1, "n_views": 130}
        line 2..N+1: one ViewV1 dict per line

    Consumers stream with bounded memory:
        with open('corpus.jsonl') as f:
            header = json.loads(next(f))
            assert header['schema_version'] == 1
            for line in f:
                view = json.loads(line)   # ONE view in RAM
                ...
    """
    yield json.dumps({
        "schema_version": corpus.schema_version,
        "n_views": len(corpus.views),
    })
    for v in corpus.views:
        yield json.dumps(_to_jsonable(asdict(v)))


def corpus_from_jsonl_lines(lines: Iterator[str]) -> CorpusV1:
    """Inverse of corpus_to_jsonl_lines. Header line determines version."""
    header = json.loads(next(lines))
    if header.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"unsupported schema_version {header.get('schema_version')!r} "
            f"in JSONL header; this build supports only {SCHEMA_VERSION}"
        )
    views = tuple(_view_from_dict(json.loads(line)) for line in lines)
    return CorpusV1(schema_version=SCHEMA_VERSION, views=views)


# ============================================================
# expand_view -- denormalize for human / convenience consumption
# ============================================================

def expand_view(view: ViewV1) -> dict:
    """Return a fully-denormalized dict for a single view.

    Re-inflates the compact form: column.base_tables_idx -> resolved
    table names; column.filters_inherited + filters_extra -> the
    flat full list of filters that apply to that column.

    This is what humans (and consumers that don't care about size)
    work with. Programmatic consumers at scale operate on the compact
    form directly.
    """
    tables = view.view_level.tables_referenced
    view_filters = view.view_level.filters

    expanded_columns: list[dict] = []
    for col in view.columns:
        col_dict = asdict(col)
        # Resolve table-name indices to actual names.
        col_dict["base_tables"] = [
            tables[i] for i in col.base_tables_idx
            if 0 <= i < len(tables)
        ]
        # Re-inflate the full filter list.
        full_filters: list[str] = []
        if col.filters_inherited:
            full_filters.extend(view_filters)
        full_filters.extend(col.filters_extra)
        col_dict["filters"] = full_filters
        # Drop the compact-only fields from the expanded view.
        col_dict.pop("base_tables_idx", None)
        col_dict.pop("filters_inherited", None)
        col_dict.pop("filters_extra", None)
        expanded_columns.append(col_dict)

    return _to_jsonable({
        "view_name": view.view_name,
        "view_level": asdict(view.view_level),
        "columns": expanded_columns,
        "inventory": [asdict(r) for r in view.inventory],
        "use_llm": view.use_llm,
    })
