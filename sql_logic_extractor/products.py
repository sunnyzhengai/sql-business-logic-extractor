"""The 4 commercial product functions, layered.

Each tool is one public function. Each function calls the *core* of the
previous tool, never the gated public version, so internal composition
works with any combination of license features unlocked.

Tool dependency chain:
    extract_columns            (Tool 1, no LLM possible)
        |
    extract_technical_lineage  (Tool 2, no LLM possible)
        |
    extract_business_logic     (Tool 3, optional LLM)
        |
    generate_report_description (Tool 4, optional LLM)

All 4 public functions go through `require_feature(...)`. The matching
`_*_core` private function does the real work, ungated, so Tool N can
call Tool N-1's core without tripping a gate the customer hasn't paid
for.

LLM client libraries (google-genai, openai, anthropic, etc.) are
imported INSIDE the LLM code branches only. A no-LLM customer's wheel
doesn't include those libs -- they're behind the `[ai]` extra in
pyproject.toml. Healthcare-safe builds are structurally incapable of
calling out to an LLM.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from sqlglot import exp, parse_one

from .license import require_feature
from .resolve import resolve_query, resolved_to_dict, preprocess_ssms


# ---------------------------------------------------------------------------
# Public output types -- stable contracts for downstream consumers.
# Each tool's output wraps the previous tool's output, never duplicates it.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnIdentifier:
    """One (db, schema, table, column) reference found in a SQL view."""
    database: Optional[str]
    schema: Optional[str]
    table: str
    column: str

    def qualified(self) -> str:
        parts = [p for p in (self.database, self.schema, self.table, self.column) if p]
        return ".".join(parts)


@dataclass
class ColumnInventory:
    """Output of Tool 1 -- the column extractor.

    A flat list of every distinct (database, schema, table, column)
    reference appearing in the input SQL. CTEs are filtered out.
    """
    sql: str
    dialect: str
    columns: list[ColumnIdentifier] = field(default_factory=list)


@dataclass
class TechnicalLineage:
    """Output of Tool 2 -- technical logic extractor.

    Embeds the Tool 1 inventory plus per-output-column lineage from the
    resolver: base columns, base tables, filters, transformation chain.
    """
    inventory: ColumnInventory                              # Tool 1's output reused
    resolved_columns: list[dict] = field(default_factory=list)
    query_filters: list[str] = field(default_factory=list)


@dataclass
class BusinessLogic:
    """Output of Tool 3 -- business logic extractor.

    Embeds the Tool 2 lineage plus an English-language definition for
    each transformed output column. `use_llm` is recorded so downstream
    consumers can tell which engine produced the translations.
    """
    lineage: TechnicalLineage                               # Tool 2's output reused
    column_translations: list[dict] = field(default_factory=list)
    use_llm: bool = False


@dataclass
class ReportDescription:
    """Output of Tool 4 -- report description generator.

    Embeds the Tool 3 business logic plus a natural-language paragraph
    describing what the report produces, its primary purpose, and the
    key computed metrics.
    """
    business_logic: BusinessLogic                           # Tool 3's output reused
    query_summary: str = ""
    primary_purpose: str = ""
    key_metrics: list[str] = field(default_factory=list)
    use_llm: bool = False


# ---------------------------------------------------------------------------
# Tool 1 -- Column extractor (no LLM possible by design)
# ---------------------------------------------------------------------------


def _qualify_table(t: exp.Table) -> tuple[Optional[str], Optional[str], str]:
    """sqlglot's `catalog` is the SQL Server database; `db` is the schema."""
    return (
        t.args["catalog"].name if t.args.get("catalog") else None,
        t.args["db"].name if t.args.get("db") else None,
        t.name,
    )


def _extract_columns_core(sql: str, dialect: str = "tsql") -> ColumnInventory:
    """Ungated core for Tool 1. Tool 2's core calls this directly."""
    clean_sql, _ = preprocess_ssms(sql)
    if not clean_sql.strip():
        clean_sql = sql.strip()
    parsed = parse_one(clean_sql, dialect=dialect)

    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}
    self_name: Optional[str] = None
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        self_name = parsed.this.name

    # Build alias -> (db, schema, table) map so column refs by alias resolve.
    qualifier: dict[str, tuple] = {}
    for t in parsed.find_all(exp.Table):
        if t.name in cte_names or t.name == self_name:
            continue
        full = _qualify_table(t)
        alias = t.alias_or_name
        if alias:
            qualifier[alias.lower()] = full
        qualifier[t.name.lower()] = full

    seen: set[tuple] = set()
    columns: list[ColumnIdentifier] = []
    for col in parsed.find_all(exp.Column):
        col_name = col.name
        if not col_name:
            continue
        tbl = (col.table or "").lower()
        if tbl in cte_names:
            continue
        db, schema, table = qualifier.get(tbl, (None, None, tbl or "")) if tbl else (None, None, "")
        key = (db, schema, table, col_name)
        if key in seen:
            continue
        seen.add(key)
        columns.append(ColumnIdentifier(database=db, schema=schema,
                                          table=table, column=col_name))

    return ColumnInventory(sql=sql, dialect=dialect, columns=columns)


def extract_columns(sql: str, dialect: str = "tsql") -> ColumnInventory:
    """Tool 1 -- enumerate every (database, schema, table, column) the SQL
    references. Always deterministic; no LLM is involved."""
    require_feature("columns")
    return _extract_columns_core(sql, dialect)


# ---------------------------------------------------------------------------
# Tool 2 -- Technical logic extractor (no LLM possible by design)
# ---------------------------------------------------------------------------


def _extract_technical_lineage_core(sql: str, dialect: str = "tsql") -> TechnicalLineage:
    """Ungated core for Tool 2. Tool 3's core calls this directly."""
    inventory = _extract_columns_core(sql, dialect)         # reuse Tool 1
    resolved = resolve_query(sql, dialect=dialect)
    rd = resolved_to_dict(resolved)

    # Collect query-level filter expressions, deduped by text.
    seen_filters: set[str] = set()
    query_filters: list[str] = []
    for col in rd.get("columns", []):
        for f in col.get("filters", []) or []:
            expr = f.get("expression", "").strip() if isinstance(f, dict) else ""
            if expr and expr not in seen_filters:
                seen_filters.add(expr)
                query_filters.append(expr)

    return TechnicalLineage(
        inventory=inventory,
        resolved_columns=rd.get("columns", []),
        query_filters=query_filters,
    )


def extract_technical_lineage(sql: str, dialect: str = "tsql") -> TechnicalLineage:
    """Tool 2 -- per-output-column lineage with WHERE/JOIN/EXISTS filter
    propagation. Always deterministic; no LLM is involved."""
    require_feature("technical_logic")
    return _extract_technical_lineage_core(sql, dialect)


# ---------------------------------------------------------------------------
# Tool 3 -- Business logic extractor (LLM is opt-in, default OFF)
# ---------------------------------------------------------------------------


def _translate_engineered(lineage: TechnicalLineage, schema: dict) -> list[dict]:
    """Pattern-library translator -- pure deterministic logic, no LLM.

    TODO (May Week 3): wire to sql_logic_extractor.translate.translate_query
    once the schema-aware translator is settled. For now this returns one
    row per output column with a placeholder definition built from the
    column name -- enough for scaffolding to be runnable."""
    out: list[dict] = []
    for col in lineage.resolved_columns:
        out.append({
            "column_name": col.get("name", ""),
            "column_type": col.get("type", "unknown"),
            "english_definition": f"[engineered] {col.get('name', '')}",
            "base_columns": col.get("base_columns", []) or [],
            "base_tables": col.get("base_tables", []) or [],
        })
    return out


def _translate_with_llm(lineage: TechnicalLineage, schema: dict, llm_client) -> list[dict]:
    """LLM-backed translator. Lazy-imports the client lib so a no-LLM
    install doesn't have it on disk.

    TODO (May Week 3): adapt cli/llm_translate.py:translate_column logic
    here. For now raises NotImplementedError so the gate-and-shape work
    is verifiable without committing to a model choice yet."""
    from . import _llm_clients  # noqa: F401  (forward-declares lazy import)
    raise NotImplementedError(
        "LLM-backed business logic translation is scheduled for May Week 3. "
        "See cli/llm_translate.py for the existing prototype to port."
    )


def _extract_business_logic_core(sql: str, schema: dict, *,
                                   use_llm: bool = False,
                                   llm_client=None,
                                   dialect: str = "tsql") -> BusinessLogic:
    """Ungated core for Tool 3. Tool 4's core calls this directly."""
    lineage = _extract_technical_lineage_core(sql, dialect)
    if use_llm:
        translations = _translate_with_llm(lineage, schema, llm_client)
    else:
        translations = _translate_engineered(lineage, schema)
    return BusinessLogic(lineage=lineage, column_translations=translations,
                          use_llm=use_llm)


def extract_business_logic(sql: str, schema: dict, *,
                            use_llm: bool = False,
                            llm_client=None,
                            dialect: str = "tsql") -> BusinessLogic:
    """Tool 3 -- English business definition for each transformed column.
    `use_llm=False` (default) uses the pattern library; `use_llm=True`
    uses an LLM and requires the business_logic_llm feature."""
    require_feature("business_logic")
    if use_llm:
        require_feature("business_logic_llm")
    return _extract_business_logic_core(sql, schema, use_llm=use_llm,
                                          llm_client=llm_client, dialect=dialect)


# ---------------------------------------------------------------------------
# Tool 4 -- Report description generator (LLM is opt-in, default OFF)
# ---------------------------------------------------------------------------


def _summarize_engineered(bl: BusinessLogic) -> tuple[str, str, list[str]]:
    """Deterministic report summary built from the structured signals.

    TODO (May Week 4): expand to a richer template using business_domains,
    granularity (window/aggregate/case), and lead filter narratives. For
    now returns a minimal but honest summary so the scaffold is runnable."""
    n = len(bl.column_translations)
    tables = sorted({t for col in bl.lineage.resolved_columns
                       for t in (col.get("base_tables", []) or [])})
    metrics = [c["column_name"] for c in bl.column_translations
                if c.get("column_type") in ("calculated", "aggregate", "case", "window")]
    summary = (
        f"Report producing {n} output column(s) sourced from "
        f"{len(tables)} base table(s)."
    )
    if bl.lineage.query_filters:
        summary += f" Constrained by {len(bl.lineage.query_filters)} filter clause(s)."
    purpose = "Data extraction" if not metrics else "Aggregated reporting"
    return summary, purpose, metrics[:5]


def _summarize_with_llm(bl: BusinessLogic, llm_client) -> tuple[str, str, list[str]]:
    """LLM-backed summary. Lazy-imports the client lib.

    TODO (May Week 4): port cli/llm_translate.py:summarize_query here."""
    raise NotImplementedError(
        "LLM-backed report summary is scheduled for May Week 4. "
        "See cli/llm_translate.py:summarize_query for the existing prototype."
    )


def _generate_report_description_core(sql: str, schema: dict, *,
                                       use_llm: bool = False,
                                       llm_client=None,
                                       dialect: str = "tsql") -> ReportDescription:
    """Ungated core for Tool 4."""
    bl = _extract_business_logic_core(sql, schema, use_llm=use_llm,
                                        llm_client=llm_client, dialect=dialect)
    if use_llm:
        summary, purpose, metrics = _summarize_with_llm(bl, llm_client)
    else:
        summary, purpose, metrics = _summarize_engineered(bl)
    return ReportDescription(business_logic=bl, query_summary=summary,
                              primary_purpose=purpose, key_metrics=metrics,
                              use_llm=use_llm)


def generate_report_description(sql: str, schema: dict, *,
                                 use_llm: bool = False,
                                 llm_client=None,
                                 dialect: str = "tsql") -> ReportDescription:
    """Tool 4 -- natural-language description of what the SQL report does.
    `use_llm=False` (default) uses a deterministic template; `use_llm=True`
    uses an LLM and requires the report_description_llm feature."""
    require_feature("report_description")
    if use_llm:
        require_feature("report_description_llm")
    return _generate_report_description_core(sql, schema, use_llm=use_llm,
                                               llm_client=llm_client, dialect=dialect)
