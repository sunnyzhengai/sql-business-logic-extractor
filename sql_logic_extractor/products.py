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
    technical_description: str = ""
    business_description: str = ""
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
    """Ungated core for Tool 1. Tool 2's core calls this directly.

    Walks every Column and Table node in the SQL AST and emits one row
    per distinct (database, schema, table, column) tuple. Filters out
    CTE references (both CTE names AND aliases pointing at CTEs) and the
    view's own name; resolves real-table aliases back to the underlying
    table via a qualifier map. SSMS USE/GO/SET boilerplate is stripped
    via the resolver's preprocess_ssms before parsing.
    """
    clean_sql, _ = preprocess_ssms(sql)
    if not clean_sql.strip():
        clean_sql = sql.strip()
    parsed = parse_one(clean_sql, dialect=dialect)

    # CTE names (e.g. "ActiveReferrals") -- always lowercased for comparison.
    cte_names = {(cte.alias_or_name or "").lower() for cte in parsed.find_all(exp.CTE)}

    self_name: Optional[str] = None
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        self_name = parsed.this.name.lower() if parsed.this.name else None

    # Two filter sets we walk Tables once to populate:
    #   - real-table qualifier map: alias_or_name (lowercased) -> (db, schema, table)
    #   - CTE alias set: lowercased aliases that point at a CTE (e.g. AR -> ActiveReferrals)
    qualifier: dict[str, tuple] = {}
    cte_aliases: set[str] = set()
    for t in parsed.find_all(exp.Table):
        name_lower = t.name.lower()
        if name_lower == self_name:
            continue
        if name_lower in cte_names:
            # This Table node is a reference TO a CTE; capture its alias so
            # we can filter outer-scope columns that read through the alias.
            alias = t.alias_or_name
            if alias:
                cte_aliases.add(alias.lower())
            cte_aliases.add(name_lower)
            continue
        full = _qualify_table(t)
        alias = t.alias_or_name
        if alias:
            qualifier[alias.lower()] = full
        qualifier[name_lower] = full

    seen: set[tuple] = set()
    columns: list[ColumnIdentifier] = []
    for col in parsed.find_all(exp.Column):
        col_name = col.name
        if not col_name:
            continue
        tbl = (col.table or "").lower()
        # Skip references through a CTE -- the columns the CTE body reads
        # from real tables are captured separately when we walk those.
        if tbl in cte_aliases:
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
    """Pattern-library translator -- deterministic, no LLM. Walks each
    resolved column's SQL through the recursive pattern library, with
    schema lookups for column descriptions."""
    from .business_logic import translate_column_engineered
    from .patterns import Context

    ctx = Context(schema=schema or {})
    out: list[dict] = []
    for col in lineage.resolved_columns:
        out.append(translate_column_engineered(col, ctx))
    return out


def _translate_with_llm(lineage: TechnicalLineage, schema: dict, llm_client) -> list[dict]:
    """LLM-backed translator. Lazy-imports the client lib INSIDE the call
    so a no-LLM install doesn't have google-genai on disk at all -- the
    structural guarantee for healthcare-safe builds."""
    from .business_logic import translate_column_llm, make_llm_client

    if llm_client is None:
        llm_client = make_llm_client()

    out: list[dict] = []
    for col in lineage.resolved_columns:
        out.append(translate_column_llm(col, schema or {}, llm_client))
    return out


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


def _summarize_engineered(bl: BusinessLogic, schema: dict) -> tuple[str, str, str, list[str]]:
    """Deterministic report summary built from structured signals -- no LLM.
    Returns (technical_description, business_description, primary_purpose, key_metrics)."""
    from .business_logic import summarize_engineered
    result = summarize_engineered(bl, schema or {})
    return (result["technical_description"], result["business_description"],
            result["primary_purpose"], result["key_metrics"])


def _summarize_with_llm(bl: BusinessLogic, llm_client) -> tuple[str, str, str, list[str]]:
    """LLM-backed report summary. Lazy-imports google.genai INSIDE the call
    so a no-LLM install doesn't pull the lib into sys.modules."""
    from .business_logic import summarize_llm, make_llm_client
    if llm_client is None:
        llm_client = make_llm_client()
    result = summarize_llm(bl, llm_client)
    return (result["technical_description"], result["business_description"],
            result["primary_purpose"], result["key_metrics"])


def _generate_report_description_core(sql: str, schema: dict, *,
                                       use_llm: bool = False,
                                       llm_client=None,
                                       dialect: str = "tsql") -> ReportDescription:
    """Ungated core for Tool 4."""
    bl = _extract_business_logic_core(sql, schema, use_llm=use_llm,
                                        llm_client=llm_client, dialect=dialect)
    if use_llm:
        technical, business, purpose, metrics = _summarize_with_llm(bl, llm_client)
    else:
        technical, business, purpose, metrics = _summarize_engineered(bl, schema or {})
    return ReportDescription(business_logic=bl,
                              technical_description=technical,
                              business_description=business,
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
