#!/usr/bin/env python3
"""Tool 3 -- Business logic translation (engine module).

Two modes, both producing one English-language definition per
transformed output column in a SQL view:

1. Engineered (default, healthcare-safe, no LLM): walks the resolved
   SQL AST against the recursive pattern library in
   ``sql_logic_extractor.patterns``. Schema-aware lookups attach
   business descriptions from a data dictionary YAML/JSON when
   available; unknown nodes/columns propagate as governance signals
   (never opaque fallbacks).

2. LLM (opt-in, requires `business_logic_llm` license feature): per-
   column LLM call with full context (resolved expression, base
   columns, filters, schema descriptions). Lazy-imports the client
   library so a no-LLM customer's wheel doesn't include it.

The engineered code path is ported from the archived prototype at
``docs/archive/cli/offline_translate.py``; the LLM path is ported from
``docs/archive/cli/llm_translate.py``. Both prototypes were validated
on the bi_complex test corpus before being lifted into the engine.
"""

from __future__ import annotations

import json
import re
from typing import Optional

from sqlglot import exp, parse_one

from .patterns import Context, Translation, translate
from .patterns.structural import _strip_correlation_keys


# ---------------------------------------------------------------------------
# Schema loading (JSON or YAML)
# ---------------------------------------------------------------------------

def load_schema(path: str) -> dict:
    """Load a schema/data-dictionary file. Auto-detects JSON vs YAML by
    extension. The pattern library builds its own ``__table_index__``
    cache on first column lookup, so no pre-processing is needed.

    JSON is recommended for production -- pyyaml isn't required at
    runtime, and SQL Server can emit JSON natively.
    """
    if path.lower().endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    # Lazy import -- pyyaml is only needed when loading .yaml schemas.
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Engineered (no-LLM) translation
# ---------------------------------------------------------------------------

def _unwrap_select(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Select):
        node = node.selects[0]
    if isinstance(node, exp.Alias):
        node = node.this
    return node


def _translate_expression(expression: str, ctx: Context) -> Translation:
    """Parse a resolved SQL expression and walk it with the pattern registry.
    Falls back to a structural unknown record if parsing fails (the recursive-
    translation principle: never emit an opaque placeholder without also
    registering the event)."""
    if not expression or not expression.strip():
        return Translation(english="(no expression)", category="unknown",
                           unknown_nodes=["empty_expression"])
    try:
        node = parse_one(expression, dialect="tsql")
    except Exception as e:
        return Translation(
            english=f"(unparseable: {expression[:60]})",
            category="unknown",
            unknown_nodes=[f"parse_error:{type(e).__name__}"],
        )
    node = _unwrap_select(node)
    return translate(node, ctx)


def _filter_text(f) -> str:
    if isinstance(f, dict):
        return (f.get("expression") or "").strip()
    return (f or "").strip()


def _walk_fragment(sql_fragment: str, ctx: Context) -> str:
    """Parse a bare SQL fragment (not wrapped in SELECT) and translate.
    Correlation keys (`col = col`) are stripped before translation so that
    join-scope filters don't leak relational plumbing into the prose."""
    if not sql_fragment:
        return ""
    try:
        node = parse_one(sql_fragment, dialect="tsql")
    except Exception:
        return sql_fragment
    node = _unwrap_select(node)
    cleaned = _strip_correlation_keys(node)
    if cleaned is None:
        return ""
    return translate(cleaned, ctx).english


def _translate_filters(filters: list, ctx: Context) -> str:
    """Translate L3 filter predicates by walking each with the registry.
    Distinguishes business filters from technical plumbing (IS NOT NULL):
    technical filters get prefixed `where ... exists`; business filters
    get natural-language rendering."""
    if not filters:
        return ""
    parts = []
    for f in filters:
        f_text = _filter_text(f)
        if not f_text:
            continue
        if re.search(r"\bIS\s+NOT\s+NULL\b", f_text, re.IGNORECASE):
            col_part = re.split(r"\s+IS\s+NOT\s+NULL", f_text, flags=re.IGNORECASE)[0].strip()
            col_part = col_part.lstrip("NOT ").strip()
            col_text = _walk_fragment(col_part, ctx)
            parts.append(f"where {col_text} exists")
            continue
        walked = _walk_fragment(f_text, ctx)
        parts.append(walked)
    return "; ".join(parts)


def classify_business_domain(col_name: str, base_tables: list, expression: str) -> str:
    """Best-effort domain bucket from name/table heuristics. Useful as a
    grouping signal in governance reports, not authoritative."""
    name_lower = (col_name or "").lower()
    tables_lower = [t.lower() for t in (base_tables or [])]
    expr_lower = (expression or "").lower()

    if any(k in name_lower for k in ("amt", "amount", "charge", "cost", "price", "revenue", "balance")):
        return "Financial"
    if any(k in name_lower for k in ("score", "metric", "kpi", "rate", "ratio")):
        return "Quality Metrics"
    if any("hsp" in t or "encounter" in t or "admit" in t for t in tables_lower):
        return "Hospital Metrics"
    if any("patient" in t or "demographic" in t for t in tables_lower):
        return "Patient Demographics"
    if "referral" in name_lower or any("referral" in t for t in tables_lower):
        return "Referrals"
    return "General"


def translate_column_engineered(resolved_col: dict, ctx: Context) -> dict:
    """Translate one resolved column (from the L3 resolver) into a
    business-logic definition using the pattern library. Output shape
    matches what Tool 3's batch CSV consumes."""
    name = resolved_col.get("name", "unknown")
    col_type = resolved_col.get("type", "unknown")
    expression = resolved_col.get("resolved_expression", "")
    base_tables = resolved_col.get("base_tables", []) or []
    base_columns = resolved_col.get("base_columns", []) or []
    filters = resolved_col.get("filters", []) or []

    t = _translate_expression(expression, ctx)
    english = t.english

    out = {
        "column_name": name,
        "column_type": col_type,
        "english_definition": english,
        "business_domain": classify_business_domain(name, base_tables, expression),
        "base_columns": base_columns,
        "base_tables": base_tables,
        "resolved_expression": expression,
    }
    if filters:
        # Add the filter narrative (column-meaning-with-WHERE) as a richer
        # field; consumers can choose either the bare definition or the
        # filter-aware one depending on use case.
        filter_narrative = _translate_filters(filters, ctx)
        if filter_narrative:
            out["english_definition_with_filters"] = (
                f"{english} (filtered where: {filter_narrative})"
            )
    if t.unknown_nodes:
        out["unknown_nodes"] = sorted(set(t.unknown_nodes))
    if t.unknown_columns:
        out["unknown_columns"] = sorted(set(t.unknown_columns))
    if t.ini_items:
        out["ini_items"] = sorted(set(t.ini_items))
    return out


# ---------------------------------------------------------------------------
# LLM-mode translation (lazy-imports the client library)
# ---------------------------------------------------------------------------

def _build_llm_context(resolved_col: dict, schema: dict) -> str:
    """Build a focused context string for the LLM: the column's resolved
    expression, base tables/columns, filters, and any schema descriptions
    available in the data dictionary."""
    lines = [
        f"Column name: {resolved_col.get('name', 'unknown')}",
        f"Column type: {resolved_col.get('type', 'unknown')}",
        f"Resolved expression: {resolved_col.get('resolved_expression', '')}",
    ]
    base_tables = resolved_col.get("base_tables", []) or []
    base_columns = resolved_col.get("base_columns", []) or []
    if base_tables:
        lines.append(f"Base tables: {', '.join(base_tables)}")
    if base_columns:
        lines.append(f"Base columns: {', '.join(base_columns)}")
    filters = resolved_col.get("filters", []) or []
    if filters:
        lines.append("Filters affecting this column:")
        for f in filters:
            lines.append(f"  - {_filter_text(f)}")

    # Pull descriptions from the schema/data dictionary if present.
    tables_index = (schema or {}).get("tables") or {}
    if isinstance(tables_index, dict) and base_tables:
        descs = []
        for t in base_tables:
            entry = tables_index.get(t) or tables_index.get(t.upper()) or tables_index.get(t.lower())
            if entry and isinstance(entry, dict):
                desc = entry.get("description")
                if desc:
                    descs.append(f"  {t}: {desc}")
        if descs:
            lines.append("Table descriptions:")
            lines.extend(descs)
    return "\n".join(lines)


_LLM_SYSTEM_PROMPT = """You translate SQL column definitions into accurate, succinct plain English.

Rules:
1. Be ACCURATE. Only describe what the SQL actually computes. No interpretations.
2. Be SUCCINCT. 1-2 sentences max. No "This column represents" preamble.
3. For CASE expressions: list the exact categories.
4. For calculations: state the formula in plain terms.
5. If filters constrain the column (WHERE/EXISTS), mention the slice they define.
6. Do NOT speculate on use cases.

Output JSON:
{
  "english_definition": "<succinct, accurate description>"
}"""


def translate_column_llm(resolved_col: dict, schema: dict, llm_client) -> dict:
    """Translate one resolved column via an LLM. Lazy-imports the client
    library; a no-LLM install will fail at import inside this branch
    (which is the desired structural guarantee for healthcare-safe builds:
    a customer with no `business_logic_llm` feature never reaches here)."""
    # Lazy import -- only loaded when LLM mode is actually used.
    from google.genai import types  # noqa: F401

    context = _build_llm_context(resolved_col, schema)
    user_prompt = (
        "Translate this SQL column to plain English. Be accurate and succinct -- "
        "only describe what the SQL computes, nothing more.\n\n" + context
    )

    name = resolved_col.get("name", "unknown")
    col_type = resolved_col.get("type", "unknown")
    base_tables = resolved_col.get("base_tables", []) or []
    base_columns = resolved_col.get("base_columns", []) or []

    try:
        response = llm_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=_LLM_SYSTEM_PROMPT,
                temperature=0.3,
                response_mime_type="application/json",
            ),
        )
        result = json.loads(response.text)
        english = result.get("english_definition", "")
    except Exception as e:
        english = f"[LLM error: {type(e).__name__}: {str(e)[:80]}]"

    return {
        "column_name": name,
        "column_type": col_type,
        "english_definition": english,
        "business_domain": classify_business_domain(name, base_tables,
                                                      resolved_col.get("resolved_expression", "")),
        "base_columns": base_columns,
        "base_tables": base_tables,
        "resolved_expression": resolved_col.get("resolved_expression", ""),
    }


def build_alias_map(sql: str, dialect: str = "tsql") -> dict[str, str]:
    """Walk a SQL view, return {alias_lower: real_table_name} for every
    non-CTE table reference. CTE aliases are excluded -- they're query-
    internal, not real database objects."""
    from .resolve import preprocess_ssms
    clean_sql, _ = preprocess_ssms(sql)
    if not clean_sql.strip():
        clean_sql = sql.strip() if sql else ""
    if not clean_sql:
        return {}
    try:
        parsed = parse_one(clean_sql, dialect=dialect)
    except Exception:
        return {}
    cte_names = {(c.alias_or_name or "").lower() for c in parsed.find_all(exp.CTE)}
    self_name = None
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        self_name = (parsed.this.name or "").lower()
    alias_map: dict[str, str] = {}
    for t in parsed.find_all(exp.Table):
        nm = (t.name or "").lower()
        if nm in cte_names or nm == self_name:
            continue
        alias = t.alias_or_name
        if alias and alias.lower() != nm and t.name:
            alias_map[alias.lower()] = t.name
    return alias_map


def clean_filter_sql(filter_expr: str, alias_map: dict[str, str],
                       dialect: str = "tsql") -> str:
    """Strip JOIN correlation keys (col = col on opposite tables) and
    resolve aliases to real table names. Returns cleaned filter SQL, or
    empty string if the filter was pure correlation plumbing.

    Two cleanups in one pass:
    1. `_strip_correlation_keys` removes predicates like `t1.X = t2.X`
       that are join keys, not row-restricting filters.
    2. Walks remaining Column nodes and rewrites their .table to the real
       table name when the alias is in alias_map.
    """
    if not filter_expr or not filter_expr.strip():
        return ""
    try:
        node = parse_one(filter_expr, dialect=dialect)
    except Exception:
        return filter_expr
    cleaned = _strip_correlation_keys(node)
    if cleaned is None:
        return ""
    for col in cleaned.find_all(exp.Column):
        tbl = (col.table or "").lower()
        if tbl in alias_map:
            col.set("table", exp.Identifier(this=alias_map[tbl]))
    return cleaned.sql(dialect=dialect)


def make_llm_client(api_key: Optional[str] = None):
    """Build a Gemini client. Lazy-imports `google.genai`. Customers using
    LLM mode bring their own API key (BYOK) -- you don't pay for their LLM
    use unless you're hosting the SaaS deployment."""
    import os
    from google import genai

    api_key = api_key or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError(
            "Gemini API key required for LLM mode. Set GEMINI_API_KEY in env "
            "or pass api_key explicitly. (Customer brings their own key for the "
            "offline / on-prem tier.)"
        )
    return genai.Client(api_key=api_key)


# ---------------------------------------------------------------------------
# Tool 4: Query-level summarisation (engineered + LLM)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase 2 helpers: naturalize engineered business prose without an LLM
# ---------------------------------------------------------------------------

_INLINE_COMMENT_RE = re.compile(
    r"(\b\d+\b|'[^']*')\s*/\*\s*([^*]+?)\s*\*/"
)


def _promote_inline_comments(filter_text: str) -> str:
    """Replace `<literal> /* business label */` with `'<business label>'`.

    Healthcare SQL frequently annotates status codes inline:
        STATUS_C = 5 /* Denied */
        COVERAGE_TYPE_C = 2 /* Managed Care */
    The comments carry the business meaning that the literal codes hide.
    Substituting the comment as a string literal makes the parsed SQL
    render naturally ("Status C = 'Denied'" instead of "Status C = 5").

    Falls through unchanged when no `<literal> /* ... */` pattern is found.
    """
    def repl(m: re.Match) -> str:
        comment = m.group(2).strip()
        # Escape single quotes inside the comment for safe SQL embedding.
        return "'" + comment.replace("'", "''") + "'"
    return _INLINE_COMMENT_RE.sub(repl, filter_text)


# Currently-effective triplet:
# `<COL_A> <= today AND <COL_B> >= today OR <COL_B> IS NULL`
# Catches all Clarity variants: EFF_DATE/TERM_DATE, MEM_EFF_FROM/MEM_EFF_TO,
# BEN_PLAN_EFF_DATE/BEN_PLAN_TERM_DT. The `\2` backreference enforces the
# end-date column repeats (the IS NULL branch).
_EFFECTIVE_TRIPLET_RE = re.compile(
    r"\b(?!(?:and|or|where|is|not)\b)([A-Z][\w\s]*?)\s*<=\s*today\s+and\s+"
    r"\b(?!(?:and|or|where|is|not)\b)([A-Z][\w\s]*?)\s*>=\s*today\s+or\s+"
    r"\2\s+is\s+null"
)

# Member-coverage variant uses From/To Date wording -- prefer the clearer
# "currently a covered member" phrasing over "currently effective".
_MEMBER_COVERED_RE = re.compile(
    r"\b(?!(?:and|or|where)\b)((?:[A-Z]\w*\s+)*Eff\s+From\s+Date)\s*<=\s*today\s+and\s+"
    r"((?:[A-Z]\w*\s+)*Eff\s+To\s+Date)\s*>=\s*today\s+or\s+"
    r"\2\s+is\s+null"
)

# `<COL> Yn = 'Y'` / `'N'` flag pattern. The negative lookahead prevents
# the lazy column-name matcher from greedily sweeping in leading conjunctions
# like "and Mem Covered Yn" -- without it, group 1 would capture "and Mem
# Covered" and produce "is and mem covered" instead of "and is mem covered".
_YN_FLAG_Y_RE = re.compile(
    r"\b(?!(?:and|or|where|is|not)\b)([A-Z][A-Za-z\s]*?)\s*Yn\s*=\s*'Y'"
)
_YN_FLAG_N_RE = re.compile(
    r"\b(?!(?:and|or|where|is|not)\b)([A-Z][A-Za-z\s]*?)\s*Yn\s*=\s*'N'"
)
_YN_FLAG_NULL_RE = re.compile(
    r"\b(?!(?:and|or|where|is|not)\b)([A-Z][A-Za-z\s]*?)\s*Yn\s+is\s+null"
)

# Trailing "C =" code-column suffix: "Status C = 'Denied'" -> "Status = 'Denied'"
_CODE_C_SUFFIX_RE = re.compile(r"\b([A-Za-z][A-Za-z\s]*?)\s+C\s*=\s*'", re.IGNORECASE)


def _naturalize_english(english: str) -> str:
    """Apply rule-based smoothing to engineered prose. Each rule targets
    one common Clarity idiom that the bare pattern library doesn't fold."""
    s = english

    # Effective-date triplets -> idiomatic English. Member-coverage variant
    # FIRST -- it's more specific than the generic Eff/Term shape.
    s = _MEMBER_COVERED_RE.sub("currently a covered member", s)
    s = _EFFECTIVE_TRIPLET_RE.sub("currently effective", s)

    # _YN flag patterns -> "is <noun>" / "is not <noun>". Cleanups:
    # - strip leading "Is " (the Translator already added it for IS_X_YN
    #   columns; otherwise we get "is is valid patient")
    # - strip trailing " Flag" (so DELETED_FLAG_YN renders as "deleted",
    #   not "deleted flag")
    def _yn_clean(col: str) -> str:
        col = col.strip()
        col = re.sub(r"^Is\s+", "", col, flags=re.IGNORECASE)
        col = re.sub(r"\s+Flag$", "", col, flags=re.IGNORECASE)
        return col.lower()
    s = _YN_FLAG_Y_RE.sub(lambda m: f"is {_yn_clean(m.group(1))}", s)
    s = _YN_FLAG_N_RE.sub(lambda m: f"is not {_yn_clean(m.group(1))}", s)
    s = _YN_FLAG_NULL_RE.sub(lambda m: f"{_yn_clean(m.group(1))} flag is unspecified", s)
    # Common Clarity idiom: `_YN = 'N' OR _YN IS NULL` -- both branches mean
    # the same thing. Collapse "is not X or X flag is unspecified" -> "is not X".
    s = re.sub(r"\bis not (\w[\w\s]*?)\s+or\s+\1\s+flag is unspecified",
                r"is not \1", s, flags=re.IGNORECASE)

    # Code-column suffix: drop the trailing " C" before "= 'value'".
    s = _CODE_C_SUFFIX_RE.sub(lambda m: f"{m.group(1)} = '", s)

    # Idiomatic cleanups
    s = re.sub(r"\bIs Valid Patient\s*=\s*'Y'", "is a valid patient", s, flags=re.IGNORECASE)
    s = re.sub(r"\bIs\s+(\w+)\s*=\s*'Y'", lambda m: f"is {m.group(1).lower()}", s, flags=re.IGNORECASE)

    # Collapse whitespace introduced by the substitutions.
    s = re.sub(r"\s+", " ", s).strip()
    return s


_BUSINESS_KEYWORDS = (
    "denied", "pending", "active", "valid", "completed", "cancelled",
    "approved", "expired",
)


def _extract_leading_adjective(business_filters: list[str]) -> str:
    """Scan the assembled filter prose for known business-slice keywords;
    return them as a comma-separated leading adjective phrase. Caps at 2 to
    keep the leading clause readable."""
    found: list[str] = []
    blob = " ".join(business_filters).lower()
    for kw in _BUSINESS_KEYWORDS:
        if re.search(rf"\b{kw}\b", blob) and kw not in found:
            found.append(kw)
        if len(found) >= 2:
            break
    if not found:
        return ""
    if len(found) == 1:
        return found[0]
    return f"{found[0]} or {found[1]}"


def summarize_engineered(business_logic, schema: dict | None = None) -> dict:
    """Deterministic query-level summary built from structured signals.
    Healthcare-safe: no LLM, no data exfiltration. Reads as a structured
    paragraph rather than fluent prose -- the LLM mode is where polish
    lives. Truthful is more important than pretty here.

    Returns dict with technical_description, business_description,
    primary_purpose, key_metrics. The `schema` is used to translate filter
    predicates into English for business_description.
    """
    lineage = business_logic.lineage
    translations = business_logic.column_translations
    n_cols = len(translations)
    col_types = [t.get("column_type", "") for t in translations]

    # Granularity signal: the dominant column type tells us the shape.
    if "window" in col_types:
        grain = "Ranked / windowed analysis"
    elif "aggregate" in col_types:
        grain = "Aggregated reporting"
    elif "case" in col_types:
        grain = "Categorisation and classification"
    else:
        grain = "Row-level extraction"

    # Domains: bucket per column, take the dominant one.
    domains = [t.get("business_domain", "") for t in translations
                if t.get("business_domain") and t.get("business_domain") != "General"]
    domain_str = ""
    if domains:
        from collections import Counter
        top = Counter(domains).most_common(1)[0][0]
        domain_str = f" for {top.lower()}"

    # Distinct base tables across all columns.
    base_tables = sorted({tbl for col in lineage.resolved_columns
                            for tbl in (col.get("base_tables", []) or [])})

    # Computed metrics = non-passthrough column names.
    metrics = [t.get("column_name", "") for t in translations
                if t.get("column_type") not in ("passthrough", "")]

    # Filter slice -- the strongest semantic signal we have.
    filter_narratives = []
    for f in lineage.query_filters:
        # Strip pure correlation keys ("col = col") and trivially true ("0 = 0")
        if not f or re.match(r"^\s*\d+\s*=\s*\d+\s*$", f.strip()):
            continue
        filter_narratives.append(f)

    # Build the technical_description as MULTI-LINE paragraphs (newlines
    # render as line breaks inside Excel/Sheets cells with text-wrap on,
    # making the description readable when shared with stakeholders).
    parts = [
        f"{grain}{domain_str}: {n_cols} output column(s) "
        f"sourced from {len(base_tables)} base table(s) ({', '.join(base_tables[:5])}"
        f"{'...' if len(base_tables) > 5 else ''})."
    ]
    if metrics:
        parts.append(
            f"Computed columns:\n  - " + "\n  - ".join(metrics[:5])
            + (f"\n  - and {len(metrics)-5} more" if len(metrics) > 5 else "")
        )
    if filter_narratives:
        parts.append("Constrained by:\n  - " + "\n  - ".join(filter_narratives))

    technical = "\n\n".join(parts)
    purpose = grain + domain_str if domain_str else grain

    # ---- business_description: no technical terms, no table/column codes,
    # no function names, no computed-column list. Verb + domain + filters
    # translated through the same engine Tool 3 uses for column English.
    grain_verb = {
        "Ranked / windowed analysis": "Identifies the most recent",
        "Aggregated reporting":       "Summarizes",
        "Categorisation and classification": "Categorizes",
        "Row-level extraction":       "Lists",
    }.get(grain, "Lists")
    business_subject = (
        domain_str.replace(" for ", "").strip().lower() if domain_str else "records"
    )
    ctx = Context(schema=schema or {})
    business_filters = []
    for f in filter_narratives:
        # PHASE 2: promote `/* business label */` comments BEFORE walking,
        # so STATUS_C = 5 /* Denied */ becomes STATUS_C = 'Denied' which the
        # pattern library renders cleanly.
        f_promoted = _promote_inline_comments(f)
        english = _walk_fragment(f_promoted, ctx).strip()
        # Strip "from <TABLE>," from EXISTS subquery rendering -- the comma
        # is what makes it safe (rules out "Mem Eff From Date" false matches).
        english = re.sub(r"\bfrom\s+[A-Za-z_][A-Za-z0-9_]*\s*,\s*", "", english,
                          flags=re.IGNORECASE)
        english = re.sub(r"\bwhere\s+where\b", "where", english, flags=re.IGNORECASE)
        # PHASE 2: naturalize common Clarity idioms (effective-date triplets,
        # _YN flag patterns, leftover "C =" code-column suffixes).
        english = _naturalize_english(english)
        if english and english.lower() not in (s.lower() for s in business_filters):
            business_filters.append(english)

    # PHASE 2: promote business-slice keywords (denied/active/valid/pending/
    # completed) detected in the filters as leading adjectives on the subject.
    leading_adjective = _extract_leading_adjective(business_filters)
    if leading_adjective:
        business_subject = f"{leading_adjective} {business_subject}"

    # business_description as multi-line paragraphs: a lead sentence
    # followed by a bulleted list of constraints. Renders cleanly in
    # Excel/Sheets and email clients with cell wrapping enabled.
    if business_filters:
        business_description = (
            f"{grain_verb} {business_subject}.\n\n"
            "Limited to:\n  - "
            + "\n  - ".join(business_filters)
        )
    else:
        business_description = f"{grain_verb} {business_subject}."

    return {
        "technical_description": technical,
        "business_description": business_description,
        "primary_purpose": purpose,
        "key_metrics": metrics[:10],
    }


_LLM_SUMMARY_SYSTEM_PROMPT = """You summarize SQL queries based on their output columns, source tables, and filter predicates.

Rules:
1. Be ACCURATE: only describe what the query actually produces.
2. Be SUCCINCT: 2-4 sentences max.
3. Identify the PRIMARY PURPOSE: what business question does this query answer?
4. Mention key entities (patients, referrals, encounters, etc.).
5. Note key computed metrics.
6. FILTERS DEFINE THE BUSINESS SLICE. If filters constrain to "denied", "active", "completed" etc., the summary MUST reflect that slice -- not the unfiltered shape.
7. Do NOT speculate on use cases or downstream applications.

Output JSON:
{
  "technical_description": "succinct technical description that reflects the business slice the filters define; OK to mention key tables/columns",
  "business_description": "the same description but in plain business language; NO table names, column names, function names, or column codes",
  "primary_purpose": "the business question this query answers",
  "key_metrics": ["list", "of", "key", "computed", "columns"]
}"""


def summarize_llm(business_logic, llm_client) -> dict:
    """LLM-backed query-level summary. Lazy-imports the client library."""
    from google.genai import types  # noqa: F401

    lineage = business_logic.lineage
    translations = business_logic.column_translations

    base_tables = sorted({tbl for col in lineage.resolved_columns
                            for tbl in (col.get("base_tables", []) or [])})
    column_lines = [f"- {t.get('column_name', '')}: {t.get('english_definition', '')}"
                    for t in translations]

    # Filter narrative: prefer engineered-translated filter prose if present,
    # else fall back to raw filter SQL.
    filter_lines = [f for f in lineage.query_filters
                    if f and not re.match(r"^\s*\d+\s*=\s*\d+\s*$", f.strip())]

    context_parts = [
        f"## Source Tables ({len(base_tables)})",
        ", ".join(base_tables),
        "",
        f"## Output Columns ({len(translations)})",
        "\n".join(column_lines),
    ]
    if filter_lines:
        context_parts += [
            "",
            f"## Query Filters ({len(filter_lines)} -- these constrain which rows the query returns)",
            "\n".join(f"- {f}" for f in filter_lines),
        ]
    user_prompt = "Summarize this SQL query based on its columns, source tables, and filters:\n\n" + \
                    "\n".join(context_parts)

    try:
        response = llm_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=_LLM_SUMMARY_SYSTEM_PROMPT,
                temperature=0.3,
                response_mime_type="application/json",
            ),
        )
        result = json.loads(response.text)
        return {
            "technical_description": result.get("technical_description", result.get("query_summary", "")),
            "business_description": result.get("business_description", ""),
            "primary_purpose": result.get("primary_purpose", ""),
            "key_metrics": result.get("key_metrics", []),
        }
    except Exception as e:
        return {
            "technical_description": f"[LLM error: {type(e).__name__}: {str(e)[:80]}]",
            "business_description": "",
            "primary_purpose": "",
            "key_metrics": [],
        }
