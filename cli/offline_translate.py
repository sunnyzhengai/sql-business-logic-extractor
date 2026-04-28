#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- L4: Translate (Offline / Recursive)

Takes L3 lineage resolution output and translates each column's resolved
SQL expression into plain English using the recursive pattern library in
``sql_logic_extractor.patterns``. Unknown nodes and columns propagate as
governance signals (never opaque fallbacks).

Preserves the CLI and output JSON shape of the legacy offline_translate.py
(archived at ``archive/offline_translate_legacy.py``):

    python3 offline_translate.py <l3_json> [--schema clarity_schema.yaml]
                                           [--output PATH] [--text]

Pipeline: L1 (extract) → L2 (normalize) → L3 (resolve) → L4 (translate) → L5 (compare)
"""

import argparse
import json
import re
from pathlib import Path
from typing import Optional

from sqlglot import exp, parse_one

from sql_logic_extractor.patterns import Context, Translation, translate


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def load_schema(path: str) -> dict:
    """Load the schema as a raw dict, auto-detecting JSON vs YAML by
    extension. The pattern library builds its own ``__table_index__`` cache
    on first column lookup, so no pre-processing is needed here.

    JSON is recommended for schemas generated from the Clarity metadata
    query (see scripts/csv_to_schema.py) — pyyaml isn't required at
    runtime and SQL Server can emit JSON natively.
    """
    if path.lower().endswith(".json"):
        with open(path, "r") as f:
            return json.load(f)
    # Lazy import — pyyaml is only needed when loading .yaml schemas. At
    # work (after the JSON pipeline switch) this branch is unreachable, so
    # pyyaml doesn't have to be installed.
    import yaml
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Ancillary: domain classification (ported from legacy offline_translate.py)
# ---------------------------------------------------------------------------

def classify_business_domain(col_name: str, base_tables: list, expression: str) -> str:
    """Classify the business domain based on column name, expression, and base tables."""
    col_upper = (col_name or "").upper()
    tables_str = " ".join(base_tables or []).upper()

    if any(x in col_upper for x in ["AGE", "BIRTH", "DOB"]):
        return "Patient Demographics"
    if any(x in col_upper for x in ["LOS", "LENGTH_OF_STAY", "STAY"]):
        return "Hospital Metrics"
    if any(x in col_upper for x in ["CHARGE", "COST", "PAYMENT", "AMOUNT", "PRICE", "FIN"]):
        return "Financial"
    if any(x in col_upper for x in ["READMI", "READMIT"]):
        return "Quality Metrics"
    if any(x in col_upper for x in ["APPT", "SCHED", "WAIT"]):
        return "Scheduling"
    if any(x in col_upper for x in ["DIAG", "DX", "ICD"]):
        return "Clinical - Diagnosis"
    if any(x in col_upper for x in ["PROC", "CPT", "SURG"]):
        return "Clinical - Procedures"
    if any(x in col_upper for x in ["MED", "DRUG", "RX", "PHARM"]):
        return "Clinical - Medications"
    if any(x in col_upper for x in ["REFER", "REF_"]):
        return "Referrals"
    if any(x in col_upper for x in ["RANK", "ROW_NUM", "SEQ"]):
        return "Ordering/Ranking"

    if "ARPB" in tables_str or "BILLING" in tables_str:
        return "Billing"
    if "HSP_ACCOUNT" in tables_str:
        return "Hospital Accounting"
    if "PAT_ENC" in tables_str:
        return "Patient Encounters"
    if "REFERRAL" in tables_str:
        return "Referrals"

    return "General"


# ---------------------------------------------------------------------------
# Expression translation (new: delegates to the recursive walker)
# ---------------------------------------------------------------------------

def _unwrap_select(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Select):
        node = node.selects[0]
    if isinstance(node, exp.Alias):
        node = node.this
    return node


def translate_expression(expression: str, ctx: Context) -> Translation:
    """Parse a resolved SQL expression and walk it with the pattern registry.

    Falls back to a structural unknown record if parsing fails — matches
    the recursive-translation principle of never emitting an opaque
    placeholder without also registering the event.
    """
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


# ---------------------------------------------------------------------------
# Filter translation (recursive walker applied to each filter predicate)
# ---------------------------------------------------------------------------

def _filter_text(f) -> str:
    """Extract the expression string from a filter (dict shape or legacy str)."""
    if isinstance(f, dict):
        return (f.get("expression") or "").strip()
    return (f or "").strip()


def translate_filters(filters: list, ctx: Context) -> str:
    """Translate L3 filter predicates by walking each with the registry.

    Distinguishes business filters from technical plumbing (IS NOT NULL)
    per the recursive-translation principle: technical filters get
    prefixed ``where … exists``; business filters get natural-language
    rendering.
    """
    if not filters:
        return ""
    parts = []
    for f in filters:
        f_text = _filter_text(f)
        if not f_text:
            continue
        if re.search(r"\bIS\s+NOT\s+NULL\b", f_text, re.IGNORECASE):
            # Technical filter — summarize instead of regurgitating.
            col_part = re.split(r"\s+IS\s+NOT\s+NULL", f_text, flags=re.IGNORECASE)[0].strip()
            col_part = col_part.lstrip("NOT ").strip()
            col_text = _walk_fragment(col_part, ctx)
            parts.append(f"where {col_text} exists")
            continue
        # Business filter — parse + walk.
        walked = _walk_fragment(f_text, ctx)
        parts.append(walked)
    return "; ".join(parts)


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
    from sql_logic_extractor.patterns.structural import _strip_correlation_keys
    cleaned = _strip_correlation_keys(node)
    if cleaned is None:
        return ""
    return translate(cleaned, ctx).english


# ---------------------------------------------------------------------------
# Column-level translation
# ---------------------------------------------------------------------------

def translate_column(resolved_col: dict, ctx: Context) -> dict:
    name = resolved_col.get("name", "unknown")
    col_type = resolved_col.get("type", "unknown")
    expression = resolved_col.get("resolved_expression", "")
    base_tables = resolved_col.get("base_tables", []) or []
    base_columns = resolved_col.get("base_columns", []) or []
    filters = resolved_col.get("filters", []) or []

    t = translate_expression(expression, ctx)
    english = t.english

    # Per-column filters are intentionally NOT included here. L3 attributes
    # filters to columns based on traversal path, which is unreliable, and the
    # same predicate ends up duplicated across every column from the same
    # scope. translate_query() lifts the union/deduped set to summary.query_filters
    # and post-processes columns with english_definition_with_filters built
    # from the query-wide narrative.
    technical_definition = {
        "resolved_expression": expression,
        "base_columns": base_columns,
        "base_tables": base_tables,
        "transformation_chain": resolved_col.get("transformation_chain", []),
    }

    out = {
        "column_name": name,
        "column_type": col_type,
        "technical_definition": technical_definition,
        "english_definition": english,
        "business_domain": classify_business_domain(name, base_tables, expression),
        "_raw_filters": filters,  # internal — consumed by translate_query, stripped before emit
    }
    # Governance signals — new in the recursive translator. Downstream tools
    # can surface these as the "patterns/columns needing authoring" backlog.
    if t.unknown_nodes:
        out["unknown_nodes"] = sorted(set(t.unknown_nodes))
    if t.unknown_columns:
        out["unknown_columns"] = sorted(set(t.unknown_columns))
    # INI-Item coordination keys (from Clarity metadata) — lets Collibra
    # export and blast-radius tooling reference Chronicles items without
    # re-querying the schema.
    if t.ini_items:
        out["ini_items"] = sorted(set(t.ini_items))
    return out


# ---------------------------------------------------------------------------
# Query-level summary (ported from legacy)
# ---------------------------------------------------------------------------

def _is_correlation_key(expr_str: str) -> bool:
    """True if the filter is a bare `column = column` relational key, not a row-restricting predicate."""
    try:
        node = parse_one(expr_str)
    except Exception:
        return False
    if isinstance(node, exp.EQ):
        return isinstance(node.left, exp.Column) and isinstance(node.right, exp.Column)
    return False


def _canonical_filter(expr_str: str) -> str:
    """Parse and re-emit with table qualifiers stripped, for dedup purposes."""
    try:
        node = parse_one(expr_str)
    except Exception:
        return expr_str
    for col in node.find_all(exp.Column):
        col.set("table", None)
    return node.sql()


def _dedupe_filters(raw_filters: list) -> list:
    """Drop correlation keys and collapse filters that differ only in alias qualifiers.
    Keeps the first-seen original form (usually the one with aliases intact).
    Accepts either plain strings (legacy) or filter dicts ``{expression, subqueries?}``."""
    seen_canonical = set()
    out = []
    for f in raw_filters:
        f_text = _filter_text(f)
        if _is_correlation_key(f_text):
            continue
        canon = _canonical_filter(f_text)
        if canon in seen_canonical:
            continue
        seen_canonical.add(canon)
        out.append(f)
    return out


def summarize_query(column_results: list, l3_data: dict) -> dict:
    all_tables = set()
    all_domains = set()
    for col in column_results:
        tech = col.get("technical_definition", {})
        for t in tech.get("base_tables", []) or []:
            all_tables.add(t)
        if col.get("business_domain"):
            all_domains.add(col["business_domain"])

    table_list = sorted(all_tables)
    domain_list = sorted(all_domains)

    # Note: no query_summary / primary_purpose emitted here. Rule-based
    # narrative summaries from structural counts are misleading — they
    # describe the shape of the query, not its business intent. The LLM
    # translator (cli/llm_translate.py) owns semantic narrative summary;
    # this offline path emits structured signals only.

    # Aggregate governance signals across all columns
    all_unknown_nodes = sorted({u for c in column_results for u in c.get("unknown_nodes", [])})
    all_unknown_columns = sorted({u for c in column_results for u in c.get("unknown_columns", [])})
    all_ini_items = sorted({u for c in column_results for u in c.get("ini_items", [])})

    # Query-wide filter set: union of filters across all columns, deduplicated.
    # Per-column L3 filter attribution is unreliable (different traversal paths
    # pick up different scope-local filters), so the query-level union is the
    # honest representation — these filters apply to the final result set.
    # Correlation keys (col = col) and alias-prefix duplicates are dropped.
    raw = []
    seen_literal = set()
    for c in column_results:
        for f in c.get("_raw_filters", []) or []:
            key = _filter_text(f)
            if key in seen_literal:
                continue
            seen_literal.add(key)
            raw.append(f)
    query_filters = _dedupe_filters(raw)

    summary = {
        "key_entities": list(all_tables)[:5],
        "key_metrics": [c["column_name"] for c in column_results
                        if c.get("column_type") in ("calculated", "aggregate", "case")][:5],
        "source_tables": table_list,
        "business_domains": domain_list,
        "column_count": len(column_results),
    }
    if all_unknown_nodes:
        summary["unknown_nodes"] = all_unknown_nodes
    if all_unknown_columns:
        summary["unknown_columns"] = all_unknown_columns
    if all_ini_items:
        summary["ini_items"] = all_ini_items
    if query_filters:
        summary["query_filters"] = query_filters
    return summary


def translate_query(l3_json_path: str, schema_path: str) -> dict:
    schema = load_schema(schema_path)
    ctx = Context(schema=schema)
    with open(l3_json_path, "r") as f:
        l3_data = json.load(f)
    column_results = [translate_column(c, ctx) for c in l3_data.get("columns", []) or []]
    summary = summarize_query(column_results, l3_data)

    # Translate the query-level filter set into English. Filters that translate
    # to empty (pure correlation keys / join plumbing) are dropped entirely.
    if summary.get("query_filters"):
        raw_filters = summary["query_filters"]
        kept_raw = []
        kept_english = []
        for f in raw_filters:
            f_text = _filter_text(f)
            translated = _walk_fragment(f_text, ctx) if f_text else ""
            if not translated:
                # Pure correlation — skip.
                continue
            kept_raw.append(f)
            kept_english.append(translated)
        summary["query_filters"] = kept_raw
        summary["query_filters_english"] = kept_english
        if not kept_raw:
            summary.pop("query_filters", None)
            summary.pop("query_filters_english", None)

    # Post-pass: strip the internal _raw_filters carrier and synthesize
    # english_definition_with_filters from the query-wide narrative so every
    # column carries the full business meaning (column intent + scope filters)
    # downstream consumers can ingest into Collibra without re-joining to summary.
    filter_suffix = ""
    if summary.get("query_filters_english"):
        filter_suffix = "; ".join(summary["query_filters_english"])
    for col in column_results:
        col.pop("_raw_filters", None)
        if filter_suffix:
            col["english_definition_with_filters"] = (
                f"{col['english_definition']} (filtered where: {filter_suffix})"
            )
        else:
            col["english_definition_with_filters"] = col["english_definition"]

    return {"summary": summary, "columns": column_results}


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def format_output(results: dict, fmt: str = "json") -> str:
    if fmt == "json":
        return json.dumps(results, indent=2)

    lines = []
    lines.append("=" * 80)
    lines.append("SQL QUERY BUSINESS LOGIC DOCUMENTATION (Offline Recursive Translation)")
    lines.append("=" * 80)
    lines.append("")

    summary = results.get("summary", {})
    lines.append("# QUERY SUMMARY")
    lines.append("")
    lines.append("   (Semantic narrative summary requires the LLM translator —")
    lines.append("    this offline path emits structured signals only.)")
    lines.append("")
    if summary.get("key_entities"):
        lines.append(f"   Key Entities: {', '.join(summary['key_entities'])}")
    if summary.get("key_metrics"):
        lines.append(f"   Key Metrics: {', '.join(summary['key_metrics'])}")
    if summary.get("source_tables"):
        lines.append(f"   Source Tables: {', '.join(summary['source_tables'])}")
    if summary.get("business_domains"):
        lines.append(f"   Business Domains: {', '.join(summary['business_domains'])}")
    lines.append(f"   Total Columns: {summary.get('column_count', 0)}")

    if summary.get("query_filters"):
        lines.append("")
        lines.append("   Query Filters (apply to the final result set):")
        raw_filters = summary["query_filters"]
        english_filters = summary.get("query_filters_english") or [_filter_text(r) for r in raw_filters]
        for raw, eng in zip(raw_filters, english_filters):
            raw_text = _filter_text(raw)
            if eng and eng != raw_text:
                lines.append(f"     - {eng}")
                lines.append(f"         [{raw_text}]")
            else:
                lines.append(f"     - {raw_text}")
            # Render nested subquery lineage if present.
            if isinstance(raw, dict):
                for i, sq in enumerate(raw.get("subqueries", []) or []):
                    tbls = sq.get("base_tables") or sorted({
                        t for c in sq.get("columns", []) for t in c.get("base_tables", [])
                    })
                    cols_ = sorted({c_ for c in sq.get("columns", []) for c_ in c.get("base_columns", [])})
                    lines.append(f"         subquery #{i + 1}: tables={tbls}")
                    if cols_:
                        lines.append(f"                        columns={cols_}")

    if summary.get("unknown_nodes") or summary.get("unknown_columns"):
        lines.append("")
        lines.append("   Governance signals:")
        if summary.get("unknown_nodes"):
            lines.append(f"     Unknown node types: {', '.join(summary['unknown_nodes'])}")
        if summary.get("unknown_columns"):
            lines.append(f"     Unknown columns: {', '.join(summary['unknown_columns'])}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("")
    lines.append("# COLUMN DEFINITIONS")
    lines.append("")

    for r in results.get("columns", []):
        lines.append(f"## {r['column_name']} ({r.get('column_type', 'unknown')})")
        lines.append(f"   Domain: {r.get('business_domain', 'Unknown')}")
        lines.append("")

        tech = r.get("technical_definition", {})
        lines.append("   ### Technical Definition")
        lines.append("")
        if tech.get("resolved_expression"):
            lines.append(f"   Expression: {tech['resolved_expression']}")
            lines.append("")
        if tech.get("base_tables"):
            lines.append(f"   Base Tables: {', '.join(tech['base_tables'])}")
        if tech.get("base_columns"):
            lines.append(f"   Base Columns: {', '.join(tech['base_columns'])}")
        lines.append("")

        lines.append("   ### Business Definition")
        lines.append("")
        lines.append(f"   {r.get('english_definition', 'No definition available')}")

        if r.get("unknown_nodes") or r.get("unknown_columns"):
            lines.append("")
            lines.append("   Governance signals:")
            if r.get("unknown_nodes"):
                lines.append(f"     Unknown nodes: {', '.join(r['unknown_nodes'])}")
            if r.get("unknown_columns"):
                lines.append(f"     Unknown columns: {', '.join(r['unknown_columns'])}")
        lines.append("")
        lines.append("-" * 80)
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="L4: Translate SQL lineage to plain English (Offline — Recursive)"
    )
    parser.add_argument("l3_json", help="Path to L3 JSON output file")
    parser.add_argument("--schema", "-s", default="clarity_schema.yaml",
                        help="Path to clarity_schema.yaml (default: clarity_schema.yaml)")
    parser.add_argument("--output", "-o",
                        help="Output file path without extension. Defaults to translate-out/<stem-of-input>.")
    parser.add_argument("--text", action="store_true",
                        help="Output human-readable text instead of JSON")
    parser.add_argument("--stdout", action="store_true",
                        help="Print to stdout instead of writing files")
    args = parser.parse_args()

    print(f"Loading L3 output: {args.l3_json}")
    print(f"Loading schema: {args.schema}")
    print("Translating columns (offline, recursive)...")
    print()

    results = translate_query(args.l3_json, args.schema)

    if args.stdout:
        print(format_output(results, "text" if args.text else "json"))
        return

    if args.output:
        out_base = Path(args.output)
    else:
        out_base = Path("translate-out") / Path(args.l3_json).stem
    out_base.parent.mkdir(parents=True, exist_ok=True)

    json_path = out_base.with_suffix(".json")
    json_path.write_text(format_output(results, "json"))
    print(f"JSON saved to: {json_path}")

    text_path = out_base.with_suffix(".txt")
    text_path.write_text(format_output(results, "text"))
    print(f"Text saved to: {text_path}")

    print(f"\nTranslated {len(results.get('columns', []))} columns.")


if __name__ == "__main__":
    main()
