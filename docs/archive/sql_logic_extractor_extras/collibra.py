#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- Collibra Export

Converts extraction output into Collibra-compatible import formats:
  - Business Glossary CSV  (business terms + definitions)
  - Data Lineage JSON      (column-level source-to-target mappings)
  - Data Dictionary CSV    (column descriptions + table relationships)

Usage:
    from sql_logic_extractor.collibra import export_collibra
    export_collibra(sql, domain="Business Glossary > Healthcare")
"""

import csv
import io
import json
from dataclasses import dataclass, field
from typing import Optional

from .extract import SQLBusinessLogicExtractor, to_dict
from .normalize import extract_definitions, BusinessDefinition
from .resolve import resolve_query, resolved_to_dict, ResolvedQuery
from .translate import translate_resolved


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class CollibraConfig:
    """Settings for Collibra export -- customize per environment."""
    glossary_domain: str = "Business Glossary > Healthcare"
    catalog_domain: str = "Physical Data Catalog > Healthcare"
    default_status: str = "Candidate"
    database: str = ""
    schema: str = "dbo"
    dialect: str = "tsql"


# ---------------------------------------------------------------------------
# Glossary CSV -- business terms + definitions
# ---------------------------------------------------------------------------

GLOSSARY_COLUMNS = [
    "Name", "Domain", "Type", "Status", "Definition",
    "Description", "Source Tables", "Source Columns",
    "Expression", "Category",
]


def _build_glossary_rows(
    translated: list[dict],
    resolved: ResolvedQuery,
    config: CollibraConfig,
    query_label: str = "",
) -> list[dict]:
    """Convert translated output into Collibra glossary term rows."""
    rows = []
    resolved_dict = resolved_to_dict(resolved)
    obj_name = resolved_dict.get("name", query_label or "query")

    for item in translated:
        name = item["name"]
        term_name = f"{obj_name}.{name}" if obj_name else name

        definition = item.get("full_business_definition", item.get("business_definition", ""))
        description = item.get("business_definition", "")

        source_tables = "; ".join(item.get("source_tables", []))
        base_columns = "; ".join(item.get("base_columns", []))
        expression = item.get("resolved_expression", item.get("direct_expression", ""))
        category = item.get("type", "")

        rows.append({
            "Name": term_name,
            "Domain": config.glossary_domain,
            "Type": "Business Term",
            "Status": config.default_status,
            "Definition": definition,
            "Description": description,
            "Source Tables": source_tables,
            "Source Columns": base_columns,
            "Expression": expression,
            "Category": category,
        })

    return rows


def glossary_csv(
    sql: str,
    config: CollibraConfig = None,
    query_label: str = "",
) -> str:
    """Generate Collibra business glossary CSV from SQL."""
    config = config or CollibraConfig()
    translated = translate_resolved(sql, dialect=config.dialect)
    resolved = resolve_query(sql, dialect=config.dialect)

    rows = _build_glossary_rows(translated, resolved, config, query_label)

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=GLOSSARY_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


# ---------------------------------------------------------------------------
# Lineage JSON -- column-level source-to-target edges
# ---------------------------------------------------------------------------

def _make_asset_ref(database: str, schema: str, table: str, column: str = None) -> dict:
    """Build a nested Collibra lineage asset reference."""
    table_ref = {
        "name": table,
        "parent": {
            "name": schema,
            "parent": {
                "name": database,
                "type": "Database",
            },
            "type": "Schema",
        },
        "type": "Table",
    }
    if column:
        return {
            "name": column,
            "parent": table_ref,
            "type": "Column",
        }
    return table_ref


def _build_lineage_edges(
    resolved: ResolvedQuery,
    config: CollibraConfig,
    target_table: str = "",
) -> list[dict]:
    """Convert resolved lineage into Collibra lineage edge format."""
    resolved_dict = resolved_to_dict(resolved)
    if not target_table:
        target_table = resolved_dict.get("name", "output")
    target_schema = resolved_dict.get("schema", config.schema)

    edges = []

    for col in resolved.columns:
        if col.type == "star":
            continue

        transformation = col.resolved_expression or col.expression or ""

        # Each base column is a source edge
        if col.base_columns:
            for bc in col.base_columns:
                parts = bc.split(".", 1)
                if len(parts) == 2:
                    src_table, src_col = parts
                else:
                    src_table, src_col = "UNKNOWN", parts[0]

                edges.append({
                    "source": _make_asset_ref(
                        config.database, config.schema, src_table, src_col,
                    ),
                    "target": _make_asset_ref(
                        config.database, target_schema, target_table, col.name,
                    ),
                    "transformation": transformation,
                })
        else:
            # Calculated column with no base column (literals, etc.)
            edges.append({
                "source": _make_asset_ref(
                    config.database, target_schema, target_table, col.name,
                ),
                "target": _make_asset_ref(
                    config.database, target_schema, target_table, col.name,
                ),
                "transformation": transformation,
            })

    return edges


def lineage_json(
    sql: str,
    config: CollibraConfig = None,
) -> str:
    """Generate Collibra lineage JSON from SQL."""
    config = config or CollibraConfig()
    resolved = resolve_query(sql, dialect=config.dialect)
    edges = _build_lineage_edges(resolved, config)
    return json.dumps({"edges": edges}, indent=2)


# ---------------------------------------------------------------------------
# Data Dictionary CSV -- column descriptions + table relationships
# ---------------------------------------------------------------------------

DICTIONARY_COLUMNS = [
    "Name", "Domain", "Type", "Description", "Definition",
    "Technical Data Type", "is part of",
]


def _build_dictionary_rows(
    resolved: ResolvedQuery,
    translated: list[dict],
    config: CollibraConfig,
) -> list[dict]:
    """Convert resolved query into Collibra data dictionary rows."""
    resolved_dict = resolved_to_dict(resolved)
    obj_name = resolved_dict.get("name", "output")
    obj_schema = resolved_dict.get("schema", config.schema)

    # Build a lookup from translated results
    trans_by_name = {t["name"]: t for t in translated}

    rows = []
    for col in resolved.columns:
        if col.type == "star":
            continue

        t = trans_by_name.get(col.name, {})
        description = t.get("business_definition", "")
        definition = t.get("full_business_definition", "")

        # Infer a rough data type from the expression type
        data_type = _infer_data_type(col)

        rows.append({
            "Name": col.name,
            "Domain": config.catalog_domain,
            "Type": "Column",
            "Description": description,
            "Definition": definition,
            "Technical Data Type": data_type,
            "is part of": f"[{obj_schema}].[{obj_name}]",
        })

    return rows


def _infer_data_type(col) -> str:
    """Best-effort data type inference from column type and expression."""
    expr = (col.resolved_expression or col.expression or "").upper()

    if col.type == "aggregate":
        if "COUNT" in expr:
            return "INTEGER"
        if any(fn in expr for fn in ("SUM", "AVG")):
            return "NUMERIC"
        if any(fn in expr for fn in ("MIN", "MAX")):
            return ""  # depends on source column type
    if col.type == "date_calculation":
        if "DATEDIFF" in expr:
            return "INTEGER"
        return "DATETIME"
    if col.type == "classification":
        return "VARCHAR"
    if col.type == "type_conversion":
        if "INT" in expr:
            return "INTEGER"
        if "DATE" in expr:
            return "DATE"
        if "VARCHAR" in expr or "CHAR" in expr:
            return "VARCHAR"

    return ""


def dictionary_csv(
    sql: str,
    config: CollibraConfig = None,
) -> str:
    """Generate Collibra data dictionary CSV from SQL."""
    config = config or CollibraConfig()
    resolved = resolve_query(sql, dialect=config.dialect)
    translated = translate_resolved(sql, dialect=config.dialect)

    rows = _build_dictionary_rows(resolved, translated, config)

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=DICTIONARY_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


# ---------------------------------------------------------------------------
# Combined export -- all three files at once
# ---------------------------------------------------------------------------

def export_collibra(
    sql: str,
    config: CollibraConfig = None,
    query_label: str = "",
    output_dir: str = None,
) -> dict:
    """Generate all three Collibra import files from a SQL query.

    Returns a dict with keys: glossary_csv, lineage_json, dictionary_csv.
    If output_dir is provided, also writes files to disk.
    """
    config = config or CollibraConfig()
    resolved = resolve_query(sql, dialect=config.dialect)
    translated = translate_resolved(sql, dialect=config.dialect)

    # Glossary
    glossary_rows = _build_glossary_rows(translated, resolved, config, query_label)
    g_out = io.StringIO()
    g_writer = csv.DictWriter(g_out, fieldnames=GLOSSARY_COLUMNS)
    g_writer.writeheader()
    g_writer.writerows(glossary_rows)
    glossary = g_out.getvalue()

    # Lineage
    edges = _build_lineage_edges(resolved, config)
    lineage = json.dumps({"edges": edges}, indent=2)

    # Dictionary
    dictionary_rows = _build_dictionary_rows(resolved, translated, config)
    d_out = io.StringIO()
    d_writer = csv.DictWriter(d_out, fieldnames=DICTIONARY_COLUMNS)
    d_writer.writeheader()
    d_writer.writerows(dictionary_rows)
    dictionary = d_out.getvalue()

    result = {
        "glossary_csv": glossary,
        "lineage_json": lineage,
        "dictionary_csv": dictionary,
    }

    if output_dir:
        import os
        os.makedirs(output_dir, exist_ok=True)
        resolved_dict = resolved_to_dict(resolved)
        prefix = resolved_dict.get("name", query_label or "query")

        with open(os.path.join(output_dir, f"{prefix}_glossary.csv"), "w") as f:
            f.write(glossary)
        with open(os.path.join(output_dir, f"{prefix}_lineage.json"), "w") as f:
            f.write(lineage)
        with open(os.path.join(output_dir, f"{prefix}_dictionary.csv"), "w") as f:
            f.write(dictionary)

    return result
