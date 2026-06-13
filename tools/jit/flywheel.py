"""Flywheel — the learning loop that makes the system smarter over time.

Two operations:
1. validate_definition() — user approved using a definition → increment weight
2. create_definition() — user built a new join path → save as new definition

Both persist to the glossary YAML files on disk.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Optional

import yaml

DATA_DIR = Path(__file__).resolve().parent / "data"


def validate_definition(
    definition_name: str,
    user_id: str = "anonymous",
    glossary_dir: Path | None = None,
) -> dict | None:
    """Record that a user validated (approved) a definition.

    Increments validation_count and usage_count, adds user to validated_by.
    Returns the updated definition dict, or None if not found.
    """
    glossary_dir = glossary_dir or DATA_DIR / "definition_glossary"
    yaml_path = glossary_dir / f"{definition_name}.yaml"

    if not yaml_path.exists():
        return None

    with open(yaml_path) as f:
        defn = yaml.safe_load(f)

    # Update counts
    defn["validation_count"] = defn.get("validation_count", 0) + 1
    defn["usage_count"] = defn.get("usage_count", 0) + 1

    # Add user to validated_by (avoid duplicates)
    validated_by = defn.get("validated_by", [])
    if user_id not in validated_by:
        validated_by.append(user_id)
    defn["validated_by"] = validated_by

    # Write back
    with open(yaml_path, "w") as f:
        yaml.dump(defn, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)

    return defn


def create_definition_from_graph(
    graph_tables: list,
    label: str,
    description: str,
    domain: str = "",
    user_id: str = "anonymous",
    glossary_dir: Path | None = None,
    learned_terms_path: Path | None = None,
) -> dict:
    """Create a new business definition from the current graph state.

    Extracts backbone (tables, joins, filters) from GraphTable objects
    and saves as a new YAML file in the glossary.

    Returns the new definition dict.
    """
    glossary_dir = glossary_dir or DATA_DIR / "definition_glossary"
    glossary_dir.mkdir(parents=True, exist_ok=True)

    # Generate definition name from label
    defn_name = re.sub(r'[^a-z0-9]+', '_', label.lower()).strip('_')

    # Ensure unique name
    existing = {p.stem for p in glossary_dir.glob("*.yaml")}
    base_name = defn_name
    counter = 1
    while defn_name in existing:
        defn_name = f"{base_name}_{counter}"
        counter += 1

    # Extract backbone from graph tables
    tables = [t.name for t in graph_tables]
    anchor = graph_tables[0].name if graph_tables else ""

    joins = []
    for t in graph_tables[1:]:
        if t.join_from:
            joins.append({
                "from": t.join_from,
                "to": t.name,
                "on": _build_on_clause(t),
                "type": t.join_type or "JOIN",
                "grain_impact": True,
            })

    # Collect all active filters
    char_filters = []
    params = []
    for t in graph_tables:
        for f in t.filters:
            if f.get("active", True):
                char_filters.append({
                    "expression": f["expression"],
                    "english": f.get("english", f["expression"]),
                    "is_definitional": True,
                })

    # Build SQL template from graph
    from tools.jit.app.query_graph_ui import build_graph_sql
    sql_template = build_graph_sql(graph_tables)
    sql_template = sql_template.replace("SELECT COUNT(*)", "SELECT DISTINCT PATIENT.PAT_ID", 1)

    defn = {
        "definition_name": defn_name,
        "label": label,
        "description": description,
        "domain": domain,
        "backbone": {
            "anchor_table": anchor,
            "tables": tables,
            "joins": joins,
            "characteristic_filters": char_filters,
            "output_grain": "patient",
        },
        "parameters": params,
        "source_reports": [],
        "source_scopes": [],
        "equivalent_definitions": [],
        "sql_template": sql_template,
        "validated_by": [user_id],
        "used_in_queries": [],
        "validation_count": 1,
        "usage_count": 1,
        "created_by": user_id,
        "created_date": date.today().isoformat(),
        "created_from_session": True,
    }

    # Save to glossary
    yaml_path = glossary_dir / f"{defn_name}.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(defn, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)

    # Also add to learned_terms if there are meaningful filter terms
    _update_learned_terms(defn, learned_terms_path)

    return defn


def save_to_user_library(
    query_label: str,
    definition_names: list[str],
    sql: str,
    result_count: int | None = None,
    user_id: str = "anonymous",
    library_path: Path | None = None,
) -> dict:
    """Save a completed query to the user library.

    Records which definitions were used, the final SQL, and the result.
    """
    library_path = library_path or DATA_DIR / "user_library.json"

    if library_path.exists():
        with open(library_path) as f:
            library = json.load(f)
    else:
        library = {"users": {}, "saved_queries": {}}

    # Ensure user exists
    if user_id not in library["users"]:
        library["users"][user_id] = {
            "queries": [],
            "validated_definitions": [],
        }

    # Generate query ID
    query_id = f"q_{len(library['saved_queries']) + 1}"

    query_entry = {
        "id": query_id,
        "label": query_label,
        "definitions_used": definition_names,
        "sql": sql,
        "result_count": result_count,
        "created_by": user_id,
        "created_date": date.today().isoformat(),
    }

    library["saved_queries"][query_id] = query_entry
    library["users"][user_id]["queries"].append(query_id)

    with open(library_path, "w") as f:
        json.dump(library, f, indent=2)

    return query_entry


def get_definition_stats(glossary_dir: Path | None = None) -> list[dict]:
    """Get validation and usage stats for all definitions, sorted by popularity."""
    glossary_dir = glossary_dir or DATA_DIR / "definition_glossary"
    stats = []
    for yf in sorted(glossary_dir.glob("*.yaml")):
        with open(yf) as f:
            defn = yaml.safe_load(f)
        stats.append({
            "name": defn["definition_name"],
            "label": defn.get("label", ""),
            "validation_count": defn.get("validation_count", 0),
            "usage_count": defn.get("usage_count", 0),
            "validated_by": defn.get("validated_by", []),
            "created_from_session": defn.get("created_from_session", False),
        })
    stats.sort(key=lambda s: -(s["validation_count"] + s["usage_count"]))
    return stats


def _build_on_clause(t) -> str:
    """Build SQL ON clause from a GraphTable."""
    if t.join_direction == "child_to_parent":
        return f"{t.join_from}.{t.join_column} = {t.name}.{t.pk_column}"
    else:
        return f"{t.name}.{t.join_column} = {t.join_from}.{t.pk_column}"


def _update_learned_terms(defn: dict, terms_path: Path | None = None):
    """Add any new terms from this definition to learned_terms.yaml."""
    terms_path = terms_path or DATA_DIR / "learned_terms.yaml"

    if terms_path.exists():
        with open(terms_path) as f:
            terms = yaml.safe_load(f) or {}
    else:
        terms = {}

    # Use the label as a potential new term
    label = defn.get("label", "")
    if not label:
        return

    key = re.sub(r'[^a-z0-9]+', '_', label.lower()).strip('_')
    if key in terms:
        return  # already exists

    bb = defn.get("backbone", {})
    terms[key] = {
        "term": label,
        "aliases": [],
        "category": defn.get("domain", ""),
        "tables": bb.get("tables", []),
        "route": " -> ".join(bb.get("tables", [])),
        "confirmed_by": defn.get("created_by", "anonymous"),
        "confirmed_date": date.today().isoformat(),
        "source_definition": defn["definition_name"],
    }

    with open(terms_path, "w") as f:
        yaml.dump(terms, f, default_flow_style=False,
                  sort_keys=False, allow_unicode=True)
