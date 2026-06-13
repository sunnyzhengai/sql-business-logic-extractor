"""LLM interpreter — classifies user intent and identifies concepts.

The LLM's role is ONLY natural language understanding:
  - Classify what the user wants (add tables, filter, question, etc.)
  - Identify concepts ("diabetic" → diagnosis, "ER" → emergency dept)
  - Map NL to search terms

The LLM does NOT:
  - Decide which tables to join (the FK graph does that)
  - Generate column names (the schema does that)
  - Write SQL (the query builder does that)
"""

from __future__ import annotations

import json
from pathlib import Path

import openai
import yaml

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def classify_and_extract(
    user_message: str,
    graph_state: list[dict],
    available_definitions: str,
    available_terms: str,
) -> dict:
    """Use LLM to classify intent and extract structured concepts.

    Returns:
      action: "add_concept" | "add_filter" | "remove" | "question"
      concepts: [str] — business concepts the user mentioned
      filter_description: str — what to filter by (NL, not SQL)
      filter_type: "date" | "status" | "threshold" | "value" | None
      filter_value: str — the value/range (e.g., "this year", ">3", "active")
      table_hint: str — if user mentioned a specific table
      explanation: str — what the LLM understood
    """

    system_prompt = f"""You are a healthcare data query assistant. The user is building a SQL query
step by step. Your job is to CLASSIFY their intent and EXTRACT concepts — NOT to decide
tables or write SQL.

CURRENT QUERY (tables already joined):
{json.dumps(graph_state, indent=2) if graph_state else "Empty — just PATIENT table"}

KNOWN BUSINESS DEFINITIONS (reusable building blocks):
{available_definitions}

KNOWN TERMS:
{available_terms}

Classify the user's message into ONE action and extract concepts. Return JSON:

{{
  "action": "add_concept" | "add_filter" | "remove" | "question",
  "explanation": "one sentence: what the user wants",
  "concepts": ["list of business concepts mentioned, e.g., 'diabetes', 'ER visits', 'no-show'"],
  "filter_description": "if action=add_filter: what to filter by in plain English",
  "filter_type": "date | status | threshold | value | null",
  "filter_value": "the specific value, e.g., 'this year', '>3', 'active', 'Emergency'",
  "table_hint": "if user mentioned a specific table name, otherwise null"
}}

RULES:
- "how many X" or "find X patients" → action = "add_concept"
- "filter to X" / "only X" / "in this year" / "restrict to" → action = "add_filter"
- "remove X" / "drop X" → action = "remove"
- "why is" / "explain" / "what does" → action = "question"
- concepts should be BUSINESS terms, not table names
- Today's date is 2026-06-13
- Return ONLY valid JSON"""

    try:
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {
            "action": "question",
            "explanation": f"LLM error: {str(e)}",
            "concepts": [],
        }


def build_definitions_summary() -> str:
    """Compact summary of available definitions for the LLM prompt."""
    glossary_dir = DATA_DIR / "definition_glossary"
    if not glossary_dir.exists():
        return "None"
    lines = []
    for yf in sorted(glossary_dir.glob("*.yaml")):
        with open(yf) as f:
            d = yaml.safe_load(f)
        label = d.get("label", "")
        tables = ", ".join(d.get("backbone", {}).get("tables", []))
        filters = "; ".join(
            f.get("english", "") for f in d.get("backbone", {}).get("characteristic_filters", []))
        lines.append(f"- {label} [tables: {tables}] [filters: {filters}]")
    return "\n".join(lines)


def build_terms_summary() -> str:
    """Compact summary of known terms for the LLM prompt."""
    terms_path = DATA_DIR / "learned_terms.yaml"
    if not terms_path.exists():
        return "None"
    with open(terms_path) as f:
        terms = yaml.safe_load(f) or {}
    lines = []
    for key, term in terms.items():
        aliases = ", ".join(term.get("aliases", [])[:3])
        lines.append(f"- {term.get('term', key)} ({term.get('category', '')}): [{aliases}]")
    return "\n".join(lines)
