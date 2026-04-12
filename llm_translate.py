#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- Layer 6: LLM-based Translation

Takes L5 lineage resolution output and translates each column's technical
definition into plain English business descriptions using an LLM.

Uses OpenAI API (GPT-4) with context from clarity_schema.yaml data dictionary.
"""

import json
import os
import yaml
from typing import Optional


def load_schema(yaml_path: str) -> dict:
    """Load and index the clarity_schema.yaml for fast lookup.

    Returns:
        {
            'tables': {table_name: {description, columns: {col_name: description}}},
            'enums': {table_name: {code: name}}  # for ZC_ tables with values
        }
    """
    with open(yaml_path, 'r') as f:
        raw = yaml.safe_load(f)

    schema = {'tables': {}, 'enums': {}}

    for table in raw.get('tables', []):
        table_name = table.get('name', '').upper()
        table_desc = table.get('description', '')

        columns = {}
        for col in table.get('columns', []):
            col_name = col.get('name', '').upper()
            col_desc = col.get('description', '')
            columns[col_name] = col_desc

        schema['tables'][table_name] = {
            'description': table_desc,
            'columns': columns
        }

        # Extract enum values for ZC_ tables
        if table.get('values'):
            enum_map = {}
            for val in table['values']:
                code = val.get('code')
                name = val.get('name', '')
                if code is not None:
                    enum_map[str(code)] = name
            schema['enums'][table_name] = enum_map

    return schema


def get_table_description(schema: dict, table_name: str) -> str:
    """Get description for a table."""
    table_name = table_name.upper()
    if table_name in schema['tables']:
        return schema['tables'][table_name]['description']
    return ""


def get_column_description(schema: dict, table_name: str, column_name: str) -> str:
    """Get description for a column."""
    table_name = table_name.upper()
    column_name = column_name.upper()
    if table_name in schema['tables']:
        return schema['tables'][table_name]['columns'].get(column_name, "")
    return ""


def get_enum_values(schema: dict, table_name: str) -> dict:
    """Get enum value mappings for a ZC_ table."""
    table_name = table_name.upper()
    return schema['enums'].get(table_name, {})


def build_column_context(resolved_col: dict, schema: dict) -> str:
    """Build context string for a single resolved column.

    Args:
        resolved_col: A column dict from L5 output
        schema: The loaded schema dictionary

    Returns:
        A formatted string with all context for the LLM
    """
    name = resolved_col.get('name', 'unknown')
    col_type = resolved_col.get('type', 'unknown')
    expression = resolved_col.get('resolved_expression', resolved_col.get('expression', ''))
    base_columns = resolved_col.get('base_columns', [])
    base_tables = resolved_col.get('base_tables', [])
    filters = resolved_col.get('filters', [])
    chain = resolved_col.get('transformation_chain', [])

    # Build context parts
    parts = []

    parts.append(f"## Column: {name}")
    parts.append(f"Type: {col_type}")
    parts.append(f"\n### SQL Expression:\n```sql\n{expression}\n```")

    # Add base table descriptions
    if base_tables:
        parts.append("\n### Source Tables:")
        for table in base_tables:
            desc = get_table_description(schema, table)
            if desc:
                parts.append(f"- **{table}**: {desc}")
            else:
                parts.append(f"- **{table}**")

    # Add base column descriptions
    if base_columns:
        parts.append("\n### Source Columns:")
        for col_ref in base_columns:
            if '.' in col_ref:
                table, col = col_ref.split('.', 1)
                desc = get_column_description(schema, table, col)
                if desc:
                    parts.append(f"- **{col_ref}**: {desc}")
                else:
                    parts.append(f"- **{col_ref}**")
            else:
                parts.append(f"- **{col_ref}**")

    # Add relevant enum values if CASE expression references ZC_ tables
    for table in base_tables:
        if table.upper().startswith('ZC_'):
            enum_vals = get_enum_values(schema, table)
            if enum_vals:
                parts.append(f"\n### Reference Values ({table}):")
                for code, name in enum_vals.items():
                    parts.append(f"- {code} = {name}")

    # Add filters
    if filters:
        parts.append("\n### Filters Applied:")
        for f in filters:
            parts.append(f"- {f}")

    # Add transformation chain summary
    if chain:
        parts.append("\n### Transformation Chain:")
        for step in chain[:5]:  # Limit to first 5 steps
            scope = step.get('scope', '')
            step_name = step.get('name', '')
            step_type = step.get('type', '')
            parts.append(f"- {scope}.{step_name} ({step_type})")

    return "\n".join(parts)


def translate_column(resolved_col: dict, schema: dict, client) -> dict:
    """Translate a single resolved column to English using LLM.

    Args:
        resolved_col: A column dict from L5 output
        schema: The loaded schema dictionary
        client: OpenAI client instance

    Returns:
        Dict with column_name, english_definition, technical_summary, etc.
    """
    context = build_column_context(resolved_col, schema)
    name = resolved_col.get('name', 'unknown')
    col_type = resolved_col.get('type', 'unknown')

    system_prompt = """You translate SQL column definitions into accurate, succinct plain English.

Rules:
1. Be ACCURATE: Only describe what the SQL actually computes. Do not add interpretations, use cases, or speculate on purpose.
2. Be SUCCINCT: 1-2 sentences max. No filler words. No "This column represents..." preamble.
3. Map closely to the technical definition: If it's a CASE statement with specific conditions, list them. If it's a calculation, state what's calculated.
4. For CASE expressions: List the exact categories/values defined.
5. For calculations: State the formula in plain terms (e.g., "Years between birth date and today").
6. Do NOT add: why it matters, how it's used, what decisions it informs, or any speculation.

Output JSON:
{
  "english_definition": "Succinct, accurate description matching the SQL logic",
  "business_domain": "Category like 'Patient Demographics', 'Appointment Metrics', 'Clinical Risk', etc."
}"""

    user_prompt = f"""Translate this SQL column to plain English. Be accurate and succinct - only describe what the SQL computes, nothing more.

{context}"""

    # Preserve the full L5 technical definition
    technical_definition = {
        'resolved_expression': resolved_col.get('resolved_expression', resolved_col.get('expression', '')),
        'base_columns': resolved_col.get('base_columns', []),
        'base_tables': resolved_col.get('base_tables', []),
        'filters': resolved_col.get('filters', []),
        'transformation_chain': resolved_col.get('transformation_chain', [])
    }

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )

        llm_result = json.loads(response.choices[0].message.content)

        # Combine L5 technical definition with LLM translation
        result = {
            'column_name': name,
            'column_type': col_type,
            # L5 Technical Definition (for SQL developers)
            'technical_definition': technical_definition,
            # LLM Translation (for business users)
            'english_definition': llm_result.get('english_definition', ''),
            'business_domain': llm_result.get('business_domain', 'Unknown'),
        }

        return result

    except Exception as e:
        return {
            'column_name': name,
            'column_type': col_type,
            'technical_definition': technical_definition,
            'english_definition': f"[Translation error: {str(e)}]",
            'business_domain': "Unknown",
        }


def summarize_query(column_results: list[dict], l5_data: dict, client) -> dict:
    """Generate a summary of the entire SQL query based on column definitions.

    Args:
        column_results: List of translated column definitions from L6
        l5_data: Original L5 JSON data
        client: OpenAI client instance

    Returns:
        Dict with query summary
    """
    # Collect all unique base tables
    all_tables = set()
    all_domains = set()
    column_summaries = []

    for col in column_results:
        tech_def = col.get('technical_definition', {})
        for table in tech_def.get('base_tables', []):
            all_tables.add(table)
        if col.get('business_domain'):
            all_domains.add(col['business_domain'])
        column_summaries.append(f"- {col['column_name']}: {col.get('english_definition', '')}")

    # Build context for LLM
    context_parts = [
        f"## Source Tables ({len(all_tables)})",
        ", ".join(sorted(all_tables)),
        "",
        f"## Business Domains",
        ", ".join(sorted(all_domains)),
        "",
        f"## Output Columns ({len(column_results)})",
        "\n".join(column_summaries)
    ]
    context = "\n".join(context_parts)

    system_prompt = """You summarize SQL queries based on their output columns and source tables.

Rules:
1. Be ACCURATE: Only describe what the query actually produces.
2. Be SUCCINCT: 2-4 sentences max.
3. Identify the PRIMARY PURPOSE of the query (what business question does it answer?)
4. Mention the key entities involved (patients, referrals, appointments, etc.)
5. Note any key metrics or calculations.
6. Do NOT speculate on use cases or downstream applications.

Output JSON:
{
  "query_summary": "Succinct description of what this query produces",
  "primary_purpose": "The main business question this query answers",
  "key_entities": ["list", "of", "main", "entities"],
  "key_metrics": ["list", "of", "key", "calculations", "or", "metrics"]
}"""

    user_prompt = f"""Summarize this SQL query based on its columns and source tables:

{context}"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        result['source_tables'] = sorted(all_tables)
        result['business_domains'] = sorted(all_domains)
        result['column_count'] = len(column_results)

        return result

    except Exception as e:
        return {
            'query_summary': f"[Summary error: {str(e)}]",
            'primary_purpose': "",
            'key_entities': [],
            'key_metrics': [],
            'source_tables': sorted(all_tables),
            'business_domains': sorted(all_domains),
            'column_count': len(column_results)
        }


def translate_query(l5_json_path: str, schema_path: str, api_key: Optional[str] = None) -> dict:
    """Translate all columns in an L5 output file to English and generate query summary.

    Args:
        l5_json_path: Path to L5 JSON output
        schema_path: Path to clarity_schema.yaml
        api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)

    Returns:
        Dict with 'columns' (list of translated definitions) and 'summary' (query summary)
    """
    from openai import OpenAI

    # Initialize client
    api_key = api_key or os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("OpenAI API key required. Set OPENAI_API_KEY or pass api_key parameter.")

    client = OpenAI(api_key=api_key)

    # Load inputs
    schema = load_schema(schema_path)

    with open(l5_json_path, 'r') as f:
        l5_data = json.load(f)

    columns = l5_data.get('columns', [])

    # Translate each column
    column_results = []
    total = len(columns)

    for i, col in enumerate(columns, 1):
        name = col.get('name', 'unknown')
        print(f"  [{i}/{total}] Translating: {name}...")

        result = translate_column(col, schema, client)
        column_results.append(result)

    # Generate query summary
    print(f"  Generating query summary...")
    summary = summarize_query(column_results, l5_data, client)

    return {
        'summary': summary,
        'columns': column_results
    }


def format_output(results: dict, format: str = 'json') -> str:
    """Format translation results for output.

    Args:
        results: Dict with 'summary' and 'columns' keys
        format: 'json' or 'text'

    Returns:
        Formatted string
    """
    if format == 'json':
        return json.dumps(results, indent=2)

    # Text format
    lines = []
    lines.append("=" * 80)
    lines.append("SQL QUERY BUSINESS LOGIC DOCUMENTATION")
    lines.append("=" * 80)
    lines.append("")

    # Query Summary section
    summary = results.get('summary', {})
    lines.append("# QUERY SUMMARY")
    lines.append("")
    lines.append(f"   {summary.get('query_summary', 'No summary available')}")
    lines.append("")
    lines.append(f"   Primary Purpose: {summary.get('primary_purpose', 'Unknown')}")
    lines.append("")
    if summary.get('key_entities'):
        lines.append(f"   Key Entities: {', '.join(summary['key_entities'])}")
    if summary.get('key_metrics'):
        lines.append(f"   Key Metrics: {', '.join(summary['key_metrics'])}")
    if summary.get('source_tables'):
        lines.append(f"   Source Tables: {', '.join(summary['source_tables'])}")
    if summary.get('business_domains'):
        lines.append(f"   Business Domains: {', '.join(summary['business_domains'])}")
    lines.append(f"   Total Columns: {summary.get('column_count', 0)}")
    lines.append("")
    lines.append("=" * 80)
    lines.append("")
    lines.append("# COLUMN DEFINITIONS")
    lines.append("")

    for r in results.get('columns', []):
        lines.append(f"## {r['column_name']} ({r.get('column_type', 'unknown')})")
        lines.append(f"   Domain: {r.get('business_domain', 'Unknown')}")
        lines.append("")

        # Technical Definition (L5 output - for SQL developers)
        tech_def = r.get('technical_definition', {})
        lines.append("   ### Technical Definition (for SQL developers)")
        lines.append("")
        if tech_def.get('resolved_expression'):
            lines.append(f"   Expression: {tech_def['resolved_expression']}")
            lines.append("")
        if tech_def.get('base_tables'):
            lines.append(f"   Base Tables: {', '.join(tech_def['base_tables'])}")
        if tech_def.get('base_columns'):
            lines.append(f"   Base Columns: {', '.join(tech_def['base_columns'])}")
        if tech_def.get('filters'):
            lines.append("   Filters:")
            for f in tech_def['filters']:
                lines.append(f"     - {f}")
        if tech_def.get('transformation_chain'):
            lines.append("   Transformation Chain:")
            for step in tech_def['transformation_chain'][:5]:
                scope = step.get('scope', '')
                step_name = step.get('name', '')
                step_type = step.get('type', '')
                lines.append(f"     -> {scope}.{step_name} ({step_type})")
            if len(tech_def['transformation_chain']) > 5:
                lines.append(f"     ... ({len(tech_def['transformation_chain']) - 5} more steps)")
        lines.append("")

        # English Definition (LLM output - for business users)
        lines.append("   ### Business Definition (for business users)")
        lines.append("")
        lines.append(f"   {r.get('english_definition', 'No definition available')}")
        lines.append("")
        lines.append("-" * 80)
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="L6: Translate SQL lineage to plain English using LLM"
    )
    parser.add_argument("l5_json", help="Path to L5 JSON output file")
    parser.add_argument("--schema", "-s", default="clarity_schema.yaml",
                        help="Path to clarity_schema.yaml (default: clarity_schema.yaml)")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--text", action="store_true",
                        help="Output human-readable text instead of JSON")
    parser.add_argument("--api-key", help="OpenAI API key (or set OPENAI_API_KEY env var)")

    args = parser.parse_args()

    print(f"Loading L5 output: {args.l5_json}")
    print(f"Loading schema: {args.schema}")
    print("Translating columns...")
    print()

    results = translate_query(args.l5_json, args.schema, args.api_key)

    output_format = 'text' if args.text else 'json'
    output = format_output(results, output_format)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f"\nOutput saved to: {args.output}")
    else:
        print("\n" + output)

    print(f"\nTranslated {len(results)} columns.")


if __name__ == "__main__":
    main()
