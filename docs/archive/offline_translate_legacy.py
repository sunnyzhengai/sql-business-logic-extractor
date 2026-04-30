#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- L4: Translate (Offline/Template-based)

Takes L3 lineage resolution output and translates each column's technical
definition into plain English business descriptions using:
  - Pattern templates (DATEDIFF, CASE, SUM, etc.)
  - Schema lookup (clarity_schema.yaml column descriptions)
  - Abbreviation expansion (HOSP_ADMSN → Hospital Admission)

NO LLM REQUIRED - fully offline, works in restricted environments.

Pipeline: L1 (parse) → L2 (normalize) → L3 (resolve) → L4 (translate) → L5 (compare)
"""

import json
import os
import re
import yaml
from typing import Optional


# ---------------------------------------------------------------------------
# Schema Loading (same as llm_translate.py)
# ---------------------------------------------------------------------------

def load_schema(yaml_path: str) -> dict:
    """Load and index the clarity_schema.yaml for fast lookup."""
    with open(yaml_path, 'r') as f:
        raw = yaml.safe_load(f)

    schema = {'tables': {}, 'enums': {}, 'columns': {}}

    for table in raw.get('tables', []):
        table_name = table.get('name', '').upper()
        table_desc = table.get('description', '')

        columns = {}
        for col in table.get('columns', []):
            col_name = col.get('name', '').upper()
            col_desc = col.get('description', '')
            columns[col_name] = col_desc
            # Also index by table.column for direct lookup
            schema['columns'][f"{table_name}.{col_name}"] = col_desc

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


# ---------------------------------------------------------------------------
# Abbreviation Expansion
# ---------------------------------------------------------------------------

# Common healthcare/Epic abbreviations
ABBREVIATIONS = {
    'ADMSN': 'Admission',
    'ADM': 'Admission',
    'DISCH': 'Discharge',
    'PAT': 'Patient',
    'ENC': 'Encounter',
    'HSP': 'Hospital',
    'HOSP': 'Hospital',
    'ACCT': 'Account',
    'DX': 'Diagnosis',
    'PROC': 'Procedure',
    'MED': 'Medication',
    'ORD': 'Order',
    'DEPT': 'Department',
    'LOC': 'Location',
    'SER': 'Service/Provider',
    'PROV': 'Provider',
    'APPT': 'Appointment',
    'SCHED': 'Scheduled',
    'CSN': 'Contact Serial Number',
    'MRN': 'Medical Record Number',
    'DOB': 'Date of Birth',
    'LOS': 'Length of Stay',
    'ED': 'Emergency Department',
    'IP': 'Inpatient',
    'OP': 'Outpatient',
    'OBS': 'Observation',
    'ICU': 'Intensive Care Unit',
    'ADT': 'Admit/Discharge/Transfer',
    'HX': 'History',
    'TX': 'Treatment',
    'RX': 'Prescription',
    'PX': 'Procedure',
    'FIN': 'Financial',
    'INS': 'Insurance',
    'AUTH': 'Authorization',
    'REF': 'Referral',
    'XFER': 'Transfer',
    'TRANS': 'Transaction',
    'AMT': 'Amount',
    'QTY': 'Quantity',
    'CNT': 'Count',
    'NUM': 'Number',
    'DT': 'Date',
    'TM': 'Time',
    'DTTM': 'Date/Time',
    'YR': 'Year',
    'MTH': 'Month',
    'STAT': 'Status',
    'CAT': 'Category',
    'CLS': 'Class',
    'TYP': 'Type',
    'CD': 'Code',
    'ID': 'Identifier',
    'DESC': 'Description',
    'NM': 'Name',
    'ADDR': 'Address',
    'PH': 'Phone',
    'FAX': 'Fax',
    'ZIP': 'ZIP Code',
    'ST': 'State',
    'CTY': 'City',
    'CNTRY': 'Country',
}


def expand_abbreviations(text: str) -> str:
    """Expand common abbreviations in column/table names."""
    # Split by underscores
    parts = text.upper().split('_')
    expanded = []

    for part in parts:
        if part in ABBREVIATIONS:
            expanded.append(ABBREVIATIONS[part])
        else:
            # Title case for unknown parts
            expanded.append(part.title())

    return ' '.join(expanded)


def get_column_description(schema: dict, table_name: str, column_name: str) -> str:
    """Get description for a column, fall back to abbreviation expansion."""
    key = f"{table_name.upper()}.{column_name.upper()}"
    if key in schema['columns']:
        return schema['columns'][key]

    # Try just the column name in any table
    column_upper = column_name.upper()
    for table_data in schema['tables'].values():
        if column_upper in table_data['columns']:
            return table_data['columns'][column_upper]

    # Fall back to abbreviation expansion
    return expand_abbreviations(column_name)


def get_enum_value(schema: dict, table_name: str, code: str) -> str:
    """Get enum value name from ZC_ table."""
    table_upper = table_name.upper()
    if table_upper in schema['enums']:
        return schema['enums'][table_upper].get(str(code), f"Code {code}")
    return f"Code {code}"


# ---------------------------------------------------------------------------
# Pattern Templates
# ---------------------------------------------------------------------------

def translate_datediff(expr: str, schema: dict) -> str:
    """Translate DATEDIFF expressions."""
    # DATEDIFF(unit, start, end)
    match = re.match(
        r'DATEDIFF\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)',
        expr,
        re.IGNORECASE
    )
    if not match:
        return None

    unit = match.group(1).upper()
    start_col = match.group(2).strip()
    end_col = match.group(3).strip()

    # Clean up column references
    start_desc = describe_column_ref(start_col, schema)
    end_desc = describe_column_ref(end_col, schema)

    unit_names = {
        'DAY': 'days',
        'HOUR': 'hours',
        'MINUTE': 'minutes',
        'MONTH': 'months',
        'YEAR': 'years',
        'WEEK': 'weeks',
    }
    unit_name = unit_names.get(unit, unit.lower() + 's')

    # Special case: age calculation
    if 'BIRTH' in start_col.upper() and unit == 'YEAR':
        return f"Age in years (at {end_desc})"

    return f"Number of {unit_name} between {start_desc} and {end_desc}"


def translate_case(expr: str, schema: dict) -> str:
    """Translate CASE expressions."""
    # Count WHEN clauses
    when_count = len(re.findall(r'\bWHEN\b', expr, re.IGNORECASE))

    # Check if it's a simple categorization
    if when_count <= 3:
        return f"Categorization with {when_count} condition(s)"
    else:
        return f"Complex categorization with {when_count} conditions"


def translate_aggregate(expr: str, func: str, schema: dict) -> str:
    """Translate aggregate functions (SUM, AVG, COUNT, etc.)."""
    # Extract the column being aggregated
    match = re.match(rf'{func}\s*\(\s*(.+)\s*\)', expr, re.IGNORECASE)
    if not match:
        return None

    inner = match.group(1).strip()

    # Handle DISTINCT
    if inner.upper().startswith('DISTINCT'):
        inner = inner[8:].strip()
        distinct = "unique "
    else:
        distinct = ""

    col_desc = describe_column_ref(inner, schema)

    func_names = {
        'SUM': f"Total of {distinct}{col_desc}",
        'AVG': f"Average {distinct}{col_desc}",
        'COUNT': f"Count of {distinct}{col_desc}",
        'MIN': f"Minimum {col_desc}",
        'MAX': f"Maximum {col_desc}",
    }

    return func_names.get(func.upper(), f"{func} of {col_desc}")


def translate_window_function(expr: str, schema: dict) -> str:
    """Translate window functions (ROW_NUMBER, RANK, LAG, etc.)."""
    expr_upper = expr.upper()

    if 'ROW_NUMBER' in expr_upper:
        # Try to extract PARTITION BY
        partition_match = re.search(r'PARTITION\s+BY\s+([^)]+?)(?:\s+ORDER|\))', expr, re.IGNORECASE)
        if partition_match:
            partition_col = describe_column_ref(partition_match.group(1).strip(), schema)
            return f"Row number within each {partition_col}"
        return "Row number in sequence"

    elif 'RANK' in expr_upper or 'DENSE_RANK' in expr_upper:
        return "Ranking position"

    elif 'LAG' in expr_upper:
        return "Value from previous row"

    elif 'LEAD' in expr_upper:
        return "Value from next row"

    elif 'SUM' in expr_upper and 'OVER' in expr_upper:
        return "Running total"

    return "Window calculation"


def describe_column_ref(col_ref: str, schema: dict) -> str:
    """Get a human-readable description for a column reference."""
    col_ref = col_ref.strip()

    # Handle functions like GETDATE(), NOW()
    if re.match(r'GETDATE\s*\(\s*\)|NOW\s*\(\s*\)', col_ref, re.IGNORECASE):
        return "current date"

    # Handle table.column
    if '.' in col_ref:
        parts = col_ref.split('.')
        if len(parts) == 2:
            table, col = parts
            # Remove alias prefix (single letter)
            if len(table) <= 2:
                return expand_abbreviations(col)
            return get_column_description(schema, table, col)

    # Just column name
    return expand_abbreviations(col_ref)


# ---------------------------------------------------------------------------
# Main Translation Logic
# ---------------------------------------------------------------------------

def classify_business_domain(col_name: str, base_tables: list, expression: str) -> str:
    """Classify the business domain based on column name and context."""
    col_upper = col_name.upper()
    expr_upper = expression.upper() if expression else ""
    tables_str = ' '.join(base_tables).upper()

    # Check column name patterns
    if any(x in col_upper for x in ['AGE', 'BIRTH', 'DOB']):
        return "Patient Demographics"
    if any(x in col_upper for x in ['LOS', 'LENGTH_OF_STAY', 'STAY']):
        return "Hospital Metrics"
    if any(x in col_upper for x in ['CHARGE', 'COST', 'PAYMENT', 'AMOUNT', 'PRICE', 'FIN']):
        return "Financial"
    if any(x in col_upper for x in ['READMI', 'READMIT']):
        return "Quality Metrics"
    if any(x in col_upper for x in ['APPT', 'SCHED', 'WAIT']):
        return "Scheduling"
    if any(x in col_upper for x in ['DIAG', 'DX', 'ICD']):
        return "Clinical - Diagnosis"
    if any(x in col_upper for x in ['PROC', 'CPT', 'SURG']):
        return "Clinical - Procedures"
    if any(x in col_upper for x in ['MED', 'DRUG', 'RX', 'PHARM']):
        return "Clinical - Medications"
    if any(x in col_upper for x in ['REFER', 'REF_']):
        return "Referrals"
    if any(x in col_upper for x in ['RANK', 'ROW_NUM', 'SEQ']):
        return "Ordering/Ranking"

    # Check table patterns
    if 'ARPB' in tables_str or 'BILLING' in tables_str:
        return "Billing"
    if 'HSP_ACCOUNT' in tables_str:
        return "Hospital Accounting"
    if 'PAT_ENC' in tables_str:
        return "Patient Encounters"
    if 'REFERRAL' in tables_str:
        return "Referrals"

    return "General"


def translate_expression(expr: str, col_type: str, schema: dict) -> str:
    """Translate a SQL expression to plain English."""
    if not expr:
        return "No expression"

    expr_upper = expr.upper()

    # Try pattern-based translation

    # DATEDIFF
    if 'DATEDIFF' in expr_upper:
        result = translate_datediff(expr, schema)
        if result:
            # Check for +1 adjustment
            if re.search(r'\+\s*1\s*$', expr):
                result += " (plus 1)"
            return result

    # CASE statement
    if expr_upper.startswith('CASE'):
        return translate_case(expr, schema)

    # Aggregates
    for func in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']:
        if expr_upper.startswith(func):
            result = translate_aggregate(expr, func, schema)
            if result:
                return result

    # Window functions
    if 'OVER' in expr_upper:
        return translate_window_function(expr, schema)

    # EXISTS subquery
    if 'EXISTS' in expr_upper:
        return "Flag indicating existence of related records"

    # COALESCE
    if 'COALESCE' in expr_upper:
        return "First non-null value from multiple columns"

    # CAST/CONVERT
    if 'CAST' in expr_upper or 'CONVERT' in expr_upper:
        return "Type-converted value"

    # Simple column reference (passthrough)
    if col_type == 'passthrough':
        return f"Direct value: {describe_column_ref(expr, schema)}"

    # Fallback: describe based on column type
    type_descriptions = {
        'calculated': 'Calculated value',
        'case': 'Conditional categorization',
        'aggregate': 'Aggregated value',
        'window': 'Window calculation',
        'literal': 'Constant value',
    }

    return type_descriptions.get(col_type, 'Derived value')


def translate_filters(filters: list, schema: dict) -> str:
    """Translate filter conditions to plain English."""
    if not filters:
        return ""

    descriptions = []
    for f in filters:
        f_upper = f.upper()

        # Handle IS NOT NULL
        if 'IS NOT NULL' in f_upper:
            col = f.split()[0]
            descriptions.append(f"where {describe_column_ref(col, schema)} exists")
            continue

        # Handle enum values (e.g., ADT_PAT_CLASS_C = 1)
        match = re.match(r'(\w+)\s*=\s*(\d+)', f)
        if match:
            col, val = match.groups()
            # Try to find enum meaning
            col_desc = describe_column_ref(col, schema)
            # Check for ZC_ enum tables
            for table_name, enum_map in schema.get('enums', {}).items():
                if val in enum_map:
                    descriptions.append(f"for {enum_map[val]} ({col_desc} = {val})")
                    break
            else:
                descriptions.append(f"where {col_desc} = {val}")
            continue

        # Generic filter
        descriptions.append(f"filtered by: {f}")

    return "; ".join(descriptions)


def translate_column(resolved_col: dict, schema: dict) -> dict:
    """Translate a single resolved column to English using templates."""
    name = resolved_col.get('name', 'unknown')
    col_type = resolved_col.get('type', 'unknown')
    expression = resolved_col.get('resolved_expression', '')
    base_tables = resolved_col.get('base_tables', [])
    base_columns = resolved_col.get('base_columns', [])
    filters = resolved_col.get('filters', [])

    # Translate the main expression
    english_def = translate_expression(expression, col_type, schema)

    # Add filter context if relevant
    filter_desc = translate_filters(filters, schema)
    if filter_desc and col_type not in ('passthrough', 'literal'):
        english_def += f" ({filter_desc})"

    # Classify business domain
    business_domain = classify_business_domain(name, base_tables, expression)

    # Build technical definition (same structure as llm_translate.py)
    technical_definition = {
        'resolved_expression': expression,
        'base_columns': base_columns,
        'base_tables': base_tables,
        'filters': filters,
        'transformation_chain': resolved_col.get('transformation_chain', [])
    }

    return {
        'column_name': name,
        'column_type': col_type,
        'technical_definition': technical_definition,
        'english_definition': english_def,
        'business_domain': business_domain,
    }


def summarize_query(column_results: list, l3_data: dict) -> dict:
    """Generate a summary of the entire SQL query."""
    # Collect all unique base tables and domains
    all_tables = set()
    all_domains = set()

    for col in column_results:
        tech_def = col.get('technical_definition', {})
        for table in tech_def.get('base_tables', []):
            all_tables.add(table)
        if col.get('business_domain'):
            all_domains.add(col['business_domain'])

    # Determine primary purpose based on domains and column types
    col_types = [col.get('column_type', '') for col in column_results]

    if 'aggregate' in col_types:
        purpose = "Aggregated reporting"
    elif 'window' in col_types:
        purpose = "Ranked/windowed analysis"
    elif 'case' in col_types:
        purpose = "Categorization and classification"
    else:
        purpose = "Data extraction"

    # Add domain context
    if 'Financial' in all_domains:
        purpose += " for financial analysis"
    elif 'Quality Metrics' in all_domains:
        purpose += " for quality metrics"
    elif 'Hospital Metrics' in all_domains:
        purpose += " for hospital operations"
    elif 'Patient Demographics' in all_domains:
        purpose += " for patient analysis"

    # Build summary
    table_list = sorted(all_tables)
    domain_list = sorted(all_domains)

    summary_text = f"Query extracts {len(column_results)} columns from {len(table_list)} table(s)"
    if domain_list:
        summary_text += f" covering {', '.join(domain_list[:3])}"
        if len(domain_list) > 3:
            summary_text += f" and {len(domain_list) - 3} more domain(s)"

    return {
        'query_summary': summary_text,
        'primary_purpose': purpose,
        'key_entities': list(all_tables)[:5],
        'key_metrics': [col['column_name'] for col in column_results
                       if col.get('column_type') in ('calculated', 'aggregate', 'case')][:5],
        'source_tables': table_list,
        'business_domains': domain_list,
        'column_count': len(column_results),
    }


def translate_query(l3_json_path: str, schema_path: str) -> dict:
    """Translate all columns in an L3 output file to English.

    This is the main entry point - same signature as llm_translate.translate_query()
    but uses templates instead of LLM.
    """
    # Load inputs
    schema = load_schema(schema_path)

    with open(l3_json_path, 'r') as f:
        l3_data = json.load(f)

    columns = l3_data.get('columns', [])

    # Translate each column
    column_results = []
    for col in columns:
        result = translate_column(col, schema)
        column_results.append(result)

    # Generate query summary
    summary = summarize_query(column_results, l3_data)

    return {
        'summary': summary,
        'columns': column_results
    }


# ---------------------------------------------------------------------------
# Output Formatting (same as llm_translate.py)
# ---------------------------------------------------------------------------

def format_output(results: dict, format: str = 'json') -> str:
    """Format translation results for output."""
    if format == 'json':
        return json.dumps(results, indent=2)

    # Text format
    lines = []
    lines.append("=" * 80)
    lines.append("SQL QUERY BUSINESS LOGIC DOCUMENTATION (Offline Translation)")
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

        # Technical Definition
        tech_def = r.get('technical_definition', {})
        lines.append("   ### Technical Definition")
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
        lines.append("")

        # English Definition
        lines.append("   ### Business Definition")
        lines.append("")
        lines.append(f"   {r.get('english_definition', 'No definition available')}")
        lines.append("")
        lines.append("-" * 80)
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="L4: Translate SQL lineage to plain English (Offline - No LLM)"
    )
    parser.add_argument("l3_json", help="Path to L3 JSON output file")
    parser.add_argument("--schema", "-s", default="clarity_schema.yaml",
                        help="Path to clarity_schema.yaml (default: clarity_schema.yaml)")
    parser.add_argument("--output", "-o", help="Output file path (without extension)")
    parser.add_argument("--text", action="store_true",
                        help="Output human-readable text instead of JSON")

    args = parser.parse_args()

    print(f"Loading L3 output: {args.l3_json}")
    print(f"Loading schema: {args.schema}")
    print("Translating columns (offline mode)...")
    print()

    results = translate_query(args.l3_json, args.schema)

    # Output both JSON and text if output path specified
    if args.output:
        # JSON output
        json_output = format_output(results, 'json')
        json_path = f"{args.output}.json"
        with open(json_path, 'w') as f:
            f.write(json_output)
        print(f"JSON saved to: {json_path}")

        # Text output
        text_output = format_output(results, 'text')
        text_path = f"{args.output}.txt"
        with open(text_path, 'w') as f:
            f.write(text_output)
        print(f"Text saved to: {text_path}")
    else:
        output_format = 'text' if args.text else 'json'
        output = format_output(results, output_format)
        print(output)

    print(f"\nTranslated {len(results.get('columns', []))} columns.")


if __name__ == "__main__":
    main()
