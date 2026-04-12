#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- L5: Compare

Compares resolved lineage (L3 output) across multiple SQL queries to find:
  - Exact duplicates (same resolved expression + base columns)
  - Structural matches (same pattern, different base tables/columns)
  - Semantic matches (same transformation type + overlapping base tables)
  - Conflicts (same name, different logic)

This is more accurate than comparing normalized expressions because:
  - CTEs are fully inlined to base tables
  - Comparison is on actual business logic, not SQL structure
  - Filters are included in the comparison

Pipeline: L1 (parse) → L2 (normalize) → L3 (resolve) → L4 (translate) → L5 (compare)
"""

import hashlib
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from resolve import resolve_query, resolved_to_dict


# ---------------------------------------------------------------------------
# Comparison structures
# ---------------------------------------------------------------------------

@dataclass
class ResolvedDefinition:
    """A single resolved column definition for comparison."""
    id: str                          # unique ID: query_label:column_name
    query_label: str                 # source query identifier
    column_name: str
    column_type: str                 # passthrough, calculated, case, aggregate, window, etc.
    resolved_expression: str         # fully resolved SQL expression
    base_tables: list[str] = field(default_factory=list)
    base_columns: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)

    # Computed signatures
    exact_signature: str = ""        # hash of resolved_expression + base_columns (sorted)
    structural_signature: str = ""   # hash of abstracted pattern

    def __post_init__(self):
        self.exact_signature = self._compute_exact_signature()
        self.structural_signature = self._compute_structural_signature()

    def _compute_exact_signature(self) -> str:
        """Hash of the exact resolved expression + sorted base columns."""
        content = self.resolved_expression.lower().strip()
        content += "|" + ",".join(sorted(self.base_columns))
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _compute_structural_signature(self) -> str:
        """Hash of abstracted pattern (columns/tables replaced with placeholders)."""
        pattern = self._abstract_expression(self.resolved_expression)
        return hashlib.sha256(pattern.encode()).hexdigest()[:16]

    def _abstract_expression(self, expr: str) -> str:
        """Replace specific column/table references with placeholders."""
        if not expr:
            return ""

        # Normalize whitespace
        result = re.sub(r'\s+', ' ', expr.lower().strip())

        # SQL keywords to preserve
        sql_keywords = {
            'select', 'from', 'where', 'and', 'or', 'not', 'in', 'is', 'null',
            'case', 'when', 'then', 'else', 'end', 'as', 'on', 'join', 'left',
            'right', 'inner', 'outer', 'full', 'cross', 'union', 'all', 'distinct',
            'group', 'by', 'having', 'order', 'asc', 'desc', 'limit', 'offset',
            'count', 'sum', 'avg', 'min', 'max', 'row_number', 'rank', 'dense_rank',
            'over', 'partition', 'rows', 'between', 'unbounded', 'preceding',
            'following', 'current', 'row', 'datediff', 'dateadd', 'cast', 'convert',
            'coalesce', 'nullif', 'isnull', 'like', 'exists', 'any', 'some',
            'day', 'month', 'year', 'hour', 'minute', 'second', 'getdate', 'now',
            'float', 'int', 'integer', 'varchar', 'char', 'numeric', 'decimal'
        }

        # Replace all identifiers with placeholder
        # Use $C$ as placeholder (won't match word boundaries)
        def replace_if_not_keyword(match):
            word = match.group(0)
            if word in sql_keywords:
                return word
            return '$C$'

        # Replace table.column patterns first
        result = re.sub(r'[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*', '$C$', result)

        # Replace remaining standalone identifiers
        result = re.sub(r'\b[a-z_][a-z0-9_]*\b', replace_if_not_keyword, result)

        # Replace string literals with <str>
        result = re.sub(r"'[^']*'", '<str>', result)

        # Replace numeric literals with <num>
        result = re.sub(r'\b\d+\.?\d*\b', '<num>', result)

        return result


@dataclass
class MatchGroup:
    """A group of definitions that match each other."""
    match_type: str              # exact, structural, semantic, conflict
    signature: str               # the signature they share
    description: str             # human-readable description
    definitions: list[dict] = field(default_factory=list)
    similarity: float = 1.0      # 1.0 for exact, <1.0 for fuzzy
    pattern: str = ""            # the abstracted pattern (for structural matches)


@dataclass
class ConflictGroup:
    """A group of definitions with same name but different logic."""
    column_name: str
    description: str
    definitions: list[dict] = field(default_factory=list)
    differences: list[str] = field(default_factory=list)  # What's different


@dataclass
class ComparisonReport:
    """Full comparison report across multiple queries."""
    total_definitions: int = 0
    total_queries: int = 0
    exact_duplicates: list[MatchGroup] = field(default_factory=list)
    structural_matches: list[MatchGroup] = field(default_factory=list)
    semantic_matches: list[MatchGroup] = field(default_factory=list)
    conflicts: list[ConflictGroup] = field(default_factory=list)  # Same name, different logic
    unique_definitions: list[dict] = field(default_factory=list)
    summary: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _def_to_dict(d: ResolvedDefinition) -> dict:
    """Convert a ResolvedDefinition to a summary dict."""
    return {
        "id": d.id,
        "query_label": d.query_label,
        "column_name": d.column_name,
        "column_type": d.column_type,
        "resolved_expression": d.resolved_expression,
        "base_tables": d.base_tables,
        "base_columns": d.base_columns,
        "filters": d.filters,
    }


def _describe_group(defs: list[ResolvedDefinition]) -> str:
    """Generate a human-readable description of a match group."""
    if not defs:
        return ""

    first = defs[0]

    # Describe based on type
    if first.column_type == "aggregate":
        return f"Aggregation: {first.resolved_expression[:100]}"
    elif first.column_type == "case":
        return f"CASE expression with {len(first.base_tables)} source table(s)"
    elif first.column_type == "window":
        return f"Window function on {', '.join(first.base_tables)}"
    elif first.column_type == "calculated":
        return f"Calculation: {first.resolved_expression[:100]}"
    else:
        return f"{first.column_type}: {first.resolved_expression[:80]}"


# ---------------------------------------------------------------------------
# Main comparator
# ---------------------------------------------------------------------------

class LineageComparator:
    """Compares resolved lineage across SQL queries."""

    def __init__(self):
        self.definitions: list[ResolvedDefinition] = []
        self.query_labels: set[str] = set()

    def add_query(self, sql: str, query_label: str = "", dialect: str = None):
        """Resolve a SQL query and add its columns to the comparison pool."""
        label = query_label or f"query_{len(self.query_labels) + 1}"

        # Get L5 resolved output
        resolved = resolve_query(sql, dialect=dialect)
        resolved_dict = resolved_to_dict(resolved)

        # Convert to ResolvedDefinition objects
        for col in resolved_dict.get('columns', []):
            defn = ResolvedDefinition(
                id=f"{label}:{col['name']}",
                query_label=label,
                column_name=col['name'],
                column_type=col.get('type', 'unknown'),
                resolved_expression=col.get('resolved_expression', ''),
                base_tables=col.get('base_tables', []),
                base_columns=col.get('base_columns', []),
                filters=col.get('filters', []),
            )
            self.definitions.append(defn)

        self.query_labels.add(label)

    def add_l5_json(self, l5_json_path: str, query_label: str = ""):
        """Add definitions from an L5 JSON output file."""
        with open(l5_json_path, 'r') as f:
            data = json.load(f)

        label = query_label or l5_json_path

        for col in data.get('columns', []):
            defn = ResolvedDefinition(
                id=f"{label}:{col['name']}",
                query_label=label,
                column_name=col['name'],
                column_type=col.get('type', 'unknown'),
                resolved_expression=col.get('resolved_expression', ''),
                base_tables=col.get('base_tables', []),
                base_columns=col.get('base_columns', []),
                filters=col.get('filters', []),
            )
            self.definitions.append(defn)

        self.query_labels.add(label)

    def compare(self, skip_trivial: bool = True) -> ComparisonReport:
        """Run all comparisons and produce a report.

        Args:
            skip_trivial: If True, skip passthrough/star columns in matching
        """
        report = ComparisonReport(
            total_definitions=len(self.definitions),
            total_queries=len(self.query_labels),
        )

        # Filter candidates
        if skip_trivial:
            candidates = [d for d in self.definitions
                         if d.column_type not in ('passthrough', 'star', 'literal')]
        else:
            candidates = self.definitions

        # Find matches
        exact = self._find_exact_duplicates(candidates)
        report.exact_duplicates = exact

        structural = self._find_structural_matches(candidates, exact)
        report.structural_matches = structural

        semantic = self._find_semantic_matches(candidates, exact, structural)
        report.semantic_matches = semantic

        # Find conflicts: same column name, different logic
        conflicts = self._find_conflicts(candidates, exact)
        report.conflicts = conflicts

        # Find unique definitions
        matched_ids = set()
        for group in exact + structural + semantic:
            for d in group.definitions:
                matched_ids.add(d["id"])

        report.unique_definitions = [
            _def_to_dict(d) for d in candidates
            if d.id not in matched_ids
        ]

        # Summary
        report.summary = {
            "total_definitions": len(self.definitions),
            "total_queries": len(self.query_labels),
            "non_trivial_definitions": len(candidates),
            "exact_duplicate_groups": len(exact),
            "exact_duplicate_definitions": sum(len(g.definitions) for g in exact),
            "structural_match_groups": len(structural),
            "structural_match_definitions": sum(len(g.definitions) for g in structural),
            "semantic_match_groups": len(semantic),
            "conflict_groups": len(conflicts),
            "unique_definitions": len(report.unique_definitions),
        }

        return report

    def _find_exact_duplicates(self, candidates: list[ResolvedDefinition]) -> list[MatchGroup]:
        """Find columns with identical resolved expressions."""
        sig_groups: dict[str, list[ResolvedDefinition]] = defaultdict(list)

        for d in candidates:
            sig_groups[d.exact_signature].append(d)

        results = []
        for sig, defs in sig_groups.items():
            # Only keep groups with definitions from 2+ different queries
            query_sources = set(d.query_label for d in defs)
            if len(defs) >= 2 and len(query_sources) >= 2:
                results.append(MatchGroup(
                    match_type="exact",
                    signature=sig,
                    description=_describe_group(defs),
                    definitions=[_def_to_dict(d) for d in defs],
                    similarity=1.0,
                ))

        return results

    def _find_structural_matches(self, candidates: list[ResolvedDefinition],
                                  exact_groups: list[MatchGroup]) -> list[MatchGroup]:
        """Find columns with same pattern but different tables/columns."""
        # Exclude already matched
        exact_ids = set()
        for g in exact_groups:
            for d in g.definitions:
                exact_ids.add(d["id"])

        remaining = [d for d in candidates if d.id not in exact_ids]

        # Group by structural signature
        sig_groups: dict[str, list[ResolvedDefinition]] = defaultdict(list)
        for d in remaining:
            if d.structural_signature:
                sig_groups[d.structural_signature].append(d)

        results = []
        for sig, defs in sig_groups.items():
            query_sources = set(d.query_label for d in defs)
            if len(defs) >= 2 and len(query_sources) >= 2:
                # Get the abstracted pattern from first definition
                pattern = defs[0]._abstract_expression(defs[0].resolved_expression)

                results.append(MatchGroup(
                    match_type="structural",
                    signature=sig,
                    description=f"Same pattern across {len(query_sources)} queries",
                    definitions=[_def_to_dict(d) for d in defs],
                    similarity=0.9,
                    pattern=pattern,
                ))

        return results

    def _find_semantic_matches(self, candidates: list[ResolvedDefinition],
                                exact_groups: list[MatchGroup],
                                structural_groups: list[MatchGroup]) -> list[MatchGroup]:
        """Find columns with same type + overlapping base tables."""
        # Exclude already matched
        matched_ids = set()
        for g in exact_groups + structural_groups:
            for d in g.definitions:
                matched_ids.add(d["id"])

        remaining = [d for d in candidates if d.id not in matched_ids]

        # Group by (column_type, frozenset of base_tables)
        type_table_groups: dict[tuple, list[ResolvedDefinition]] = defaultdict(list)

        for d in remaining:
            if d.base_tables:
                key = (d.column_type, frozenset(d.base_tables))
                type_table_groups[key].append(d)

        results = []
        for (col_type, tables), defs in type_table_groups.items():
            query_sources = set(d.query_label for d in defs)
            if len(defs) >= 2 and len(query_sources) >= 2:
                results.append(MatchGroup(
                    match_type="semantic",
                    signature=f"{col_type}:{','.join(sorted(tables))}",
                    description=f"Same type '{col_type}' on tables: {', '.join(sorted(tables))}",
                    definitions=[_def_to_dict(d) for d in defs],
                    similarity=0.7,
                ))

        return results

    def _find_conflicts(self, candidates: list[ResolvedDefinition],
                        exact_groups: list[MatchGroup]) -> list[ConflictGroup]:
        """Find columns with same name but different logic across queries.

        This is critical for data governance - same business term defined differently.
        """
        # Get IDs of exact matches (these are NOT conflicts)
        exact_ids = set()
        for g in exact_groups:
            for d in g.definitions:
                exact_ids.add(d["id"])

        # Group all definitions by column name (case-insensitive)
        name_groups: dict[str, list[ResolvedDefinition]] = defaultdict(list)
        for d in candidates:
            name_lower = d.column_name.lower()
            name_groups[name_lower].append(d)

        results = []
        for name, defs in name_groups.items():
            # Need definitions from 2+ different queries
            query_sources = set(d.query_label for d in defs)
            if len(query_sources) < 2:
                continue

            # Check if ALL definitions in this group are exact matches to each other
            all_exact_match = all(d.id in exact_ids for d in defs)
            if all_exact_match:
                continue  # Not a conflict - they all match

            # Check if there are different signatures (different logic)
            signatures = set(d.exact_signature for d in defs)
            if len(signatures) <= 1:
                continue  # All same logic

            # Found a conflict! Same name, different logic
            differences = self._describe_differences(defs)

            results.append(ConflictGroup(
                column_name=defs[0].column_name,
                description=f"'{defs[0].column_name}' defined differently in {len(query_sources)} queries",
                definitions=[_def_to_dict(d) for d in defs],
                differences=differences,
            ))

        return results

    def _describe_differences(self, defs: list[ResolvedDefinition]) -> list[str]:
        """Describe what's different between definitions with same name."""
        differences = []

        # Check for different source tables
        all_tables = [set(d.base_tables) for d in defs]
        if len(set(frozenset(t) for t in all_tables)) > 1:
            table_summary = [f"{d.query_label}: {', '.join(d.base_tables) or 'none'}" for d in defs]
            differences.append(f"Different source tables: {'; '.join(table_summary)}")

        # Check for different expressions
        expressions = set(d.resolved_expression for d in defs)
        if len(expressions) > 1:
            differences.append(f"Different expressions ({len(expressions)} variations)")

        # Check for different column types
        types = set(d.column_type for d in defs)
        if len(types) > 1:
            differences.append(f"Different types: {', '.join(types)}")

        # Check for different filters
        all_filters = [set(d.filters) for d in defs]
        if len(set(frozenset(f) for f in all_filters)) > 1:
            filter_counts = [f"{d.query_label}: {len(d.filters)} filters" for d in defs]
            differences.append(f"Different filters: {'; '.join(filter_counts)}")

        return differences


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def report_to_dict(report: ComparisonReport) -> dict:
    """Convert a ComparisonReport to a JSON-serializable dict."""
    return {
        "summary": report.summary,
        "conflicts": [
            {
                "column_name": g.column_name,
                "description": g.description,
                "differences": g.differences,
                "definitions": g.definitions,
            }
            for g in report.conflicts
        ],
        "exact_duplicates": [
            {
                "match_type": g.match_type,
                "signature": g.signature,
                "description": g.description,
                "similarity": g.similarity,
                "definitions": g.definitions,
            }
            for g in report.exact_duplicates
        ],
        "structural_matches": [
            {
                "match_type": g.match_type,
                "signature": g.signature,
                "description": g.description,
                "similarity": g.similarity,
                "pattern": g.pattern,
                "definitions": g.definitions,
            }
            for g in report.structural_matches
        ],
        "semantic_matches": [
            {
                "match_type": g.match_type,
                "signature": g.signature,
                "description": g.description,
                "similarity": g.similarity,
                "definitions": g.definitions,
            }
            for g in report.semantic_matches
        ],
        "unique_definitions": report.unique_definitions,
    }


def format_report(report: ComparisonReport, format: str = 'json') -> str:
    """Format the comparison report."""
    if format == 'json':
        return json.dumps(report_to_dict(report), indent=2)

    # Text format
    lines = []
    lines.append("=" * 80)
    lines.append("LINEAGE-BASED COMPARISON REPORT")
    lines.append("=" * 80)
    lines.append("")

    # Summary
    lines.append("# SUMMARY")
    lines.append("")
    s = report.summary
    lines.append(f"   Total Queries: {s.get('total_queries', 0)}")
    lines.append(f"   Total Definitions: {s.get('total_definitions', 0)}")
    lines.append(f"   Non-Trivial Definitions: {s.get('non_trivial_definitions', 0)}")
    lines.append("")
    if s.get('conflict_groups', 0) > 0:
        lines.append(f"   ⚠️  CONFLICTS (same name, different logic): {s.get('conflict_groups', 0)}")
    lines.append(f"   Exact Duplicate Groups: {s.get('exact_duplicate_groups', 0)}")
    lines.append(f"   Structural Match Groups: {s.get('structural_match_groups', 0)}")
    lines.append(f"   Semantic Match Groups: {s.get('semantic_match_groups', 0)}")
    lines.append(f"   Unique Definitions: {s.get('unique_definitions', 0)}")
    lines.append("")
    lines.append("=" * 80)
    lines.append("")

    # Conflicts (most important - show first)
    if report.conflicts:
        lines.append("# ⚠️  CONFLICTS - SAME NAME, DIFFERENT LOGIC")
        lines.append("   (These require review for data governance)")
        lines.append("")
        for i, group in enumerate(report.conflicts, 1):
            lines.append(f"## Conflict {i}: {group.description}")
            lines.append("")
            for diff in group.differences:
                lines.append(f"   ❌ {diff}")
            lines.append("")
            lines.append("   Definitions:")
            for d in group.definitions:
                lines.append(f"   - {d['query_label']}: {d['column_name']} ({d['column_type']})")
                lines.append(f"     Expression: {d['resolved_expression'][:100]}...")
                lines.append(f"     Tables: {', '.join(d['base_tables'])}")
            lines.append("")
        lines.append("-" * 80)
        lines.append("")

    # Exact duplicates
    if report.exact_duplicates:
        lines.append("# EXACT DUPLICATES")
        lines.append("   (Same resolved expression + base columns)")
        lines.append("")
        for i, group in enumerate(report.exact_duplicates, 1):
            lines.append(f"## Group {i}: {group.description}")
            lines.append("")
            for d in group.definitions:
                lines.append(f"   - {d['query_label']}: {d['column_name']} ({d['column_type']})")
                lines.append(f"     Expression: {d['resolved_expression'][:80]}...")
            lines.append("")
        lines.append("-" * 80)
        lines.append("")

    # Structural matches
    if report.structural_matches:
        lines.append("# STRUCTURAL MATCHES")
        lines.append("   (Same pattern, different tables/columns)")
        lines.append("")
        for i, group in enumerate(report.structural_matches, 1):
            lines.append(f"## Group {i}: {group.description}")
            lines.append(f"   Pattern: {group.pattern[:80]}")
            lines.append("")
            for d in group.definitions:
                lines.append(f"   - {d['query_label']}: {d['column_name']} ({d['column_type']})")
                lines.append(f"     Tables: {', '.join(d['base_tables'])}")
            lines.append("")
        lines.append("-" * 80)
        lines.append("")

    # Semantic matches
    if report.semantic_matches:
        lines.append("# SEMANTIC MATCHES")
        lines.append("   (Same type + same base tables)")
        lines.append("")
        for i, group in enumerate(report.semantic_matches, 1):
            lines.append(f"## Group {i}: {group.description}")
            lines.append("")
            for d in group.definitions:
                lines.append(f"   - {d['query_label']}: {d['column_name']}")
            lines.append("")
        lines.append("-" * 80)
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    import glob as globmod

    parser = argparse.ArgumentParser(
        description="Compare SQL queries based on resolved lineage (L5)"
    )
    parser.add_argument("inputs", nargs="+",
                        help="SQL files or L5 JSON files to compare")
    parser.add_argument("--json-input", action="store_true",
                        help="Treat inputs as L5 JSON files instead of SQL")
    parser.add_argument("--dialect", "-d", default=None,
                        help="SQL dialect for parsing")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--text", action="store_true",
                        help="Output human-readable text instead of JSON")
    parser.add_argument("--include-trivial", action="store_true",
                        help="Include passthrough/literal columns in comparison")

    args = parser.parse_args()

    # Expand glob patterns
    input_files = []
    for pattern in args.inputs:
        matched = globmod.glob(pattern)
        if matched:
            input_files.extend(matched)
        else:
            input_files.append(pattern)

    if len(input_files) < 2:
        print("Error: Need at least 2 files to compare")
        return

    print(f"Comparing {len(input_files)} files...")

    comparator = LineageComparator()

    for fpath in input_files:
        import os
        label = os.path.splitext(os.path.basename(fpath))[0]
        print(f"  Loading: {label}")

        if args.json_input or fpath.endswith('.json'):
            comparator.add_l5_json(fpath, query_label=label)
        else:
            with open(fpath, 'r') as f:
                sql = f.read()
            comparator.add_query(sql, query_label=label, dialect=args.dialect)

    print(f"  Running comparison...")
    report = comparator.compare(skip_trivial=not args.include_trivial)

    output_format = 'text' if args.text else 'json'
    output = format_report(report, output_format)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f"\nOutput saved to: {args.output}")
    else:
        print("\n" + output)

    # Print summary
    s = report.summary
    print(f"\nComparison complete:")
    print(f"  - {s.get('exact_duplicate_groups', 0)} exact duplicate groups")
    print(f"  - {s.get('structural_match_groups', 0)} structural match groups")
    print(f"  - {s.get('semantic_match_groups', 0)} semantic match groups")


if __name__ == "__main__":
    main()
