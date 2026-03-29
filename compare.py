#!/usr/bin/env python3
"""
SQL Business Logic Extractor — Layer 3: Compare

Compares business definitions across multiple SQL queries to find:
  - Exact duplicates (same normalized expression)
  - Structural matches (same pattern, different columns/tables)
  - Semantic similarity (same category + overlapping sources)
"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from normalize import BusinessDefinition, extract_definitions, definitions_to_dict


# ---------------------------------------------------------------------------
# Comparison results
# ---------------------------------------------------------------------------

@dataclass
class MatchGroup:
    """A group of definitions that match each other."""
    match_type: str          # exact, structural, semantic
    signature: str           # the signature they share
    description: str         # human-readable description of the shared logic
    definitions: list[dict] = field(default_factory=list)  # [{id, name, query_file, ...}]
    similarity: float = 1.0  # 1.0 for exact, <1.0 for fuzzy
    difference: Optional[str] = None  # what differs (for structural/semantic)


@dataclass
class ComparisonReport:
    """Full comparison report across multiple queries."""
    total_definitions: int = 0
    total_queries: int = 0
    exact_duplicates: list[MatchGroup] = field(default_factory=list)
    structural_matches: list[MatchGroup] = field(default_factory=list)
    semantic_matches: list[MatchGroup] = field(default_factory=list)
    unique_definitions: list[dict] = field(default_factory=list)
    summary: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Comparator
# ---------------------------------------------------------------------------

class BusinessLogicComparator:
    """Compares business definitions across SQL queries."""

    def __init__(self):
        self.all_definitions: list[BusinessDefinition] = []
        self.query_labels: list[str] = []

    def add_query(self, sql: str, query_file: str = "", query_label: str = "",
                  dialect: str = None):
        """Extract definitions from a SQL query and add to comparison pool."""
        label = query_label or query_file or f"query_{len(self.query_labels) + 1}"
        defs = extract_definitions(sql, query_file=query_file, query_label=label, dialect=dialect)
        self.all_definitions.extend(defs)
        if label not in self.query_labels:
            self.query_labels.append(label)

    def add_definitions(self, defs: list[BusinessDefinition]):
        """Add pre-extracted definitions."""
        self.all_definitions.extend(defs)
        for d in defs:
            label = d.query_label or d.query_file
            if label and label not in self.query_labels:
                self.query_labels.append(label)

    def compare(self) -> ComparisonReport:
        """Run all comparisons and produce a report."""
        report = ComparisonReport(
            total_definitions=len(self.all_definitions),
            total_queries=len(self.query_labels),
        )

        exact = self._find_exact_duplicates()
        report.exact_duplicates = exact

        structural = self._find_structural_matches(exact)
        report.structural_matches = structural

        semantic = self._find_semantic_matches(exact, structural)
        report.semantic_matches = semantic

        # Find definitions that don't appear in any match group
        matched_ids = set()
        for group in exact + structural + semantic:
            for d in group.definitions:
                matched_ids.add(d["id"])

        report.unique_definitions = [
            _def_summary(d) for d in self.all_definitions
            if d.id not in matched_ids
        ]

        # Summary stats
        report.summary = {
            "total_definitions": len(self.all_definitions),
            "total_queries": len(self.query_labels),
            "exact_duplicate_groups": len(exact),
            "exact_duplicate_definitions": sum(len(g.definitions) for g in exact),
            "structural_match_groups": len(structural),
            "semantic_match_groups": len(semantic),
            "unique_definitions": len(report.unique_definitions),
        }

        return report

    def _find_exact_duplicates(self) -> list[MatchGroup]:
        """Group definitions with identical normalized expressions (same signature)."""
        sig_groups: dict[str, list[BusinessDefinition]] = defaultdict(list)
        for d in self.all_definitions:
            if d.category in ("passthrough", "star", "constant"):
                continue
            sig_groups[d.signature].append(d)

        results = []
        for sig, defs in sig_groups.items():
            # Only keep groups with definitions from different queries
            query_sources = set()
            for d in defs:
                query_sources.add(d.query_label or d.query_file or d.id)
            if len(defs) >= 2 and len(query_sources) >= 2:
                results.append(MatchGroup(
                    match_type="exact",
                    signature=sig,
                    description=_describe_group(defs),
                    definitions=[_def_summary(d) for d in defs],
                    similarity=1.0,
                ))
        return results

    def _find_structural_matches(self, exact_groups: list[MatchGroup]) -> list[MatchGroup]:
        """Group definitions with same structural pattern but different columns."""
        # Exclude definitions already in exact match groups
        exact_ids = set()
        for g in exact_groups:
            for d in g.definitions:
                exact_ids.add(d["id"])

        candidates = [d for d in self.all_definitions
                      if d.id not in exact_ids
                      and d.category not in ("passthrough", "star", "constant")]

        struct_groups: dict[str, list[BusinessDefinition]] = defaultdict(list)
        for d in candidates:
            if d.structural_signature:
                key = f"{d.category}:{d.structural_signature}"
                struct_groups[key].append(d)

        results = []
        for key, defs in struct_groups.items():
            query_sources = set()
            for d in defs:
                query_sources.add(d.query_label or d.query_file or d.id)
            if len(defs) >= 2 and len(query_sources) >= 2:
                # Compute similarity based on source overlap
                similarity = _compute_group_similarity(defs)
                diff = _describe_differences(defs)

                results.append(MatchGroup(
                    match_type="structural",
                    signature=key,
                    description=_describe_group(defs),
                    definitions=[_def_summary(d) for d in defs],
                    similarity=similarity,
                    difference=diff,
                ))
        return results

    def _find_semantic_matches(self, exact_groups: list[MatchGroup],
                                structural_groups: list[MatchGroup]) -> list[MatchGroup]:
        """Find definitions with same category and overlapping source tables."""
        already_matched = set()
        for g in exact_groups + structural_groups:
            for d in g.definitions:
                already_matched.add(d["id"])

        candidates = [d for d in self.all_definitions
                      if d.id not in already_matched
                      and d.category not in ("passthrough", "star", "constant", "filter",
                                             "equality_filter", "null_check")]

        # Group by category + subcategory + overlapping source tables
        cat_groups: dict[str, list[BusinessDefinition]] = defaultdict(list)
        for d in candidates:
            key = f"{d.category}:{d.subcategory or 'any'}"
            cat_groups[key].append(d)

        results = []
        for key, defs in cat_groups.items():
            if len(defs) < 2:
                continue

            # Within each category, find pairs with overlapping source tables
            clusters = self._cluster_by_table_overlap(defs)
            for cluster in clusters:
                query_sources = set()
                for d in cluster:
                    query_sources.add(d.query_label or d.query_file or d.id)
                if len(cluster) >= 2 and len(query_sources) >= 2:
                    similarity = _compute_group_similarity(cluster)
                    diff = _describe_differences(cluster)
                    results.append(MatchGroup(
                        match_type="semantic",
                        signature=key,
                        description=_describe_group(cluster),
                        definitions=[_def_summary(d) for d in cluster],
                        similarity=similarity,
                        difference=diff,
                    ))
        return results

    def _cluster_by_table_overlap(self, defs: list[BusinessDefinition]) -> list[list[BusinessDefinition]]:
        """Cluster definitions that share at least one source table."""
        if len(defs) <= 1:
            return [defs] if defs else []

        # Simple single-linkage clustering
        clusters: list[set[int]] = []
        for i, d in enumerate(defs):
            merged = False
            d_tables = set(t.lower() for t in d.source_tables)
            if not d_tables:
                continue
            for cluster in clusters:
                for j in cluster:
                    other_tables = set(t.lower() for t in defs[j].source_tables)
                    if d_tables & other_tables:
                        cluster.add(i)
                        merged = True
                        break
                if merged:
                    break
            if not merged:
                clusters.append({i})

        return [[defs[i] for i in cluster] for cluster in clusters if len(cluster) >= 2]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _def_summary(d: BusinessDefinition) -> dict:
    """Summarize a definition for inclusion in match groups."""
    result = {
        "id": d.id,
        "name": d.name,
        "category": d.category,
        "normalized_expression": d.normalized_expression,
        "source_tables": d.source_tables,
        "query_file": d.query_file,
        "query_label": d.query_label,
    }
    if d.subcategory:
        result["subcategory"] = d.subcategory
    if d.pattern:
        result["pattern"] = d.pattern
    if d.filters_context:
        result["filters_context"] = d.filters_context
    return result


def _describe_group(defs: list[BusinessDefinition]) -> str:
    """Generate a human-readable description of what a group shares."""
    if not defs:
        return ""
    first = defs[0]
    cat = first.category
    subcat = first.subcategory
    names = list(set(d.name for d in defs))

    desc = f"{cat}"
    if subcat:
        desc += f" ({subcat})"
    if len(names) <= 3:
        desc += f": {', '.join(names)}"
    else:
        desc += f": {', '.join(names[:3])}, +{len(names)-3} more"
    return desc


def _describe_differences(defs: list[BusinessDefinition]) -> str:
    """Describe what differs between definitions in a group."""
    if len(defs) < 2:
        return ""

    diffs = []

    # Check name differences
    names = set(d.name for d in defs)
    if len(names) > 1:
        diffs.append(f"different names: {', '.join(sorted(names))}")

    # Check source table differences
    all_tables = [set(d.source_tables) for d in defs]
    if len(set(frozenset(t) for t in all_tables)) > 1:
        diffs.append("different source tables")

    # Check column differences
    all_cols = [set(d.source_columns) for d in defs]
    if len(set(frozenset(c) for c in all_cols)) > 1:
        diffs.append("different source columns")

    # Check filter differences
    all_filters = [set(tuple(d.filters_context)) for d in defs]
    if len(set(frozenset(f) for f in all_filters)) > 1:
        diffs.append("different filter context")

    return "; ".join(diffs) if diffs else "minor expression differences"


def _compute_group_similarity(defs: list[BusinessDefinition]) -> float:
    """Compute average pairwise similarity within a group."""
    if len(defs) < 2:
        return 1.0

    total_sim = 0.0
    pairs = 0
    for i in range(len(defs)):
        for j in range(i + 1, len(defs)):
            total_sim += _pairwise_similarity(defs[i], defs[j])
            pairs += 1

    return round(total_sim / pairs, 3) if pairs > 0 else 1.0


def _pairwise_similarity(a: BusinessDefinition, b: BusinessDefinition) -> float:
    """Compute similarity between two definitions (0.0 to 1.0)."""
    score = 0.0
    weights = 0.0

    # Category match (must match to even be compared)
    if a.category == b.category:
        score += 0.2
    weights += 0.2

    # Subcategory match
    if a.subcategory == b.subcategory:
        score += 0.1
    weights += 0.1

    # Structural signature match
    if a.structural_signature == b.structural_signature:
        score += 0.3
    weights += 0.3

    # Source table overlap (Jaccard)
    a_tables = set(t.lower() for t in a.source_tables)
    b_tables = set(t.lower() for t in b.source_tables)
    if a_tables or b_tables:
        jaccard = len(a_tables & b_tables) / len(a_tables | b_tables)
        score += 0.2 * jaccard
    weights += 0.2

    # Source column overlap (Jaccard on column names only, ignoring table prefix)
    a_cols = set(c.split(".")[-1].lower() for c in a.source_columns)
    b_cols = set(c.split(".")[-1].lower() for c in b.source_columns)
    if a_cols or b_cols:
        col_jaccard = len(a_cols & b_cols) / len(a_cols | b_cols)
        score += 0.2 * col_jaccard
    weights += 0.2

    return round(score / weights, 3) if weights > 0 else 0.0


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def report_to_dict(report: ComparisonReport) -> dict:
    """Convert report to plain dict."""
    def match_to_dict(m: MatchGroup) -> dict:
        result = {
            "match_type": m.match_type,
            "description": m.description,
            "definitions": m.definitions,
        }
        if m.similarity < 1.0:
            result["similarity"] = m.similarity
        if m.difference:
            result["difference"] = m.difference
        return result

    result = {
        "summary": report.summary,
    }

    if report.exact_duplicates:
        result["exact_duplicates"] = [match_to_dict(g) for g in report.exact_duplicates]
    if report.structural_matches:
        result["structural_matches"] = [match_to_dict(g) for g in report.structural_matches]
    if report.semantic_matches:
        result["semantic_matches"] = [match_to_dict(g) for g in report.semantic_matches]
    if report.unique_definitions:
        result["unique_definitions"] = report.unique_definitions

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse
    import sys
    import os

    parser = argparse.ArgumentParser(
        description="SQL Business Logic Comparator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=r"""
Examples:
  # Compare two SQL files
  %(prog)s query1.sql query2.sql

  # Compare all SQL files in a directory
  %(prog)s --dir ./sql_queries/

  # Extract definitions from a single query (no comparison)
  %(prog)s --extract "SELECT a, b+c AS d FROM t WHERE x > 1"
  %(prog)s --extract --file query.sql

  # Pipe SQL and extract
  echo "SELECT 1" | %(prog)s --extract --stdin

  # Set dialect
  %(prog)s --dialect tsql query1.sql query2.sql
        """,
    )
    parser.add_argument("files", nargs="*", help="SQL files to compare")
    parser.add_argument("--dir", help="Directory of SQL files to compare")
    parser.add_argument("--extract", nargs="?", const="", default=None,
                        help="Extract definitions only (no comparison). Pass SQL string or use --file/--stdin")
    parser.add_argument("--file", "-f", help="Read SQL from file (for --extract)")
    parser.add_argument("--stdin", action="store_true", help="Read SQL from stdin (for --extract)")
    parser.add_argument("--dialect", "-d", default=None,
                        help="SQL dialect (tsql, bigquery, snowflake, postgres, mysql, etc.)")
    parser.add_argument("--compact", action="store_true", help="Compact JSON output")
    parser.add_argument("--include-unique", action="store_true",
                        help="Include unique (non-matching) definitions in output")

    args = parser.parse_args()

    # Mode 1: Extract definitions from a single query
    if args.extract is not None:
        sql = ""
        if args.file:
            with open(args.file) as f:
                sql = f.read()
        elif args.stdin:
            sql = sys.stdin.read()
        elif args.extract:
            sql = args.extract
        else:
            print("Error: provide SQL string, --file, or --stdin", file=sys.stderr)
            sys.exit(1)

        defs = extract_definitions(sql, dialect=args.dialect)
        output = {"definitions": definitions_to_dict(defs)}
        indent = None if args.compact else 2
        print(json.dumps(output, indent=indent, default=str))
        return

    # Mode 2: Compare multiple SQL files
    files = list(args.files)
    if args.dir:
        dir_path = args.dir
        for fname in sorted(os.listdir(dir_path)):
            if fname.endswith(".sql"):
                files.append(os.path.join(dir_path, fname))

    if len(files) < 2:
        print("Error: need at least 2 SQL files to compare", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    comparator = BusinessLogicComparator()
    for fpath in files:
        with open(fpath) as f:
            sql = f.read()
        label = os.path.basename(fpath)
        comparator.add_query(sql, query_file=fpath, query_label=label, dialect=args.dialect)

    report = comparator.compare()
    output = report_to_dict(report)

    if not args.include_unique:
        output.pop("unique_definitions", None)

    indent = None if args.compact else 2
    print(json.dumps(output, indent=indent, default=str))


if __name__ == "__main__":
    main()
