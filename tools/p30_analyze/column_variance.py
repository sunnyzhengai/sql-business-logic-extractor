"""Column-variance analysis: surface "same name, multiple definitions" per community.

For each community, walk its primary member views and group their
main-scope columns by (column_name, source_tables). Within each group,
count how many DISTINCT fingerprints the column has -- a fingerprint
is the canonical-form hash of the column's SQL expression, computed
during corpus extraction. If a group has >= 2 distinct fingerprints,
that's a RECONCILIATION CANDIDATE: the modeling team needs to decide
which definition to canonicalize before building the data model.

Two flavors of variance get distinguished by the source-tables key:

  Definitional variance (same source, different computation):
      MEMBER_ID computed as P.PAT_ID            in view A
      MEMBER_ID computed as RTRIM(P.PAT_ID)    in view B
      Both sourced from PATIENT -> same key -> 2 distinct fingerprints
      -> reconciliation candidate.

  Naming collision (same name, different source):
      MEMBER_ID from PATIENT      in view A
      MEMBER_ID from COVERAGE     in view B
      Different sources -> different keys -> not grouped together
      (they're reported separately; the naming collision is a different
      kind of finding -- "same word, different concept" -- which
      `term_disagreements` will handle later).

This module only handles the same-source-different-definition case.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable


def analyze_column_variance(
    views: Iterable[dict],
    community_to_primary: dict[int, set[str]],
) -> dict[int, list[dict]]:
    """For each community, return its column-variance findings.

    Parameters
    ----------
    views : iterable of ViewV1 dicts (the BUSINESS views; typically the
        output of `filter_business_views`).
    community_to_primary : map of community_idx -> set of view names whose
        primary community is this one (from p30_analyze.primary_community.
        assign_views_to_communities).

    Returns
    -------
    community_idx -> list of variance records. Each record:
        {
            "column_name":            "MEMBER_ID",
            "source_tables":          ["PATIENT"],            # frozenset-as-list
            "n_views":                4,                       # total views with this column
            "n_distinct_fingerprints": 2,                       # variance count
            "definitions": [                                    # one entry per fingerprint,
                                                                # sorted by view-count descending
                {
                    "fingerprint":            "ab12cd...",
                    "technical_description":  "P.PAT_ID",
                    "business_description":   "Patient identifier",
                    "views":                  ["v1", "v2", "v3"],   # views using this definition
                },
                {...}
            ],
        }
    Records are sorted by importance:
        more views > more variance > alphabetical by column_name.
    Communities with no variance return an empty list.
    """
    # Index views by name for O(1) lookup.
    view_by_name: dict[str, dict] = {
        v.get("view_name"): v for v in views if v.get("view_name")
    }

    result: dict[int, list[dict]] = {}
    for community_idx, primary_views in community_to_primary.items():
        # Walk every column of every view's main scope, indexed by
        # (column_name, source_tables_signature) -> list of records.
        # Each record carries the fingerprint so we can group later.
        grouped: dict[tuple[str, tuple[str, ...]], list[dict]] = defaultdict(list)

        for view_name in primary_views:
            view = view_by_name.get(view_name)
            if view is None:
                continue
            for scope in view.get("scopes") or []:
                # Only main-scope columns are user-visible "outputs".
                # CTE-internal columns aren't governance-comparison units.
                if scope.get("kind") != "main":
                    continue
                for col in scope.get("columns") or []:
                    col_name = col.get("column_name") or ""
                    fingerprint = col.get("fingerprint")
                    if not col_name or not fingerprint:
                        continue
                    # Normalize the source-tables list to a sorted tuple
                    # so equal sets compare equal as dict keys.
                    base_tables = tuple(sorted(col.get("base_tables") or []))
                    key = (col_name, base_tables)
                    grouped[key].append({
                        "view_name": view_name,
                        "fingerprint": fingerprint,
                        "technical_description": col.get("technical_description") or "",
                        "business_description": col.get("business_description") or "",
                    })

        # For each (column_name, source_tables) group, count distinct
        # fingerprints. Only retain groups with >= 2 distinct ones
        # (those are the reconciliation candidates).
        variance_records: list[dict] = []
        for (col_name, source_tables), records in grouped.items():
            by_fp: dict[str, list[dict]] = defaultdict(list)
            for rec in records:
                by_fp[rec["fingerprint"]].append(rec)
            if len(by_fp) < 2:
                continue

            definitions = []
            for fp, fp_records in by_fp.items():
                # Take the first record's tech/business descriptions as
                # representative (all records with this fingerprint
                # produced equivalent canonical forms; minor formatting
                # differences are fine).
                definitions.append({
                    "fingerprint": fp,
                    "technical_description": fp_records[0]["technical_description"],
                    "business_description": fp_records[0]["business_description"],
                    "views": sorted(r["view_name"] for r in fp_records),
                })
            # Sort definitions by view-count descending so the most-common
            # variant lists first (likely the "canonical" candidate).
            definitions.sort(key=lambda d: (-len(d["views"]), d["fingerprint"]))

            variance_records.append({
                "column_name": col_name,
                "source_tables": list(source_tables),
                "n_views": len(records),
                "n_distinct_fingerprints": len(by_fp),
                "definitions": definitions,
            })

        # Sort the community's variance findings by importance:
        # most-views first, then most-variance, then alphabetical.
        variance_records.sort(key=lambda v: (
            -v["n_views"],
            -v["n_distinct_fingerprints"],
            v["column_name"],
        ))
        result[community_idx] = variance_records

    return result


def count_reconciliation_candidates(
    variance: dict[int, list[dict]],
) -> int:
    """Total reconciliation candidates across all communities.

    Useful for console-output summary lines. A candidate is one
    (community, column_name, source_tables) triple with >= 2 distinct
    fingerprints.
    """
    return sum(len(records) for records in variance.values())
