#!/usr/bin/env python3
"""Diagnose why ZC code-to-name annotations aren't appearing in cohort output.

Three checks, run in order. The first one whose output looks wrong
tells you which fix to apply.

Notebook usage:

    from tools.operate.check_zc_lookups import (
        check_corpus_lookups,
        check_zc_csv,
        diagnose,
        probe_zc_filter,
    )

    # Run all three checks at once:
    diagnose(
        corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        zc_csv_path='/lakehouse/default/Files/schemas/zc_values.csv',
    )

    # Or individually:
    check_corpus_lookups('/lakehouse/default/Files/outputs/corpus.jsonl')
    check_zc_csv('/lakehouse/default/Files/schemas/zc_values.csv')

    # Inspect ONE concrete filter that should be annotated -- useful when
    # corpus has lookups but cohorts.md isn't showing them. Compare the
    # printed expression+english to the rendered line in cohorts.md.
    probe_zc_filter(
        '/lakehouse/default/Files/outputs/corpus.jsonl',
        zc_table_substr='COVERAGE_TYPE',
        code='2',
    )

CLI:

    python -m tools.operate.check_zc_lookups <corpus.jsonl> [zc_values.csv] \\
        [--probe-zc-table COVERAGE_TYPE] [--probe-code 2] [--probe-max 1]
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# CHECK 1 -- did extract_corpus actually populate filter.zc_lookups?
# ---------------------------------------------------------------------------

def check_corpus_lookups(corpus_path: str | Path) -> int:
    """Walk corpus.jsonl, count how many filters got zc_lookups populated.

    Interpretation:
      total_lookups == 0:  extract_corpus didn't load zc_values.csv (or
                            loaded zero rows). Continue to check_zc_csv.
      total_lookups > 0 but cohorts.md still missing /* name */:
                            zc_lookups exist but cohort renderer isn't
                            picking them up. Tell me to investigate.
    """
    p = Path(corpus_path)
    if not p.is_file():
        print(f"[Check 1] ERROR: corpus.jsonl not found at {p}")
        return 0

    print(f"[Check 1] Walking {p} ...")
    total_lookups = 0
    n_filters = 0
    sample: list[dict] = []
    with p.open(encoding="utf-8") as f:
        next(f, None)  # skip header line
        for line in f:
            line = line.strip()
            if not line:
                continue
            view = json.loads(line)
            for s in view.get("scopes") or []:
                for filt in s.get("filters") or []:
                    n_filters += 1
                    lookups = filt.get("zc_lookups") or []
                    total_lookups += len(lookups)
                    if lookups and len(sample) < 5:
                        sample.append({
                            "view": view.get("view_name", ""),
                            "scope": s.get("id", ""),
                            "filter_expr": (filt.get("expression") or "")[:80],
                            "lookups": lookups,
                        })

    print(f"[Check 1] Filters scanned:           {n_filters}")
    print(f"[Check 1] Total zc_lookups embedded: {total_lookups}")
    if total_lookups == 0:
        print(f"[Check 1] => extract_corpus did NOT populate zc_lookups.")
        print(f"           Run check_zc_csv() next to see if the CSV is")
        print(f"           readable, then verify zc_values_path was passed")
        print(f"           to extract_corpus.")
    else:
        print(f"[Check 1] Sample lookups:")
        for s in sample:
            print(f"  {s['view']}.{s['scope']}: {s['filter_expr']}")
            for L in s["lookups"]:
                print(f"     -> column={L.get('column')}  "
                       f"code={L.get('code')}  name={L.get('name')!r}")
        print(f"[Check 1] => zc_lookups ARE embedded. If cohorts.md still")
        print(f"           lacks /* name */ annotations, the cohort renderer")
        print(f"           is the issue. Re-run extract_cohorts on the")
        print(f"           latest corpus.jsonl.")
    return total_lookups


# ---------------------------------------------------------------------------
# CHECK 2 -- is the zc_values.csv readable with the expected header?
# ---------------------------------------------------------------------------

def check_zc_csv(zc_csv_path: str | Path) -> int:
    """Open zc_values.csv and verify the header is `zc_table,code,name`.

    The most common SSMS gotcha: the saved file has uppercase headers
    (`ZC_TABLE,CODE,NAME`) or no header at all. extract_corpus's CSV
    reader looks for the lowercase forms specifically; mismatched
    headers silently load zero rows.
    """
    p = Path(zc_csv_path)
    if not p.is_file():
        print(f"[Check 2] ERROR: zc_values.csv not found at {p}")
        print(f"          Verify the path you passed to extract_corpus(zc_values_path=...)")
        return 0

    print(f"[Check 2] Reading {p} ...")
    expected = {"zc_table", "code", "name"}
    with p.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    print(f"[Check 2] Headers detected:    {headers}")
    print(f"[Check 2] Rows loaded:         {len(rows)}")

    headers_lower = {h.lower() for h in headers}
    if not expected.issubset(headers_lower):
        missing = expected - headers_lower
        print(f"[Check 2] => MISSING headers: {sorted(missing)}")
        print(f"           extract_corpus reads the CSV with csv.DictReader and")
        print(f"           looks for fields named exactly: zc_table, code, name")
        print(f"           (lowercase). Fix:")
        print(f"           - If the CSV has uppercase headers, edit line 1 to lowercase.")
        print(f"           - If the CSV has NO header row, prepend `zc_table,code,name`.")
        return 0

    case_mismatch = expected - set(headers)
    if case_mismatch:
        print(f"[Check 2] => Headers are present but case-mismatched: {sorted(case_mismatch)}")
        print(f"           extract_corpus's reader is case-sensitive on field names.")
        print(f"           Edit the CSV's first line to match exactly: zc_table,code,name")
        return 0

    if not rows:
        print(f"[Check 2] => Headers OK but file has zero data rows.")
        print(f"           Re-run extract_zc_values.sql in SSMS and re-save.")
        return 0

    distinct_zcs = {r["zc_table"] for r in rows if r.get("zc_table")}
    print(f"[Check 2] Distinct ZC tables:  {len(distinct_zcs)}")
    print(f"[Check 2] First 3 rows:")
    for r in rows[:3]:
        print(f"  {dict(r)}")
    print(f"[Check 2] => CSV looks valid. If Check 1 still showed zero")
    print(f"           lookups, you didn't pass zc_values_path to")
    print(f"           extract_corpus, OR the path you passed differs")
    print(f"           from where this CSV actually lives.")
    return len(rows)


# ---------------------------------------------------------------------------
# Combined diagnostic -- run both checks and emit a clear next-step
# ---------------------------------------------------------------------------

def diagnose(
    corpus_path: str | Path,
    zc_csv_path: str | Path | None = None,
) -> None:
    """Run both checks in order and print a summary.

    `zc_csv_path` is optional -- if omitted, only Check 1 runs (useful
    when you've already verified the CSV)."""
    print("=" * 60)
    n_lookups = check_corpus_lookups(corpus_path)
    print()
    if zc_csv_path is not None:
        print("=" * 60)
        check_zc_csv(zc_csv_path)
        print()
    print("=" * 60)
    print("Next step:")
    if n_lookups > 0:
        print("  -> zc_lookups are embedded. Re-run extract_cohorts to")
        print("     get the /* name */ annotations.")
    else:
        print("  -> zc_lookups are missing. Most common causes (in order):")
        print("       1. extract_corpus was run WITHOUT zc_values_path=...")
        print("          (so the loader fell back to the default path and")
        print("           found nothing).")
        print("       2. zc_values.csv has wrong headers (Check 2 details).")
        print("       3. zc_values.csv path passed to extract_corpus")
        print("          doesn't match where the CSV actually lives.")
        print("     Fix whichever applies, then re-run extract_corpus AND")
        print("     extract_cohorts in that order.")


# ---------------------------------------------------------------------------
# CHECK 3 -- inspect ONE concrete filter that has a zc_lookup
# ---------------------------------------------------------------------------

def probe_zc_filter(
    corpus_path: str | Path,
    zc_table_substr: str = "",
    code: str | None = None,
    column_substr: str = "",
    max_results: int = 1,
) -> int:
    """Find filters in corpus.jsonl whose zc_lookups match the given
    criteria and dump everything we'd need to debug a missing
    annotation: expression, english, kind, full lookups list.

    Args:
        corpus_path:     Path to corpus.jsonl.
        zc_table_substr: Case-insensitive substring match on
                         lookup.zc_table. Empty string matches any.
                         e.g., "COVERAGE_TYPE" or "ZC_PAT_STATUS".
        code:            Exact string match on lookup.code (e.g., "2").
                         None = any code.
        column_substr:   Case-insensitive substring match on
                         lookup.column. Empty = any.
        max_results:     Stop after this many matching filters.

    Notebook usage:

        # Most common: find filters with COVERAGE_TYPE_C = 2
        probe_zc_filter(corpus_path, zc_table_substr="COVERAGE_TYPE", code="2")

        # Or just any filter that resolved a ZC lookup at all:
        probe_zc_filter(corpus_path)

        # First 5 filters touching ZC_PAT_STATUS:
        probe_zc_filter(corpus_path, zc_table_substr="ZC_PAT_STATUS",
                         max_results=5)
    """
    p = Path(corpus_path)
    if not p.is_file():
        print(f"ERROR: corpus.jsonl not found at {p}")
        return 0

    zc_substr_upper = (zc_table_substr or "").upper()
    col_substr_upper = (column_substr or "").upper()
    n_found = 0

    with p.open(encoding="utf-8") as f:
        next(f, None)
        for line in f:
            line = line.strip()
            if not line:
                continue
            view = json.loads(line)
            for s in view.get("scopes") or []:
                for filt in s.get("filters") or []:
                    lookups = filt.get("zc_lookups") or []
                    matches = [
                        z for z in lookups
                        if (not zc_substr_upper
                            or zc_substr_upper in (z.get("zc_table") or "").upper())
                        and (code is None or (z.get("code") or "") == code)
                        and (not col_substr_upper
                             or col_substr_upper in (z.get("column") or "").upper())
                    ]
                    if not matches:
                        continue
                    n_found += 1
                    print("=" * 60)
                    print(f"MATCH #{n_found}")
                    print(f"  VIEW:        {view.get('view_name', '')}")
                    print(f"  SCOPE:       {s.get('id', '')}")
                    print(f"  KIND:        {filt.get('kind')}")
                    print(f"  EXPRESSION:  {filt.get('expression')}")
                    print(f"  ENGLISH:     {filt.get('english')}")
                    print(f"  ZC_LOOKUPS:  {lookups}")
                    print(f"  MATCHED:     {matches}")
                    if n_found >= max_results:
                        return n_found
    if n_found == 0:
        criteria = []
        if zc_table_substr:
            criteria.append(f"zc_table contains {zc_table_substr!r}")
        if code is not None:
            criteria.append(f"code == {code!r}")
        if column_substr:
            criteria.append(f"column contains {column_substr!r}")
        if not criteria:
            criteria.append("(any zc_lookup at all)")
        print(f"No matches: " + ", ".join(criteria))
    return n_found


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=("Diagnose why ZC code-to-name annotations aren't "
                      "appearing in cohort output."),
    )
    parser.add_argument("corpus", help="Path to corpus.jsonl")
    parser.add_argument(
        "zc_csv", nargs="?", default=None,
        help="(optional) Path to zc_values.csv -- runs the CSV-readability check too",
    )
    parser.add_argument(
        "--probe-zc-table", default="",
        help="If set, also probe for filters matching this zc_table substring "
              "(e.g., 'COVERAGE_TYPE'). Dumps the first matching filter's "
              "expression / english / lookups for inspection.",
    )
    parser.add_argument(
        "--probe-code", default=None,
        help="When used with --probe-zc-table, narrow to filters whose "
              "lookup code equals this (e.g., '2').",
    )
    parser.add_argument(
        "--probe-max", type=int, default=1,
        help="Maximum number of probe matches to dump (default: 1).",
    )
    args = parser.parse_args()
    diagnose(args.corpus, args.zc_csv)
    if args.probe_zc_table:
        print()
        print("=" * 60)
        print(f"PROBE: zc_table contains {args.probe_zc_table!r}"
              + (f", code={args.probe_code!r}" if args.probe_code else ""))
        print("=" * 60)
        probe_zc_filter(
            args.corpus,
            zc_table_substr=args.probe_zc_table,
            code=args.probe_code,
            max_results=args.probe_max,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
