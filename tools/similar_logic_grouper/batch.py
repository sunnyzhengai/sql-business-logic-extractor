#!/usr/bin/env python3
"""Tool 5 -- find similar-logic clusters across a folder of SQL views.

For each view, runs the resolver to get one resolved_expression per
output column. Fingerprints the expression's normalized AST. Groups
columns by fingerprint and reports clusters where the same fingerprint
appears in 2+ views.

Output CSV (one row per cluster):
    fingerprint, n_views, n_columns, sample_expression, columns

Where `columns` is a "; "-joined list of "<view>.<column>" identifiers.

Notebook usage:
    from tools.similar_logic_grouper.batch import find_similar_logic
    find_similar_logic(input_dir='/lakehouse/default/Files/views',
                       output_csv='/lakehouse/default/Files/outputs/similar_logic.csv')

CLI usage:
    python -m tools.similar_logic_grouper.batch <input_dir> [-o out.csv]
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

from sql_logic_extractor.products import extract_technical_lineage
from tools.similar_logic_grouper.fingerprint import fingerprint


def _read_sql_file(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le")[1:]
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")[1:]
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8")[1:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-16-le", errors="replace")


def collect_fingerprints(input_dir: str, *, dialect: str = "tsql",
                          min_columns: int = 2) -> list[dict]:
    """Walk views in `input_dir`, fingerprint each output column, and
    return one cluster row per fingerprint that occurs in `min_columns`+
    different (view, column) entries."""
    in_dir = Path(input_dir)
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        raise SystemExit(f"No .sql files in {in_dir}")

    # fingerprint -> list of (view_name, column_name, expression)
    clusters: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    skipped: list[tuple[str, str]] = []

    for path in sql_files:
        view_name = path.stem
        sql = _read_sql_file(path)
        try:
            lineage = extract_technical_lineage(sql, dialect=dialect)
        except Exception as e:
            skipped.append((path.name, f"{type(e).__name__}: {e}"))
            continue

        for col in lineage.resolved_columns:
            expr = col.get("resolved_expression", "") or ""
            if col.get("type") == "passthrough":
                # Bare passthroughs (SELECT t.x AS x) are rarely interesting
                # for definition dedup; skip them to keep the report focused.
                continue
            fp = fingerprint(expr, dialect=dialect)
            if not fp:
                continue
            clusters[fp].append((view_name, col.get("name", ""), expr))

    rows: list[dict] = []
    for fp, members in clusters.items():
        unique_views = {m[0] for m in members}
        if len(unique_views) < min_columns:
            # Cluster only meaningful when it spans multiple views.
            continue
        sample_expr = members[0][2]
        rows.append({
            "fingerprint": fp,
            "n_views": len(unique_views),
            "n_columns": len(members),
            "sample_expression": sample_expr[:300] + ("..." if len(sample_expr) > 300 else ""),
            "columns": "; ".join(f"{v}.{c}" for v, c, _ in members),
        })

    rows.sort(key=lambda r: (-r["n_views"], -r["n_columns"]))

    if skipped:
        print("WARNING: views skipped (parse error):", file=sys.stderr)
        for name, err in skipped:
            print(f"  {name}: {err}", file=sys.stderr)

    return rows


def find_similar_logic(input_dir: str,
                        output_csv: str = "similar_logic_grouper.csv",
                        *, dialect: str = "tsql",
                        min_columns: int = 2) -> int:
    rows = collect_fingerprints(input_dir, dialect=dialect, min_columns=min_columns)
    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["fingerprint", "n_views", "n_columns", "sample_expression", "columns"]
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} similarity cluster(s) -> {out}")
    if rows:
        top = rows[0]
        print(f"  Largest cluster: {top['n_views']} views, "
              f"{top['n_columns']} column(s) -- e.g. {top['columns'][:80]}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Find columns across views that share the same business logic."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="similar_logic_grouper.csv")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--min-columns", type=int, default=2,
                          help="Minimum distinct views a fingerprint must span "
                                "to qualify as a cluster (default: 2).")
    args = parser.parse_args()
    return find_similar_logic(args.input_dir, args.output,
                                 dialect=args.dialect, min_columns=args.min_columns)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call find_similar_logic("
              "input_dir=..., output_csv=...) from a cell.")
    else:
        sys.exit(main())
