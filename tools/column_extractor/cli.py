"""CLI entry point for Tool 1 -- Column extractor."""

import argparse
import csv
import sys
from pathlib import Path

from sql_logic_extractor.products import extract_columns


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract column inventory from a SQL file.")
    parser.add_argument("input", help="Path to .sql file")
    parser.add_argument("-o", "--output", default=None,
                        help="CSV output path (default: stdout)")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()

    sql = Path(args.input).read_text(encoding="utf-8", errors="replace")
    inventory = extract_columns(sql, dialect=args.dialect)

    rows = [
        {"database": c.database or "",
         "schema": c.schema or "",
         "table": c.table,
         "column": c.column,
         "qualified": c.qualified()}
        for c in inventory.columns
    ]
    fieldnames = ["database", "schema", "table", "column", "qualified"]

    out_stream = sys.stdout if args.output is None else open(args.output, "w",
                                                             encoding="utf-8-sig", newline="")
    try:
        writer = csv.DictWriter(out_stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if args.output is not None:
            out_stream.close()

    print(f"\n{len(rows)} columns extracted from {args.input}", file=sys.stderr)
    return 0


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- import "
              "sql_logic_extractor.products.extract_columns directly.")
    else:
        sys.exit(main())
