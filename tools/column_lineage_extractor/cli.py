"""CLI entry point for Tool 1 -- Column extractor."""

import argparse
import csv
import sys
from pathlib import Path

from sql_logic_extractor.products import extract_columns


def _read_sql_file(path: Path) -> str:
    """Read a SQL file, handling SSMS's default UTF-16 LE BOM and other
    common encodings. SSMS scripts views as UTF-16 LE by default; reading
    such a file as UTF-8 corrupts the first byte and breaks parsing."""
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract column inventory from a SQL file.")
    parser.add_argument("input", help="Path to .sql file")
    parser.add_argument("-o", "--output", default=None,
                        help="CSV output path (default: stdout)")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()

    sql = _read_sql_file(Path(args.input))
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
