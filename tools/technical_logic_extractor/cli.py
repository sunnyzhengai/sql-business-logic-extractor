"""CLI entry point for Tool 2 -- Technical logic extractor."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from sql_logic_extractor.products import extract_technical_lineage


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract technical lineage from a SQL file.")
    parser.add_argument("input", help="Path to .sql file")
    parser.add_argument("-o", "--output", default=None,
                        help="JSON output path (default: stdout)")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()

    sql = Path(args.input).read_text(encoding="utf-8", errors="replace")
    lineage = extract_technical_lineage(sql, dialect=args.dialect)

    payload = {
        "inventory": {
            "columns": [
                {"database": c.database, "schema": c.schema,
                 "table": c.table, "column": c.column}
                for c in lineage.inventory.columns
            ],
        },
        "resolved_columns": lineage.resolved_columns,
        "query_filters": lineage.query_filters,
    }
    text = json.dumps(payload, indent=2)
    if args.output is None:
        print(text)
    else:
        Path(args.output).write_text(text, encoding="utf-8")
    print(f"\n{len(lineage.resolved_columns)} output columns + "
          f"{len(lineage.query_filters)} filters extracted from {args.input}",
          file=sys.stderr)
    return 0


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- import "
              "sql_logic_extractor.products.extract_technical_lineage directly.")
    else:
        sys.exit(main())
