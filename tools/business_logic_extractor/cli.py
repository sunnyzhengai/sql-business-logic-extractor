"""CLI entry point for Tool 3 -- Business logic extractor."""

import argparse
import json
import sys
from pathlib import Path

from sql_logic_extractor.products import extract_business_logic


def _load_schema(path: str | None) -> dict:
    if not path:
        return {}
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml
        except ImportError:
            print("ERROR: pyyaml is required for YAML schema input. "
                  "Install with: pip install sql-logic-extractor[business]",
                  file=sys.stderr)
            sys.exit(2)
        return yaml.safe_load(text) or {}
    return json.loads(text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract business logic per column from a SQL file.")
    parser.add_argument("input", help="Path to .sql file")
    parser.add_argument("--schema", default=None, help="Path to schema YAML/JSON (data dictionary)")
    parser.add_argument("-o", "--output", default=None, help="JSON output path (default: stdout)")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--use-llm", action="store_true",
                        help="Enable LLM-enhanced translations (requires business_logic_llm feature)")
    args = parser.parse_args()

    sql = Path(args.input).read_text(encoding="utf-8", errors="replace")
    schema = _load_schema(args.schema)

    bl = extract_business_logic(sql, schema, use_llm=args.use_llm,
                                  dialect=args.dialect)

    payload = {
        "use_llm": bl.use_llm,
        "column_translations": bl.column_translations,
        "query_filters": bl.lineage.query_filters,
    }
    text = json.dumps(payload, indent=2)
    if args.output is None:
        print(text)
    else:
        Path(args.output).write_text(text, encoding="utf-8")
    print(f"\n{len(bl.column_translations)} columns translated "
          f"({'LLM' if bl.use_llm else 'engineered'} mode)", file=sys.stderr)
    return 0


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- import "
              "sql_logic_extractor.products.extract_business_logic directly.")
    else:
        sys.exit(main())
