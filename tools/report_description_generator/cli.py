"""CLI entry point for Tool 4 -- Report description generator."""

import argparse
import json
import sys
from pathlib import Path

from sql_logic_extractor.products import generate_report_description


def _load_schema(path: str | None) -> dict:
    if not path:
        return {}
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    if path.endswith((".yaml", ".yml")):
        try:
            import yaml
        except ImportError:
            print("ERROR: pyyaml required. Install with: "
                  "pip install sql-logic-extractor[business]", file=sys.stderr)
            sys.exit(2)
        return yaml.safe_load(text) or {}
    return json.loads(text)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a report description from a SQL file.")
    parser.add_argument("input", help="Path to .sql file")
    parser.add_argument("--schema", default=None, help="Path to schema YAML/JSON (data dictionary)")
    parser.add_argument("-o", "--output", default=None, help="JSON output path (default: stdout)")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--use-llm", action="store_true",
                        help="Enable LLM-enhanced summary (requires report_description_llm feature)")
    args = parser.parse_args()

    sql = Path(args.input).read_text(encoding="utf-8", errors="replace")
    schema = _load_schema(args.schema)

    desc = generate_report_description(sql, schema, use_llm=args.use_llm,
                                         dialect=args.dialect)

    payload = {
        "use_llm": desc.use_llm,
        "technical_description": desc.technical_description,
        "business_description": desc.business_description,
        "primary_purpose": desc.primary_purpose,
        "key_metrics": desc.key_metrics,
    }
    text = json.dumps(payload, indent=2)
    if args.output is None:
        print(text)
    else:
        Path(args.output).write_text(text, encoding="utf-8")
    print(f"\nReport description generated "
          f"({'LLM' if desc.use_llm else 'engineered'} mode)", file=sys.stderr)
    return 0


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- import "
              "sql_logic_extractor.products.generate_report_description directly.")
    else:
        sys.exit(main())
