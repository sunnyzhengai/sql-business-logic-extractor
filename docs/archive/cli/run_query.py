#!/usr/bin/env python3
"""Run the L3 resolve + L4 offline-translate pipeline on queries/<name>/input.sql.

All outputs are written next to the input, so everything for one query lives in
a single folder:

    queries/<name>/
        input.sql          (your SQL)
        resolved.json      (L3 lineage + filter extraction)
        translated.json    (L4 English, structured)
        translated.txt     (L4 English, human-readable)

Usage:
    python run_query.py <query_name>                # uses dialect=tsql
    python run_query.py <query_name> --dialect snowflake
    python run_query.py bi_complex -s custom_schema.yaml

Lists available queries if no argument is given.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _child_env() -> dict:
    """Inject project root onto PYTHONPATH so subprocesses can import
    `sql_logic_extractor` without requiring a pip install (Homebrew Python
    blocks system-wide installs)."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{PROJECT_ROOT}{os.pathsep}{existing}" if existing else str(PROJECT_ROOT)
    return env


def _resolve_cmd():
    """Always launch the resolver as the installed package — _child_env() puts the
    project root on PYTHONPATH so this works whether or not the package is pip-installed."""
    return [sys.executable, "-m", "sql_logic_extractor.resolve"]


def list_queries():
    root = Path("queries")
    if not root.exists():
        print("No queries/ folder found.")
        return
    entries = sorted(p.name for p in root.iterdir() if p.is_dir() and (p / "input.sql").exists())
    if not entries:
        print("No query folders with input.sql found under queries/.")
        return
    print("Available queries:")
    for name in entries:
        print(f"  {name}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("query_name", nargs="?",
                        help="Folder name under queries/ (omit to list available queries)")
    parser.add_argument("--dialect", "-d", default="tsql")
    parser.add_argument("--schema", "-s", default="clarity_schema.yaml")
    args = parser.parse_args()

    if not args.query_name:
        list_queries()
        return

    qdir = Path("queries") / args.query_name
    sql_path = qdir / "input.sql"
    if not sql_path.exists():
        print(f"Error: {sql_path} not found.", file=sys.stderr)
        print()
        list_queries()
        sys.exit(1)

    env = _child_env()
    resolved_path = qdir / "resolved.json"
    print(f"[1/2] Resolving: {sql_path}")
    with resolved_path.open("w") as f:
        subprocess.run(
            _resolve_cmd() + ["-f", str(sql_path), "-d", args.dialect],
            stdout=f, check=True, env=env,
        )
    print(f"      → {resolved_path}")

    translated_base = qdir / "translated"
    print(f"[2/2] Translating: {resolved_path}")
    subprocess.run(
        [sys.executable, str(Path(__file__).parent / "offline_translate.py"),
         str(resolved_path), "-s", args.schema,
         "-o", str(translated_base)],
        check=True, env=env,
    )
    print()
    print(f"Inspect outputs in: {qdir}/")


if __name__ == "__main__":
    main()
