#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Batch view/proc description generator (Tool 4 over whole folders).

Reads every .sql file in one or more VIEW folders and one or more PROC
folders, generates a high-level (view-level) description for each, and
writes them all to a single Markdown output file.

  - VIEW folders: each .sql is described directly. The parsing rules strip
    any CREATE VIEW / SSMS boilerplate.
  - PROC folders: each .sql is first run through `select_into_to_cte`, which
    turns a view-shaped or temp-table-staging ("reporting") proc into a
    single view-shaped SELECT. Genuine ETL / multi-output procs raise
    ProcNotViewShaped and are recorded as SKIPPED with the reason -- they
    are not view-shaped and would otherwise produce a wrong description.

The LLM provider + key come from the environment (or a local .env), exactly
like the single-file path: SLE_LLM_PROVIDER + the matching key. The LLM
client is built ONCE and reused across every file.

Run it (one-line driver -- no notebook cell needed):

    python -m tools.report_description_generator.describe_folders \
        --views /lakehouse/.../views_reporting /lakehouse/.../views_cookrpt \
        --procs /lakehouse/.../procs_a        /lakehouse/.../procs_b \
        --schema /lakehouse/.../data/dictionaries/clarity_schema.json \
        --out    /lakehouse/.../outputs/descriptions.md

Or import the function:

    from tools.report_description_generator.describe_folders import describe_folders
    describe_folders(view_dirs=[...], proc_dirs=[...],
                     schema_path="...", out_path="...")
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

# Make the package importable when run as a bare script from any directory
# (e.g. `python /lakehouse/.../describe_folders.py`). Repo root is three
# levels up: tools/report_description_generator/describe_folders.py.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Load a local .env (SLE_LLM_PROVIDER + key) if python-dotenv is installed.
# Optional -- env vars set another way still work.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from sql_logic_extractor.products import generate_report_description
from sql_logic_extractor.proc_normalize import ProcNotViewShaped, select_into_to_cte
from tools.report_description_generator.cli import _load_schema


# Human-readable gloss for each ProcNotViewShaped reason, shown in the output
# so a steward reading the file understands WHY a proc was skipped.
_SKIP_REASON_HELP = {
    "select_into_persistent": "writes into a real (persistent) table -- this is an ETL proc, not view-shaped.",
    "unsupported_statement": "contains INSERT/UPDATE/MERGE/DECLARE/flow-control -- not a pure stage-and-read.",
    "temp_redefined": "writes the same temp table more than once (accumulation) -- not CTE-equivalent.",
    "multiple_terminal_selects": "returns more than one result set -- a view has a single output query.",
    "no_terminal_select": "stages temp tables but never returns a final SELECT.",
    "undefined_temp_reference": "reads a temp table staged outside this proc -- not self-contained.",
    "empty_body": "no statements found after the procedure wrapper.",
}

# Detect whether a file is actually a stored procedure (vs a view that landed
# in a proc folder). Used to decide whether a ProcNotViewShaped rejection means
# "skip this ETL proc" or "this isn't a proc, try describing it directly".
_IS_CREATE_PROC_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROC(?:EDURE)?\b", re.IGNORECASE
)


def _read_sql(path: Path) -> str:
    """Read a .sql file tolerating SSMS's UTF-16-LE / BOM defaults."""
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


def _describe_one(sql: str, schema: dict, llm_client, *, is_proc: bool) -> tuple[object, str]:
    """Describe one SQL file. Returns (report_or_None, status).

    status is "ok", "skipped:<reason>", or "error:<msg>". For proc files we
    normalize temp-table staging into CTEs first; if that's rejected as
    not-view-shaped we still try describing as-is (covers a plain view that
    landed in a proc folder) before giving up with the reason.
    """
    target_sql = sql
    if is_proc:
        try:
            target_sql = select_into_to_cte(sql)
        except ProcNotViewShaped as e:
            # A real CREATE PROCEDURE the normalizer rejected is genuinely not
            # view-shaped (ETL / mutation / multi-output) -- SKIP with the
            # reason rather than describe a partial/wrong result from whatever
            # happens to parse. Only fall back to direct description when the
            # file isn't actually a proc (e.g. a CREATE VIEW misfiled here).
            if _IS_CREATE_PROC_RE.search(sql):
                return None, f"skipped:{e.reason}"
            try:
                rpt = generate_report_description(sql, schema, use_llm=True,
                                                  llm_client=llm_client)
                return rpt, "ok"
            except Exception:
                return None, f"skipped:{e.reason}"

    try:
        rpt = generate_report_description(target_sql, schema, use_llm=True,
                                          llm_client=llm_client)
        return rpt, "ok"
    except Exception as e:  # parse/resolve/LLM failure for this one file
        return None, f"error:{type(e).__name__}: {e}"[:200]


def _iter_sql(dirs: list[str]) -> list[tuple[Path, str]]:
    """Yield (path, folder_label) for every *.sql in each directory."""
    out: list[tuple[Path, str]] = []
    for d in dirs or []:
        p = Path(d)
        if not p.is_dir():
            print(f"WARNING: not a directory, skipping: {p}", file=sys.stderr)
            continue
        for f in sorted(p.glob("*.sql")):
            out.append((f, p.name))
    return out


def describe_folders(
    view_dirs: list[str],
    proc_dirs: list[str],
    *,
    out_path: str,
    schema_path: str | None = None,
    provider: str | None = None,
    dialect: str = "tsql",
    limit: int | None = None,
) -> dict:
    """Describe every .sql in the given view + proc folders to one Markdown file.

    Args:
        view_dirs: folders whose .sql are treated as views (described directly).
        proc_dirs: folders whose .sql are treated as procs (normalized first).
        out_path: Markdown file to write.
        schema_path: optional Clarity schema JSON/YAML for richer descriptions.
        provider: LLM provider override; defaults to SLE_LLM_PROVIDER / env.
        dialect: SQL dialect (default tsql).
        limit: process at most this many files total (handy for a dry run).

    Returns a dict with per-file results and a status tally.
    """
    # Build the LLM client ONCE and reuse it for every file.
    from sql_logic_extractor.llm_client import make_llm_client
    llm_client = make_llm_client(provider=provider)

    schema = _load_schema(schema_path) if schema_path else {}

    # Collect work: (path, folder_label, is_proc).
    work: list[tuple[Path, str, bool]] = []
    work += [(f, lbl, False) for f, lbl in _iter_sql(view_dirs)]
    work += [(f, lbl, True) for f, lbl in _iter_sql(proc_dirs)]
    if limit is not None:
        work = work[:limit]

    if not work:
        print("No .sql files found in the given folders.", file=sys.stderr)
        return {"results": [], "tally": {}}

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    tally = {"ok": 0, "skipped": 0, "error": 0}
    t_start = time.time()

    print(f"Describing {len(work)} file(s) -> {out}\n")

    # Stream results to the Markdown file so a long run is recoverable and you
    # can watch it grow.
    with out.open("w", encoding="utf-8") as fh:
        fh.write("# SQL Descriptions\n\n")
        fh.write(f"- Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        fh.write(f"- Schema: {schema_path or '(none)'}\n")
        fh.write(f"- Files: {len(work)}\n\n---\n\n")
        fh.flush()

        for i, (path, label, is_proc) in enumerate(work, 1):
            kind = "proc" if is_proc else "view"
            t0 = time.time()
            try:
                sql = _read_sql(path)
                report, status = _describe_one(sql, schema, llm_client, is_proc=is_proc)
            except Exception as e:  # unreadable file, etc.
                report, status = None, f"error:{type(e).__name__}: {e}"[:200]

            bucket = status.split(":", 1)[0]
            tally[bucket] = tally.get(bucket, 0) + 1
            results.append({"file": str(path), "kind": kind, "status": status})

            # ---- write this file's section ----
            rel = f"{label}/{path.name}"
            if status == "ok" and report is not None:
                fh.write(f"## [{kind}] {rel}\n\n")
                fh.write(f"**Primary purpose:** {report.primary_purpose or '(n/a)'}\n\n")
                fh.write(f"**Description:** {report.business_description or '(empty)'}\n\n")
                if report.key_metrics:
                    fh.write(f"**Key metrics:** {', '.join(report.key_metrics)}\n\n")
            elif bucket == "skipped":
                reason = status.split(":", 1)[1]
                gloss = _SKIP_REASON_HELP.get(reason, "")
                fh.write(f"## [{kind}] {rel} — SKIPPED ({reason})\n\n")
                fh.write(f"_{gloss}_\n\n" if gloss else "")
            else:  # error
                msg = status.split(":", 1)[1] if ":" in status else status
                fh.write(f"## [{kind}] {rel} — ERROR\n\n")
                fh.write(f"```\n{msg}\n```\n\n")
            fh.flush()

            print(f"[{i}/{len(work)}] {kind:4s} {rel}  ({time.time()-t0:.1f}s)  {status.split(':',1)[0]}")

    dt = time.time() - t_start
    print(f"\nDone in {dt:.0f}s -> {out}")
    print(f"  ok={tally.get('ok',0)}  skipped={tally.get('skipped',0)}  error={tally.get('error',0)}")
    return {"results": results, "tally": tally, "out_path": str(out)}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch high-level descriptions for view + proc folders."
    )
    parser.add_argument("--views", nargs="*", default=[],
                        help="One or more folders of view .sql files.")
    parser.add_argument("--procs", nargs="*", default=[],
                        help="One or more folders of stored-proc .sql files.")
    parser.add_argument("--schema", default=None,
                        help="Path to Clarity schema JSON/YAML (optional).")
    parser.add_argument("--out", required=True,
                        help="Output Markdown file path.")
    parser.add_argument("--provider", default=None,
                        help="LLM provider override (azure|openai|gemini). "
                             "Defaults to SLE_LLM_PROVIDER / env.")
    parser.add_argument("-d", "--dialect", default="tsql")
    parser.add_argument("--limit", type=int, default=None,
                        help="Process at most N files total (dry run).")
    args = parser.parse_args()

    if not args.views and not args.procs:
        print("Provide at least one --views or --procs folder.", file=sys.stderr)
        return 1

    describe_folders(
        view_dirs=args.views,
        proc_dirs=args.procs,
        out_path=args.out,
        schema_path=args.schema,
        provider=args.provider,
        dialect=args.dialect,
        limit=args.limit,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
