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
import os
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

def _load_env_robust() -> None:
    """Load KEY=VALUE pairs from a .env (repo root or cwd) into the environment.

    Tolerates the UTF-16 / BOM / cp1252 encodings Windows editors (Notepad)
    produce -- the SAME encoding gremlin as the SQL files -- instead of using
    python-dotenv's strict-UTF-8 read, which crashes the import on a UTF-16
    .env. NEVER raises: a missing or unreadable .env just means env vars come
    from the real environment.

    The .env is AUTHORITATIVE: its values OVERRIDE whatever is already in the
    environment. (We used to setdefault, but a stale provider/key left in the
    session memory then silently won over an edited .env -- so editing .env and
    re-running had no effect until a restart. Override fixes that.)
    """
    candidates = [Path(__file__).resolve().parents[2] / ".env", Path.cwd() / ".env"]
    for envp in candidates:
        try:
            if not envp.is_file():
                continue
            raw = envp.read_bytes()
            if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
                text = raw.decode("utf-16")              # BOM picks endianness
            else:
                text = None
                for enc in ("utf-8-sig", "utf-16", "latin-1"):
                    try:
                        text = raw.decode(enc)
                        break
                    except UnicodeDecodeError:
                        continue
                if text is None:
                    text = raw.decode("latin-1", errors="replace")
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip().strip('"').strip("'")
        except Exception:
            continue  # a bad .env must never crash the import


_load_env_robust()

from sql_logic_extractor.products import generate_report_description
from sql_logic_extractor.proc_normalize import ProcNotViewShaped, select_into_to_cte
from tools.report_description_generator.cli import _load_schema
# Reuse the project's canonical BOM-aware reader instead of a local copy --
# this is the loader the rest of the pipeline already uses for SSMS exports.
from tools.shared.sql_loader import read_sql_robust


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


def _render(target_sql: str, schema: dict, llm_client, per_column_llm: bool,
            table_scores: dict | None = None,
            detailed: bool = False) -> tuple[object, str]:
    """Run Tool 4 on already-prepared SQL. Returns (report_or_None, status).

    `per_column_llm=True` is full quality: one LLM call PER COLUMN to translate
    each, plus one for the summary. `per_column_llm=False` is FAST: engineered
    (mechanical) column translations -- no per-column LLM call -- and the LLM is
    used ONLY for the final high-level summary, so it's ~1 call/view instead of
    one-per-column. For large corpora of high-level descriptions, fast is the
    right trade: the per-column polish barely changes the rolled-up summary.

    `table_scores` is an optional dict mapping bare table name (upper) to
    (score, role) from table_importance.  Passed through to the description
    generator so it can emphasize center tables.

    `detailed=True` generates a parallel detailed_description alongside the
    high-level summary, for side-by-side comparison.
    """
    try:
        if per_column_llm:
            rpt = generate_report_description(target_sql, schema, use_llm=True,
                                              llm_client=llm_client,
                                              table_scores=table_scores,
                                              detailed=detailed)
        else:
            from sql_logic_extractor.products import (
                ReportDescription, extract_business_logic,
            )
            from sql_logic_extractor.business_logic import (
                summarize_llm, summarize_engineered, summarize_detailed_llm,
            )
            bl = extract_business_logic(target_sql, schema, use_llm=False)
            res = summarize_llm(bl, llm_client, table_scores=table_scores)
            detailed_desc = ""
            if detailed:
                engineered = summarize_engineered(bl, schema or {},
                                                     table_scores=table_scores)
                detailed_desc = summarize_detailed_llm(
                    bl, llm_client,
                    engineered_summary=engineered,
                    table_scores=table_scores,
                )
            rpt = ReportDescription(
                business_logic=bl,
                technical_description=res.get("technical_description", ""),
                business_description=res.get("business_description", ""),
                detailed_description=detailed_desc,
                primary_purpose=res.get("primary_purpose", ""),
                key_metrics=res.get("key_metrics", []),
                use_llm=True,
            )
    except Exception as e:  # parse/resolve/LLM failure for this one file
        return None, f"error:{type(e).__name__}: {e}"[:200]
    # summarize_llm SWALLOWS LLM failures: it stashes the error in
    # technical_description as "[LLM error: ...]" and leaves business_description
    # blank. Surface that as an error so it isn't a silent "(empty)".
    tech = rpt.technical_description or ""
    if tech.startswith("[LLM error"):
        return None, f"error:{tech[:180]}"
    return rpt, "ok"


# Direct-from-SQL description for objects the structured path can't handle
# (ETL / procedural procs, or a genuine parser gap). No structured lineage --
# just a high-level read of the SQL by the LLM. This is what guarantees ZERO
# skips: every object gets a description, one way or the other.
_RAW_DESC_SYSTEM = """You summarize a T-SQL stored procedure or view in plain business language.
This object could not be parsed into a single view, so describe it directly from the SQL.

Rules:
1. ACCURATE -- describe only what the SQL actually does.
2. HIGH-LEVEL -- 2-4 sentences.
3. Note what it READS (main source tables) and what it PRODUCES or LOADS (target tables, for ETL/load procs).
4. Plain business language; no SQL/column dumps.

Output JSON:
{"business_description": "...", "primary_purpose": "what it answers or what it loads", "key_metrics": ["notable outputs, if any"]}"""


class _RawReport:
    """Minimal report shape for the raw fallback (same fields the writer reads)."""
    def __init__(self, business_description: str, primary_purpose: str, key_metrics: list):
        self.business_description = business_description
        self.primary_purpose = primary_purpose
        self.key_metrics = key_metrics
        self.technical_description = ""


def _describe_raw_llm(sql: str, llm_client) -> tuple[object, str]:
    """Describe raw SQL directly via the LLM (no structured parse). Returns
    (report, "ok:raw") or (None, "error:..."). Used as the no-skip fallback."""
    from sql_logic_extractor.resolve import preprocess_ssms
    try:
        clean, _ = preprocess_ssms(sql)
    except Exception:
        clean = sql
    text = (clean or sql).strip()[:12000]      # cap context size
    try:
        res = llm_client.complete_json(_RAW_DESC_SYSTEM, "T-SQL to summarize:\n\n" + text)
    except Exception as e:
        return None, f"error:raw_llm {type(e).__name__}: {e}"[:180]
    biz = (res.get("business_description") or "").strip()
    if not biz:
        return None, "error:raw_llm returned empty"
    return _RawReport(biz, res.get("primary_purpose", ""), res.get("key_metrics", [])), "ok:raw"


def _describe_one(sql: str, schema: dict, llm_client, *, is_proc: bool,
                  per_column_llm: bool = True,
                  raw_fallback: bool = True,
                  table_scores: dict | None = None,
                  detailed: bool = False) -> tuple[object, str]:
    """Describe one SQL file. Returns (report_or_None, status).

    status is "ok" (structured), "ok:raw" (direct-from-SQL fallback),
    "skipped:<reason>", or "error:<msg>". With raw_fallback=True (the no-skip
    mode), anything the structured path can't handle is described directly from
    the SQL by the LLM instead of skipped.
    """
    target_sql = sql
    if is_proc:
        try:
            target_sql = select_into_to_cte(sql)
        except ProcNotViewShaped as e:
            # Not view-shaped (ETL / procedural). Don't skip -- describe the raw
            # SQL directly. (If raw_fallback is off, fall back to a skip.)
            if not _IS_CREATE_PROC_RE.search(sql):
                # actually a misfiled view -- try the structured path on it
                rpt, status = _render(sql, schema, llm_client, per_column_llm,
                                      table_scores=table_scores, detailed=detailed)
                if status == "ok":
                    return rpt, status
            if raw_fallback:
                return _describe_raw_llm(sql, llm_client)
            return None, f"skipped:{e.reason}"

    rpt, status = _render(target_sql, schema, llm_client, per_column_llm,
                          table_scores=table_scores, detailed=detailed)
    if status.startswith("error") and raw_fallback:
        return _describe_raw_llm(sql, llm_client)      # structured failed -> raw
    return rpt, status


def _iter_sql(paths: list[str]) -> list[tuple[Path, str]]:
    """Yield (path, folder_label) for each entry. Each entry may be either a
    DIRECTORY (every *.sql in it) or a single *.sql FILE -- so you can point
    the run at whole folders OR a hand-picked list of files (e.g. just the
    ones that errored), and even mix the two."""
    out: list[tuple[Path, str]] = []
    for d in paths or []:
        p = Path(d)
        if p.is_file() and p.suffix.lower() == ".sql":
            out.append((p, p.parent.name))      # single file
        elif p.is_dir():
            for f in sorted(p.glob("*.sql")):    # whole folder
                out.append((f, p.name))
        else:
            print(f"WARNING: not a .sql file or directory, skipping: {p}",
                  file=sys.stderr)
    return out


def describe_folders(
    view_dirs: list[str],
    proc_dirs: list[str],
    *,
    out_path: str,
    schema_path: str | None = None,
    corpus_path: str | None = None,
    provider: str | None = None,
    dialect: str = "tsql",
    limit: int | None = None,
    per_column_llm: bool = True,
    raw_fallback: bool = True,
    detailed: bool = False,
) -> dict:
    """Describe every .sql in the given view + proc folders to one Markdown file.

    Args:
        view_dirs: folders whose .sql are treated as views (described directly).
        proc_dirs: folders whose .sql are treated as procs (normalized first).
        out_path: Markdown file to write.
        schema_path: optional Clarity schema JSON/YAML for richer descriptions.
        corpus_path: optional corpus.jsonl path. When provided, table-importance
            scores are computed from the corpus graph and passed to the
            description generator so it can emphasize center tables and
            de-emphasize lookups.
        provider: LLM provider override; defaults to SLE_LLM_PROVIDER / env.
        dialect: SQL dialect (default tsql).
        limit: process at most this many files total (handy for a dry run).
        detailed: when True (and LLM is available), generates a parallel
            detailed_description alongside the high-level business_description,
            grounded to the engineered technical scaffold.

    Returns a dict with per-file results and a status tally.
    """
    # Build the LLM client ONCE and reuse it for every file.
    from sql_logic_extractor.llm_client import make_llm_client
    llm_client = make_llm_client(provider=provider)

    schema = _load_schema(schema_path) if schema_path else {}

    # When a corpus is available, compute table-importance scores so
    # descriptions can emphasize center tables and de-emphasize lookups.
    table_scores: dict | None = None
    if corpus_path:
        try:
            from tools.p20_index.graph_builder import build_corpus_graph
            from tools.p30_analyze.projection import extract_table_projection
            from tools.p30_analyze.bridges import detect_bridge_tables, project_without_bridges
            from tools.p30_analyze.communities import detect_table_communities
            from tools.p30_analyze.table_importance import (
                rank_all_communities, build_table_scores_lookup,
            )
            g = build_corpus_graph(corpus_path)
            table_g = extract_table_projection(g)
            bridges = detect_bridge_tables(table_g)
            table_g_no_bridges = project_without_bridges(table_g, bridges)
            communities = detect_table_communities(table_g_no_bridges)
            # Include bridge tables back into their neighboring communities
            # so they get scores too (they're important, just not for clustering).
            for bridge_id in bridges:
                # Add bridge to the community it has most edges to
                best_comm, best_count = 0, 0
                for ci, comm in enumerate(communities):
                    count = sum(1 for t in comm if table_g.has_edge(bridge_id, t))
                    if count > best_count:
                        best_comm, best_count = ci, count
                if best_count > 0:
                    communities[best_comm].add(bridge_id)
            rankings = rank_all_communities(g, communities, schema_path=schema_path)
            table_scores = build_table_scores_lookup(rankings)
            print(f"Table importance: {len(table_scores)} tables scored from corpus\n")
        except Exception as e:
            print(f"WARNING: Could not compute table importance from corpus: {e}",
                  file=sys.stderr)
            table_scores = None

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
                sql = read_sql_robust(path)
                report, status = _describe_one(sql, schema, llm_client, is_proc=is_proc,
                                               per_column_llm=per_column_llm,
                                               raw_fallback=raw_fallback,
                                               table_scores=table_scores,
                                               detailed=detailed)
            except Exception as e:  # unreadable file, etc.
                report, status = None, f"error:{type(e).__name__}: {e}"[:200]

            bucket = status.split(":", 1)[0]
            tally[bucket] = tally.get(bucket, 0) + 1
            results.append({"file": str(path), "kind": kind, "status": status})

            # ---- write this file's section ----
            rel = f"{label}/{path.name}"
            if bucket == "ok" and report is not None:
                raw = status == "ok:raw"          # described directly from SQL (no lineage)
                tag = "  _(described directly from SQL)_" if raw else ""
                fh.write(f"## [{kind}] {rel}{tag}\n\n")
                fh.write(f"**Primary purpose:** {report.primary_purpose or '(n/a)'}\n\n")
                fh.write(f"**Description:** {report.business_description or '(empty)'}\n\n")
                if hasattr(report, "detailed_description") and report.detailed_description:
                    fh.write(f"**Detailed description:**\n\n{report.detailed_description}\n\n")
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


def scan_parse_errors(
    view_dirs: list[str],
    proc_dirs: list[str],
    *,
    schema_path: str | None = None,
    out_path: str = "parse_errors.md",
    dialect: str = "tsql",
) -> dict:
    """Fast PARSE-ONLY scan (NO LLM) over all files, to surface every distinct
    parser gap AT ONCE instead of one-at-a-time.

    Runs each file through the same parse/extract path the batch uses but with
    use_llm=False (mechanical, no API calls -> fast + free), catches errors, and
    groups them by a normalized signature (line/col numbers removed) so the same
    construct collapses to one entry. Writes a Markdown report: per distinct
    error, the count + a few example files and the offending line. Send me that
    report and I can write rules for all the patterns in one pass.
    """
    import re as _re
    from collections import defaultdict
    from sql_logic_extractor.resolve import preprocess_ssms

    schema = _load_schema(schema_path) if schema_path else {}
    work = ([(f, False) for f, _ in _iter_sql(view_dirs)] +
            [(f, True) for f, _ in _iter_sql(proc_dirs)])

    def _sig(msg: str) -> str:
        return _re.sub(r"[Ll]ine \d+,? ?[Cc]ol(?:umn)?:? ?\d+",
                       "Line N, Col N", msg).strip()[:160]

    def _offending_line(target_sql: str, msg: str) -> str:
        m = _re.search(r"[Ll]ine (\d+)", msg)
        if not m or not target_sql:
            return ""
        try:
            clean, _ = preprocess_ssms(target_sql)
            lines = (clean or target_sql).splitlines()
            i = int(m.group(1)) - 1
            return lines[i].strip()[:160] if 0 <= i < len(lines) else ""
        except Exception:
            return ""

    groups: dict[str, list] = defaultdict(list)
    n_ok = n_skip = n_err = 0

    for path, is_proc in work:
        target = ""
        try:
            sql = read_sql_robust(path)
            target = sql
            if is_proc:
                try:
                    target = select_into_to_cte(sql)
                except ProcNotViewShaped:
                    n_skip += 1          # not view-shaped != parse error
                    continue
            generate_report_description(target, schema, use_llm=False)
            n_ok += 1
        except Exception as e:
            n_err += 1
            msg = f"{type(e).__name__}: {e}".splitlines()[0]
            groups[_sig(msg)].append((path.name, _offending_line(target, msg)))

    ordered = sorted(groups.items(), key=lambda kv: -len(kv[1]))
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as fh:
        fh.write("# Parse-error scan\n\n")
        fh.write(f"- ok: {n_ok}   skipped (not view-shaped): {n_skip}   "
                 f"errored: {n_err}\n")
        fh.write(f"- distinct error patterns: {len(ordered)}\n\n---\n\n")
        for sig, examples in ordered:
            fh.write(f"## ({len(examples)}x) {sig}\n\n")
            for fname, snip in examples[:5]:
                fh.write(f"- `{fname}`" + (f" — `{snip}`\n" if snip else "\n"))
            fh.write("\n")

    print(f"scan: ok={n_ok} skipped={n_skip} error={n_err} | "
          f"{len(ordered)} distinct patterns -> {out}")
    return {"ok": n_ok, "skipped": n_skip, "error": n_err,
            "patterns": len(ordered), "out_path": str(out)}


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
    parser.add_argument("--corpus", default=None,
                        help="Path to corpus.jsonl. When provided, table-importance "
                             "scores are computed so descriptions emphasize center "
                             "tables and de-emphasize lookups.")
    parser.add_argument("--detailed", action="store_true", default=False,
                        help="Generate a parallel detailed description (grounded "
                             "to the engineered scaffold) alongside the high-level "
                             "summary. Adds ~1 extra LLM call per file.")
    args = parser.parse_args()

    if not args.views and not args.procs:
        print("Provide at least one --views or --procs folder.", file=sys.stderr)
        return 1

    describe_folders(
        view_dirs=args.views,
        proc_dirs=args.procs,
        out_path=args.out,
        schema_path=args.schema,
        corpus_path=args.corpus,
        provider=args.provider,
        dialect=args.dialect,
        limit=args.limit,
        detailed=args.detailed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
