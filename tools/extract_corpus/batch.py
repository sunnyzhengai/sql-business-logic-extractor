#!/usr/bin/env python3
"""Tool 11 -- single-pass corpus extractor.

Notebook usage:

    from tools.extract_corpus.batch import extract_corpus
    extract_corpus(
        input_dir='/lakehouse/default/Files/views_healthy',
        output_path='/lakehouse/default/Files/outputs/corpus.jsonl',
        schema_path='/lakehouse/default/Files/schemas/clarity_schema.json',
    )

CLI:
    python -m tools.extract_corpus.batch <input_dir> [-o corpus.jsonl] [--schema ...]

For each view:
  1. Read SQL (BOM-aware).
  2. Single call to `generate_report_description(sql, schema, use_llm=False)`
     -- this internally chains the resolver + Tool 3 (English) +
     Tool 4 (summary), so we get everything in ONE resolver pass.
  3. Enrich per-column with author_notes (comment_attachment) and
     fingerprint (similar_logic_grouper.fingerprint).
  4. Build a ViewV1 with the compact representation (view-level
     filters / tables_referenced stored ONCE; columns reference by
     index; column-specific filters split into filters_extra).
  5. Append a JSON line to corpus.jsonl. Stream-write + flush so a
     kill mid-run keeps everything finished so far.

Output:
  - corpus.jsonl: header line + one ViewV1 per line.
  - corpus_progress.txt: per-view timing log (same pattern as run_all).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from sql_logic_extractor.business_logic import load_schema
from sql_logic_extractor.comment_attachment import (
    attach_to_columns,
    extract_view_level_notes,
)
from sql_logic_extractor.corpus_schema import (
    SCHEMA_VERSION,
    ColumnV1,
    InventoryRefV1,
    ReportV1,
    TermV1,
    ViewLevelV1,
    ViewV1,
    _to_jsonable,
)
from sql_logic_extractor.products import generate_report_description
from sql_logic_extractor.term_extraction import (
    extract_terms,
    load_default_synonyms,
)
from tools.similar_logic_grouper.fingerprint import fingerprint as ast_fingerprint


# ---------- file reader (BOM-aware, matches engine's pattern) -------------

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


# ---------- helpers --------------------------------------------------------

def _normalize_filter_strs(raw_filters) -> list[str]:
    """Filter entries can be list[str] OR list[dict{expression}].
    Normalize to list[str], dropping empty/None."""
    out: list[str] = []
    for f in raw_filters or []:
        if isinstance(f, dict):
            expr = f.get("expression") or ""
            if expr:
                out.append(expr)
        elif isinstance(f, str):
            if f:
                out.append(f)
    return out


def _ordered_dedup(items) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return tuple(out)


# ---------- main per-view builder -----------------------------------------

def _build_view(view_name: str, sql: str, desc, dialect: str) -> ViewV1:
    """Convert one ReportDescription (Tool 4 output, which embeds Tools
    1-3) into a CorpusV1.ViewV1. Single-pass: no extra parses or
    resolver work happens here.
    """
    bl = desc.business_logic
    lineage = bl.lineage
    inventory = lineage.inventory

    # ---- View-level: filters, tables, notes, report -------------------

    view_filters = _ordered_dedup(lineage.query_filters or [])

    # tables_referenced: union of every column's base_tables + every
    # inventory entry's table, ordered by first appearance.
    table_seen: list[str] = []
    seen_set: set[str] = set()
    for trans in bl.column_translations:
        for t in trans.get("base_tables", []) or []:
            if t and t not in seen_set:
                seen_set.add(t)
                table_seen.append(t)
    for c in inventory.columns:
        if c.table and c.table not in seen_set:
            seen_set.add(c.table)
            table_seen.append(c.table)
    tables_referenced = tuple(table_seen)

    view_level_notes = tuple(extract_view_level_notes(sql))

    report = ReportV1(
        technical_description=desc.technical_description,
        business_description=desc.business_description,
        primary_purpose=desc.primary_purpose,
        key_metrics=tuple(desc.key_metrics or []),
        column_count=len(bl.column_translations),
        use_llm=desc.use_llm,
    )
    view_level = ViewLevelV1(
        filters=view_filters,
        tables_referenced=tables_referenced,
        view_level_notes=view_level_notes,
        report=report,
    )

    # ---- Per-column: enrich with comments + terms + fingerprint -------

    # Make mutable copies so attach_to_columns can populate author_notes
    # without mutating the underlying business_logic translations.
    enriched: list[dict] = [dict(t) for t in bl.column_translations]
    attach_to_columns(sql, enriched, dialect=dialect)

    terms_list = extract_terms(
        view_name=view_name,
        column_translations=enriched,
        query_filters=view_filters,
        synonyms=load_default_synonyms(),
    )
    terms_by_name: dict[str, object] = {t.column_name: t for t in terms_list}

    table_idx = {t: i for i, t in enumerate(tables_referenced)}
    view_filter_set = set(view_filters)

    columns: list[ColumnV1] = []
    for trans in enriched:
        col_name = (trans.get("column_name") or "").strip()
        if not col_name:
            continue

        base_tables = trans.get("base_tables") or []
        base_tables_idx = tuple(
            table_idx[t] for t in base_tables if t in table_idx
        )
        base_columns = tuple(trans.get("base_columns") or [])

        # Filter split: column.filters in the resolver output already
        # includes view-level filters (they propagate). Anything in
        # col.filters that's NOT in view_filters is column-specific.
        col_filters = _normalize_filter_strs(trans.get("filters") or [])
        filters_extra = tuple(f for f in col_filters if f not in view_filter_set)
        # filters_inherited is True when ALL view filters appear on this
        # column (the typical case post-resolver). We default to True
        # (compact) when there's no view-level filter set to inherit.
        filters_inherited = (
            (not view_filters)
            or all(vf in col_filters for vf in view_filters)
        )
        # If filters_inherited would be False but view_filters is empty,
        # treat as True (no inheritance to disagree with).
        if not view_filters:
            filters_inherited = True

        # Term lookup (extract_terms may have skipped this column per
        # inclusion rules; emit empty TermV1 in that case).
        t_obj = terms_by_name.get(col_name)
        if t_obj is not None:
            term = TermV1(
                name_tokens=tuple(sorted(t_obj.name_tokens)),
                is_passthrough=t_obj.is_passthrough,
                name_is_structural=t_obj.name_is_structural,
                has_filters=t_obj.has_filters,
            )
        else:
            term = TermV1(
                name_is_structural=True,
                is_passthrough=(trans.get("column_type") == "passthrough"),
            )

        # Fingerprint (None if expression doesn't parse / is empty)
        fp = ast_fingerprint(trans.get("resolved_expression") or "", dialect=dialect)

        columns.append(ColumnV1(
            column_name=col_name,
            column_type=trans.get("column_type", "unknown"),
            resolved_expression=trans.get("resolved_expression") or "",
            base_tables_idx=base_tables_idx,
            base_columns=base_columns,
            filters_inherited=filters_inherited,
            filters_extra=filters_extra,
            english_definition=trans.get("english_definition") or "",
            english_definition_with_filters=trans.get("english_definition_with_filters") or "",
            business_domain=trans.get("business_domain") or "",
            author_notes=tuple(trans.get("author_notes") or []),
            term=term,
            fingerprint=fp,
        ))

    # ---- Inventory: one entry per (table, column) reference -----------

    inv_entries: list[InventoryRefV1] = []
    for c in inventory.columns:
        inv_entries.append(InventoryRefV1(
            table=c.table or "",
            column=c.column or "",
            database=c.database or "",
            schema=c.schema or "",
            reference_type="column",
            confidence="high" if (c.database or c.schema) else "medium",
        ))

    return ViewV1(
        view_name=view_name,
        view_level=view_level,
        columns=tuple(columns),
        inventory=tuple(inv_entries),
        use_llm=desc.use_llm,
    )


# ---------- error fallback ------------------------------------------------

def _error_view(view_name: str, error_msg: str, sql: str | None = None) -> ViewV1:
    """When a view fails, emit a minimal ViewV1 so the corpus has a
    placeholder row instead of silently dropping the file."""
    notes = []
    if sql:
        try:
            notes = list(extract_view_level_notes(sql))
        except Exception:
            pass
    return ViewV1(
        view_name=view_name,
        view_level=ViewLevelV1(
            view_level_notes=tuple(notes),
            report=ReportV1(
                technical_description=f"PARSE/RESOLVE ERROR: {error_msg}",
                primary_purpose="parse_error",
            ),
        ),
    )


# ---------- batch entry point ---------------------------------------------

def extract_corpus(
    input_dir: str,
    output_path: str = "corpus.jsonl",
    *,
    schema_path: str | None = None,
    dialect: str = "tsql",
) -> int:
    """Walk views, build a CorpusV1, stream-write to JSONL."""
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory", file=sys.stderr)
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}", file=sys.stderr)
        return 1

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    progress_path = out.parent / "corpus_progress.txt"

    schema = load_schema(schema_path) if schema_path else {}

    # Per-view counts for the summary
    n_ok = 0
    n_failed = 0
    t_start = time.time()

    with progress_path.open("w") as pf:
        pf.write(f"# extract_corpus started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        pf.write(f"# input_dir: {in_dir}\n")
        pf.write(f"# {len(sql_files)} view(s) to process\n")
        pf.flush()

    # Stream-write JSONL: header line first, then one view per line.
    # Flush after each line so a kill keeps progress.
    with out.open("w", encoding="utf-8", newline="") as f:
        f.write(json.dumps({
            "schema_version": SCHEMA_VERSION,
            "n_views": len(sql_files),
        }) + "\n")
        f.flush()

        for i, path in enumerate(sql_files, 1):
            view_name = path.stem
            t0 = time.time()
            try:
                sql = _read_sql_file(path)
                desc = generate_report_description(
                    sql, schema, use_llm=False, dialect=dialect,
                )
                view = _build_view(view_name, sql, desc, dialect)
                n_ok += 1
            except Exception as e:
                view = _error_view(view_name, f"{type(e).__name__}: {e}",
                                    sql=locals().get("sql"))
                n_failed += 1

            # Convert to JSON-friendly dict (tuples -> lists), write.
            from dataclasses import asdict
            f.write(json.dumps(_to_jsonable(asdict(view))) + "\n")
            f.flush()

            elapsed = time.time() - t0
            line = (f"[{i}/{len(sql_files)}] {view_name}  ({elapsed:.1f}s)  "
                     f"cols={len(view.columns)} inv={len(view.inventory)} "
                     f"filters={len(view.view_level.filters)}")
            print(line, flush=True)
            with progress_path.open("a") as pf:
                pf.write(line + "\n")
                pf.flush()

    total = time.time() - t_start
    with progress_path.open("a") as pf:
        pf.write(f"# finished at {time.strftime('%Y-%m-%d %H:%M:%S')} "
                  f"after {total:.1f}s\n")

    print(f"\ncorpus.jsonl written to {out}  ({len(sql_files)} views, "
          f"{n_ok} ok, {n_failed} failed, {total:.1f}s)")
    print(f"  Progress log: {progress_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Single-pass corpus extractor producing CorpusV1 JSONL."
    )
    parser.add_argument("input_dir")
    parser.add_argument("-o", "--output", default="corpus.jsonl")
    parser.add_argument("--schema", default=None)
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()
    return extract_corpus(
        args.input_dir, args.output,
        schema_path=args.schema, dialect=args.dialect,
    )


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call extract_corpus("
              "input_dir=..., output_path=...) from a cell.")
    else:
        sys.exit(main())
