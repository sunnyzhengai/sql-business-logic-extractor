#!/usr/bin/env python3
"""Tool 11 -- single-pass corpus extractor (Phase D, scope-correct tree).

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
  2. Parse + Layer 1 extract -> Layer 3 scope-correct resolve. Produces
     a ResolvedScopeTree with one scope per structural unit (CTE,
     derived table, subquery, set-op branch, main SELECT, lateral).
  3. For each scope: translate its columns to English via Tool 3's
     engineered translator; translate its filters via the same pattern
     library; emit a ScopeV1 with no cross-scope filter inheritance.
  4. Enrich main-scope columns with author_notes (comment_attachment),
     terms, and AST fingerprints.
  5. Build a ViewV1 (tree-shaped) and append a JSON line to corpus.jsonl.

Output:
  - corpus.jsonl: header + one ViewV1 per line. Each ViewV1 is a graph
    fragment (scopes are nodes; reads_from_* and base_columns are edges).
  - corpus_progress.txt: per-view timing log.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

from sqlglot import exp, parse_one

from sql_logic_extractor.business_logic import (
    classify_business_domain,
    load_schema,
)
from sql_logic_extractor.comment_attachment import (
    attach_to_columns,
    extract_view_level_notes,
)
from sql_logic_extractor.corpus_schema import (
    SCHEMA_VERSION,
    ColumnV1,
    FilterV1,
    InventoryRefV1,
    ReportV1,
    ScopeV1,
    TermV1,
    ViewV1,
    _to_jsonable,
)
from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.patterns import Context, translate
from sql_logic_extractor.products import _extract_columns_core
from sql_logic_extractor.resolve import (
    LineageResolver,
    ResolvedScope,
    ResolvedScopeTree,
    ScopedColumn,
    preprocess_ssms,
)
from sql_logic_extractor.term_extraction import (
    extract_terms,
    load_default_synonyms,
)
from tools.similar_logic_grouper.fingerprint import fingerprint as ast_fingerprint


# ---------- file reader (BOM-aware) ---------------------------------------

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


# ---------- English translation helper ------------------------------------

def _translate_fragment(sql_frag: str, ctx: Context, dialect: str) -> str:
    """Walk a SQL fragment through the pattern library; return English.
    Falls back to the raw SQL on parse/translate failure."""
    s = (sql_frag or "").strip()
    if not s:
        return ""
    try:
        node = parse_one(s, dialect=dialect)
        if node is None:
            return s
        if isinstance(node, exp.Select) and node.selects:
            node = node.selects[0]
        if isinstance(node, exp.Alias):
            node = node.this
        english = (translate(node, ctx).english or "").strip()
        return english or s
    except Exception:
        return s


# ---------- scope -> ScopeV1 ----------------------------------------------

def _build_filter_v1(rf, ctx: Context, dialect: str) -> FilterV1:
    """Translate a ScopedFilter's expression for the business form."""
    expr = (rf.expression or "").strip()
    english = _translate_fragment(expr, ctx, dialect) if expr else ""
    return FilterV1(
        expression=expr,
        english=english,
        kind=rf.kind or "where",
        subquery_scope_ids=tuple(rf.subquery_scope_ids or []),
    )


def _build_column_v1(
    sc: ScopedColumn,
    ctx: Context,
    dialect: str,
) -> ColumnV1:
    """Translate one ScopedColumn into a ColumnV1.

    `technical_description` is the column's SQL as resolved within its
    own scope. `business_description` is the engineered English translation
    of that SQL. Filters live on the scope, not the column.
    """
    expr = (sc.resolved_expression or sc.expression or "").strip()
    english = _translate_fragment(expr, ctx, dialect) if expr else ""
    fp = ast_fingerprint(expr, dialect=dialect) if expr else None
    base_tables = tuple(sc.base_tables or [])
    domain = classify_business_domain(sc.name, list(base_tables), expr)
    return ColumnV1(
        column_name=sc.name or "",
        column_type=sc.type or "unknown",
        technical_description=expr,
        business_description=english,
        business_domain=domain,
        base_columns=tuple(sc.base_columns or []),
        base_tables=base_tables,
        author_notes=(),                  # filled in for main-scope columns below
        term=TermV1(name_is_structural=True),  # filled in below
        fingerprint=fp,
    )


def _build_scope_v1(
    rs: ResolvedScope,
    ctx: Context,
    dialect: str,
) -> ScopeV1:
    return ScopeV1(
        id=rs.id,
        kind=rs.kind,
        filters=tuple(_build_filter_v1(f, ctx, dialect) for f in rs.filters),
        columns=tuple(_build_column_v1(c, ctx, dialect) for c in rs.columns),
        reads_from_scopes=tuple(rs.reads_from_scopes or []),
        reads_from_tables=tuple(rs.reads_from_tables or []),
    )


# ---------- view-level bullet report (per-scope sections) ------------------

def _format_scope_bullets(scope: ScopeV1, *, business: bool) -> list[str]:
    """Render one scope as a bullet section. `business=True` uses the
    English filter translations; otherwise raw SQL."""
    lines: list[str] = []
    header = f"Scope: {scope.id} ({scope.kind})"
    lines.append(header)

    if scope.reads_from_scopes:
        lines.append(f"  Reads from scopes: {', '.join(scope.reads_from_scopes)}")
    if scope.reads_from_tables:
        lines.append(f"  Reads from tables: {', '.join(scope.reads_from_tables)}")

    if scope.filters:
        lines.append("  Filters:")
        for f in scope.filters:
            text = f.english if business and f.english else f.expression
            lines.append(f"    - [{f.kind}] {text}")

    if scope.columns:
        lines.append("  Columns:")
        for c in scope.columns:
            text = c.business_description if business and c.business_description else c.technical_description
            lines.append(f"    - {c.column_name}: {text}")

    return lines


def _build_report(view_name: str, scopes: tuple[ScopeV1, ...]) -> ReportV1:
    tech_blocks: list[str] = []
    biz_blocks: list[str] = []
    for s in scopes:
        tech_blocks.append("\n".join(_format_scope_bullets(s, business=False)))
        biz_blocks.append("\n".join(_format_scope_bullets(s, business=True)))

    main_columns = next((s.columns for s in scopes if s.id == "main"), ())
    metrics = tuple(c.column_name for c in main_columns
                    if c.column_type not in ("passthrough", ""))[:10]
    primary_purpose = _infer_primary_purpose(main_columns)

    return ReportV1(
        technical_description="\n\n".join(tech_blocks),
        business_description="\n\n".join(biz_blocks),
        primary_purpose=primary_purpose,
        key_metrics=metrics,
        column_count=len(main_columns),
        use_llm=False,
    )


def _infer_primary_purpose(columns: tuple[ColumnV1, ...]) -> str:
    """Heuristic dominant-grain label, mirroring the legacy Tool 4 logic."""
    if not columns:
        return ""
    types = [c.column_type for c in columns]
    if "window" in types:
        return "Ranked / windowed analysis"
    if "aggregate" in types:
        return "Aggregated reporting"
    if "case" in types:
        return "Categorisation and classification"
    return "Row-level extraction"


# ---------- inventory (Tool 1) --------------------------------------------

def _build_inventory(sql: str, dialect: str) -> tuple[InventoryRefV1, ...]:
    inv = _extract_columns_core(sql, dialect=dialect)
    out: list[InventoryRefV1] = []
    for c in inv.columns:
        out.append(InventoryRefV1(
            table=c.table or "",
            column=c.column or "",
            database=c.database or "",
            schema=c.schema or "",
            reference_type="column",
            confidence="high" if (c.database or c.schema) else "medium",
        ))
    return tuple(out)


# ---------- per-column governance enrichment ------------------------------

def _enrich_main_scope(scopes: list[ScopeV1], view_name: str, sql: str, dialect: str) -> list[ScopeV1]:
    """Attach author_notes + terms to main-scope columns. CTE/subquery
    columns are NOT comment-attached today (the engine's
    comment_attachment library is not scope-aware); revisit if BI views
    start carrying meaningful inline comments inside CTEs.

    Returns a new scopes list (tuples are frozen, so we rebuild).
    """
    main_idx = next((i for i, s in enumerate(scopes) if s.id == "main"), None)
    if main_idx is None:
        return scopes
    main = scopes[main_idx]
    if not main.columns:
        return scopes

    # Run comment_attachment on a mutable dict-form of main columns.
    main_dicts = [{
        "column_name": c.column_name,
        "resolved_expression": c.technical_description,
        "author_notes": [],
    } for c in main.columns]
    try:
        attach_to_columns(sql, main_dicts, dialect=dialect)
    except Exception:
        pass

    # Term extraction over main columns.
    term_dicts = [{
        "column_name": c.column_name,
        "column_type": c.column_type,
        "resolved_expression": c.technical_description,
        "english_definition": c.business_description,
    } for c in main.columns]
    try:
        terms = extract_terms(
            view_name=view_name,
            column_translations=term_dicts,
            query_filters=tuple(f.expression for f in main.filters),
            synonyms=load_default_synonyms(),
        )
    except Exception:
        terms = []
    terms_by_name = {t.column_name: t for t in terms}

    enriched_cols: list[ColumnV1] = []
    for c, md in zip(main.columns, main_dicts):
        notes = tuple(md.get("author_notes") or [])
        t_obj = terms_by_name.get(c.column_name)
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
                is_passthrough=(c.column_type == "passthrough"),
            )
        enriched_cols.append(ColumnV1(
            column_name=c.column_name,
            column_type=c.column_type,
            technical_description=c.technical_description,
            business_description=c.business_description,
            business_domain=c.business_domain,
            base_columns=c.base_columns,
            base_tables=c.base_tables,
            author_notes=notes,
            term=term,
            fingerprint=c.fingerprint,
        ))

    new_main = ScopeV1(
        id=main.id,
        kind=main.kind,
        filters=main.filters,
        columns=tuple(enriched_cols),
        reads_from_scopes=main.reads_from_scopes,
        reads_from_tables=main.reads_from_tables,
    )
    new_scopes = list(scopes)
    new_scopes[main_idx] = new_main
    return new_scopes


# ---------- main per-view builder -----------------------------------------

def _build_view(view_name: str, sql: str, schema: dict | None, dialect: str) -> ViewV1:
    """Build a v3 ViewV1 (scope tree) for one SQL view.

    Preprocesses SSMS script boilerplate (`SET ANSI_NULLS ON`, `GO`,
    header comments produced by SSMS "Script View as CREATE TO File")
    via `preprocess_ssms` -- without this step sqlglot cannot parse
    the typical Fabric / SSMS export format. All downstream SQL-walking
    helpers (extractor, comment_attachment, inventory) consume the
    cleaned SQL so they see a coherent CREATE VIEW / SELECT statement.
    """
    clean_sql, _meta = preprocess_ssms(sql)
    if not clean_sql or not clean_sql.strip():
        clean_sql = sql.strip()

    extractor = SQLBusinessLogicExtractor(dialect=dialect)
    logic = to_dict(extractor.extract(clean_sql))

    resolver = LineageResolver(logic)
    tree: ResolvedScopeTree = resolver.resolve_all_scoped()

    ctx = Context(schema=schema or {})
    scopes_v1 = [_build_scope_v1(rs, ctx, dialect) for rs in tree.scopes]

    # Enrich main scope columns with author notes + term metadata.
    scopes_v1 = _enrich_main_scope(scopes_v1, view_name, clean_sql, dialect)

    inventory = _build_inventory(clean_sql, dialect)
    view_level_notes = tuple(extract_view_level_notes(clean_sql))
    report = _build_report(view_name, tuple(scopes_v1))

    return ViewV1(
        view_name=view_name,
        report=report,
        view_level_notes=view_level_notes,
        scopes=tuple(scopes_v1),
        view_outputs=tuple(tree.view_outputs),
        inventory=inventory,
    )


def _error_view(view_name: str, error_msg: str, sql: str | None = None) -> ViewV1:
    notes: list[str] = []
    if sql:
        try:
            notes = list(extract_view_level_notes(sql))
        except Exception:
            pass
    return ViewV1(
        view_name=view_name,
        report=ReportV1(
            technical_description=f"PARSE/RESOLVE ERROR: {error_msg}",
            primary_purpose="parse_error",
        ),
        view_level_notes=tuple(notes),
    )


# ---------- batch entry point ---------------------------------------------

def extract_corpus(
    input_dir: str,
    output_path: str = "corpus.jsonl",
    *,
    schema_path: str | None = None,
    dialect: str = "tsql",
) -> int:
    """Walk views, build a CorpusV1 (v3 scope tree), stream-write to JSONL."""
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

    n_ok = 0
    n_failed = 0
    t_start = time.time()

    with progress_path.open("w") as pf:
        pf.write(f"# extract_corpus started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        pf.write(f"# input_dir: {in_dir}\n")
        pf.write(f"# {len(sql_files)} view(s) to process\n")
        pf.flush()

    with out.open("w", encoding="utf-8", newline="") as f:
        f.write(json.dumps({
            "schema_version": SCHEMA_VERSION,
            "n_views": len(sql_files),
        }) + "\n")
        f.flush()

        for i, path in enumerate(sql_files, 1):
            view_name = path.stem
            t0 = time.time()
            err: str | None = None
            try:
                sql = _read_sql_file(path)
                view = _build_view(view_name, sql, schema, dialect)
                n_ok += 1
            except Exception as e:
                err = f"{type(e).__name__}: {e}"
                view = _error_view(view_name, err, sql=locals().get("sql"))
                n_failed += 1

            f.write(json.dumps(_to_jsonable(asdict(view))) + "\n")
            f.flush()

            elapsed = time.time() - t0
            n_scopes = len(view.scopes)
            n_main_cols = next((len(s.columns) for s in view.scopes if s.id == "main"), 0)
            base = (f"[{i}/{len(sql_files)}] {view_name}  ({elapsed:.1f}s)  "
                     f"scopes={n_scopes} main_cols={n_main_cols} "
                     f"inv={len(view.inventory)}")
            line = base if err is None else f"{base}  ERROR: {err[:160]}"
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
        description="Single-pass corpus extractor producing CorpusV1 v3 JSONL."
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
