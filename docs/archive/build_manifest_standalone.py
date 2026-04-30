import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Optional

from sqlglot import exp, parse_one
from sqlglot.optimizer.qualify import qualify


def _read_sql(path: Path) -> str:
    """Read a SQL file, handling SSMS's default UTF-16 LE BOM and other common
    encodings. SSMS scripts views as UTF-16 LE by default, which causes
    'Invalid token' errors when read as UTF-8."""
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le")[1:]      # strip BOM character
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")[1:]
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8")[1:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-16-le", errors="replace")


_GO_RE = re.compile(r"^\s*GO\s*$", re.IGNORECASE)
_USE_RE = re.compile(r"^\s*USE\s+", re.IGNORECASE)
_SET_RE = re.compile(
    r"^\s*SET\s+(ANSI_NULLS|QUOTED_IDENTIFIER|NOCOUNT|TRANSACTION|ARITHABORT|"
    r"NUMERIC_ROUNDABORT|CONCAT_NULL_YIELDS_NULL|ANSI_PADDING|ANSI_WARNINGS|"
    r"DATEFORMAT|DATEFIRST)\b",
    re.IGNORECASE,
)
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


# SQL reserved words that occasionally leak through sqlglot's parser as
# unqualified Column nodes — typically from window-function frame clauses
# like `ROWS UNBOUNDED PRECEDING` or `CURRENT ROW`. These are not real
# column references and would clutter the manifest with confusing rows
# pointing at no real table. Filtered ONLY when the reference has no
# table qualifier — a quoted column literally named `[ROW]` referenced as
# `t.[ROW]` survives because that's a real column on a real table.
_KEYWORD_FALSE_POSITIVES = {
    "row", "rows", "range",
    "current", "unbounded", "preceding", "following",
    "default", "null", "true", "false",
}


def _strip_ssms_boilerplate(sql: str) -> str:
    """Remove USE / GO / SET-option statements and header block comments that
    SSMS prefixes onto exported view DDL. sqlglot.parse_one expects a single
    statement — this leaves it with just the CREATE VIEW."""
    sql = _BLOCK_COMMENT_RE.sub("", sql)
    cleaned: list[str] = []
    for line in sql.split("\n"):
        if _GO_RE.match(line) or _USE_RE.match(line) or _SET_RE.match(line):
            continue
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def _qualify_table(t: exp.Table) -> tuple[Optional[str], Optional[str], str]:
    """(database, schema, table) for a sqlglot Table node. In sqlglot's MySQL-
    rooted naming, `catalog` is the database and `db` is the schema."""
    return (
        t.args["catalog"].name if t.args.get("catalog") else None,
        t.args["db"].name if t.args.get("db") else None,
        t.name,
    )


def _build_qualifier_map(parsed: exp.Expression, cte_names: set[str]) -> dict[str, tuple]:
    """{alias_or_name (lowercased): (db, schema, table)} so we can resolve any
    alias the qualify pass leaves behind back to a fully qualified database
    object. Keys are lowercased because sqlglot's qualify pass normalises
    case on identifiers."""
    mapping: dict[str, tuple] = {}
    for t in parsed.find_all(exp.Table):
        if t.name in cte_names:
            continue
        full = _qualify_table(t)
        alias = t.alias_or_name
        if alias and alias != t.name:
            mapping[alias.lower()] = full
        mapping[t.name.lower()] = full
    return mapping


def _flatten_cte_columns(parsed: exp.Expression, cte_names: set[str],
                          dialect: str) -> dict[tuple[str, str], list[tuple]]:
    """For each CTE, qualify its body separately and emit:
        {(cte_alias_lower, col_name_lower): [(db, schema, table), ...]}

    so a top-level reference like `ar.referral_id` can be resolved back to the
    underlying base tables that the CTE reads from. One level of indirection;
    chained CTEs (CTE B reads from CTE A reads from base) are best-effort —
    columns bubble up as the union of every base table touched."""
    cte_col_map: dict[tuple[str, str], list[tuple]] = {}
    # Collect CTEs in declaration order so chained CTEs can be partially
    # resolved by reading already-built entries.
    for cte in parsed.find_all(exp.CTE):
        alias = (cte.alias_or_name or "").lower()
        body = cte.this  # the SELECT expression inside the CTE
        try:
            qualified_body = qualify(body.copy(), dialect=dialect)
        except Exception:
            qualified_body = body

        # Local qualifier map for tables inside *this* CTE's body.
        local_qual: dict[str, tuple] = {}
        for t in body.find_all(exp.Table):
            if t.name in cte_names and (t.name.lower() != alias):
                continue  # reference to another CTE — handled via cte_col_map
            full = _qualify_table(t)
            a = t.alias_or_name
            if a and a != t.name:
                local_qual[a] = full
            local_qual[t.name] = full

        # Tables this CTE selects from (used as fallback when qualify can't
        # pin a column to a specific table — e.g. SELECT * FROM x).
        local_tables: list[tuple] = list({_qualify_table(t)
                                          for t in body.find_all(exp.Table)
                                          if t.name not in cte_names})

        for col_node in qualified_body.find_all(exp.Column):
            col_name = (col_node.name or "").lower()
            if not col_name:
                continue
            tbl = col_node.table
            if tbl and tbl.lower() in cte_names and tbl.lower() != alias:
                # Column from a chained CTE — pull through whatever we already
                # know about that CTE's source.
                inner_sources = cte_col_map.get((tbl.lower(), col_name), [])
                cte_col_map.setdefault((alias, col_name), []).extend(inner_sources)
                continue
            if tbl:
                src = local_qual.get(tbl) or local_qual.get(tbl.lower())
                if src is None:
                    # Unknown alias — punt to the CTE's table set.
                    for s in local_tables:
                        cte_col_map.setdefault((alias, col_name), []).append(s)
                else:
                    cte_col_map.setdefault((alias, col_name), []).append(src)
            else:
                # No table on the column reference — attribute to all the
                # CTE's source tables (best effort).
                for s in local_tables:
                    cte_col_map.setdefault((alias, col_name), []).append(s)

    return cte_col_map


def _error_row(view_path: Path, msg: str) -> dict:
    return {
        "view_file": view_path.name,
        "view_name": view_path.stem,
        "referenced_database": "",
        "referenced_schema": "",
        "referenced_table": "",
        "referenced_column": msg,
        "reference_type": "parse_error",
        "confidence": "low",
    }


def extract_view_refs(view_path: Path, dialect: str = "tsql") -> list[dict]:
    """Parse one view file → list of manifest rows. Importable for per-file
    use in Spark / mssparkutils-driven loops."""
    sql = _read_sql(view_path)
    sql = _strip_ssms_boilerplate(sql)
    if not sql:
        return [_error_row(view_path, "EMPTY: file contained only SSMS boilerplate (USE/GO/SET)")]

    try:
        parsed = parse_one(sql, dialect=dialect)
    except Exception as e:
        return [_error_row(view_path, f"PARSE ERROR: {e}")]

    cte_names = {cte.alias_or_name for cte in parsed.find_all(exp.CTE)}

    self_name: Optional[str] = None
    if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Table):
        self_name = parsed.this.name

    qualifier_map = _build_qualifier_map(parsed, cte_names)
    cte_col_map = _flatten_cte_columns(parsed, {n.lower() for n in cte_names}, dialect)
    cte_names_lower = {n.lower() for n in cte_names}

    # Distinct (db, schema, table) tuples actually in scope — used to fan out
    # unqualified column references (no alias prefix). Without a schema we can't
    # tell which of the joined tables a bare column belongs to, so we emit a
    # low-confidence row per in-scope table and let the ETL team disambiguate.
    in_scope_tables: list[tuple] = sorted(
        {
            _qualify_table(t) for t in parsed.find_all(exp.Table)
            if t.name not in cte_names and t.name != self_name
        },
        key=lambda x: (x[0] or "", x[1] or "", x[2] or ""),
    )

    # Map every alias-to-a-CTE back to the CTE name. Without this, `FROM
    # ActiveReferrals AR` makes downstream `AR.col` references look opaque —
    # we'd need both `ar.col` (alias) and `activereferrals.col` (CTE name) to
    # find the entry in cte_col_map.
    cte_alias_to_name: dict[str, str] = {n.lower(): n.lower() for n in cte_names}
    for t in parsed.find_all(exp.Table):
        if t.name in cte_names:
            alias = t.alias_or_name
            if alias and alias != t.name:
                cte_alias_to_name[alias.lower()] = t.name.lower()

    # qualify() propagates table aliases → real table names on every Column.
    # It can fail on CREATE VIEW wrappers; fall back to qualifying just the
    # inner SELECT, then to the un-qualified parse if that also fails.
    try:
        qualified = qualify(parsed.copy(), dialect=dialect)
    except Exception:
        if isinstance(parsed, exp.Create) and parsed.expression is not None:
            try:
                qualified = qualify(parsed.expression.copy(), dialect=dialect)
            except Exception:
                qualified = parsed
        else:
            qualified = parsed

    view_name = self_name or view_path.stem

    rows: list[dict] = []
    seen: set[tuple] = set()

    # Column-level references.
    for col_node in qualified.find_all(exp.Column):
        col_name = col_node.name
        if not col_name:
            continue
        tbl = col_node.table

        # If the column references a CTE alias (or the CTE name directly),
        # look up the CTE's expanded column → base-table map and emit a row
        # per underlying table. This closes the gap between sqlglot's qualify
        # pass (which leaves CTE refs opaque) and what the manifest actually
        # needs (real database objects).
        if tbl and tbl.lower() in cte_alias_to_name:
            cte_canonical = cte_alias_to_name[tbl.lower()]
            sources = cte_col_map.get((cte_canonical, col_name.lower()), [])
            for db, schema, qualified_tbl in sources:
                row_key = (view_path.name, db, schema, qualified_tbl, col_name)
                if row_key in seen:
                    continue
                seen.add(row_key)
                rows.append({
                    "view_file": view_path.name,
                    "view_name": view_name,
                    "referenced_database": db or "",
                    "referenced_schema": schema or "",
                    "referenced_table": qualified_tbl or "",
                    "referenced_column": col_name,
                    "reference_type": "column",
                    "confidence": "high" if (db or schema) else "medium",
                })
            continue

        # Unqualified column reference (no alias prefix) — fan out one row per
        # in-scope non-CTE table, marked low-confidence. The ETL team can
        # filter on confidence to find references that need manual
        # disambiguation. Falls back to a single empty-table row if the view
        # has no in-scope tables (extremely unusual, but defensive). Note
        # `tbl` may be either None or empty string '' depending on whether
        # qualify() succeeded — both indicate "no table on this column".
        if not tbl:
            # Filter SQL-keyword false positives like `ROW` from window-function
            # frame clauses. Only when unqualified — a real column on a real
            # table referenced as `t.ROW` is preserved.
            if col_name.lower() in _KEYWORD_FALSE_POSITIVES:
                continue
            if in_scope_tables:
                for db, schema, qualified_tbl in in_scope_tables:
                    row_key = (view_path.name, db, schema, qualified_tbl, col_name)
                    if row_key in seen:
                        continue
                    seen.add(row_key)
                    rows.append({
                        "view_file": view_path.name,
                        "view_name": view_name,
                        "referenced_database": db or "",
                        "referenced_schema": schema or "",
                        "referenced_table": qualified_tbl or "",
                        "referenced_column": col_name,
                        "reference_type": "column",
                        "confidence": "low",
                    })
            else:
                row_key = (view_path.name, None, None, None, col_name)
                if row_key in seen:
                    continue
                seen.add(row_key)
                rows.append({
                    "view_file": view_path.name,
                    "view_name": view_name,
                    "referenced_database": "",
                    "referenced_schema": "",
                    "referenced_table": "",
                    "referenced_column": col_name,
                    "reference_type": "column",
                    "confidence": "low",
                })
            continue

        db, schema, qualified_tbl = qualifier_map.get(tbl.lower(), (None, None, tbl))
        row_key = (view_path.name, db, schema, qualified_tbl, col_name)
        if row_key in seen:
            continue
        seen.add(row_key)
        rows.append({
            "view_file": view_path.name,
            "view_name": view_name,
            "referenced_database": db or "",
            "referenced_schema": schema or "",
            "referenced_table": qualified_tbl or "",
            "referenced_column": col_name,
            "reference_type": "column",
            "confidence": "high" if (db or schema) else "medium",
        })

    # Table-level references — catches SELECT *, EXISTS, COUNT(*), etc.
    for tbl_node in parsed.find_all(exp.Table):
        if tbl_node.name in cte_names:
            continue
        if tbl_node.name == self_name:
            continue
        db, schema, name = _qualify_table(tbl_node)
        row_key = (view_path.name, db, schema, name, "*")
        if row_key in seen:
            continue
        seen.add(row_key)
        rows.append({
            "view_file": view_path.name,
            "view_name": view_name,
            "referenced_database": db or "",
            "referenced_schema": schema or "",
            "referenced_table": name,
            "referenced_column": "*",
            "reference_type": "table",
            "confidence": "high" if (db or schema) else "medium",
        })
    return rows

def build_manifest(input_dir: str, output_csv: str = "manifest.csv",
                   dialect: str = "tsql") -> int:
    """Notebook-callable entry point. Walks input_dir for *.sql files, parses
    each, writes a single CSV. Returns 0 on success, 1 on usage error."""
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory")
        return 1

    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}")
        return 1

    fieldnames = [
        "view_file", "view_name",
        "referenced_database", "referenced_schema",
        "referenced_table", "referenced_column",
        "reference_type", "confidence",
    ]

    all_rows: list[dict] = []
    for path in sql_files:
        print(f"Parsing: {path.name}")
        all_rows.extend(extract_view_refs(path, dialect=dialect))

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    # UTF-8 with BOM so Excel opens it cleanly on the ETL team's side.
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    err_count = sum(1 for r in all_rows if r["reference_type"] == "parse_error")
    print(f"\nWrote {len(all_rows)} rows from {len(sql_files)} view(s) → {out}")
    if err_count:
        print(f"  ({err_count} view(s) failed to parse — see 'parse_error' rows in CSV)")
    return 0

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a view-migration manifest CSV from a folder of SQL view DDL files."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="manifest.csv",
                        help="Output CSV path (default: manifest.csv)")
    parser.add_argument("-d", "--dialect", default="tsql",
                        help="sqlglot dialect (default: tsql)")
    args = parser.parse_args()
    return build_manifest(args.input_dir, args.output, args.dialect)


def _is_notebook() -> bool:
    """Detect Jupyter / Fabric notebook execution. When True, skip the CLI
    argparse path — kernels pass `-f /path/to/connection.json` in sys.argv
    which argparse doesn't recognize, breaking paste-the-whole-file-and-run.
    Inside a notebook the user calls build_manifest(...) directly anyway."""
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected — call build_manifest("
              "input_dir=..., output_csv=..., dialect='tsql') from a cell.")
    else:
        sys.exit(main())
