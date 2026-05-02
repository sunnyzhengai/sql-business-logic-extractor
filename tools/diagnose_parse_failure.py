#!/usr/bin/env python3
"""Diagnose why sqlglot can't parse a particular .sql view.

Reads a single .sql file, runs it through the engine's preprocessor and
sqlglot, and prints a STRUCTURED report you can paste back. The report
redacts string literals and numeric data values by default (so it's safe
to share even when the underlying view contains sensitive data) -- but
keeps SQL keywords, identifiers, and structural tokens intact, since
those are what tells us what the parser hates.

Usage in a Fabric notebook cell:

    import sys
    sys.path.insert(0, '/lakehouse/default/Files')   # repo root in your lakehouse
    from tools.diagnose_parse_failure import diagnose
    diagnose('/lakehouse/default/Files/views/the_failing_view.sql')

Or CLI:

    python -m tools.diagnose_parse_failure /path/to/view.sql

What the script does, in order:

  Stage 1 -- parse raw file as-is.
  Stage 2 -- parse after engine's preprocess_ssms (strips USE/GO/SET).
  Stage 3 -- try a sequence of common workarounds; report which (if any)
             unblocks the parse: bracket-newline collapse, NOLOCK strip,
             OPTION clause strip.
  Stage 4 -- print the failing token's line/col + a 5-line redacted
             context window around it.

Paste the entire output back into the conversation -- it's redacted and
gives a precise pointer to the construct sqlglot can't handle.
"""

import re
import sys
from pathlib import Path

import sqlglot

# Repo root for relative imports when run as a script.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from sql_logic_extractor.resolve import preprocess_ssms


# ---------- redaction --------------------------------------------------

_STRING_LIT_RE = re.compile(r"'(?:[^']|'')*'")
_NUMBER_LIT_RE = re.compile(r"\b\d+\b")


def _redact(text: str) -> str:
    """Replace string-literal contents and bare numbers. Keeps SQL
    structure intact for debugging."""
    text = _STRING_LIT_RE.sub("'***'", text)
    text = _NUMBER_LIT_RE.sub("N", text)
    return text


# ---------- file reader (matches the engine's BOM handling) ------------

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


# ---------- parse attempt ----------------------------------------------

def _try_parse(sql: str) -> str | None:
    """Returns None on success; the exception message on failure."""
    try:
        sqlglot.parse_one(sql, dialect="tsql")
        return None
    except Exception as e:
        return f"{type(e).__name__}: {e}"


_LINE_COL_RE = re.compile(r"[Ll]ine\s+(\d+),\s*[Cc]ol\s+(\d+)")


def _extract_line_col(error_msg: str) -> tuple[int, int] | None:
    m = _LINE_COL_RE.search(error_msg or "")
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def _print_context(sql: str, line: int, col: int, *, window: int = 5) -> None:
    """Print [line-window..line+window] in redacted form, marking the column."""
    lines = sql.split("\n")
    start = max(0, line - 1 - window)
    end = min(len(lines), line + window)
    print(f"  Context (lines {start + 1}..{end}, redacted):")
    for i in range(start, end):
        marker = ">>" if i == line - 1 else "  "
        print(f"  {marker} {i + 1:4d}: {_redact(lines[i])}")
        if i == line - 1 and col > 0:
            # Caret at the failing column.
            print(f"        {' ' * (col - 1)}^")


# ---------- transforms (each tries to UNBLOCK the parse) ---------------

def _collapse_multiline_brackets(sql: str) -> str:
    """Internal whitespace inside [bracket-quoted] identifiers -> single space."""
    return re.sub(
        r"\[([^\[\]]*)\]",
        lambda m: f"[{re.sub(r'\\s+', ' ', m.group(1)).strip()}]",
        sql, flags=re.DOTALL,
    )


def _strip_table_hints(sql: str) -> str:
    """Strip T-SQL `WITH (NOLOCK)` / `WITH (INDEX(...))` / `WITH (HOLDLOCK)`
    style table hints that sqlglot trips on in some versions."""
    return re.sub(
        r"\bWITH\s*\(\s*(?:NOLOCK|HOLDLOCK|READUNCOMMITTED|"
        r"INDEX\s*\([^)]+\)|FORCESEEK|UPDLOCK|TABLOCK|TABLOCKX|XLOCK)"
        r"(?:\s*,\s*\w+(?:\s*\([^)]+\))?)*\s*\)",
        "",
        sql, flags=re.IGNORECASE,
    )


def _strip_option_clause(sql: str) -> str:
    """Strip trailing `OPTION (...)` query hint clauses."""
    return re.sub(
        r"\bOPTION\s*\([^)]*\)\s*;?", "", sql, flags=re.IGNORECASE,
    )


def _strip_set_xact_isolation(sql: str) -> str:
    """Strip `SET TRANSACTION ISOLATION LEVEL ...` and similar SET stmts."""
    return re.sub(
        r"^\s*SET\s+(?:TRANSACTION\s+ISOLATION\s+LEVEL\s+\w+(?:\s+\w+)*"
        r"|NOCOUNT\s+(?:ON|OFF)|XACT_ABORT\s+(?:ON|OFF)|ARITHABORT\s+(?:ON|OFF))\s*;?",
        "",
        sql, flags=re.IGNORECASE | re.MULTILINE,
    )


def _strip_print_statements(sql: str) -> str:
    """Strip `PRINT '...';` debug statements that sometimes leak in."""
    return re.sub(r"^\s*PRINT\s+[^;]+;?\s*$", "", sql,
                    flags=re.IGNORECASE | re.MULTILINE)


# ---------- main diagnose ----------------------------------------------

def diagnose(sql_path: str) -> None:
    path = Path(sql_path)
    if not path.is_file():
        print(f"ERROR: not a file: {path}")
        return

    raw = _read_sql_file(path)
    print(f"==== Diagnosing: {path.name} ({len(raw)} chars, "
          f"{raw.count(chr(10)) + 1} lines) ====\n")

    # Stage 1 ---------------------------------------------------------
    print("---- Stage 1: parse RAW file (no preprocessing) ----")
    err1 = _try_parse(raw)
    if err1 is None:
        print("OK -- raw file parses cleanly. The issue may be in your "
              "calling code, not in the SQL itself.")
        return
    print(f"FAIL: {err1[:300]}")
    lc1 = _extract_line_col(err1)
    if lc1:
        _print_context(raw, *lc1)
    print()

    # Stage 2 ---------------------------------------------------------
    print("---- Stage 2: parse after preprocess_ssms ----")
    try:
        clean, _ = preprocess_ssms(raw)
    except Exception as e:
        print(f"preprocess_ssms itself raised: {type(e).__name__}: {e}")
        clean = raw
    err2 = _try_parse(clean)
    if err2 is None:
        print("OK -- preprocess_ssms unblocks the parse. The engine "
              "already does this internally, so you shouldn't see this "
              "in normal Tool runs.")
        return
    print(f"FAIL: {err2[:300]}")
    lc2 = _extract_line_col(err2)
    if lc2:
        _print_context(clean, *lc2)
    print()

    # Stage 3 ---------------------------------------------------------
    transforms = [
        ("collapse multi-line bracket identifiers", _collapse_multiline_brackets),
        ("strip table hints (WITH (NOLOCK) etc.)",  _strip_table_hints),
        ("strip OPTION (...) clause",                _strip_option_clause),
        ("strip SET TRANSACTION/NOCOUNT/etc.",      _strip_set_xact_isolation),
        ("strip PRINT statements",                   _strip_print_statements),
    ]

    print("---- Stage 3: try common workarounds ----")
    cumulative = clean
    fixed_by = None
    for name, fn in transforms:
        candidate = fn(cumulative)
        if candidate == cumulative:
            print(f"  - {name}: no-op (pattern not present)")
            continue
        if _try_parse(candidate) is None:
            print(f"  - {name}: PARSES")
            fixed_by = name
            cumulative = candidate
            break
        else:
            print(f"  - {name}: applied, still fails")
            cumulative = candidate

    print()
    if fixed_by:
        print(f"---- Conclusion: parse succeeds after '{fixed_by}'. ----")
        print("Likely root cause: that construct. Either the engine's "
              "preprocess_ssms should strip it, or sqlglot needs a newer "
              "version. Share this report back to get the engine fix in.")
    else:
        # Stage 4 ----------------------------------------------------
        print("---- Stage 4: parse still fails -- final failing token ----")
        final_err = _try_parse(cumulative)
        print(f"Error: {final_err[:300]}")
        lc = _extract_line_col(final_err or "")
        if lc:
            _print_context(cumulative, *lc, window=10)
        print("\nNone of the standard workarounds fixed it. Paste this "
              "ENTIRE output back -- the redacted context window above "
              "shows the exact construct sqlglot can't handle.")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: diagnose_parse_failure.py <path/to/view.sql>",
              file=sys.stderr)
        return 2
    diagnose(sys.argv[1])
    return 0


if __name__ == "__main__":
    sys.exit(main())
