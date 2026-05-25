"""Canonical SQL file loader -- encoding + SSMS preamble in one call.

When a new diagnostic, analyzer, or extractor reads a raw .sql file,
it should use `load_clean_sql()` to avoid two recurring traps:

  1. SSMS exports as UTF-16 LE with BOM by default; Python's default
     UTF-8 read produces garbled text that sqlglot can't parse.
  2. SSMS prepends USE / GO / SET ANSI_NULLS / Object header / CREATE
     wrapper; sqlglot trips on all of these unless preprocess_ssms
     has run.

This module centralizes the encoding-detection AND the preprocess step
so new code doesn't have to remember to do both. Existing copies of
this logic in:

    tools/operate/diagnose_parse_failure.py
    tools/operate/check_corpus_encoding.py
    tools/operate/survey_proc_categories.py
    tools/p20_index/term_extraction.py
    notebooks/verify_union_fix.py
    notebooks/inspect_view_scope_tree.py

should migrate to this module as they're touched (no need for a
flag-day refactor).

Public API:

    read_sql_robust(path) -> str
        Read with BOM-aware encoding detection. Returns the file
        content as a Python string with the BOM character stripped.
        Use this when you need RAW SQL (e.g., the verify-encoding
        survey, the parse-failure diagnostic that wants to see what
        the file actually looks like).

    load_clean_sql(path) -> tuple[str, dict]
        Read + preprocess_ssms in one call. Returns (clean_sql,
        metadata). Use this when you want parseable SQL ready for
        sqlglot / SQLBusinessLogicExtractor.

Design constraints:
  - Read-only. Writing to Fabric lakehouse via Python is unreliable
    (see Trap #6 in docs/parsing_field_guide.md); convert + upload
    via PowerShell is the proven path for the rare cases that need it.
  - No silent normalization. If preprocess returns empty, the caller
    sees an empty string + the metadata. The decision on whether to
    fall back to raw belongs to the caller (different diagnostics
    want different behavior).
  - Independent of corpus structure. Works on a single .sql file;
    doesn't need a CorpusV1 / ViewV1 around it.
"""

from __future__ import annotations

from pathlib import Path


def read_sql_robust(path: str | Path) -> str:
    """Read a SQL file with BOM-aware encoding detection.

    Handles the three encodings that show up in SSMS / mssql-scripter
    exports across versions:

      - UTF-16 LE with BOM (\\xff\\xfe...) -- SSMS "Unicode" default.
      - UTF-16 BE with BOM (\\xfe\\xff...) -- rare; some legacy tools.
      - UTF-8 with BOM (\\xef\\xbb\\xbf...) -- some SSMS UTF-8 mode.
      - Plain UTF-8 or ASCII (no BOM) -- mssql-scripter default.

    Strips the BOM character from the returned string so downstream
    parsers don't see it.
    """
    raw = Path(path).read_bytes()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le")[1:]
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")[1:]
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8")[1:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        # Last-resort fallback: a file with no BOM that's actually
        # UTF-16. `errors="replace"` ensures we get SOME string back
        # rather than raising; the caller's parser will likely fail
        # downstream and that surfaces the real diagnosis.
        return raw.decode("utf-16-le", errors="replace")


def load_clean_sql(path: str | Path) -> tuple[str, dict]:
    """Read a .sql file and return preprocessed SQL ready for sqlglot.

    Pipeline:
      1. Read with BOM-aware encoding detection.
      2. Run preprocess_ssms to strip the USE/GO/SET preamble, the
         Object header comment, and the CREATE wrapper.

    Returns
    -------
    (clean_sql, metadata) where metadata is a dict that may contain
    keys `object_type`, `schema`, `name`, `script_date` from the SSMS
    Object header.

    Notes
    -----
    If preprocess_ssms returns empty (e.g., the file isn't an SSMS
    export and has no CREATE statement), the empty string is returned
    as-is. The caller decides whether to fall back to the raw SQL --
    different diagnostics want different behavior.
    """
    # Import locally to avoid pulling resolve.py at module load time
    # (it has heavier dependencies than this thin loader).
    from sql_logic_extractor.resolve import preprocess_ssms

    raw = read_sql_robust(path)
    return preprocess_ssms(raw)
