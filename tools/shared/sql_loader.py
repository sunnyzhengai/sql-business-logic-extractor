"""Canonical SQL file loader: encoding + SSMS preamble in one call.

Use `load_clean_sql(path)` in any new diagnostic / analyzer / script
that reads a raw .sql file. It handles two recurring traps:
  1. SSMS exports as UTF-16 LE with BOM; Python's default UTF-8 read
     produces garbled text.
  2. SSMS prepends USE / GO / SET / Object header / CREATE wrapper;
     sqlglot trips on all of these.

See docs/parsing_field_guide.md cards 1, 2, 3 for the traps.
"""

from __future__ import annotations

from pathlib import Path


# BOM signatures: each maps to its Python codec name.
# Documented in docs/parsing_field_guide.md card 1.
_UTF16_LE_BOM = b"\xff\xfe"
_UTF16_BE_BOM = b"\xfe\xff"
_UTF8_BOM = b"\xef\xbb\xbf"


def read_sql_robust(path):
    """Read a SQL file with BOM-aware encoding detection.

    Strips the BOM character from the returned string so downstream
    parsers don't see it. Falls back to UTF-16 LE with replacement
    on undetected UTF-16 (rare but happens).
    """
    raw = Path(path).read_bytes()
    if raw.startswith(_UTF16_LE_BOM):
        return raw.decode("utf-16-le")[1:]
    if raw.startswith(_UTF16_BE_BOM):
        return raw.decode("utf-16-be")[1:]
    if raw.startswith(_UTF8_BOM):
        return raw.decode("utf-8")[1:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-16-le", errors="replace")


def load_clean_sql(path):
    """Read a .sql file and return preprocessed SQL ready for sqlglot.

    Returns (clean_sql, metadata) where metadata may contain
    object_type, schema, name, script_date from the SSMS Object
    header. If preprocess returns empty, returns it as-is; caller
    decides whether to fall back to raw.
    """
    from sql_logic_extractor.resolve import preprocess_ssms
    raw = read_sql_robust(path)
    return preprocess_ssms(raw)


def _is_lakehouse_path(path):
    """True if path looks like a Fabric lakehouse mount path.

    Used by callers that want to detect writes to the lakehouse
    (which can be unreliable depending on the mount state).
    """
    p = str(path)
    return p.startswith("/lakehouse/") and "/Files/" in p


def write_to_lakehouse(local_path, lakehouse_path):
    """Copy a local file to a lakehouse path via Fabric's fs API.

    Tries notebookutils.fs.cp first, then mssparkutils.fs.cp (older
    Fabric runtimes), then falls back to shutil.copy (local dev/CI).

    NOTE: on some Fabric runtimes fs.cp raises Py4JJavaError when
    the lakehouse mount is in an ambiguous or failed state. If you
    hit that, fix the mount (re-pin the default lakehouse, restart
    kernel) rather than working around it in code.
    """
    src_uri = "file://" + str(local_path)
    dst = str(lakehouse_path)
    try:
        import notebookutils
        notebookutils.fs.cp(src_uri, dst, recurse=False)
        return
    except ImportError:
        pass
    try:
        import mssparkutils
        mssparkutils.fs.cp(src_uri, dst, recurse=False)
        return
    except ImportError:
        pass
    import shutil
    shutil.copy(local_path, lakehouse_path)
