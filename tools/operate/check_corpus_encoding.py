"""Check the file-level encoding of a corpus directory of .sql files.

When SSMS exports objects via "Generate Scripts" with the Unicode
option, the result is UTF-16 LE with a BOM. Python's default
`open(path)` reads as UTF-8 and silently produces garbage on UTF-16
input -- which then surfaces downstream as misleading "invalid
expression / unexpected token" parse failures in the sqlglot pass.

This script samples a corpus directory, inspects the first bytes of
each `.sql` file, and reports a verdict:

  - **UTF-16 LE (with BOM)** -- needs conversion to UTF-8 before
    the rest of the pipeline reads it. PowerShell one-liner:
        Get-ChildItem *.sql -Recurse | ForEach-Object {
            $c = Get-Content $_.FullName -Encoding Unicode -Raw
            Set-Content -Path $_.FullName -Value $c -Encoding UTF8
        }
    Or do the conversion in Python (see `convert_to_utf8()` below).
  - **UTF-8 with BOM** -- usually fine; some loaders trip on the
    BOM but most strip it. Worth flagging.
  - **UTF-8 / ASCII (no BOM)** -- normal. If parse failures still
    appear, they're real T-SQL parse issues, not encoding.

Run from the repo root, OR import into a Fabric notebook cell:

    # As a script:
    python -m tools.operate.check_corpus_encoding /lakehouse/default/Files/data/mychart_views

    # As an import in a notebook cell:
    from tools.operate.check_corpus_encoding import check_corpus_encoding
    check_corpus_encoding('/lakehouse/default/Files/data/mychart_views')
"""

from __future__ import annotations

import sys
from pathlib import Path


# BOM (byte-order-mark) byte signatures. Reading the first 2-4 bytes
# of a file tells us with near-certainty what encoding it was saved as.
BOM_SIGNATURES = {
    b"\xff\xfe":         "UTF-16 LE (BOM)",
    b"\xfe\xff":         "UTF-16 BE (BOM)",
    b"\xef\xbb\xbf":     "UTF-8 (BOM)",
}


# Try to detect Fabric's notebookutils / mssparkutils. The lakehouse
# mount at /lakehouse/default/Files/... is READ-ONLY for plain Python
# `open(file, "w")` -- writes execute without raising but silently
# don't persist to OneLake. Writes have to go through Fabric's fs API.
# Outside Fabric (local dev, CI), these imports fail and we fall back
# to plain Python writes.
_FABRIC_FS = None
try:
    import notebookutils  # type: ignore
    _FABRIC_FS = notebookutils.fs
except ImportError:
    try:
        import mssparkutils  # type: ignore
        _FABRIC_FS = mssparkutils.fs
    except ImportError:
        pass


def _write_text(path: Path, text: str) -> None:
    """Write `text` to `path` as UTF-8. In Fabric, uses notebookutils/
    mssparkutils.fs.put because plain Python `open(w)` silently fails
    on the lakehouse mount path. Outside Fabric, uses plain Python.
    """
    if _FABRIC_FS is not None:
        # Fabric's fs.put accepts mount paths and the abfss:// URI form.
        _FABRIC_FS.put(str(path), text, overwrite=True)
    else:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)


def _detect_encoding(first_bytes: bytes) -> str:
    """Classify a file's encoding from its leading bytes.

    Checks for BOMs first; falls back to "UTF-8 / ASCII (no BOM)"
    if no BOM is present. We don't try to distinguish ASCII from
    UTF-8-without-BOM because Python's `open(path)` reads both
    transparently as UTF-8 -- they're functionally identical
    for the extractor's purposes.
    """
    # UTF-8 BOM is 3 bytes, UTF-16 BOMs are 2 bytes. Try longest first.
    if first_bytes[:3] == b"\xef\xbb\xbf":
        return BOM_SIGNATURES[b"\xef\xbb\xbf"]
    if first_bytes[:2] == b"\xff\xfe":
        return BOM_SIGNATURES[b"\xff\xfe"]
    if first_bytes[:2] == b"\xfe\xff":
        return BOM_SIGNATURES[b"\xfe\xff"]
    return "UTF-8 / ASCII (no BOM)"


def check_corpus_encoding(
    corpus_dir: str | Path,
    sample_size: int = 3,
    sample_chars: int = 200,
) -> dict:
    """Inspect a corpus directory and report each .sql file's encoding.

    Parameters
    ----------
    corpus_dir : str | Path
        Directory containing the .sql files to inspect.
    sample_size : int, default 3
        Number of files to print full text-samples for. The full
        encoding tally is computed across ALL files; only the sample
        gets a readable preview to spot-check.
    sample_chars : int, default 200
        How many characters to print in the readable preview per
        sampled file.

    Returns
    -------
    dict with keys:
        n_files       -- total .sql files found
        by_encoding   -- {encoding_label: count}
        files         -- list of (filename, encoding_label)
        verdict       -- one-line bucket the corpus falls into

    The function ALSO prints a human-readable report to stdout for
    immediate use in a notebook cell. The return value is for
    downstream programmatic use (tests, follow-up triage).
    """
    corpus_path = Path(corpus_dir)
    print(f"Checking: {corpus_path}")
    if not corpus_path.is_dir():
        print(f"  ERROR: directory does not exist")
        return {"n_files": 0, "by_encoding": {}, "files": [], "verdict": "no-such-dir"}

    # Walk only the top level; the corpus convention is flat per folder.
    sql_files = sorted(corpus_path.glob("*.sql"))
    print(f"  Found {len(sql_files)} .sql files\n")

    if not sql_files:
        return {"n_files": 0, "by_encoding": {}, "files": [], "verdict": "empty-dir"}

    # Tally encoding across every file.
    by_encoding: dict[str, int] = {}
    files_with_enc: list[tuple[str, str]] = []
    for f in sql_files:
        with open(f, "rb") as fh:
            head = fh.read(4)
        enc = _detect_encoding(head)
        by_encoding[enc] = by_encoding.get(enc, 0) + 1
        files_with_enc.append((f.name, enc))

    # Print the tally.
    print("Encoding tally:")
    for enc, n in sorted(by_encoding.items(), key=lambda kv: -kv[1]):
        print(f"  {n:>3}  {enc}")
    print()

    # Print a readable preview for the first `sample_size` files so
    # the reader can eyeball: is this actually CREATE VIEW SQL, or
    # garbage characters?
    print(f"Readable preview (first {sample_size} files):\n")
    for f in sql_files[:sample_size]:
        print(f"  --- {f.name} ---")
        with open(f, "rb") as fh:
            raw_head = fh.read(8)
        print(f"  raw bytes: {raw_head!r}")
        # Read the text with the encoding we detected so the preview
        # shows meaningful SQL even for UTF-16 files.
        enc = _detect_encoding(raw_head)
        if enc.startswith("UTF-16 LE"):
            text_enc = "utf-16-le"
        elif enc.startswith("UTF-16 BE"):
            text_enc = "utf-16-be"
        elif enc.startswith("UTF-8 (BOM)"):
            text_enc = "utf-8-sig"
        else:
            text_enc = "utf-8"
        try:
            with open(f, "r", encoding=text_enc) as fh:
                preview = fh.read(sample_chars)
            # Strip leading BOM character if utf-16 didn't already.
            preview = preview.lstrip("﻿")
            print(f"  preview ({text_enc}):")
            for line in preview.splitlines()[:10]:
                print(f"    {line}")
        except Exception as e:
            print(f"  preview ERROR: {e}")
        print()

    # Verdict bucket -- single line summarizing the corpus state.
    if "UTF-16 LE (BOM)" in by_encoding or "UTF-16 BE (BOM)" in by_encoding:
        verdict = (
            "VERDICT: UTF-16 detected. The pipeline reads files as UTF-8 "
            "by default and will mis-parse these. Convert with the "
            "PowerShell or Python helper below before re-running extract."
        )
    elif "UTF-8 (BOM)" in by_encoding and len(by_encoding) == 1:
        verdict = (
            "VERDICT: UTF-8 with BOM. Most readers handle this, but if "
            "you see parse failures, try `encoding='utf-8-sig'` in the loader."
        )
    elif "UTF-8 / ASCII (no BOM)" in by_encoding and len(by_encoding) == 1:
        verdict = (
            "VERDICT: clean UTF-8 / ASCII. Encoding is not the cause of "
            "parse failures. Investigate as real T-SQL parse issues."
        )
    else:
        verdict = (
            "VERDICT: MIXED encodings in this directory. Convert "
            "non-UTF-8 files individually, then re-run."
        )
    print(verdict)

    return {
        "n_files": len(sql_files),
        "by_encoding": by_encoding,
        "files": files_with_enc,
        "verdict": verdict,
    }


def convert_to_utf8(corpus_dir: str | Path, dry_run: bool = True) -> int:
    """Convert any UTF-16 .sql files in `corpus_dir` to UTF-8 in place.

    Pass `dry_run=False` to actually rewrite the files; the default
    just lists what would change. Files already in UTF-8 (with or
    without BOM) are left alone.

    Returns the number of files converted (or that would be converted
    in dry-run mode).
    """
    corpus_path = Path(corpus_dir)
    n_converted = 0
    for f in sorted(corpus_path.glob("*.sql")):
        with open(f, "rb") as fh:
            head = fh.read(4)
        enc_label = _detect_encoding(head)
        # Map our label back to a Python codec for reading.
        if enc_label.startswith("UTF-16 LE"):
            read_enc = "utf-16-le"
        elif enc_label.startswith("UTF-16 BE"):
            read_enc = "utf-16-be"
        else:
            # Already UTF-8 (with or without BOM) -- skip.
            continue

        action = "Would convert" if dry_run else "Converting"
        print(f"  {action}: {f.name} ({enc_label} -> UTF-8)")

        if not dry_run:
            # Read with the source encoding, strip any leading BOM,
            # write back as plain UTF-8 via _write_text (which routes
            # through Fabric's fs API when running in a Fabric notebook).
            with open(f, "r", encoding=read_enc) as fh:
                text = fh.read().lstrip("﻿")
            _write_text(f, text)
        n_converted += 1

    if dry_run:
        print(f"\nDry run: {n_converted} files would be converted.")
        print("Re-run with dry_run=False to apply.")
    else:
        fabric_note = " (via Fabric fs.put)" if _FABRIC_FS is not None else ""
        print(f"\nConverted {n_converted} files to UTF-8{fabric_note}.")
    return n_converted


def main(argv: list[str]) -> int:
    """CLI entry point: python -m tools.operate.check_corpus_encoding <dir>"""
    if len(argv) < 2:
        print("Usage: python -m tools.operate.check_corpus_encoding <corpus_dir>")
        print()
        print("Examples:")
        print("  python -m tools.operate.check_corpus_encoding "
              "/lakehouse/default/Files/data/mychart_views")
        return 1
    check_corpus_encoding(argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
