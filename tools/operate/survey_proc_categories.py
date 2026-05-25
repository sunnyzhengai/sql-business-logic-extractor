"""Phase A of stored-proc extraction: classify each .sql file as
read-only / reporting / ETL based on DML statement counts.

Usage from a Fabric notebook:

    from tools.operate.survey_proc_categories import survey_proc_directory
    survey_proc_directory("/lakehouse/default/Files/data/mychart_sps/")

Or as a CLI:

    python -m tools.operate.survey_proc_categories /path/to/proc_dir

For each file, counts:

  - DML writes: INSERT INTO, UPDATE ... SET, DELETE FROM, MERGE INTO,
                TRUNCATE TABLE
  - SELECT statements (top-level only, excluding subqueries/CTEs as
    best as regex can manage)
  - EXEC calls (sub-proc invocations -- a Phase E concern)

Categorizes each proc:

  - read_only    : zero DML writes; one or more SELECTs
  - reporting    : DML writes only into temp tables (#TempA) or table
                   variables (@TableVar); returns SELECTs from those
  - etl          : DML writes into persistent tables (no #/@ prefix)
  - mixed        : any other shape (procs that do both)
  - parse_skipped : file didn't appear to contain a CREATE PROCEDURE

The categorization is REGEX-based and approximate -- intended as a
SURVEY artifact, not a parse-quality verdict. A proc flagged as
read_only here might still surprise us when sqlglot runs against it
in Phase C; we use the survey to triage, not to commit.

Output: a markdown summary + per-proc CSV, plus a printed verdict
tally so the user sees the bucket distribution at a glance.
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from typing import Iterable


# DML and proc-wrapper patterns. Word-boundary anchored; case-insensitive
# applied via re.IGNORECASE at use sites.
_HAS_CREATE_PROCEDURE = re.compile(
    r"\bCREATE\s+(?:OR\s+ALTER\s+)?PROC(?:EDURE)?\b", re.IGNORECASE
)
_DML_PATTERNS = {
    "INSERT_INTO":    re.compile(r"\bINSERT\s+(?:INTO\s+)?(\S+)", re.IGNORECASE),
    "UPDATE_SET":     re.compile(r"\bUPDATE\s+(\S+)\s+SET\b",     re.IGNORECASE),
    "DELETE_FROM":    re.compile(r"\bDELETE\s+(?:FROM\s+)?(\S+)", re.IGNORECASE),
    "MERGE_INTO":     re.compile(
        r"\bMERGE\s+(?:INTO\s+)?(\S+)\s+(?:USING|AS)\b", re.IGNORECASE
    ),
    "TRUNCATE_TABLE": re.compile(r"\bTRUNCATE\s+TABLE\s+(\S+)", re.IGNORECASE),
}
_SELECT_COUNT_RE = re.compile(r"\bSELECT\b", re.IGNORECASE)
_EXEC_RE = re.compile(r"\bEXEC(?:UTE)?\s+(\w+(?:\.\w+)?)", re.IGNORECASE)


def _is_temp_target(target: str) -> bool:
    """A DML target that's a temp table (#X) or table variable (@X) is
    scope-internal -- the proc isn't writing to a persistent table."""
    t = target.strip().strip(",;()").strip()
    # Bracket-quoted variants get normalized.
    t = t.strip("[]")
    return t.startswith("#") or t.startswith("@")


def _classify(write_targets: list[str], select_count: int, has_create_proc: bool) -> str:
    """Bucket the proc based on its DML-write profile."""
    if not has_create_proc:
        return "parse_skipped"
    persistent_writes = [t for t in write_targets if not _is_temp_target(t)]
    temp_writes = [t for t in write_targets if _is_temp_target(t)]
    if not write_targets:
        # No writes at all -- pure read.
        return "read_only" if select_count > 0 else "parse_skipped"
    if persistent_writes and select_count > 0:
        # Writes to persistent tables AND returns SELECTs -- mixed.
        if temp_writes:
            return "mixed"  # writes to both temp and persistent
        # Could still be ETL with a final status SELECT; the user
        # decides. Flag as mixed for steward review.
        return "mixed"
    if persistent_writes:
        return "etl"
    if temp_writes:
        return "reporting"
    return "mixed"


def survey_one_file(path: Path) -> dict:
    """Survey one .sql file. Returns a dict of counts + verdict."""
    text = path.read_text(encoding="utf-8", errors="replace")
    has_create_proc = bool(_HAS_CREATE_PROCEDURE.search(text))
    select_count = len(_SELECT_COUNT_RE.findall(text))
    exec_count = len(_EXEC_RE.findall(text))

    write_counts: dict[str, int] = {}
    write_targets: list[str] = []
    for name, pattern in _DML_PATTERNS.items():
        matches = pattern.findall(text)
        write_counts[name] = len(matches)
        write_targets.extend(matches)

    verdict = _classify(write_targets, select_count, has_create_proc)

    return {
        "filename":        path.name,
        "verdict":         verdict,
        "has_create_proc": has_create_proc,
        "select_count":    select_count,
        "exec_count":      exec_count,
        **{f"n_{k.lower()}": v for k, v in write_counts.items()},
        "n_persistent_writes": sum(
            1 for t in write_targets if not _is_temp_target(t)
        ),
        "n_temp_writes":   sum(1 for t in write_targets if _is_temp_target(t)),
        "write_targets":   ", ".join(write_targets[:10]),
    }


def survey_proc_directory(
    corpus_dir: str | Path,
    output_csv: str | Path | None = None,
) -> dict:
    """Survey every .sql file in `corpus_dir` and bucket by category.

    Prints a verdict tally + per-file one-line summary. If
    `output_csv` is provided, writes the full survey to that path
    for follow-up analysis.

    Returns the dict of per-file rows for programmatic use.
    """
    corpus_path = Path(corpus_dir)
    if not corpus_path.is_dir():
        print(f"ERROR: {corpus_path} is not a directory")
        return {}

    sql_files = sorted(corpus_path.glob("*.sql"))
    if not sql_files:
        print(f"WARNING: no .sql files in {corpus_path}")
        return {}

    print(f"Surveying {len(sql_files)} files in {corpus_path}\n")
    rows = [survey_one_file(f) for f in sql_files]

    # Verdict tally.
    tally: dict[str, int] = {}
    for row in rows:
        tally[row["verdict"]] = tally.get(row["verdict"], 0) + 1

    print("Category tally:")
    for verdict, n in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {n:>3}  {verdict}")
    print()

    # Per-file table.
    print(f"{'filename':<50s} {'verdict':<15s} {'sels':>5s} {'persist':>8s} {'temp':>5s} {'exec':>5s}")
    print("-" * 95)
    for row in rows:
        print(
            f"{row['filename']:<50s} {row['verdict']:<15s} "
            f"{row['select_count']:>5d} {row['n_persistent_writes']:>8d} "
            f"{row['n_temp_writes']:>5d} {row['exec_count']:>5d}"
        )

    # Optional CSV dump for follow-up analysis.
    if output_csv:
        out = Path(output_csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nFull survey CSV: {out}")

    # Interpretation hint based on tally.
    print()
    if tally.get("read_only", 0) >= len(rows) * 0.7:
        print(
            "INTERPRETATION: >= 70% are read_only. Phase C (read-only "
            "extractor) gets you most of the corpus -- proceed with that "
            "first; defer Phase D (ETL) until later."
        )
    elif tally.get("etl", 0) >= len(rows) * 0.5:
        print(
            "INTERPRETATION: >= 50% are etl. Phase C alone won't get you "
            "most of the corpus -- prioritize Phase D in parallel."
        )
    else:
        print(
            "INTERPRETATION: mixed corpus. Plan Phase C + D together; "
            "neither alone covers most of the procs."
        )

    return {"rows": rows, "tally": tally}


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python -m tools.operate.survey_proc_categories <proc_dir>")
        return 1
    survey_proc_directory(argv[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
