#!/usr/bin/env python3
"""Tool 9 -- per-view resolver timing audit.

Runs `extract_technical_lineage` (the heaviest step in the pipeline,
which is what `run_all` spends most of its time in) on each view with a
hard timeout. Records elapsed seconds + status. Continues past timeouts
so you get a complete corpus picture in ONE pass.

Output CSV columns:

    view_name, status, elapsed_sec, n_columns, error_message

Status values:
  - ok       -- resolved cleanly within the deadline
  - timeout  -- exceeded `timeout_sec` (likely pathological for the
                resolver; investigate or set aside before run_all)
  - error    -- raised an exception (parse error, resolver crash)

Notebook usage:

    from tools.timing_audit.batch import audit_timing
    audit_timing(
        input_dir='/lakehouse/default/Files/views',
        output_csv='/lakehouse/default/Files/outputs/timing_audit.csv',
        timeout_sec=30,
    )

After the audit, sort the CSV by elapsed_sec descending. The slow tail
is your investigation list. Setting aside any 'timeout' rows lets
run_all complete on the rest in predictable time.

Implementation note: uses signal.SIGALRM for the wall-clock deadline.
Linux-only, main-thread-only -- which matches Fabric notebook contexts.
The timeout fires reliably on pure-Python code; if a C extension holds
the GIL in a tight loop, the alarm fires but the interrupt may not
take effect until control returns to Python (rare for sqlglot, which
is mostly Python).
"""

import argparse
import csv
import signal
import sys
import time
from pathlib import Path

from sql_logic_extractor.products import extract_technical_lineage


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


class _Timeout(Exception):
    pass


def _alarm_handler(signum, frame):
    raise _Timeout()


def _resolve_with_timeout(sql: str, dialect: str, timeout_sec: int):
    """Run extract_technical_lineage; raise _Timeout if over deadline."""
    old = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(timeout_sec)
    try:
        return extract_technical_lineage(sql, dialect=dialect)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def audit_timing(input_dir: str,
                  output_csv: str = "timing_audit.csv",
                  *, timeout_sec: int = 30,
                  dialect: str = "tsql") -> int:
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory", file=sys.stderr)
        return 1
    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}", file=sys.stderr)
        return 1

    out = Path(output_csv)
    out.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["view_name", "status", "elapsed_sec", "n_columns",
                    "error_message"]

    # Stream rows to disk after each view so a kill keeps progress.
    n_ok = n_timeout = n_error = 0
    with out.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        f.flush()

        for i, path in enumerate(sql_files, 1):
            row = {
                "view_name": path.stem,
                "status": "",
                "elapsed_sec": "",
                "n_columns": "",
                "error_message": "",
            }
            t0 = time.time()
            try:
                sql = _read_sql_file(path)
                lineage = _resolve_with_timeout(sql, dialect, timeout_sec)
                row["status"] = "ok"
                row["elapsed_sec"] = f"{time.time() - t0:.2f}"
                row["n_columns"] = len(lineage.resolved_columns)
                n_ok += 1
            except _Timeout:
                row["status"] = "timeout"
                row["elapsed_sec"] = f"{timeout_sec}+"
                row["error_message"] = (
                    f"resolver did not return within {timeout_sec}s -- "
                    f"investigate or set aside"
                )
                n_timeout += 1
            except Exception as e:
                row["status"] = "error"
                row["elapsed_sec"] = f"{time.time() - t0:.2f}"
                row["error_message"] = f"{type(e).__name__}: {str(e)[:200]}"
                n_error += 1

            writer.writerow(row)
            f.flush()
            print(f"[{i}/{len(sql_files)}] {row['status']:<8} "
                  f"{row['elapsed_sec']:>7}s  {path.name}", flush=True)

    print(f"\nTiming audit -> {out}")
    print(f"  ok:      {n_ok}")
    print(f"  timeout: {n_timeout}  (investigate or set aside before run_all)")
    print(f"  error:   {n_error}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Per-view resolver timing audit with hard timeouts."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-o", "--output", default="timing_audit.csv")
    parser.add_argument("-t", "--timeout-sec", type=int, default=30,
                          help="Per-view wall-clock deadline (default: 30s)")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()
    return audit_timing(args.input_dir, args.output,
                          timeout_sec=args.timeout_sec, dialect=args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call audit_timing("
              "input_dir=..., output_csv=..., timeout_sec=30) from a cell.")
    else:
        sys.exit(main())
