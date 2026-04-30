#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convert a Clarity metadata CSV export to clarity_schema.json.

Source query (run at work against Clarity):

    SELECT
        TBL.TABLE_NAME,
        TBL.TABLE_ID,
        TBL.TABLE_INTRODUCTION,
        COL.COLUMN_NAME,
        COL.DESCRIPTION,
        INI.COLUMN_INI,
        INI.COLUMN_ITEM
    FROM CLARITY.dbo.CLARITY_TBL TBL
    JOIN CLARITY.dbo.CLARITY_COL COL ON TBL.TABLE_ID = COL.TABLE_ID
    JOIN CLARITY.dbo.CLARITY_COL_INIITM INI ON COL.COLUMN_ID = INI.COLUMN_ID
    WHERE TABLE_NAME IN (...)
      AND TBL.TBL_DESCRIPTOR_OVR IS NOT NULL;

The trailing `TBL_DESCRIPTOR_OVR IS NOT NULL` clause is intentional — it
deduplicates CLARITY_TBL entries that appear twice. Do not remove it
without confirming the dedup is handled another way.

The column-to-(ini, item) relationship is 1:1 for Clarity tables, so
we emit flat `ini` and `item` fields per column rather than a list.

Usage:

    python3 scripts/csv_to_schema.py query_output.csv clarity_schema.json
"""

import csv
import json
import sys
from collections import OrderedDict
from pathlib import Path


REQUIRED_COLUMNS = {
    "TABLE_NAME",
    "COLUMN_NAME",
    # The rest are optional but typically present from the canonical query.
}


def csv_to_schema(csv_path: str, out_path: str) -> None:
    tables: "OrderedDict[str, dict]" = OrderedDict()

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise SystemExit(
                f"CSV is missing required columns: {sorted(missing)}. "
                f"Got headers: {reader.fieldnames}"
            )

        for row in reader:
            tname = (row.get("TABLE_NAME") or "").strip()
            if not tname:
                continue
            if tname not in tables:
                tables[tname] = {
                    "name": tname,
                    "id": (row.get("TABLE_ID") or "").strip() or None,
                    "description": (row.get("TABLE_INTRODUCTION") or "").strip() or None,
                    "columns": [],
                }
            col = {
                "name": (row.get("COLUMN_NAME") or "").strip(),
                "description": (row.get("DESCRIPTION") or "").strip() or None,
            }
            ini = (row.get("COLUMN_INI") or "").strip()
            item = (row.get("COLUMN_ITEM") or "").strip()
            if ini:
                col["ini"] = ini
            if item:
                col["item"] = item
            tables[tname]["columns"].append(col)

    # Strip keys with None values for a cleaner output.
    def clean(d: dict) -> dict:
        return {k: v for k, v in d.items() if v is not None}

    schema = {
        "tables": [
            {**clean(t), "columns": [clean(c) for c in t["columns"]]}
            for t in tables.values()
        ]
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)

    print(
        f"Wrote {out_path}: {len(schema['tables'])} table(s), "
        f"{sum(len(t['columns']) for t in schema['tables'])} column(s)."
    )


def main() -> None:
    if len(sys.argv) != 3:
        print("usage: csv_to_schema.py <input.csv> <output.json>", file=sys.stderr)
        raise SystemExit(2)
    csv_to_schema(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
