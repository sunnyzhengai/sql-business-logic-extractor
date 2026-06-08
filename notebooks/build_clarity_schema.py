"""Build a LOCAL schema file from Epic Clarity CLARITY_TBL + CLARITY_COL CSVs.

Joins the two dictionary exports on TABLE_ID and writes the nested
{tables:[{name, columns:[{name,type,description,short_description,ini,item}]}]}
shape that Tool 3/4 consume (see sql_logic_extractor/patterns/columns.py).

IMPORTANT -- KEEP THE OUTPUT LOCAL. The Clarity data dictionary is Epic's
proprietary content; this repo is public. Write OUT_PATH to a Lakehouse
folder and do NOT commit it. Only this converter (code) lives in git.

The Fabric CSVs have NO header row (bcp/sqlcmd/Spark export omit it), so we
read with header=None and supply the column order explicitly. If your copy
DOES have a header, set HAS_HEADER=True.

Run in a Fabric cell after %pip install pandas pyyaml (pandas is preinstalled
in Fabric). Paste, edit the EDIT block, run.
"""

# %% [Cell: build local Clarity schema]

import json
from pathlib import Path

import pandas as pd

# ============================================================
# EDIT
# ============================================================
DICT_DIR = "/lakehouse/default/Files/data/dictionaries"           # where the two CSVs live
OUT_PATH = "/lakehouse/default/Files/data/dictionaries/clarity_schema.json"  # LOCAL output -- DO NOT COMMIT
VIEW_TABLES = None        # e.g. {"PATIENT", "REFERRAL"} to keep only those; None = all tables
HAS_HEADER = False        # Fabric exports have no header row; set True if your copy has one
ENCODING = "utf-8"        # if descriptions show garbled accents, try "latin-1"
# ============================================================

# Column order from Sunny's headered local copies. Used when HAS_HEADER=False
# to label the headerless Fabric CSVs. If a future export reorders columns,
# update these lists (or set HAS_HEADER=True so pandas reads the real header).
TBL_COLS = [
    "TABLE_ID", "TABLE_NAME", "EXTRACT_FILENAME", "RELEASED_VERSION_C",
    "LAST_MOD_VERSION_C", "BS_TEMPLATE_ID", "DEPENDENT_INI", "IS_EXTRACTED_YN",
    "LOAD_FREQUENCY", "LOAD_TYPE", "ROUTINE_NAME", "ORA_DATA_TBLSPACE",
    "ORA_INDEX_TBLSPACE", "ORA_OVRFL_TBLSPACE", "IS_PARTITIONED_YN",
    "PARTITION_TYPE", "PARTITION_RANGE",
]
COL_COLS = [
    "COLUMN_ID", "COLUMN_NAME", "TABLE_ID", "COL_DESCRIPTOR",
    "COL_DESCRIPTOR_OVR", "DATA_TYPE", "CLARITY_PRECISION", "CLARITY_SCALE",
    "HOUR_FORMAT", "RELEASED_VERSION_C", "LAST_MOD_VERSION_C", "IS_EXTRACTED_YN",
    "FORMAT_INI", "FORMAT_ITEM", "DESCRIPTION", "CM_PHY_OWNER_ID",
    "IS_PRESERVED_YN", "COLUMN_NOTES", "DEPRECATED_YN", "REAL_TM_ENABLED_YN",
    "RECORD_STATUS_C", "TRANSLATED_YN", "TRANS_EXTENSION_ID", "REPL_CHAR_YN",
    "REPLACEMENT_COLUMNS",
]


def _read(filename: str, names: list[str]) -> pd.DataFrame:
    """Read one dictionary CSV. dtype=str keeps IDs exact; keep_default_na
    keeps empty cells as "" (not NaN) so the mapping logic stays simple."""
    kw = dict(dtype=str, keep_default_na=False,
              encoding=ENCODING, encoding_errors="replace")
    path = f"{DICT_DIR}/{filename}"
    if HAS_HEADER:
        return pd.read_csv(path, **kw)
    return pd.read_csv(path, header=None, names=names, **kw)


tbl = _read("CLARITY_TBL.csv", TBL_COLS)
col = _read("CLARITY_COL.csv", COL_COLS)

# --- sanity check: confirm the headerless columns landed where we think ---
# If these don't look right (e.g. TABLE_NAME column holds numbers), the file's
# column ORDER differs from TBL_COLS/COL_COLS -- fix the lists and re-run.
print("TBL parsed shape:", tbl.shape, "| expected cols:", len(TBL_COLS))
print("COL parsed shape:", col.shape, "| expected cols:", len(COL_COLS))
print("TBL sample:", tbl[["TABLE_ID", "TABLE_NAME"]].head(2).to_dict("records"))
print("COL sample:", col[["TABLE_ID", "COLUMN_NAME", "DATA_TYPE", "DESCRIPTION"]]
      .head(2).to_dict("records"))
print("-" * 60)

# TABLE_ID -> TABLE_NAME. (This CLARITY_TBL export carries no table-level
# description column, so table descriptions are left blank.)
id2name = dict(zip(tbl["TABLE_ID"], tbl["TABLE_NAME"]))

want = {v.upper() for v in VIEW_TABLES} if VIEW_TABLES else None
tables: dict[str, dict] = {}
unmatched = 0

for r in col.itertuples(index=False):
    tname = id2name.get(r.TABLE_ID)
    if not tname:
        unmatched += 1            # column whose TABLE_ID isn't in CLARITY_TBL
        continue
    if want is not None and tname.upper() not in want:
        continue

    # type, with precision/scale when present (skip for date-ish types).
    typ = r.DATA_TYPE or "unknown"
    if r.CLARITY_PRECISION and not typ.upper().startswith(("DATE", "TIME")):
        typ = f"{typ}({r.CLARITY_PRECISION}" + \
              (f",{r.CLARITY_SCALE}" if r.CLARITY_SCALE else "") + ")"

    # description = full text; short_description = concise label the
    # translator prefers (override first, else Epic's standard descriptor).
    description = r.DESCRIPTION or r.COL_DESCRIPTOR or r.COL_DESCRIPTOR_OVR or r.COLUMN_NAME
    short = r.COL_DESCRIPTOR_OVR or r.COL_DESCRIPTOR or None

    t = tables.setdefault(tname, {"name": tname, "columns": []})
    t["columns"].append({
        "name": r.COLUMN_NAME,
        "type": typ,
        "description": description,
        "short_description": short,
        "ini": r.FORMAT_INI or None,
        "item": r.FORMAT_ITEM or None,
    })

schema = {"tables": list(tables.values())}

# ensure_ascii=True so re-uploading via a Windows browser (cp1252) can't
# corrupt non-ASCII bytes -- matches the existing csv_to_schema.py policy.
Path(OUT_PATH).write_text(json.dumps(schema, ensure_ascii=True, indent=2),
                          encoding="utf-8")

ncols = sum(len(t["columns"]) for t in schema["tables"])
print(f"✓ wrote {OUT_PATH}")
print(f"  {len(schema['tables'])} tables, {ncols} columns "
      f"({unmatched} column rows had no matching TABLE_ID)")
print("⚠ LOCAL ONLY -- this contains Epic Clarity content; do not commit it.")
print("\nNext: in describe_one_view.py set  SCHEMA_PATH = OUT_PATH")
