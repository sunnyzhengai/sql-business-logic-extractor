"""
Extract DDL (CREATE statements) for every view + stored procedure
in a list of schemas on one database, and write one .sql file per object.

Edit the CONFIG block below, then run:
    python extract_ddl.py

Output layout:
    ddl_dump/
        <schema>/
            views/
                <view_name>.sql
            procs/
                <proc_name>.sql
"""

import os
import pyodbc

# ============================ CONFIG ============================
SERVER   = "YOUR_SERVER_NAME"        # e.g. "sqlprod01.corp.local" or "SQLPROD01\\INSTANCE"
DATABASE = "YOUR_DATABASE"
SCHEMAS  = ["schema_one", "schema_two"]   # the 2 schemas to extract from

OUTPUT_DIR = "ddl_dump"              # local folder; created if missing
# ===============================================================


# Pulls all view + stored proc DDL in one query.
# - sys.sql_modules.definition is the full CREATE text.
# - We restrict to views (V) and stored procs (P) only.
DDL_QUERY = """
SELECT
    s.name        AS schema_name,
    o.name        AS object_name,
    o.type_desc   AS object_type,
    m.definition  AS ddl
FROM sys.sql_modules m
JOIN sys.objects  o ON m.object_id = o.object_id
JOIN sys.schemas  s ON o.schema_id = s.schema_id
WHERE o.type IN ('V', 'P')
  AND s.name IN ({schema_list})
ORDER BY s.name, o.type_desc, o.name;
"""


def build_conn_str() -> str:
    sql_user = os.environ.get("SQL_USER")
    sql_pwd  = os.environ.get("SQL_PASSWORD")
    base = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    if sql_user and sql_pwd:
        return base + f"UID={sql_user};PWD={sql_pwd};"
    return base + "Trusted_Connection=yes;"


def subdir_for(object_type: str) -> str:
    # SQL Server type_desc values for our filtered set:
    #   VIEW                  -> views
    #   SQL_STORED_PROCEDURE  -> procs
    return "views" if object_type == "VIEW" else "procs"


def safe_filename(name: str) -> str:
    # Strip characters that would break a filesystem path on Windows/Linux.
    bad = '<>:"/\\|?*'
    return "".join("_" if c in bad else c for c in name)


def main() -> int:
    if not SCHEMAS:
        print("CONFIG error: SCHEMAS is empty.")
        return 1

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    schema_list_sql = ", ".join(f"'{s}'" for s in SCHEMAS)
    query = DDL_QUERY.format(schema_list=schema_list_sql)

    print(f"Connecting to {SERVER} / {DATABASE} ...")
    counts = {"VIEW": 0, "SQL_STORED_PROCEDURE": 0, "SKIPPED_NO_DDL": 0}

    with pyodbc.connect(build_conn_str()) as conn:
        rows = conn.execute(query).fetchall()
        print(f"Found {len(rows)} object(s) across schemas {SCHEMAS}")

        for schema_name, object_name, object_type, ddl in rows:
            if not ddl:
                counts["SKIPPED_NO_DDL"] += 1
                print(f"  SKIP (no DDL): {schema_name}.{object_name}")
                continue

            sub = subdir_for(object_type)
            out_dir = os.path.join(OUTPUT_DIR, schema_name, sub)
            os.makedirs(out_dir, exist_ok=True)

            fname = safe_filename(object_name) + ".sql"
            out_path = os.path.join(out_dir, fname)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(ddl)

            counts[object_type] = counts.get(object_type, 0) + 1

    print("\nDone.")
    print(f"  Views written:  {counts['VIEW']}")
    print(f"  Procs written:  {counts['SQL_STORED_PROCEDURE']}")
    if counts["SKIPPED_NO_DDL"]:
        print(f"  Skipped (no DDL, e.g. encrypted): {counts['SKIPPED_NO_DDL']}")
    print(f"  Output folder:  {OUTPUT_DIR}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
