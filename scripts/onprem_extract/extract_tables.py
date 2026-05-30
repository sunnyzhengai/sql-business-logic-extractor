"""
Extract full contents of specific tables from an on-prem SQL Server to CSV.

Edit the CONFIG block below, then run:
    python extract_tables.py

Output: one CSV per table in OUTPUT_DIR.
"""

import os
import pyodbc
import pandas as pd

# ============================ CONFIG ============================
SERVER   = "YOUR_SERVER_NAME"        # e.g. "sqlprod01.corp.local" or "SQLPROD01\\INSTANCE"
DATABASE = "YOUR_DATABASE"

# List the tables you want to extract. Format: (schema, table).
TABLES = [
    ("dbo", "TableOne"),
    ("dbo", "TableTwo"),
]

OUTPUT_DIR = "table_dump"            # local folder; created if missing
# ===============================================================


def pick_driver() -> str:
    """Return the best SQL Server ODBC driver installed on this machine.

    Preference order (newest -> oldest). The newer drivers are preferred
    because they support modern TLS and the Encrypt/TrustServerCertificate
    options the connection string below relies on.
    """
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",  # legacy built-in, last resort
    ]
    installed = set(pyodbc.drivers())
    for name in preferred:
        if name in installed:
            return name
    raise RuntimeError(
        "No SQL Server ODBC driver found. Install 'Microsoft ODBC Driver 18 "
        "for SQL Server' from Microsoft. Installed drivers: "
        f"{sorted(installed)}"
    )


def build_conn_str() -> str:
    """Build a pyodbc connection string for the configured SERVER/DATABASE.

    Uses SQL auth if SQL_USER + SQL_PASSWORD env vars are set, otherwise
    falls back to Windows Integrated Auth.
    """
    sql_user = os.environ.get("SQL_USER")
    sql_pwd  = os.environ.get("SQL_PASSWORD")
    driver = pick_driver()
    base = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
    )
    # Encrypt/TrustServerCertificate are only meaningful on the modern drivers.
    # Adding them to the legacy "SQL Server" driver can cause connect failures.
    if driver.startswith("ODBC Driver"):
        base += "Encrypt=yes;TrustServerCertificate=yes;"
    if sql_user and sql_pwd:
        return base + f"UID={sql_user};PWD={sql_pwd};"
    return base + "Trusted_Connection=yes;"


def extract_table(conn, schema: str, table: str, out_dir: str) -> None:
    qualified = f"[{schema}].[{table}]"
    print(f"  reading {qualified} ...", end=" ", flush=True)
    df = pd.read_sql(f"SELECT * FROM {qualified}", conn)
    out_path = os.path.join(out_dir, f"{schema}.{table}.csv")
    df.to_csv(out_path, index=False)
    print(f"{len(df):,} rows -> {out_path}")


def main() -> int:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Connecting to {SERVER} / {DATABASE} ...")
    with pyodbc.connect(build_conn_str()) as conn:
        print(f"Extracting {len(TABLES)} table(s) into {OUTPUT_DIR}/")
        for schema, table in TABLES:
            try:
                extract_table(conn, schema, table, OUTPUT_DIR)
            except Exception as e:
                print(f"  FAILED on {schema}.{table}: {e}")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
