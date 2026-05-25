# On-prem extract scripts

Run these **on a machine with line-of-sight to the on-prem SQL Server**
(typically your work laptop on the corporate network), NOT inside a Fabric notebook.

Each script writes output to a local folder. Upload that folder to Fabric
Lakehouse Files (drag-and-drop in the Fabric UI, or use `azcopy`) so your
Fabric notebooks can read it.

## Scripts

- `extract_tables.py` — dump full contents of a small list of tables to CSV (or parquet).
- `extract_ddl.py` — dump DDL for every view + stored procedure in N schemas to one `.sql` file each.

## One-time setup

```bash
pip install pyodbc pandas
# Driver: install Microsoft ODBC Driver 18 for SQL Server (or 17) for your OS
# https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server
```

## Auth

Both scripts default to Windows Integrated Auth (`Trusted_Connection=yes`).
If you need SQL auth instead, set `SQL_USER` and `SQL_PASSWORD` env vars
and the scripts will use them automatically.
