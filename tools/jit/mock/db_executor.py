"""Thin SQL executor over any DB-API 2.0 connection.

This is the Fabric swap point. Replace sqlite3.connect() with
pyodbc.connect(fabric_conn_str) and everything else stays the same.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Get a connection to a SQLite database."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def execute_sql(conn, sql: str, params: tuple = ()) -> dict[str, Any]:
    """Execute SQL and return results as a dict.

    Returns
    -------
    {
        "columns": ["col1", "col2", ...],
        "rows": [[val1, val2, ...], ...],
        "row_count": int
    }
    """
    cursor = conn.cursor()
    cursor.execute(sql, params)

    if cursor.description is None:
        # DDL or INSERT/UPDATE with no result set
        conn.commit()
        return {"columns": [], "rows": [], "row_count": cursor.rowcount}

    columns = [desc[0] for desc in cursor.description]
    rows = [list(row) for row in cursor.fetchall()]
    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
    }


def execute_count(conn, sql: str, params: tuple = ()) -> int:
    """Execute a COUNT(*) query and return the scalar result."""
    result = execute_sql(conn, sql, params)
    if result["rows"]:
        return result["rows"][0][0]
    return 0
