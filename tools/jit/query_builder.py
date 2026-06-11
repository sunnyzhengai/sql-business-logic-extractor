"""Step-by-step SQL generator for the query builder.

Takes a join path (from query_graph.find_join_paths) and produces one
SQL statement per step, each building on the previous by adding one JOIN.

The user sees row counts change at each step — the core of the HITL
validation loop.
"""

from __future__ import annotations


def generate_step_sql(
    path: list[dict],
    filters: dict[str, str] | None = None,
) -> list[dict]:
    """Generate step-by-step COUNT(*) SQL from a join path.

    Parameters
    ----------
    path    : list of step dicts from find_join_paths(), each with
              table, join_from, fk_column, pk_column, direction
    filters : optional dict mapping table name (upper) -> WHERE clause
              fragment (without the WHERE keyword). Applied at the step
              where that table is joined.

    Returns
    -------
    List of step dicts, each with:
      - sql: the SQL statement for this step
      - description: human-readable explanation
      - table: the table added at this step
      - step_number: 1-based step index
    """
    if not path:
        return []

    filters = filters or {}
    steps = []
    tables_so_far: list[dict] = []

    for i, step in enumerate(path):
        tables_so_far.append(step)
        table = step["table"]

        if i == 0:
            # Base table — no JOIN
            sql = f"SELECT COUNT(*) FROM {table}"
            # Apply filter if present for the base table
            table_filter = filters.get(table.upper())
            if table_filter:
                sql += f"\nWHERE {table_filter}"
            description = f"Start with {table}"
        else:
            # Build cumulative SQL with all joins so far
            base = tables_so_far[0]["table"]
            sql = f"SELECT COUNT(*) FROM {base}"

            # Collect all filters that apply to tables joined so far
            where_clauses = []
            base_filter = filters.get(base.upper())
            if base_filter:
                where_clauses.append(base_filter)

            for j in range(1, i + 1):
                s = tables_so_far[j]
                join_table = s["table"]
                prev_table = s["join_from"]
                fk_col = s.get("fk_column", "")
                pk_col = s.get("pk_column", "")
                direction = s.get("direction", "")

                # Build the ON clause based on FK direction
                if direction == "child_to_parent":
                    # prev (child) has the FK pointing to current (parent)
                    on_clause = f"{prev_table}.{fk_col} = {join_table}.{pk_col}"
                elif direction == "parent_to_child":
                    # current (child) has the FK pointing to prev (parent)
                    on_clause = f"{join_table}.{fk_col} = {prev_table}.{pk_col}"
                else:
                    on_clause = f"{prev_table}.{fk_col} = {join_table}.{pk_col}"

                sql += f"\nJOIN {join_table} ON {on_clause}"

                # Collect filter for this table
                table_filter = filters.get(join_table.upper())
                if table_filter:
                    where_clauses.append(table_filter)

            if where_clauses:
                sql += "\nWHERE " + " AND ".join(where_clauses)

            description = f"Join {step['join_from']} to {table}"

        steps.append({
            "sql": sql,
            "description": description,
            "table": table,
            "step_number": i + 1,
        })

    return steps
