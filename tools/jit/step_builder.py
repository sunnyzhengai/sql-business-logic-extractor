"""Step Builder — generates CTE-chained SQL from user-selected definitions.

Takes a sequence of DefinitionHits and builds cumulative SQL where
each step wraps the prior steps as CTEs, filtering to the prior
population via PAT_ID IN (SELECT PAT_ID FROM prior_cte).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class QueryStep:
    """One step in the CTE-chained query."""
    step_number: int
    label: str                   # human-readable, e.g., "Diabetic patients"
    definition_name: str
    cte_name: str                # SQL CTE alias for this step
    tables: list[str]
    sql: str                     # the complete SQL for this step (includes all prior CTEs)
    count_sql: str               # SELECT COUNT(*) version
    description: str             # what this step does
    status: str = "pending"      # pending, approved, executed, modified, skipped
    result_count: Optional[int] = None
    prior_count: Optional[int] = None


def _sanitize_cte_name(name: str) -> str:
    """Convert a definition name to a valid SQL CTE alias."""
    return name.replace("-", "_").replace(" ", "_").lower()


def build_step_plan(
    definitions: list[dict],
    filter_overrides: dict[str, dict] | None = None,
    output_format: str = "count",
) -> list[QueryStep]:
    """Build a step-by-step query plan from selected definitions.

    Parameters
    ----------
    definitions     : list of definition glossary entries (dicts with
                      backbone, sql_template, etc.), in user-chosen order.
                      First definition is the base population.
    filter_overrides: optional dict mapping definition_name → dict of
                      parameter overrides (e.g., {"threshold": 5})
    output_format   : "count", "percentage", or "list"

    Returns
    -------
    List of QueryStep, one per definition. Each step's SQL includes
    all prior CTEs chained together.
    """
    filter_overrides = filter_overrides or {}
    steps = []
    prior_cte_names: list[str] = []
    prior_cte_sqls: list[tuple[str, str]] = []  # (cte_name, cte_body)

    for i, defn in enumerate(definitions):
        step_num = i + 1
        name = defn["definition_name"]
        cte_name = _sanitize_cte_name(name)
        label = defn.get("label", name)
        bb = defn.get("backbone", {})
        tables = bb.get("tables", [])

        # Get the SQL template
        sql_template = defn.get("sql_template", "")
        if not sql_template:
            # Fallback: describe what would be here
            sql_template = f"-- No SQL template for {name}"

        # Apply parameter overrides
        overrides = filter_overrides.get(name, {})
        for param in defn.get("parameters", []):
            param_name = param["name"]
            param_value = overrides.get(param_name, param.get("default"))
            if param_value is not None:
                sql_template = sql_template.replace(
                    "{" + param_name + "}", str(param_value))

        # Build the CTE body
        cte_body = sql_template

        # If not the first step, add population filter
        if prior_cte_names:
            last_cte = prior_cte_names[-1]
            # Check if the template already has a WHERE clause
            if "WHERE" in cte_body.upper():
                cte_body += f"\n  AND PAT_ID IN (SELECT PAT_ID FROM {last_cte})"
            else:
                cte_body += f"\nWHERE PAT_ID IN (SELECT PAT_ID FROM {last_cte})"

        # Build the full SQL with all prior CTEs
        full_cte_parts = []
        for prev_name, prev_body in prior_cte_sqls:
            full_cte_parts.append(f"{prev_name} AS (\n{_indent(prev_body)}\n)")

        # Add this step's CTE
        full_cte_parts.append(f"{cte_name} AS (\n{_indent(cte_body)}\n)")

        if full_cte_parts:
            with_clause = "WITH " + ",\n".join(full_cte_parts)
        else:
            with_clause = ""

        # The final SELECT depends on the step
        count_select = f"SELECT COUNT(DISTINCT PAT_ID) AS patient_count FROM {cte_name}"
        list_select = f"SELECT DISTINCT PAT_ID FROM {cte_name}"

        if with_clause:
            count_sql = f"{with_clause}\n{count_select}"
            list_sql = f"{with_clause}\n{list_select}"
        else:
            count_sql = f"SELECT COUNT(DISTINCT PAT_ID) AS patient_count FROM (\n{_indent(cte_body)}\n)"
            list_sql = f"SELECT DISTINCT PAT_ID FROM (\n{_indent(cte_body)}\n)"

        # Description
        if i == 0:
            desc = f"Base population: {label}"
        else:
            desc = f"Filter to: {label}"

        step = QueryStep(
            step_number=step_num,
            label=label,
            definition_name=name,
            cte_name=cte_name,
            tables=tables,
            sql=list_sql,
            count_sql=count_sql,
            description=desc,
        )
        steps.append(step)

        # Track for next iteration
        prior_cte_names.append(cte_name)
        prior_cte_sqls.append((cte_name, cte_body))

    # If percentage output, add a final ratio step
    if output_format == "percentage" and len(steps) >= 2:
        base_cte = steps[0].cte_name
        final_cte = steps[-1].cte_name

        ratio_parts = []
        for prev_name, prev_body in prior_cte_sqls:
            ratio_parts.append(f"{prev_name} AS (\n{_indent(prev_body)}\n)")

        with_clause = "WITH " + ",\n".join(ratio_parts)
        ratio_sql = (
            f"{with_clause}\n"
            f"SELECT\n"
            f"  (SELECT COUNT(DISTINCT PAT_ID) FROM {final_cte}) AS numerator,\n"
            f"  (SELECT COUNT(DISTINCT PAT_ID) FROM {base_cte}) AS denominator,\n"
            f"  ROUND(CAST((SELECT COUNT(DISTINCT PAT_ID) FROM {final_cte}) AS FLOAT) /\n"
            f"        CAST((SELECT COUNT(DISTINCT PAT_ID) FROM {base_cte}) AS FLOAT) * 100, 1)\n"
            f"    AS percentage"
        )

        steps.append(QueryStep(
            step_number=len(steps) + 1,
            label="Calculate percentage",
            definition_name="_percentage",
            cte_name="_ratio",
            tables=[],
            sql=ratio_sql,
            count_sql=ratio_sql,
            description=f"Percentage: {steps[-1].label} / {steps[0].label}",
        ))

    return steps


def _indent(text: str, spaces: int = 4) -> str:
    """Indent each line of text."""
    prefix = " " * spaces
    return "\n".join(prefix + line for line in text.split("\n"))
