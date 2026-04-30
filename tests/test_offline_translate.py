"""Golden-file tests for the recursive offline translator.

Each case parses a SQL expression, walks it with the pattern registry,
and asserts the output matches a checked-in golden file in
``tests/golden/offline_translate/``.

Running:

    # Normal: assert equality
    pytest tests/test_offline_translate.py

    # Regenerate goldens (e.g., after intentional template changes)
    UPDATE_GOLDEN=1 pytest tests/test_offline_translate.py

The golden format is intentionally plain text: first line is the
English description; subsequent ``#``-prefixed lines carry metadata
(category, base columns/tables, unknown-node/column governance signals).
"""

import os
from pathlib import Path

import pytest
import yaml
from sqlglot import exp, parse_one

from sql_logic_extractor.patterns import Context, translate


FIXTURES_DIR = Path(__file__).parent / "golden" / "offline_translate"
SCHEMA_PATH = Path(__file__).parent.parent / "data" / "schemas" / "clarity_schema.yaml"
UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN") == "1"

CASES = [
    ("passthrough_in_schema", "PATIENT.PAT_ID"),
    ("passthrough_abbrev_fallback", "PATIENT.PAT_MRN_ID"),
    ("datediff_years", "DATEDIFF(YEAR, BIRTH_DATE, GETDATE())"),
    ("datediff_days_cols", "DATEDIFF(DAY, HOSP_ADMSN_TIME, HOSP_DISCH_TIME)"),
    ("simple_case_enum", "CASE e.ADT_PAT_CLASS_C WHEN 1 THEN 'Inpatient' WHEN 2 THEN 'Outpatient' ELSE 'Other' END"),
    ("searched_case_age", "CASE WHEN age < 18 THEN 'Ped' ELSE 'Adult' END"),
    ("count_star", "COUNT(*)"),
    ("count_of_conditional", "COUNT(CASE WHEN DISCH_DISPOSITION_C = 1 THEN 1 END)"),
    ("sum_of_conditional", "SUM(CASE WHEN days_to_readmit <= 30 THEN 1 ELSE 0 END)"),
    ("avg_column", "AVG(los_days)"),
    ("lag_over_partition_order", "LAG(HOSP_DISCH_TIME) OVER (PARTITION BY PAT_ID ORDER BY HOSP_ADMSN_TIME)"),
    ("percentage_composite", "CAST(SUM(x) AS FLOAT) / COUNT(*) * 100"),
    ("arithmetic_mul", "los * cost"),
]


@pytest.fixture(scope="module")
def ctx() -> Context:
    with open(SCHEMA_PATH) as f:
        schema = yaml.safe_load(f)
    return Context(schema=schema)


def _unwrap_select(node: exp.Expression) -> exp.Expression:
    if isinstance(node, exp.Select):
        node = node.selects[0]
    if isinstance(node, exp.Alias):
        node = node.this
    return node


def _render(name: str, sql: str, t) -> str:
    return (
        f"{t.english}\n"
        f"# sql: {sql}\n"
        f"# category: {t.category}\n"
        f"# subcategory: {t.subcategory}\n"
        f"# base_columns: {sorted(set(t.base_columns))}\n"
        f"# base_tables: {sorted(set(t.base_tables))}\n"
        f"# unknown_nodes: {sorted(set(t.unknown_nodes))}\n"
        f"# unknown_columns: {sorted(set(t.unknown_columns))}\n"
    )


@pytest.mark.parametrize("name,sql", CASES)
def test_golden(name: str, sql: str, ctx: Context) -> None:
    node = _unwrap_select(parse_one(sql, dialect="tsql"))
    result = translate(node, ctx)
    actual = _render(name, sql, result)

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    golden_file = FIXTURES_DIR / f"{name}.txt"

    if UPDATE_GOLDEN or not golden_file.exists():
        golden_file.write_text(actual)
        if not UPDATE_GOLDEN:
            pytest.fail(
                f"Golden file was missing — created {golden_file.name}. "
                f"Inspect it, commit it, and re-run."
            )
        return

    expected = golden_file.read_text()
    assert actual == expected, (
        f"\nCase '{name}' output diverged from golden.\n"
        f"SQL: {sql}\n"
        f"--- EXPECTED ---\n{expected}"
        f"--- ACTUAL ---\n{actual}"
        f"To accept the new output, rerun with UPDATE_GOLDEN=1."
    )
