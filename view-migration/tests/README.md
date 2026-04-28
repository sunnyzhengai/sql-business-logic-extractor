# View-migration manifest builder — test suite

Regression fixtures for `scripts/build_manifest_standalone.py`. Each `.sql`
file in `views/` is one focused test case. The runner feeds every fixture
through `extract_view_refs()` and prints what came out — passing means no
parse errors, the column/table list reads as expected.

## Running

```bash
python3 view-migration/tests/run_tests.py
```

## Coverage

| Fixture | Exercises |
|---|---|
| `01_basic_three_part.sql` | 3-part `db.schema.table` and 2-part `schema.table` qualification |
| `02_cte_with_exists.sql` | CTE filtering + CTE column flattening + EXISTS subquery |
| `03_convert_and_cast.sql` | T-SQL `CONVERT(...)` and `CAST(...)` should not swallow inner column refs |
| `04_alias_equals_syntax.sql` | older T-SQL `[alias] = expr` (vs. `expr AS [alias]`), bracketed identifiers |
| `05_multi_join_select_star.sql` | INNER + LEFT JOIN + `SELECT *` |
| `06_chained_ctes.sql` | CTE B reads from CTE A reads from base table |
| `07_unqualified_columns.sql` | bare column references with no alias prefix |
| `08_case_when_subquery.sql` | `CASE WHEN`, scalar subquery in SELECT, IN-subquery in WHERE |
| `09_ssms_utf16_boilerplate.sql` | SSMS scripted view with `USE`/`GO`/`SET` boilerplate; runner generates a UTF-16 LE BOM'd binary copy at runtime to also exercise the encoding handler without committing binary fixtures into git |

## Adding a new test

Drop a `.sql` file into `views/` named `NN_short_description.sql`. The
runner picks it up automatically. Top-of-file comment should explain
what feature this test is meant to catch.

## Debugging a real production view

Sanitise sensitive identifiers, save as `99_my_problem_view.sql` in
`views/`, and re-run. The runner will show exactly which columns and
tables come out for that view, which is usually enough to spot what's
missing or misqualified. **Add the case as a permanent test** so future
script changes can't silently break it again.

## Known imperfection

`06_chained_ctes.sql` currently produces a few intermediate `RawReferrals.*`
rows alongside the (correct) `REFERRAL.*` flattened rows. Over-emit, not
under-emit — better than losing info, but worth tightening if it shows up
in real-world output as confusing duplicates.
