# SQL Parsing Field Guide

When the extractor reports a parse failure on a corpus, this guide tells you what's actually wrong, how to confirm it, and what the pipeline already does about it (or what to do if the pattern is new).

## Who this is for

A BI engineer or data steward running the extractor against a SQL corpus — typically Epic Clarity views exported from SSMS, but the patterns apply to any T-SQL or ANSI-SQL workload. The guide assumes you can read a Python traceback and run a notebook cell; it doesn't assume sqlglot internals or Fabric tooling expertise.

## How to use this guide

1. **Triage flow** below tells you which diagnostic to run first when you see a failure.
2. Each **trap card** lists the symptom, a one-line detection check, the root cause, what the pipeline already handles, and how to prevent the trap in the next export.
3. The cross-references to commit SHAs and file paths are the canonical implementation. If the doc and the code disagree, trust the code and file a PR to update the doc.
4. The "Add a new pattern" template at the end is what to copy when you hit something not in here.

---

## Triage flow

When the extractor reports failures, run these three diagnostics in order. Each one rules out a class of problem; whichever one fires first is your starting card below.

```
                ┌─────────────────────────────────────┐
                │ Run check_corpus_encoding(corpus)  │
                │ (tools/operate/check_corpus_       │
                │  encoding.py)                       │
                └────────────┬────────────────────────┘
                             │
            ┌────────────────┴──────────────────┐
            │                                   │
       UTF-16 detected                  clean UTF-8 / ASCII
            │                                   │
       → Trap #1                                ▼
                            ┌────────────────────────────────┐
                            │ Run inspect_one_view_after_    │
                            │ preprocess on ONE failing file │
                            │ (notebooks/inspect_one_view_   │
                            │  after_preprocess.py)          │
                            └────────────┬───────────────────┘
                                         │
                          ┌──────────────┴─────────────────┐
                          │                                │
                  sqlglot OK on cleaned          sqlglot FAIL on cleaned
                  but Cell 4/5 still errors      with a specific line/col
                          │                                │
                  → Module-cache problem          → Look at first 30 lines
                  → Trap #7                          of cleaned output and
                                                     match to traps #2–#5
                                                  → If no match, see
                                                     "Add a new pattern"
```

If you can't get past `check_corpus_encoding` because writes don't persist, you're hitting Trap #6 — read it before doing anything else.

---

## Trap #1 — UTF-16 LE BOM encoding (SSMS "Unicode" default)

**Symptom.** Parse errors that don't point at anything sensible. Sometimes the first 4 bytes of the file are `\xff\xfe` followed by what looks like garbled text. Python's default `open()` reads the file as UTF-8 and produces a string with `\x00` between every character.

**Detection.**
```python
print(open(view_path, 'rb').read(4))
```
- `b'\xff\xfe' + anything` → UTF-16 LE with BOM.
- `b'\xfe\xff' + anything` → UTF-16 BE with BOM (rarer).
- `b'\xef\xbb\xbf'` → UTF-8 with BOM (usually fine).
- ASCII chars (e.g. `b'CREA'`, `b'/*\n'`) → encoding is fine; the parse error is something else.

The faster bulk version is `tools/operate/check_corpus_encoding.py` — walks a directory, tallies encodings, prints a verdict.

**Root cause.** SSMS's "Generate Scripts" wizard offers two encoding options: ANSI (system code page, lossy on non-ASCII) and "Unicode" (UTF-16 LE with BOM). It defaults to Unicode. Python on Linux/Fabric defaults to UTF-8 when reading. The mismatch silently produces garbage text that no parser can handle.

**What the pipeline does.** `tools/operate/check_corpus_encoding.py` detects the encoding. `convert_to_utf8(corpus_dir, dry_run=False)` rewrites UTF-16 files to UTF-8 in place. In Fabric the conversion uses `notebookutils.fs.put` (see Trap #6) — outside Fabric, plain Python writes.

**Prevention.** Either:
- Install `mssql-scripter` (`pip install mssql-scripter`). It defaults to UTF-8 with no encoding flag needed. Recommended for any repeat-export workflow.
- Or, in newer SSMS versions, look for "UTF-8" as an explicit encoding option in Generate Scripts → Advanced. If only "Unicode" is offered, you're stuck with the conversion step.
- Or convert locally on the workstation with `tools/operate/convert_utf16_to_utf8.ps1` before uploading.

---

## Trap #2 — SSMS preamble (USE / GO / SET ANSI_NULLS / Object header)

**Symptom.** `ParseError: Invalid expression / Unexpected token. Line 4, Col: 3` (or similar). The line and column point at `GO`, `SET`, or some other SSMS-export header construct.

**Detection.** Open the file and look at the first 10 lines. If you see any of:
- `USE [DatabaseName]`
- `GO`
- `SET ANSI_NULLS ON` / `SET QUOTED_IDENTIFIER OFF` / any `SET ... ON|OFF`
- `/****** Object: View [schema].[name]    Script Date: ... ******/`

then this is an SSMS-export preamble and the parser is choking on it.

**Root cause.** SSMS's Generate Scripts wizard prefixes every exported view/proc with session-context boilerplate: `USE [db]` to switch database, `GO` to terminate the batch, `SET ANSI_NULLS / QUOTED_IDENTIFIER` to declare SQL behavior, and an `Object:` comment header for metadata. None of this is SQL — `GO` is sqlcmd's batch terminator, not a SQL keyword — and sqlglot can't make sense of it.

**What the pipeline does.** A parsing rule called `strip_ssms_preamble` (in `sql_logic_extractor/parsing_rules/rules.py`) finds the first `CREATE [OR ALTER|REPLACE] (VIEW|PROCEDURE|FUNCTION|TRIGGER)` in the file and discards everything before it. Implementation is one regex with a lookahead — no keyword whitelist to maintain. The Object metadata (schema, name, script date) is extracted before the strip by `_extract_object_header` in `resolve.py`, so the useful pieces survive.

**Prevention.** Same as Trap #1 — use `mssql-scripter` if you can. It produces clean `CREATE VIEW [name] AS ...` with no preamble. SSMS Generate Scripts always emits the preamble in older versions; the rule handles it without manual cleanup.

---

## Trap #3 — `CREATE VIEW` and `AS` on separate lines with long bracketed names

**Symptom.** Even after SSMS preamble is stripped, you see `ParseError: Required keyword: 'this' missing for <class 'sqlglot.expressions.core.Alias'>` — usually at the first `WITH` or `SELECT` after a `CREATE VIEW`. The error message is cryptic and doesn't point at the real problem.

**Detection.** Run `notebooks/inspect_one_view_after_preprocess.py` on a failing view. Look at the cleaned SQL output: if line 1 is `'AS'`, line 2 is `''`, line 3 is `'WITH'` or `'SELECT'`, you're hitting this trap.

**Root cause.** When SSMS exports a view with a long bracketed name like `[Reporting].[V_CCHCS_DXP_HP_Mychart_PBI]`, it formats the CREATE statement across two lines:

```sql
CREATE VIEW [Reporting].[V_CCHCS_DXP_HP_Mychart_PBI]
AS

WITH ...
```

The preprocess state machine matches the `CREATE VIEW [name]` line and sets `body_started = True`. Then on the next iteration it sees a bare `AS` line. Without the fix, that AS gets appended to the cleaned SQL body, sqlglot reads `AS\n\nWITH ...` at the top of the input, and bails with the cryptic Alias error.

**What the pipeline does.** A narrow check in `preprocess_ssms` (in `sql_logic_extractor/resolve.py`): if `body_started` was just set and no non-blank content has been accumulated yet, skip blank lines AND a bare `AS` token. Legitimate `AS` inside the body (CTE aliases, table aliases) is unaffected because by then real content has been appended.

**Prevention.** Nothing you can do at export time — this is just how SSMS formats long view names. The fix is automatic.

---

## Trap #4 — Explicit column list with divider between `)` and `AS`

**Symptom.** Same Alias error as Trap #3, but the cleaned SQL shows a multi-line column list at the top:

```
1:  '('
2:  ''
3:  '\t\t[PAT_ID]'
4:  '\t\t,[LOB_NAME]'
... (more columns) ...
13: ')'
14: ''
15: ''
16: '----------- (divider)'
17: 'AS'
18: ''
19: 'WITH'
```

**Detection.** Same as Trap #3 — run `inspect_one_view_after_preprocess`. If the cleaned SQL begins with `(` and a column list, this is the trap.

**Root cause.** T-SQL allows an optional explicit column list on CREATE VIEW:

```sql
CREATE VIEW [Reporting].[V_CCHCS_DXP_HP_Mychart_PBI]
(
    [PAT_ID]
    ,[LOB_NAME]
    ...
)

-- ------------------------------------------------------------
AS

WITH YearMonth AS (...)
```

The column list renames the SELECT's outputs. sqlglot doesn't parse this form. The `create_view_explicit_column_list` parsing rule strips the column list — but only when there's *whitespace* between the closing `)` and `AS`. Many developers add a divider line (a row of dashes or a `-- section marker`) between them to visually separate the column declaration from the body. The divider breaks the rule's match, and the entire column list + divider + AS leaks into the cleaned SQL.

**What the pipeline does.** The rule's pattern uses `[\s\S]*?` (non-greedy any-character) between `)` and `AS`, so blanks, line comments, block comments, and dash dividers are all consumed. The cleaned SQL ends up as just the body.

**Prevention.** If you're authoring views, you don't need to change anything — dividers and column lists are fine. The rule handles them.

---

## Trap #5 — T-SQL `column = expression` aliasing

**Symptom.** Parse failure deep in a SELECT, on something like:

```sql
SELECT YearMonth = CONVERT(CHAR(6), DD.CALENDAR_DT, 112)
FROM DateDimension DD
```

sqlglot reports the error at the column-name position.

**Detection.** Look at the failing line — if it has the shape `<identifier> = <expression>` inside a SELECT, this is the trap.

**Root cause.** T-SQL allows two equivalent syntaxes for aliasing a column:
- `expression AS alias` (ANSI standard, every dialect supports it)
- `alias = expression` (T-SQL-specific, common in Microsoft shops)

sqlglot's default dialect mode doesn't recognize the second form — it interprets `=` as an equality predicate and fails. T-SQL dialect mode does.

**What the pipeline does.** Every sqlglot call in the production pipeline passes `dialect='tsql'`. The CLI flag for `tools/p20_index/term_extraction.py` is `--dialect tsql` and that's the default. The diagnostic script `notebooks/diagnose_one_parse_error.py` tries both no-dialect and `dialect='tsql'` so you can see the difference.

**Prevention.** If you're starting a new corpus, check that your runner is passing `dialect='tsql'`. It is by default for the SSIS / Clarity / MyChart pilots. If your corpus is a non-Microsoft dialect (PostgreSQL, Snowflake, BigQuery), specify that dialect instead.

---

## Trap #6 — Fabric lakehouse mount is read-only for plain Python writes

**Symptom.** You convert UTF-16 files to UTF-8 with `convert_to_utf8(dir, dry_run=False)`. The function reports `Converted N files to UTF-8`. You re-run `check_corpus_encoding` and the files are *still* UTF-16. No error message anywhere.

**Detection.**
```python
from sql_logic_extractor.parsing_rules import _FABRIC_FS  # ignore the underscore; this is the runtime flag
# If running in Fabric this is non-None.
```

If you're in a Fabric notebook and writes through plain `open(file, 'w')` don't persist, this is the trap.

**Root cause.** The Fabric lakehouse mount path (`/lakehouse/default/Files/...`) accepts POSIX-style reads but silently no-ops plain Python writes. Writes to OneLake must go through Fabric's filesystem API: `notebookutils.fs.put` or (older naming) `mssparkutils.fs.put`. Plain Python `open(w)` returns without raising, but nothing reaches OneLake — and the mount cache continues serving the pre-write content.

**What the pipeline does.** `tools/operate/check_corpus_encoding.py` auto-detects Fabric at module-load time and routes writes through `notebookutils.fs.put` when available. The fallback is plain Python for local / CI environments.

That said: in some Fabric runtimes even `fs.put` doesn't reliably persist to existing files. If `convert_to_utf8` reports `Converted N files (via Fabric fs.put)` but the encoding tally is still UTF-16 afterward, the in-Fabric write isn't working — convert locally instead, using `tools/operate/convert_utf16_to_utf8.ps1` on the source workstation before uploading.

**Prevention.** Export UTF-8 directly (mssql-scripter, or modern SSMS UTF-8 option). The conversion step is a workaround, not a long-term solution.

---

## Trap #7 — Fabric notebook module cache stickiness

**Symptom.** You sync a fix to one of the parsing files, re-run a cell, and see the *same error as before*. An `inspect.getsource` check confirms the new code is loaded, but the failure still happens.

**Detection.**
```python
import sql_logic_extractor.resolve as r, inspect
print('SENTINEL_STRING_FROM_NEW_CODE' in inspect.getsource(r.preprocess_ssms))
```
If this prints `True` but you still see the old error in some other cell, that other cell is using a cached reference.

**Root cause.** Fabric notebooks (and Jupyter generally) cache imported modules in `sys.modules`. When you sync a file change to OneLake, an already-imported function in some other cell still holds the *old* function reference. The cell that imported the new code sees the new version; the cell that imported the old version is stuck.

**What the pipeline does.** The diagnostic scripts (`inspect_one_view_after_preprocess.py`, `diagnose_one_parse_error.py`) drop the module cache before re-importing, so they always see the current code:

```python
import sys
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]
```

Pipeline cells (Cell 3, Cell 4, Cell 5) don't do this automatically — you have to add the snippet at the top of any cell that imports our modules after a sync.

**Prevention.** The bullet-proof option is to restart the kernel after every sync. Slower but eliminates all cache surprises.

---

## Add a new pattern

When you hit a parse failure not covered above:

1. Run `notebooks/inspect_one_view_after_preprocess.py` on the failing view. The 30-line cleaned-SQL printout (with `repr()` so whitespace is visible) is the single most useful artifact for diagnosing a new pattern.
2. Identify the minimal SQL construct that fails — copy the smallest snippet you can produce that reproduces the error.
3. Decide where the fix belongs:
   - **Parsing rule** (`sql_logic_extractor/parsing_rules/rules.py`) — if the fix is a regex on the raw SQL. Add a Rule entry + a matching fixture under `fixtures/<rule_id>/{input.sql, expected_clean.sql}`. The fixture-driven test auto-discovers new rules.
   - **`preprocess_ssms` state machine** (`sql_logic_extractor/resolve.py`) — if the fix needs line-by-line logic that a regex can't express.
   - **sqlglot dialect option** — if a dialect flag fixes it without code changes.
4. Add a card to this file using the template below.

### Pattern card template

```markdown
## Trap #N — <one-line name>

**Symptom.** The exact error message or behavior. Quote the Python exception verbatim if possible.

**Detection.** One Python expression or shell command that confirms the diagnosis.

**Root cause.** Why this happens. Aim for "a future reader who doesn't know our codebase understands why the fix exists."

**What the pipeline does.** The specific commit SHA + file:line(s) that handle this. Cross-reference, don't duplicate the code.

**Prevention.** What the user can do at export time / setup time to avoid the trap entirely.
```

Each card should be self-contained — a reader landing on one card via Cmd-F shouldn't need to read the rest of the file to apply the fix.

---

## When to update vs. when to extend

- **Update an existing card** when the underlying fix evolves (a parsing rule's regex changes, a new edge case is added). Keep the commit SHA reference current.
- **Add a new card** when you hit a NEW pattern not covered by an existing one. Don't shoehorn new patterns into existing cards.
- **Retire a card** (move to an "Archived patterns" section at the bottom) when the underlying trap is no longer possible — e.g., if a vendor fixes the SSMS export format. Don't delete; the historical context is useful when the same pattern recurs from a different source.

---

## Cross-references

- Parsing rules registry: `sql_logic_extractor/parsing_rules/rules.py`
- Preprocess state machine: `sql_logic_extractor/resolve.py` (`preprocess_ssms`)
- Diagnostic notebooks: `notebooks/diagnose_one_parse_error.py`, `notebooks/inspect_one_view_after_preprocess.py`, `notebooks/verify_and_diagnose_one_error.py`
- Encoding triage: `tools/operate/check_corpus_encoding.py`
- Local UTF-16 → UTF-8 conversion: `tools/operate/convert_utf16_to_utf8.ps1`
- Wiki concept on Clarity table families (related domain knowledge): `wiki/concepts/clarity-table-families.md`
