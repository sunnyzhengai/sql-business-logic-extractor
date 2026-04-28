# SQL View Migration Tooling — Requirements

**Workstream:** Healthcare org SSIS → Microsoft Fabric migration
**Status:** Requirements draft (2026-04-28)
**Isolation note:** This is a separate work track from the `sql_logic_extractor` commercialization (Collibra connector). It lives under this repo because it reuses the parser/resolver, but it is not part of the product.

---

## 1. Goal

The ETL team is moving SSIS packages onto Fabric. When they re-platform, some database/schema/table names that our SQL views depend on will change. We don't know the new names yet — that's exactly what the ETL team will hand us back as a mapping.

Build tooling that lets a BI analyst:

1. Pull all on-prem SQL Server views down as files
2. Produce a CSV manifest of every database/schema/table/column those views reference
3. Hand that CSV to the ETL team
4. Receive the CSV back with old → new names filled in
5. Apply the mapping to rewrite the view DDL automatically
6. Land the rewritten views on Fabric

Manual rewriting of N views (where N is large enough that the user is asking how to batch-export them) is the failure mode this tool prevents.

---

## 2. End-to-end workflow

```
[on-prem SQL Server]
        │
        │  (1) Export views
        ▼
   views/*.sql ─────────────────────────┐
        │                               │
        │  (2) Parse + extract refs     │
        ▼                               │
   manifest.csv ──→ ETL team            │
        ▲                               │
        │  (3) Returns with new names   │
        │      filled in                │
   mapping.csv                          │
        │                               │
        │  (4) Apply mapping            │
        ▼                               │
   views_rewritten/*.sql ◄──────────────┘
        │
        │  (5) Deploy to Fabric / load in notebook
        ▼
   [Fabric]
```

---

## 3. Functional requirements

### 3.1 Export views from SQL Server (Step 1)

- **R1.1** — Connect to on-prem SQL Server and bulk-export view DDL.
- **R1.2** — Output **one `.sql` file per view**, not a single concatenated file.
  - Rationale: parseable independently, diff-able in git, can re-export a single view without re-running the whole batch.
  - Filename convention: `<database>__<schema>__<view_name>.sql` (double underscore to disambiguate from dotted SQL Server identifiers).
- **R1.3** — Include the `CREATE VIEW` (or `CREATE OR ALTER VIEW`) header verbatim. Don't lose comments or formatting.
- **R1.4** — Filter scope: support exporting all views in a database, or a specific schema, or an explicit list. Don't dump the whole server by default.
- **R1.5** — Idempotent: re-running the export should overwrite cleanly without leaving stale files for views that have been dropped server-side.

**Implementation options to evaluate:**
- SSMS "Generate Scripts" wizard (manual, good for one-off but not repeatable)
- `sqlcmd` + `sys.sql_modules` query (scriptable, no dependencies)
- Python + `pyodbc` (scriptable, integrates with the rest of this tooling)

Recommend Python + `pyodbc` for consistency with the rest of the toolchain.

### 3.2 Parse views and extract references (Step 2)

- **R2.1** — Walk every `views/*.sql` file and produce a single CSV.
- **R2.2** — CSV columns (one row per **distinct (view, database, schema, table, column)** reference):
  - `view_file` — path or filename of the view this reference came from
  - `view_name` — `<database>.<schema>.<view_name>` of the view itself
  - `referenced_database` — source database (may be NULL if implicit)
  - `referenced_schema` — source schema (e.g. `dbo`, `etl_staging`)
  - `referenced_table` — table or view being read from
  - `referenced_column` — column name (or `*` if the view selects `*`)
  - `reference_type` — one of: `from_table`, `join_table`, `subquery_table`, `cte` (so the ETL team knows whether a name change is structural or just a column rename)
  - `confidence` — `high` (explicitly qualified) / `medium` (alias-resolved) / `low` (ambiguous, manual review needed)
- **R2.3** — Handle 3-part names (`db.schema.table`), 2-part names (`schema.table`), 1-part names (bare `table`), and CTEs/aliases without polluting the manifest with intermediate names.
- **R2.4** — Surface unresolved references explicitly so the user knows where the parser gave up.

**Reuse note:** The existing `sql_logic_extractor` package in the parent directory already does ~80% of this — it parses SQL, walks CTEs, resolves aliases, and emits column-level lineage with `base_tables` and `base_columns`. The work here is mostly **shaping its output into the CSV the ETL team needs**, not building a new parser.

### 3.3 ETL team handoff (Step 3)

- **R3.1** — `manifest.csv` is the deliverable to the ETL team. Format must be readable in Excel without surprises (UTF-8 with BOM, escape commas/newlines in identifiers).
- **R3.2** — Include a top-of-file comment row or a separate README explaining each column, so a stakeholder who has never seen the tool can fill in the mapping.

### 3.4 Receive mapping back (Step 4)

- **R4.1** — Accept a `mapping.csv` from the ETL team with at least these columns:
  - `old_database`, `old_schema`, `old_table`, `old_column` (matching one or more rows in `manifest.csv`)
  - `new_database`, `new_schema`, `new_table`, `new_column` (the Fabric destination)
  - `notes` (free text — e.g. "this column was renamed AND moved to a different schema")
- **R4.2** — Support partial mappings: not every row in `manifest.csv` needs a mapping (some references won't move, some are still TBD). Apply only the rows that are filled in.
- **R4.3** — Validate the incoming CSV: every `old_*` tuple should match at least one row in `manifest.csv`. Flag stale or hallucinated rows.

### 3.5 Apply the mapping to rewrite views (Step 5)

- **R5.1** — For each view, generate a rewritten DDL where every old reference is replaced by its new reference.
- **R5.2** — Output to a parallel folder: `views/foo.sql` → `views_rewritten/foo.sql`. Never overwrite originals.
- **R5.3** — Preserve formatting, whitespace, and comments. The diff between `views/` and `views_rewritten/` should be small and reviewable.
- **R5.4** — Generate a per-view change report (`views_rewritten/<view>.diff.md`) listing which references were renamed. Stakeholders need this for change-management sign-off.
- **R5.5** — When a view depends on a name with no mapping yet, leave the original reference in place AND flag it in the report.
- **R5.6** — Handle 3-part name expansion correctly: if `dbo.foo` becomes `fabric_db.dbo.foo`, the rewrite must produce 3-part names where the original had 2.

### 3.6 Fabric notebook consumption (Step 6 — open)

- **R6.1** — Confirm whether a Fabric notebook can read a local file path on the user's desktop. Likely answer: **no** — Fabric notebooks run against OneLake / Lakehouse storage, not the local filesystem. Files probably need to be uploaded to a Lakehouse Files area, an ADLS Gen2 mount, or pasted as inline strings.
- **R6.2** — Pick a deployment mechanism (Lakehouse Files, Git integration, manual paste-in-notebook) — **decide before building rewrite tooling**, because the output format may need to be e.g. a notebook cell of CREATE VIEW statements rather than `.sql` files.

---

## 4. Inputs / outputs (concrete artifacts)

| Artifact | Direction | Format | Owner |
|---|---|---|---|
| `views/<db>__<schema>__<view>.sql` | output of Step 1 | one DDL per file | this tooling |
| `manifest.csv` | output of Step 2, input to ETL team | CSV with the columns in §3.2 | this tooling → ETL team |
| `mapping.csv` | output of ETL team, input to Step 5 | CSV with the columns in §3.4 | ETL team → this tooling |
| `views_rewritten/<view>.sql` | output of Step 5 | rewritten DDL | this tooling |
| `views_rewritten/<view>.diff.md` | output of Step 5 | per-view change log | this tooling |

---

## 5. Open questions / dependencies

1. **Fabric notebook input path (R6).** Cannot be settled by us — needs to be answered by Fabric admin or by experimentation in a notebook. Critical because it shapes what Step 5 produces.
2. **ETL team CSV format.** §3.4 is what we want; the ETL team may push back. Send them a sample `manifest.csv` early and confirm the round-trip schema before they're deep into the mapping work.
3. **Scope of "all views."** Need a list of source databases. Some on-prem servers may be in scope, others not. BI manager to confirm with the ETL team which databases are migrating.
4. **Dialects.** All on-prem views are T-SQL today. After migration they run on Fabric (Synapse SQL / Spark SQL depending on engine). The rewrite step may need to do more than name swaps — function and type translations could be required. Out of scope for this requirements doc; flag for follow-up.
5. **Transitive dependencies.** A view might select from another view that itself references base tables. The manifest should record the *immediate* references the parser sees, but the ETL team may also need transitive impact. Decide whether the manifest should include a `depends_on_view` column.
6. **Ownership / read access.** Tooling needs SQL Server credentials to enumerate `sys.sql_modules`. Confirm that the BI manager has read access to all in-scope databases, or have the ETL team run the export.

---

## 6. Reuse from `sql_logic_extractor`

The parent project already provides:

- **L1 (extract)** — parse T-SQL views into AST (`sql_logic_extractor.extract`)
- **L2 (normalize)** — normalize expressions (`sql_logic_extractor.normalize`)
- **L3 (resolve)** — resolve aliases, CTEs, derived tables; emit `base_tables` and `base_columns` per output column (`sql_logic_extractor.resolve`)

What this workstream needs to add:

- A SQL Server view-export script (Step 1) — new, ~50 lines of `pyodbc`
- A "manifest builder" that flattens the L3 output into the §3.2 CSV shape — new, but thin (the data is all already in `resolved.json`)
- A "mapping applier" that reads `mapping.csv` and rewrites view DDL with old→new substitutions (Step 5) — new, the trickiest piece because of identifier quoting and 2-part vs. 3-part name handling

The recursive translator and English-rendering work from the parent project are **not needed here** — the ETL team wants identifiers, not natural language.

---

## 7. Out of scope

- LLM-powered translation or summarization of the views (parent project's job).
- Collibra connector / governance export (parent project's job).
- Generic SQL dialect translation (T-SQL → Spark SQL function rewrites). Different problem.
- Authorization / data-access changes (Fabric workspace permissions, RLS). Out of scope; coordinate with security/admin.

---

## 8. Source questions this document answers

The user asked, in one breath:

1. *"Is there a way to download sql views in sql server onto my desktop?"* → §3.1, R1.1
2. *"I want to batch download multiple views at once."* → §3.1, R1.4
3. *"Do they all save into one sql file? or can i save into individual?"* → §3.1, R1.2 — **individual files, by design**
4. *"In fabric notebook, can i point to these files on my desktop and use them as input?"* → §3.6, R6.1 — **probably no, needs research**
5. *"I want to systematically parse the views now to extract all schema, table and columns being referenced."* → §3.2, mostly free from the parent project
6. *"Analyze which ones need to change."* → handled by giving ETL team the manifest and getting back a mapping; tool does not predict, tool reports
7. *"The ETL team needs to know what database, schema, tables and columns our sql code are using."* → §3.3, R3.1 (manifest CSV is the handoff)
8. *"If i hand them this csv, they provide a mapping to changed new names, can i automate the updating of my views?"* → §3.5, the rewrite step closes the loop
