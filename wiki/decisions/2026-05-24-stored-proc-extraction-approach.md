---
date: 2026-05-24
status: proposed
---

## Context

The MyChart pilot extraction now works cleanly for 11 views. Yang's
team also exported ~22 stored procedures into `data/mychart_sps/` that
are sitting untouched. Stored procs are the next obvious extension --
they hold a meaningful chunk of the team's governable SQL logic
(refresh routines, ETL pipelines, reporting wrappers), and customers
in healthcare BI shops typically have proc count comparable to or
exceeding their view count.

The current extractor + matrix pipeline is built around view shape:
one CREATE VIEW per file, one SELECT body, output columns + filters +
joins + lineage to base tables. Procs are structurally different
enough that they don't fit this shape verbatim. We need to decide
how to model them in the pipeline without breaking the existing
view-based artifacts.

## Decision

Treat procs as a SEPARATE first-class corpus shape, not a variant of
ViewV1. New ProcV1 schema, new extractor path, new analysis modules
where the shape differs, REUSE existing modules where the shape is
the same. Three sub-categories of proc handled progressively:

1. **Read-only procs (parameterized views).** A proc that's a single
   SELECT plus parameters maps closely to a view with a `parameters`
   field. Reuse the existing extractor with a parameter-binding
   pass.

2. **Reporting procs (temp-table-shaped).** A proc that builds a
   result set in temp tables / table variables and returns it.
   Extract the SELECT logic as if it were a view; treat temp tables
   as scope-internal (similar to CTEs). Flag the proc as
   "reporting" in the proc metadata.

3. **ETL procs (write-target-shaped).** A proc that genuinely
   modifies persistent tables -- INSERT/UPDATE/DELETE/MERGE into
   `dbo.fact_X`. These have a fundamentally different shape: their
   primary artifact is a **write-target → source-tables** mapping
   per write statement, not a SELECT's output columns.

The proc analysis surfaces two new matrix variants alongside the
existing three:

- **Write-target matrix** (procs only): rows = target tables, columns
  = procs in this community. Shows which procs write to what.
  Equivalent of the view table-matrix.
- **Read-source matrix** (procs only): rows = source tables, columns
  = procs. Reuses the view table-matrix's logic and renderer; just
  applied to procs.

Procs are clustered into communities by their write-target overlap
(primary axis for ETL procs) AND read-source overlap (primary axis
for read-only / reporting procs). Two different projection graphs;
which one drives community detection per proc depends on its
sub-category.

## Alternatives considered

**A. Treat procs as views.** Run them through the existing view
extractor and accept the lossy mapping (procs with multiple writes
get only their last SELECT extracted). Rejected: loses the data-flow
information that's the whole point of governance on procs. A
reporting proc that returns a result set might be salvageable this
way; an ETL proc with three INSERTs into different tables is
fundamentally not a view.

**B. Treat procs as DAG nodes only.** Just track proc-to-proc and
proc-to-table edges, don't extract internal logic. Rejected: leaves
the governance work undone. Stewards want to know what filters a
proc applies before writing to fact_X; "proc X writes to fact_X" is
the start of that conversation, not the answer.

**C. Hand-curate a separate proc taxonomy outside the extractor.**
Skip code work, ship a markdown template for stewards to fill in.
Rejected: doesn't scale beyond 5 procs and defeats the product
thesis.

The chosen approach (separate ProcV1 schema + sub-category triage +
matrix variants) keeps the data model honest while reusing the
existing matrix-renderer + community-detection infrastructure where
it applies.

## Consequences

### What this locks in

- `sql_logic_extractor/corpus_schema.py` grows a `ProcV1` dataclass
  alongside `ViewV1`. Parallel structure: `name`, `parameters`,
  `scopes`, but also `writes` (list of write-target statements) and
  `sub_category` (`read_only` / `reporting` / `etl` / `mixed`).
- `tools/p10_extract/` grows a `proc_batch.py` that walks `.sql`
  files, classifies each as proc / view (CREATE PROCEDURE vs CREATE
  VIEW), and routes to the appropriate extractor. Produces a
  combined corpus with mixed ViewV1 + ProcV1 entries.
- `tools/p20_index/graph_builder.py` learns to emit two new node
  types: `write_target` (the table a proc writes to) and `proc`
  (parallel to `view`). The graph projection for community detection
  picks between read-source and write-target depending on sub-
  category.
- `tools/p50_present/community_matrix.py` learns to emit two new
  matrices (write-target, read-source) for proc-bearing communities.
  The existing table / filter / base-column matrices stay for views.
- `tools/operate/validate_graph_pivot.py` extends to recognize mixed
  corpora and produce both view-style and proc-style artifacts per
  community.

### What stays the same

- View extraction, view analysis, view matrices -- unchanged.
- The community modeling spec (`p40_synthesize/community_modeling_spec.py`)
  -- extended with a `## Stored procs in this community` section but
  the existing tables + joins + starter SQL structure is preserved.
- The parsing-rules registry (`parsing_rules/rules.py`) -- new rules
  added for proc-specific T-SQL constructs (`SET @var =`,
  `BEGIN ... END`, parameter declaration, `EXEC` calls) but existing
  rules unchanged. SSMS preamble rule already handles the proc
  preamble; reuse.

### Risks / known unknowns

- **sqlglot's handling of procedural T-SQL.** sqlglot parses DML
  statements within procs (INSERT/UPDATE/DELETE/MERGE) but its
  treatment of control flow (IF/WHILE/CURSOR/TRY-CATCH) and
  dynamic SQL (`EXEC sp_executesql`) is incomplete in places. The
  field guide (`docs/parsing_field_guide.md`) will grow new cards as
  proc-specific patterns surface. First action of any proc work is
  a survey: parse all 22 MyChart procs with `dialect='tsql'` and
  bucket results.
- **Dynamic SQL.** `EXEC(@sql)` / `sp_executesql` where `@sql` is
  built at runtime is opaque to static analysis. Flag the proc as
  "contains dynamic SQL" in its ProcV1 metadata; surface as a steward
  review item rather than trying to extract.
- **Temp tables and table variables.** `#TempA` and `@TableVar` are
  scope-internal to the proc. Treat them like CTEs: scope-qualified
  lineage, NOT base tables. The existing scope-tree machinery handles
  this with minor extension.
- **Procs calling procs.** Cross-proc dependency graph
  (`EXEC OtherProc @param`) is its own structural axis. Phase E
  (deferred): emit a per-corpus call graph for governance review.
  Not in v1 scope.

### Phasing

To avoid the same multi-day detour that the parsing chapter became,
phase the work and validate at each step:

- **Phase A -- Survey (1 day).** Parse all 22 MyChart procs with the
  existing field-guide-equipped parser (no schema changes). Output:
  a CSV with proc-name → parse status + first failing token. Update
  the field guide with any new patterns. This bounds the unknowns
  before we touch schema.

- **Phase B -- Categorize (0.5 day).** Walk the parseable procs and
  classify each as read-only / reporting / ETL based on the AST
  (presence and count of DML statements vs SELECTs). Output:
  inventory markdown with categories + write targets per proc.

- **Phase C -- Read-only path (1-2 days).** Add `parameters` to
  ViewV1 OR a thin ProcV1 wrapper around it; extract read-only procs
  through the existing view path with parameter binding. End-to-end
  validates the pipeline can carry proc artifacts without touching
  the harder cases.

- **Phase D -- ETL path (2-3 days).** Add ProcV1 with `writes`.
  Build the write-target graph projection + new community
  detection axis + new matrix renderer. End-to-end validates against
  the ETL subset of MyChart procs.

- **Phase E -- Reporting path + cross-proc graph (deferred).**
  Reporting procs sit between read-only and ETL; cross-proc
  dependencies are a governance artifact more than a modeling one.
  Both deferred until the read-only and ETL paths are stable.

### Decision check-ins

After Phase A's survey output is in hand, revisit this decision. If
sqlglot's proc-parsing is significantly worse than expected (>50% of
procs fail), the phasing or even the whole approach may need to
shift toward "do nothing fancy, treat procs as opaque text blobs
indexed by name." Survey first.

## See also

- `docs/parsing_field_guide.md` -- where new proc-specific parsing
  cards go as patterns surface during the survey.
- `wiki/concepts/clarity-table-families.md` -- the grain taxonomy
  proc writes also flow into (e.g. a proc writing to F_PAT_ENC_HX
  is at encounter grain; a proc populating a per-measurement
  flowsheet is finer).
- `tools/p50_present/community_matrix.py` -- the renderer we extend
  for the write-target and read-source proc matrices.
- ViewV1 schema in `sql_logic_extractor/corpus_schema.py` -- the
  parallel structure ProcV1 grows alongside.
