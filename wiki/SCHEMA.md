# Wiki Schema

Human-curated conceptual knowledge for the sql-logic-extractor project, following Karpathy's LLM Wiki pattern. Paired with the auto-generated code graph at `graphify-out/` (structural) and code wiki at `graphify-out/wiki/` (per-module articles).

## Layout

```
wiki/
  SCHEMA.md         - this file
  index.md          - human entry point; links concepts, decisions, recent log
  log.md            - append-only ledger (ingestions, decisions, commits)
  raw/              - drop-zone for unprocessed sources (papers, screenshots, customer notes, URLs)
  concepts/         - one markdown page per domain concept; stable URLs
  decisions/        - dated design decisions: YYYY-MM-DD-<slug>.md
```

## What goes where

| Content | Location |
|---|---|
| A paper you want ingested | `raw/<slug>.pdf` or `raw/<slug>.md` |
| A screenshot of Collibra UI | `raw/<slug>.png` |
| Customer feedback note | `raw/<slug>.md` |
| A stable domain concept (e.g. "Column-level lineage") | `concepts/<kebab-case>.md` |
| A design choice with tradeoffs | `decisions/<date>-<slug>.md` |
| Structural code knowledge | automatic — `graphify-out/wiki/` |

## Concept page format

```markdown
---
name: <title>
aliases: [other names]
see_also: [related concept slugs]
sources: [raw/<file>, ...]
---

## Definition
(1-3 sentences, plain language)

## Why it matters here
(how this shows up in sql-logic-extractor / Collibra connector)

## Open questions
(what we don't know yet)
```

## Decision page format

```markdown
---
date: YYYY-MM-DD
status: proposed | accepted | superseded
---

## Context
(what forced the choice)

## Decision
(what we picked)

## Alternatives considered
(what we rejected and why)

## Consequences
(what this locks in)
```

## Maintenance cadence

- **On ingestion** (`/ingest`): sources in `raw/` get summarized, relevant concept pages updated, one line logged.
- **On commit** (post-commit hook + Claude rule): one line appended to `log.md` referencing affected pages.
- **Weekly lint**: Claude health-checks contradictions, orphans, missing cross-refs.

Humans curate sources; Claude maintains structure.
