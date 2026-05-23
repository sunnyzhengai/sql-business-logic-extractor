---
name: Clarity table families
aliases: [Epic Clarity table prefixes, PAT_ vs CLARITY_ vs ZC_, Clarity fact vs dim classification, grain in Clarity]
see_also: [recursive-translation-principle, sql-as-definitional-moments]
sources: []
---

## Definition

Clarity is Epic's relational reporting database — a nightly extract from
the operational Chronicles store. Its table names use stable prefix
conventions that encode each table's *role* (fact / dimension / code
lookup) and its *grain* (one row per what). For the SQL extractor, this
classification is structural ground truth: similarity scoring,
conformed-dimension detection, and unified-model proposals all hinge on
reading the prefix correctly.

## The families

| Prefix / pattern | Role | Typical grain | Examples |
|---|---|---|---|
| `PAT_*` | Fact — patient-anchored events | varies; see below | `PAT_ENC` (encounters), `PAT_PCP` (PCP relationships), `PAT_ENC_DX` (encounter diagnoses), `PAT_ENC_RX` (medication orders on encounter) |
| `PATIENT` | Conformed dimension | one row per patient | `PATIENT` |
| `CLARITY_*` | Conformed dimension master | one row per entity | `CLARITY_SER` (providers/serial-master), `CLARITY_DEP` (departments), `CLARITY_EDG` (diagnosis groupers), `CLARITY_LOC` (locations), `CLARITY_PRC` (procedures) |
| `ZC_*` | Code lookup ("Category" tables) | one row per code | `ZC_APPT_STATUS`, `ZC_SEX`, `ZC_MARITAL_STATUS` |
| `HSP_*` | Fact — hospital/inpatient events | one row per admission or per event | `HSP_ACCOUNT`, `HSP_ADMIT_DX` |
| `ORDER_*` | Fact — orders | one row per order or order-line | `ORDER_PROC`, `ORDER_MED`, `ORDER_RESULTS` |
| `FLOWSHEET` (`IP_FLWSHT_*`) | Fact — clinical measurements | one row per measurement | `FLOWSHEET`, `IP_FLWSHT_REC` |
| `RFL_*`, `REFERRAL*` | Fact — referrals + history | one row per referral / per status change | `REFERRAL`, `RFL_HX_ACT` |
| `CLM_*`, `INV_*` | Fact — claims / billing | one row per claim / invoice / line | `CLM_CLAIM`, `INV_BASIC_INFO` |

The bare table name without a prefix (e.g. `PATIENT`, `FLOWSHEET`) is
the master row of that family; suffixes like `_HX` (history),
`_DETAIL`, `_LINE`, `_DX`, `_RX`, `_NOTE` denote child line-level
tables hanging off that master.

## Why it matters here

**1. Conformed dimensions don't count toward similarity.** Every
patient-facing view touches `PATIENT`. Two views both joining
`CLARITY_SER` tells you almost nothing about whether they're
modeling-twins. Similarity scoring rides on the *fact* tables (`PAT_*`,
`HSP_*`, `ORDER_*`, `FLOWSHEET`, etc.); `CLARITY_*` / `ZC_*` / `PATIENT`
are decorative. The `community_modeling_spec.py` already groups them as
"Conformed dimensions" and "Lookups" — that grouping is the same one
that should drive similarity weighting.

**2. Same family ≠ same grain.** This is the granularity rule the
modeler cares about. `PAT_ENC` is at encounter grain (one row per
encounter). `FLOWSHEET` is at measurement grain — joining the two
*expands* the result set from encounters to measurements-per-encounter.
`PAT_ENC_DX` similarly expands encounters to diagnoses-per-encounter.

So even when two views are clearly in the same domain (both about
patient encounters), if one of them joins a grain-expanding table, it
probably wants its own model — its output row means something
different. The unified model can still reference the same underlying
facts; it just shouldn't pretend all four views ride the same SELECT.

**3. Code lookups are noise during community detection.** `ZC_*`
tables are joined hundreds of times across the corpus purely to
decode integers to display strings (`STATUS_C` → `STATUS_NAME`). They
should be treated as bridge nodes for graph projection or excluded
outright — otherwise every encounter view looks artificially close to
every other view via the `ZC_APPT_STATUS` join.

**4. The prefix is the de-facto schema documentation.** Epic provides
no catalog file for Clarity; the prefix conventions are how analysts
navigate. The extractor's tooling — bridge detection, leaf detection,
domain grouping — should treat the prefix as authoritative metadata
when no other schema source is available.

## Grain quick-reference

When in doubt, ask "one row per what?":

- `PATIENT` → per patient
- `PAT_ENC` → per encounter
- `PAT_PCP` → per (patient, provider, effective-date) — usually one current row per patient
- `PAT_ENC_DX`, `PAT_ENC_RX`, `PAT_ENC_PX` → per (encounter, child-item) — many rows per encounter
- `FLOWSHEET` → per (encounter, measurement, timestamp) — many rows per encounter
- `HSP_ADMIT_DX` → per (hospital account, diagnosis) — many rows per admission
- `CLM_CLAIM` → per claim; `CLM_CLAIM_LINE` → per claim-line (many per claim)
- `ZC_*`, `CLARITY_*` → per code / per master entity

A modeling rule of thumb: **if a candidate-member view joins a table
whose grain is finer than the view's declared output grain, the view
is doing an aggregation — and the unified model has to decide whether
to push that aggregation down into the model itself or leave it to
each consumer.** That's a steward conversation, not an automated
choice.

## Open questions

- Is `FLOWSHEET` ever genuinely modelable alongside `PAT_ENC` views, or
  is the grain mismatch always sharp enough to warrant separation? My
  current guess: separate is safer, since the modeler can still build a
  `last-BP-per-encounter` view *on top of* a flowsheet model.
- Should the extractor surface `_HX` (history) sibling tables as a
  distinct sub-family? They're temporal — a join brings in
  point-in-time semantics that change query meaning.
- How should the tooling treat custom `V_*` views authored by local BI
  teams? They use the same prefix convention loosely (`V_CCHP_*` in
  this corpus = CCHP-specific extracts), but they're not Epic-shipped
  Clarity tables — they're a local layer on top.
- Are there other Epic-shop dialects that diverge from these prefixes
  (Cogito Data Model, Caboodle warehouse)? Caboodle uses `Fact*` /
  `Dim*` Kimball-style naming directly — the family-prefix logic
  doesn't apply there. The extractor probably needs a per-warehouse
  classification function rather than hardcoding Clarity prefixes.
