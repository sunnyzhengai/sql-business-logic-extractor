# Mock v2: Patient Access community -- feature matrix

Six synthetic views, ordered as the user described. R6 is the intentional outlier (clinical-quality, not patient-access).

Three matrices, ordered structural -> filters -> base columns per the design review. Each shows:

  - **rows** sorted by coverage descending. Dense rows (>= 50% coverage) are **bolded** and marked with a `●` in the coverage column -- they're the community's common ground.
  - a **`coverage`** column on the right -- how many views use that row.
  - an **`alignment`** footer row -- how much of the dense common ground each view participates in. Low alignment = structural outlier signal, quantified.

## 1. Table matrix  (structural shape)

Which tables does each view touch? Includes tables used in any scope -- main, CTEs, subqueries. This is the substrate that drove community detection, so we lead with it.

| table | R1 | R2 | R3 | R4 | R5 | R6 | coverage |
|---|---|---|---|---|---|---|---|
| **PATIENT** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | 6/6  ● |
| **PAT_ENC** |   | ✓ | ✓ | ✓ | ✓ | ✓ | 5/6  ● |
| **CLARITY_SER** | ✓ | ✓ |   | ✓ | ✓ |   | 4/6  ● |
| **ZC_APPT_STATUS** |   | ✓ | ✓ | ✓ | ✓ |   | 4/6  ● |
| **CLARITY_DEP** |   |   | ✓ | ✓ | ✓ |   | 3/6  ● |
| PAT_PCP | ✓ | ✓ |   |   |   |   | 2/6 |
| FLOWSHEET |   |   |   |   |   | ✓ | 1/6 |
| PAT_ENC_DX |   |   |   |   |   | ✓ | 1/6 |
| **alignment** (% of dense rows used) | **40%** | **80%** | **80%** | **100%** | **100%** | **40%** | _dense = ≥ 50% coverage; ● marks dense_ |

## 2. Filter / cohort matrix

Each row is a cohort-defining filter (from any scope: WHERE, HAVING, JOIN ON, CTE filter, subquery filter). Dense rows are the community's common scopes.

| filter / cohort definition | R1 | R2 | R3 | R4 | R5 | R6 | coverage |
|---|---|---|---|---|---|---|---|
| Encounter status = Closed |   | ✓ | ✓ |   |   |   | 2/6 |
| PCP relationship is current | ✓ | ✓ |   |   |   |   | 2/6 |
| Patient is active | ✓ | ✓ |   |   |   |   | 2/6 |
| BP measurement is most recent |   |   |   |   |   | ✓ | 1/6 |
| Department specialty = Dermatology |   |   |   |   | ✓ |   | 1/6 |
| Diagnosis includes diabetes (ICD-10 E10-E11) |   |   |   |   |   | ✓ | 1/6 |
| Encounter date in last 30 days |   |   |   |   | ✓ |   | 1/6 |
| Encounter status = Cancelled |   |   |   | ✓ |   |   | 1/6 |
| Encounter status = No Show |   |   |   |   | ✓ |   | 1/6 |
| FLOWSHEET row = BP measurement |   |   |   |   |   | ✓ | 1/6 |
| Time to close < 24 hours |   | ✓ |   |   |   |   | 1/6 |
| **alignment** (% of dense rows used) | **0%** | **0%** | **0%** | **0%** | **0%** | **0%** | _dense = ≥ 50% coverage; ● marks dense_ |

## 3. Base column matrix  (semantic data)

Each row is a `TABLE.COLUMN` pair the view references anywhere -- in SELECT, in calculated-column derivation, in filter predicates, in join conditions, in CTE / subquery bodies. This traces calculated columns back to the underlying data, so views that compute `pct_closed_24h` and `close_rate` (different output names, same base columns) now show overlap.

| base column (TABLE.COLUMN) | R1 | R2 | R3 | R4 | R5 | R6 | coverage |
|---|---|---|---|---|---|---|---|
| **PATIENT.PAT_ID** | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | 6/6  ● |
| **PAT_ENC.ENC_DATE** |   | ✓ | ✓ | ✓ | ✓ | ✓ | 5/6  ● |
| **PAT_ENC.PAT_ID** |   | ✓ | ✓ | ✓ | ✓ | ✓ | 5/6  ● |
| **CLARITY_SER.PROV_ID** | ✓ | ✓ |   | ✓ | ✓ |   | 4/6  ● |
| **CLARITY_SER.PROV_NAME** | ✓ | ✓ |   | ✓ | ✓ |   | 4/6  ● |
| **PAT_ENC.STATUS_C** |   | ✓ | ✓ | ✓ | ✓ |   | 4/6  ● |
| **ZC_APPT_STATUS.STATUS_C** |   | ✓ | ✓ | ✓ | ✓ |   | 4/6  ● |
| **ZC_APPT_STATUS.STATUS_NAME** |   | ✓ | ✓ | ✓ | ✓ |   | 4/6  ● |
| **CLARITY_DEP.DEPT_ID** |   |   | ✓ | ✓ | ✓ |   | 3/6  ● |
| **CLARITY_DEP.DEPT_NAME** |   |   | ✓ | ✓ | ✓ |   | 3/6  ● |
| **PATIENT.PAT_NAME** |   |   |   | ✓ | ✓ | ✓ | 3/6  ● |
| **PAT_ENC.DEPT_ID** |   |   | ✓ | ✓ | ✓ |   | 3/6  ● |
| **PAT_ENC.PROV_ID** |   | ✓ |   | ✓ | ✓ |   | 3/6  ● |
| PATIENT.STATUS_C | ✓ | ✓ |   |   |   |   | 2/6 |
| PAT_ENC.CLOSE_DATE |   | ✓ | ✓ |   |   |   | 2/6 |
| PAT_PCP.IS_CURRENT | ✓ | ✓ |   |   |   |   | 2/6 |
| PAT_PCP.PAT_ID | ✓ | ✓ |   |   |   |   | 2/6 |
| PAT_PCP.PROV_ID | ✓ | ✓ |   |   |   |   | 2/6 |
| CLARITY_DEP.SPECIALTY |   |   |   |   | ✓ |   | 1/6 |
| FLOWSHEET.MEAS_TIME |   |   |   |   |   | ✓ | 1/6 |
| FLOWSHEET.MEAS_TYPE |   |   |   |   |   | ✓ | 1/6 |
| FLOWSHEET.MEAS_VALUE |   |   |   |   |   | ✓ | 1/6 |
| FLOWSHEET.PAT_ID |   |   |   |   |   | ✓ | 1/6 |
| PAT_ENC_DX.DX_CODE |   |   |   |   |   | ✓ | 1/6 |
| PAT_ENC_DX.ENC_ID |   |   |   |   |   | ✓ | 1/6 |
| PAT_ENC_DX.PAT_ID |   |   |   |   |   | ✓ | 1/6 |
| **alignment** (% of dense rows used) | **23%** | **69%** | **69%** | **100%** | **100%** | **31%** | _dense = ≥ 50% coverage; ● marks dense_ |

## View legend

- **R1** = `R1_PCP_PANEL_SIZE`
- **R2** = `R2_PCP_ENC_CLOSED_24H_PCT`
- **R3** = `R3_DEPT_ENC_CLOSE_RATE`
- **R4** = `R4_CANCELLATION_REPORT`
- **R5** = `R5_DERM_NOSHOW_LAST_MONTH`
- **R6** = `R6_DIABETIC_BP_CONTROL`

## Read these three matrices together

Three independent axes of evidence (tables, filters, base columns).
When all three say the same thing, the conclusion is strong; when
they disagree, it's a steward conversation.

**Look at R6's alignment scores across the three matrices.** If R6
scores low on all three, it's a strong outlier signal. If it scores
low only on (say) columns but high on tables, that's a different
story -- the view shares structural shape but does something
unique with that shape.

**Look at R4 vs R5.** If they score similarly on all three,
they're a near-twin pair -- candidate for consolidation into one
parameterized model.

**Look at R2 vs R3.** Yesterday's mock missed that they share
encounter-close logic because their OUTPUT column names differ
(`pct_closed_24h` vs `close_rate`). The base-column matrix now
shows them sharing `PAT_ENC.STATUS_C`, `PAT_ENC.ENC_DATE`,
`PAT_ENC.CLOSE_DATE`, `ZC_APPT_STATUS.STATUS_C` -- the underlying
data they actually touch. This is the semantic similarity that
surface-name comparison missed.


## Where the borrowed academic ideas would apply

The three matrices ABOVE are still hand-eyeballed -- a reviewer scans
and recognizes patterns. Here's where the literature offers algorithms
to AUTOMATE the recognition. None of these are required for the first
version; they are enhancements to bring in if/when the manual matrices
get too tall for a human to scan.

### Frequent itemset mining  (Apriori, FP-Growth)

Applied here, an "itemset" would be a set of features (table-set, or
filter-set, or base-column-set) that frequently co-occur across views.

Concrete example from this 6-view community: in the FILTER matrix,
`Patient is active` and `PCP relationship is current` co-occur in 2
views (R1, R2) and never appear apart. An itemset miner would flag
them as a 2-element frequent itemset -- "these two filters are always
together; consider treating them as ONE composite cohort definition
('active patient with a current PCP') in the unified model."

In a larger community (say 100 views), itemset mining would surface
co-occurring filter clusters that the human can't spot by eye.

### Biclustering  (Madeira & Oliveira, 2004)

Applied here, a "bicluster" is a subset of VIEWS that co-cluster with
a subset of FEATURES. Algorithmically: find a submatrix where the
density of ✓ is much higher than the surrounding matrix.

Concrete example from this 6-view community: in the BASE COLUMN matrix,
R4 and R5 form a bicluster with `PATIENT.PAT_NAME`, `PAT_ENC.PROV_ID`,
`PAT_ENC.DEPT_ID`, `PAT_ENC.ENC_DATE`, `CLARITY_SER.*`, `CLARITY_DEP.*`.
That's 8+ base columns that both R4 and R5 use, and other views use
fewer of. A biclustering algorithm would automatically discover this
sub-cluster and propose "R4 and R5 could be one parameterized model."

### Workload-driven physical design  (DB systems literature)

This is the closest academic kin to what we're doing overall. Given a
workload (a set of queries), propose materialized views or indexes
that serve the workload well. We're proposing certified data models
from a workload of existing views -- same shape, governance goal
instead of performance goal.

Concrete application: the table matrix's "dense" rows are exactly the
candidate tables for a materialized view. The filter matrix's "dense"
rows are exactly the candidate WHERE-clause predicates to push down
into the materialized view. The base-column matrix's "dense" rows are
the candidate column projection set.

For now, we are producing these candidates as a HUMAN-readable matrix.
A workload-design algorithm would produce them as a recommendation
("materialize PATIENT + PAT_ENC + CLARITY_SER + CLARITY_DEP with
encounter_status filter; this serves 4 of 6 views").
