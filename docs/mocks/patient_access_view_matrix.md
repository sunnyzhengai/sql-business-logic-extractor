# Mock: Patient Access community -- feature matrix

Synthetic 6-view community. Goal: validate the matrix-as-design idea.
Reports 1-5 are patient-access; Report 6 (diabetic BP control) is the
intentional outlier -- clinical quality, not access. If the matrix design
works, R6 should be visibly isolated.

The `coverage` column at the right of each matrix counts how many views
use that row -- the densest rows are the community's common ground.

## Filter / cohort matrix

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

## Output column matrix

| output column | R1 | R2 | R3 | R4 | R5 | R6 | coverage |
|---|---|---|---|---|---|---|---|
| provider_id | ✓ | ✓ |   | ✓ | ✓ |   | 4/6 |
| provider_name | ✓ | ✓ |   | ✓ | ✓ |   | 4/6 |
| department_id |   |   | ✓ | ✓ | ✓ |   | 3/6 |
| department_name |   |   | ✓ | ✓ | ✓ |   | 3/6 |
| patient_id |   |   |   | ✓ | ✓ | ✓ | 3/6 |
| patient_name |   |   |   | ✓ | ✓ | ✓ | 3/6 |
| encounter_date |   |   |   | ✓ | ✓ |   | 2/6 |
| total_encounters |   | ✓ | ✓ |   |   |   | 2/6 |
| close_rate |   |   | ✓ |   |   |   | 1/6 |
| closed_24h_count |   | ✓ |   |   |   |   | 1/6 |
| closed_count |   |   | ✓ |   |   |   | 1/6 |
| is_bp_controlled |   |   |   |   |   | ✓ | 1/6 |
| last_diastolic |   |   |   |   |   | ✓ | 1/6 |
| last_systolic |   |   |   |   |   | ✓ | 1/6 |
| panel_count | ✓ |   |   |   |   |   | 1/6 |
| pct_closed_24h |   | ✓ |   |   |   |   | 1/6 |

## View legend

- **R1** = `R1_PCP_PANEL_SIZE`
- **R2** = `R2_PCP_ENC_CLOSED_24H_PCT`
- **R3** = `R3_DEPT_ENC_CLOSE_RATE`
- **R4** = `R4_CANCELLATION_REPORT`
- **R5** = `R5_DERM_NOSHOW_LAST_MONTH`
- **R6** = `R6_DIABETIC_BP_CONTROL`

## What jumps out (matrix-only reading)

**R6 is visibly isolated in both matrices.** Its three filters and three
metric columns (`last_systolic`, `last_diastolic`, `is_bp_controlled`)
are unique to it -- they have 1/6 coverage and sit at the bottom of
each matrix once sorted by frequency descending. A modeler scanning
this immediately sees R6 doesn't share the community's center of mass.

**R4 and R5 are near-twins.** Their output columns are identical save
for nothing visible at this granularity. The only divergence is the
filter row -- R4 = Cancelled, R5 = No Show + Dermatology + last 30d.
A modeler glancing at the column matrix concludes: "these two should
be one parameterized model, not two reports."

**R1 and R2 are a model-extension pair.** Both have the active-patient
+ current-PCP filters that no other view uses. R2 extends R1 with
encounter activity. The pattern is visible because rows 'Patient is
active' and 'PCP relationship is current' have 2/6 coverage, both on
R1 and R2 specifically.

**Common ground for the model**: every shared row (coverage >= 3) is
a candidate default scope for the unified model. In this small mock
no row hits 3/6 -- which is itself a signal: this community has
weak structural overlap, possibly because R6 is dragging the average
down. Removing R6 (treating it as wrongly-clustered) would likely
concentrate the remaining 5 views into a much denser common ground.
