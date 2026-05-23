"""One-shot prototype: feature-matrix visualization of a community.

Synthesizes the same 6-view "Patient Access" community from yesterday's
mock, but reworked per design-review feedback:

  1. Three matrices in priority order: TABLES, FILTERS, BASE COLUMNS.
     (Old version had output columns, which conflated surface naming
     with underlying data -- R2's `pct_closed_24h` and R3's `close_rate`
     were appearing as different things despite sharing base columns.
     Replaced with a base-column matrix that traces calculated columns
     back to the columns they derive from.)

  2. Per-view "alignment score" footer on each matrix -- the fraction
     of dense rows (coverage >= 50%) this view participates in. Low
     score = structural outlier. Makes the outlier signal quantitative.

  3. Each matrix includes annotations pointing at the academic ideas
     a future enhancement could borrow (biclustering, frequent itemset
     mining, workload-driven physical design). Concrete; not theory.

The matrices are intended as a DESIGN MOCK, not a production renderer.
The goal is to validate that this representation surfaces the right
signals -- weak members visibly isolated, near-twin views visibly
clustered, common ground visibly dense.

Run as:
    python -m tools.diagnostics.mock_view_matrix

Output is written to: /tmp/mock_view_matrix.md
"""

from __future__ import annotations

from pathlib import Path


# ---------------------------------------------------------------------------
# Synthetic fixtures -- 6 "Patient Access" community views.
# ---------------------------------------------------------------------------
#
# Each view dict captures three feature sets:
#
#   - tables           every table the view touches (in any scope --
#                       main, CTE, subquery)
#   - filters          cohort definitions (in any scope)
#   - base_columns     `TABLE.COLUMN` pairs the view references in any
#                       capacity: select, calculated-column derivation,
#                       filter predicate, join condition. This is the
#                       semantic data the view touches, not the output
#                       labels it exposes.

VIEWS = [
    {
        "view_name": "R1_PCP_PANEL_SIZE",
        "tables": ["PATIENT", "PAT_PCP", "CLARITY_SER"],
        "filters": [
            "Patient is active",
            "PCP relationship is current",
        ],
        "base_columns": [
            "PATIENT.PAT_ID", "PATIENT.STATUS_C",
            "PAT_PCP.PAT_ID", "PAT_PCP.PROV_ID", "PAT_PCP.IS_CURRENT",
            "CLARITY_SER.PROV_ID", "CLARITY_SER.PROV_NAME",
        ],
    },
    {
        "view_name": "R2_PCP_ENC_CLOSED_24H_PCT",
        "tables": ["PATIENT", "PAT_PCP", "PAT_ENC", "CLARITY_SER", "ZC_APPT_STATUS"],
        "filters": [
            "Patient is active",
            "PCP relationship is current",
            "Encounter status = Closed",
            "Time to close < 24 hours",
        ],
        "base_columns": [
            "PATIENT.PAT_ID", "PATIENT.STATUS_C",
            "PAT_PCP.PAT_ID", "PAT_PCP.PROV_ID", "PAT_PCP.IS_CURRENT",
            "PAT_ENC.PAT_ID", "PAT_ENC.PROV_ID",
            "PAT_ENC.ENC_DATE", "PAT_ENC.CLOSE_DATE",
            "PAT_ENC.STATUS_C",
            "CLARITY_SER.PROV_ID", "CLARITY_SER.PROV_NAME",
            "ZC_APPT_STATUS.STATUS_C", "ZC_APPT_STATUS.STATUS_NAME",
        ],
    },
    {
        "view_name": "R3_DEPT_ENC_CLOSE_RATE",
        "tables": ["PATIENT", "PAT_ENC", "CLARITY_DEP", "ZC_APPT_STATUS"],
        "filters": [
            "Encounter status = Closed",
        ],
        "base_columns": [
            "PATIENT.PAT_ID",
            "PAT_ENC.PAT_ID", "PAT_ENC.DEPT_ID",
            "PAT_ENC.ENC_DATE", "PAT_ENC.CLOSE_DATE",
            "PAT_ENC.STATUS_C",
            "CLARITY_DEP.DEPT_ID", "CLARITY_DEP.DEPT_NAME",
            "ZC_APPT_STATUS.STATUS_C", "ZC_APPT_STATUS.STATUS_NAME",
        ],
    },
    {
        "view_name": "R4_CANCELLATION_REPORT",
        "tables": ["PATIENT", "PAT_ENC", "ZC_APPT_STATUS", "CLARITY_SER", "CLARITY_DEP"],
        "filters": [
            "Encounter status = Cancelled",
        ],
        "base_columns": [
            "PATIENT.PAT_ID", "PATIENT.PAT_NAME",
            "PAT_ENC.PAT_ID", "PAT_ENC.PROV_ID", "PAT_ENC.DEPT_ID",
            "PAT_ENC.ENC_DATE",
            "PAT_ENC.STATUS_C",
            "ZC_APPT_STATUS.STATUS_C", "ZC_APPT_STATUS.STATUS_NAME",
            "CLARITY_SER.PROV_ID", "CLARITY_SER.PROV_NAME",
            "CLARITY_DEP.DEPT_ID", "CLARITY_DEP.DEPT_NAME",
        ],
    },
    {
        "view_name": "R5_DERM_NOSHOW_LAST_MONTH",
        "tables": ["PATIENT", "PAT_ENC", "ZC_APPT_STATUS", "CLARITY_DEP", "CLARITY_SER"],
        "filters": [
            "Encounter status = No Show",
            "Department specialty = Dermatology",
            "Encounter date in last 30 days",
        ],
        "base_columns": [
            "PATIENT.PAT_ID", "PATIENT.PAT_NAME",
            "PAT_ENC.PAT_ID", "PAT_ENC.PROV_ID", "PAT_ENC.DEPT_ID",
            "PAT_ENC.ENC_DATE",
            "PAT_ENC.STATUS_C",
            "ZC_APPT_STATUS.STATUS_C", "ZC_APPT_STATUS.STATUS_NAME",
            "CLARITY_DEP.DEPT_ID", "CLARITY_DEP.DEPT_NAME", "CLARITY_DEP.SPECIALTY",
            "CLARITY_SER.PROV_ID", "CLARITY_SER.PROV_NAME",
        ],
    },
    {
        "view_name": "R6_DIABETIC_BP_CONTROL",       # <-- the intentional outlier
        "tables": ["PATIENT", "PAT_ENC", "PAT_ENC_DX", "FLOWSHEET"],
        "filters": [
            "Diagnosis includes diabetes (ICD-10 E10-E11)",
            "FLOWSHEET row = BP measurement",
            "BP measurement is most recent",
        ],
        "base_columns": [
            "PATIENT.PAT_ID", "PATIENT.PAT_NAME",
            "PAT_ENC.PAT_ID", "PAT_ENC.ENC_DATE",
            "PAT_ENC_DX.PAT_ID", "PAT_ENC_DX.ENC_ID", "PAT_ENC_DX.DX_CODE",
            "FLOWSHEET.PAT_ID", "FLOWSHEET.MEAS_TYPE",
            "FLOWSHEET.MEAS_VALUE", "FLOWSHEET.MEAS_TIME",
        ],
    },
]


SHORT_NAMES = {
    "R1_PCP_PANEL_SIZE": "R1",
    "R2_PCP_ENC_CLOSED_24H_PCT": "R2",
    "R3_DEPT_ENC_CLOSE_RATE": "R3",
    "R4_CANCELLATION_REPORT": "R4",
    "R5_DERM_NOSHOW_LAST_MONTH": "R5",
    "R6_DIABETIC_BP_CONTROL": "R6",
}


# ---------------------------------------------------------------------------
# Matrix construction
# ---------------------------------------------------------------------------


def _build_matrix(
    views: list[dict], feature_key: str,
) -> tuple[list[str], dict[str, dict[str, bool]]]:
    """Build a `feature -> view_short -> bool` matrix from one feature axis.

    feature_key is "tables" / "filters" / "base_columns" -- the field on
    each view whose list-of-strings we matrix-ize.
    """
    feature_counts: dict[str, int] = {}
    for v in views:
        for f in v.get(feature_key, []):
            feature_counts[f] = feature_counts.get(f, 0) + 1

    # Densest features first; ties broken alphabetically.
    feature_order = sorted(
        feature_counts.keys(),
        key=lambda f: (-feature_counts[f], f),
    )

    membership: dict[str, dict[str, bool]] = {f: {} for f in feature_order}
    for v in views:
        short = SHORT_NAMES[v["view_name"]]
        v_features = set(v.get(feature_key, []))
        for f in feature_order:
            membership[f][short] = f in v_features

    return feature_order, membership


def _per_view_alignment_score(
    feature_order: list[str],
    membership: dict[str, dict[str, bool]],
    view_short_names: list[str],
    dense_threshold: float = 0.5,
) -> dict[str, float]:
    """For each view, compute: fraction of dense rows this view hits.

    A row is "dense" if at least `dense_threshold` fraction of views
    light it up. A view's alignment score = (rows in dense set that
    this view participates in) / (total dense rows).

    Low score = this view shares little with the community's center
    of mass = outlier candidate.
    """
    n_views = len(view_short_names)
    dense_count_threshold = max(1, int(dense_threshold * n_views))

    dense_features = [
        f for f in feature_order
        if sum(membership[f].values()) >= dense_count_threshold
    ]
    if not dense_features:
        return {short: 0.0 for short in view_short_names}

    scores: dict[str, float] = {}
    for short in view_short_names:
        hits = sum(1 for f in dense_features if membership[f].get(short, False))
        scores[short] = hits / len(dense_features)
    return scores


def _render_matrix_md(
    title: str,
    subtitle: str,
    feature_order: list[str],
    membership: dict[str, dict[str, bool]],
    view_short_names: list[str],
    feature_col_label: str,
    alignment_scores: dict[str, float],
    dense_threshold: float = 0.5,
) -> str:
    """Render one matrix to a pipe-table markdown block."""
    lines: list[str] = []
    lines.append(f"## {title}")
    lines.append("")
    lines.append(subtitle)
    lines.append("")
    header_cells = [feature_col_label] + view_short_names + ["coverage"]
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("|" + "|".join(["---"] * len(header_cells)) + "|")

    n_views = len(view_short_names)
    dense_count_threshold = max(1, int(dense_threshold * n_views))

    for feature in feature_order:
        row: list[str] = []
        # Mark rows that count as "dense" (>= threshold).
        n_hits = sum(membership[feature].values())
        is_dense = n_hits >= dense_count_threshold
        # Bold the feature name when dense -- visually pops the
        # community's common ground.
        feature_label = f"**{feature}**" if is_dense else feature
        row.append(feature_label)
        for short in view_short_names:
            row.append("✓" if membership[feature].get(short, False) else " ")
        coverage_cell = f"{n_hits}/{n_views}" + ("  ●" if is_dense else "")
        row.append(coverage_cell)
        lines.append("| " + " | ".join(row) + " |")

    # Alignment-score footer row.
    score_row = ["**alignment** (% of dense rows used)"]
    for short in view_short_names:
        s = alignment_scores.get(short, 0.0)
        score_row.append(f"**{int(round(s * 100))}%**")
    score_row.append(f"_dense = ≥ {int(dense_threshold * 100)}% coverage; ● marks dense_")
    lines.append("| " + " | ".join(score_row) + " |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Inline annotations: where academic ideas apply concretely
# ---------------------------------------------------------------------------


_ACADEMIC_NOTES_HEADER = """
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
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    view_short_names = [SHORT_NAMES[v["view_name"]] for v in VIEWS]

    # 1. TABLES matrix (structural -- the root of community detection)
    table_order, table_membership = _build_matrix(VIEWS, "tables")
    table_scores = _per_view_alignment_score(
        table_order, table_membership, view_short_names,
    )
    tables_md = _render_matrix_md(
        title="1. Table matrix  (structural shape)",
        subtitle=(
            "Which tables does each view touch? Includes tables used in "
            "any scope -- main, CTEs, subqueries. This is the substrate "
            "that drove community detection, so we lead with it."
        ),
        feature_order=table_order,
        membership=table_membership,
        view_short_names=view_short_names,
        feature_col_label="table",
        alignment_scores=table_scores,
    )

    # 2. FILTERS matrix (cohort definitions)
    filter_order, filter_membership = _build_matrix(VIEWS, "filters")
    filter_scores = _per_view_alignment_score(
        filter_order, filter_membership, view_short_names,
    )
    filters_md = _render_matrix_md(
        title="2. Filter / cohort matrix",
        subtitle=(
            "Each row is a cohort-defining filter (from any scope: WHERE, "
            "HAVING, JOIN ON, CTE filter, subquery filter). Dense rows "
            "are the community's common scopes."
        ),
        feature_order=filter_order,
        membership=filter_membership,
        view_short_names=view_short_names,
        feature_col_label="filter / cohort definition",
        alignment_scores=filter_scores,
    )

    # 3. BASE COLUMN matrix (semantic data)
    column_order, column_membership = _build_matrix(VIEWS, "base_columns")
    column_scores = _per_view_alignment_score(
        column_order, column_membership, view_short_names,
    )
    columns_md = _render_matrix_md(
        title="3. Base column matrix  (semantic data)",
        subtitle=(
            "Each row is a `TABLE.COLUMN` pair the view references anywhere "
            "-- in SELECT, in calculated-column derivation, in filter "
            "predicates, in join conditions, in CTE / subquery bodies. "
            "This traces calculated columns back to the underlying data, "
            "so views that compute `pct_closed_24h` and `close_rate` "
            "(different output names, same base columns) now show overlap."
        ),
        feature_order=column_order,
        membership=column_membership,
        view_short_names=view_short_names,
        feature_col_label="base column (TABLE.COLUMN)",
        alignment_scores=column_scores,
    )

    legend_lines = ["## View legend", ""]
    for v in VIEWS:
        short = SHORT_NAMES[v["view_name"]]
        legend_lines.append(f"- **{short}** = `{v['view_name']}`")
    legend_md = "\n".join(legend_lines)

    intro = (
        "# Mock v2: Patient Access community -- feature matrix\n"
        "\n"
        "Six synthetic views, ordered as the user described. R6 is the "
        "intentional outlier (clinical-quality, not patient-access).\n"
        "\n"
        "Three matrices, ordered structural -> filters -> base columns "
        "per the design review. Each shows:\n"
        "\n"
        "  - **rows** sorted by coverage descending. Dense rows "
        "(>= 50% coverage) are **bolded** and marked with a `●` in the "
        "coverage column -- they're the community's common ground.\n"
        "  - a **`coverage`** column on the right -- how many views use that row.\n"
        "  - an **`alignment`** footer row -- how much of the dense common "
        "ground each view participates in. Low alignment = structural "
        "outlier signal, quantified.\n"
    )

    interpretation = (
        "\n"
        "## Read these three matrices together\n"
        "\n"
        "Three independent axes of evidence (tables, filters, base columns).\n"
        "When all three say the same thing, the conclusion is strong; when\n"
        "they disagree, it's a steward conversation.\n"
        "\n"
        "**Look at R6's alignment scores across the three matrices.** If R6\n"
        "scores low on all three, it's a strong outlier signal. If it scores\n"
        "low only on (say) columns but high on tables, that's a different\n"
        "story -- the view shares structural shape but does something\n"
        "unique with that shape.\n"
        "\n"
        "**Look at R4 vs R5.** If they score similarly on all three,\n"
        "they're a near-twin pair -- candidate for consolidation into one\n"
        "parameterized model.\n"
        "\n"
        "**Look at R2 vs R3.** Yesterday's mock missed that they share\n"
        "encounter-close logic because their OUTPUT column names differ\n"
        "(`pct_closed_24h` vs `close_rate`). The base-column matrix now\n"
        "shows them sharing `PAT_ENC.STATUS_C`, `PAT_ENC.ENC_DATE`,\n"
        "`PAT_ENC.CLOSE_DATE`, `ZC_APPT_STATUS.STATUS_C` -- the underlying\n"
        "data they actually touch. This is the semantic similarity that\n"
        "surface-name comparison missed.\n"
    )

    full = "\n".join([
        intro,
        tables_md, "",
        filters_md, "",
        columns_md, "",
        legend_md,
        interpretation,
        _ACADEMIC_NOTES_HEADER,
    ])

    out_path = Path("/tmp/mock_view_matrix.md")
    out_path.write_text(full, encoding="utf-8")
    print(f"Wrote: {out_path}")
    print()
    print(full)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
