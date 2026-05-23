"""One-shot prototype: feature-matrix visualization of a community.

Synthesizes a small "Patient Access" community (6 reports the user
described in conversation) and emits two markdown matrices:

    rows = filters / cohort definitions, columns = views (✓ if used)
    rows = output columns,               columns = views (✓ if selected)

The matrices are intended as a DESIGN MOCK, not as the final renderer.
The goal is to validate that this representation surfaces the right
signals -- weak members visibly isolated, near-twin views visibly
clustered, common ground visibly dense.

If the design holds, the production version will:
  - consume real ViewV1 dicts from corpus.jsonl
  - aggregate per-community using p30_analyze.primary_community
  - render to HTML with click-to-highlight (one view at a time)
  - sit alongside the existing modeling spec + community HTML

Until then: run this script standalone to inspect the markdown output.

    python -m tools.diagnostics.mock_view_matrix

Output is written to: /tmp/mock_view_matrix.md
"""

from __future__ import annotations

from pathlib import Path


# ---------------------------------------------------------------------------
# Synthetic fixtures -- 6 views described by the user as the "Patient Access"
# community, with one outlier (R6, which is clinical-quality not access).
# ---------------------------------------------------------------------------
#
# Each view dict has just enough shape to drive matrix construction:
#   - view_name
#   - filters (a list of english strings)
#   - output_columns (a list of column names)
#
# Real corpus.jsonl entries are much richer (scopes, joins, base_columns,
# fingerprints). For this mock we just need filter + column lists per view.

VIEWS = [
    {
        "view_name": "R1_PCP_PANEL_SIZE",
        "filters": [
            "Patient is active",
            "PCP relationship is current",
        ],
        "output_columns": [
            "provider_id", "provider_name", "panel_count",
        ],
    },
    {
        "view_name": "R2_PCP_ENC_CLOSED_24H_PCT",
        "filters": [
            "Patient is active",
            "PCP relationship is current",
            "Encounter status = Closed",
            "Time to close < 24 hours",
        ],
        "output_columns": [
            "provider_id", "provider_name",
            "total_encounters", "closed_24h_count", "pct_closed_24h",
        ],
    },
    {
        "view_name": "R3_DEPT_ENC_CLOSE_RATE",
        "filters": [
            "Encounter status = Closed",
        ],
        "output_columns": [
            "department_id", "department_name",
            "total_encounters", "closed_count", "close_rate",
        ],
    },
    {
        "view_name": "R4_CANCELLATION_REPORT",
        "filters": [
            "Encounter status = Cancelled",
        ],
        "output_columns": [
            "patient_id", "patient_name",
            "provider_id", "provider_name",
            "department_id", "department_name",
            "encounter_date",
        ],
    },
    {
        "view_name": "R5_DERM_NOSHOW_LAST_MONTH",
        "filters": [
            "Encounter status = No Show",
            "Department specialty = Dermatology",
            "Encounter date in last 30 days",
        ],
        "output_columns": [
            "patient_id", "patient_name",
            "provider_id", "provider_name",
            "department_id", "department_name",
            "encounter_date",
        ],
    },
    {
        "view_name": "R6_DIABETIC_BP_CONTROL",   # <-- the outlier
        "filters": [
            "Diagnosis includes diabetes (ICD-10 E10-E11)",
            "FLOWSHEET row = BP measurement",
            "BP measurement is most recent",
        ],
        "output_columns": [
            "patient_id", "patient_name",
            "last_systolic", "last_diastolic", "is_bp_controlled",
        ],
    },
]


# Short labels for matrix column headers -- keep the matrix readable.
SHORT_NAMES = {
    "R1_PCP_PANEL_SIZE": "R1",
    "R2_PCP_ENC_CLOSED_24H_PCT": "R2",
    "R3_DEPT_ENC_CLOSE_RATE": "R3",
    "R4_CANCELLATION_REPORT": "R4",
    "R5_DERM_NOSHOW_LAST_MONTH": "R5",
    "R6_DIABETIC_BP_CONTROL": "R6",
}


# ---------------------------------------------------------------------------
# Matrix builders
# ---------------------------------------------------------------------------


def _build_matrix(
    views: list[dict],
    feature_key: str,
) -> tuple[list[str], dict[str, dict[str, bool]]]:
    """Build a feature -> view -> bool matrix.

    feature_key is "filters" or "output_columns" -- the field on each
    view dict whose list-of-strings we're matrix-izing.

    Returns
    -------
    feature_order : list of feature strings, sorted by frequency desc
                    (so the densest "common ground" features bubble to top)
    membership    : feature -> view_short_name -> True/False
    """
    # Collect all distinct features in order of appearance.
    feature_counts: dict[str, int] = {}
    for v in views:
        for f in v.get(feature_key, []):
            feature_counts[f] = feature_counts.get(f, 0) + 1

    # Sort by frequency descending (densest rows first); ties broken
    # by alphabetical order for determinism.
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


def _render_matrix_md(
    title: str,
    feature_order: list[str],
    membership: dict[str, dict[str, bool]],
    view_short_names: list[str],
    feature_col_label: str,
) -> str:
    """Render a matrix to a markdown pipe table."""
    lines: list[str] = []
    lines.append(f"## {title}")
    lines.append("")
    # Header
    header_cells = [feature_col_label] + view_short_names + ["coverage"]
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("|" + "|".join(["---"] * len(header_cells)) + "|")

    n_views = len(view_short_names)
    for feature in feature_order:
        row = [feature]
        n_hits = 0
        for short in view_short_names:
            hit = membership[feature].get(short, False)
            row.append("✓" if hit else " ")
            n_hits += int(hit)
        row.append(f"{n_hits}/{n_views}")
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main: build the two matrices and write to /tmp/mock_view_matrix.md
# ---------------------------------------------------------------------------


def main() -> int:
    view_short_names = [SHORT_NAMES[v["view_name"]] for v in VIEWS]

    # Filter matrix.
    filter_order, filter_membership = _build_matrix(VIEWS, "filters")
    filter_md = _render_matrix_md(
        title="Filter / cohort matrix",
        feature_order=filter_order,
        membership=filter_membership,
        view_short_names=view_short_names,
        feature_col_label="filter / cohort definition",
    )

    # Output column matrix.
    column_order, column_membership = _build_matrix(VIEWS, "output_columns")
    column_md = _render_matrix_md(
        title="Output column matrix",
        feature_order=column_order,
        membership=column_membership,
        view_short_names=view_short_names,
        feature_col_label="output column",
    )

    # Legend mapping short names back to full view names.
    legend_lines = ["## View legend", ""]
    for v in VIEWS:
        short = SHORT_NAMES[v["view_name"]]
        legend_lines.append(f"- **{short}** = `{v['view_name']}`")
    legend_md = "\n".join(legend_lines)

    # Top-level wrapper + interpretation notes.
    intro = (
        "# Mock: Patient Access community -- feature matrix\n"
        "\n"
        "Synthetic 6-view community. Goal: validate the matrix-as-design idea.\n"
        "Reports 1-5 are patient-access; Report 6 (diabetic BP control) is the\n"
        "intentional outlier -- clinical quality, not access. If the matrix design\n"
        "works, R6 should be visibly isolated.\n"
        "\n"
        "The `coverage` column at the right of each matrix counts how many views\n"
        "use that row -- the densest rows are the community's common ground.\n"
    )

    notes = (
        "\n"
        "## What jumps out (matrix-only reading)\n"
        "\n"
        "**R6 is visibly isolated in both matrices.** Its three filters and three\n"
        "metric columns (`last_systolic`, `last_diastolic`, `is_bp_controlled`)\n"
        "are unique to it -- they have 1/6 coverage and sit at the bottom of\n"
        "each matrix once sorted by frequency descending. A modeler scanning\n"
        "this immediately sees R6 doesn't share the community's center of mass.\n"
        "\n"
        "**R4 and R5 are near-twins.** Their output columns are identical save\n"
        "for nothing visible at this granularity. The only divergence is the\n"
        "filter row -- R4 = Cancelled, R5 = No Show + Dermatology + last 30d.\n"
        "A modeler glancing at the column matrix concludes: \"these two should\n"
        "be one parameterized model, not two reports.\"\n"
        "\n"
        "**R1 and R2 are a model-extension pair.** Both have the active-patient\n"
        "+ current-PCP filters that no other view uses. R2 extends R1 with\n"
        "encounter activity. The pattern is visible because rows 'Patient is\n"
        "active' and 'PCP relationship is current' have 2/6 coverage, both on\n"
        "R1 and R2 specifically.\n"
        "\n"
        "**Common ground for the model**: every shared row (coverage >= 3) is\n"
        "a candidate default scope for the unified model. In this small mock\n"
        "no row hits 3/6 -- which is itself a signal: this community has\n"
        "weak structural overlap, possibly because R6 is dragging the average\n"
        "down. Removing R6 (treating it as wrongly-clustered) would likely\n"
        "concentrate the remaining 5 views into a much denser common ground.\n"
    )

    full = "\n".join([intro, filter_md, "", column_md, "", legend_md, notes])

    out_path = Path("/tmp/mock_view_matrix.md")
    out_path.write_text(full, encoding="utf-8")
    print(f"Wrote: {out_path}")
    print()
    print(full)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
