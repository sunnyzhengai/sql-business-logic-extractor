"""Per-community feature-matrix renderer (Phase 4 -- v4 matrix design).

For each community, render one markdown file with THREE matrices stacked:

  1. Table matrix       (structural shape)
  2. Filter matrix      (cohort definitions / parameterization candidates)
  3. Base column matrix (semantic data the view actually touches)

Each matrix is rows = features (tables / filters / TABLE.COLUMN pairs),
columns = views in this community. Dense rows (>= threshold coverage)
are bolded. Per-view footers report:

  - **alignment**         the fraction of dense rows this view uses;
                          low alignment = structural outlier signal.
  - **grain-changers**    (table matrix only) signed integer per view:
                          +N = N finer-grain joins; -N = anchor N
                          levels coarser than cohort.

This is the production renderer of the v4 mock designed at
`tools/diagnostics/mock_view_matrix.py` and documented at
`docs/mocks/patient_access_view_matrix.md`. The synthetic fixture in
the mock is replaced here with real corpus data fed by the orchestrator
in `tools/operate/validate_graph_pivot.py`.

Output: one file per community at
    <output_dir>/community_matrices/community_<NN>_<top_table>_matrix.md
"""

from __future__ import annotations

from pathlib import Path


# ---------------------------------------------------------------------------
# Clarity-specific grain classification.
#
# These defaults reflect the Clarity prefix taxonomy documented at
# `wiki/concepts/clarity-table-families.md`:
#
#   PAT_*       -- facts (patient-anchored events)
#   PATIENT     -- conformed dim
#   CLARITY_*   -- conformed dim masters
#   ZC_*        -- code lookups
#   HSP_*       -- facts (hospital events)
#   ORDER_*     -- facts (orders)
#   FLOWSHEET*  -- facts (clinical measurements; finer-grain than encounter)
#   RFL_*       -- facts (referrals)
#   CLM_*       -- facts (claims)
#
# Production version will read cardinality from the Clarity metadata
# table the user will provide; until then we classify by prefix.
#
# Grain levels are RELATIVE to the COMMUNITY COHORT grain (typically
# encounter for patient-access communities). Level 0 = cohort, +1 =
# finer by one step, -1 = coarser by one step. None = dim or code,
# no grain shift on join.
# ---------------------------------------------------------------------------

CLARITY_TABLE_GRAIN: dict[str, dict] = {
    # Patient master (conformed dim).
    "PATIENT":           {"label": "dim",                "category": "dim",  "level": None},

    # Encounter-grain fact (the typical cohort).
    "PAT_ENC":           {"label": "cohort",             "category": "fact", "level": 0},

    # Coarser-than-encounter facts (relationship grain).
    "PAT_PCP":           {"label": "↓ per patient",      "category": "fact", "level": -1},

    # Finer-than-encounter facts (encounter-child line tables).
    "PAT_ENC_DX":        {"label": "↑ per dx",           "category": "fact", "level": +1},
    "PAT_ENC_RX":        {"label": "↑ per rx",           "category": "fact", "level": +1},
    "PAT_ENC_PX":        {"label": "↑ per px",           "category": "fact", "level": +1},
    "PAT_ENC_NOTE":      {"label": "↑ per note",         "category": "fact", "level": +1},

    # FLOWSHEET family -- per-measurement grain, finer than encounter.
    "FLOWSHEET":         {"label": "↑ per measurement",  "category": "fact", "level": +1},
    "IP_FLWSHT_REC":     {"label": "↑ per measurement",  "category": "fact", "level": +1},
    "IP_FLWSHT_MEAS":    {"label": "↑ per measurement",  "category": "fact", "level": +1},

    # Clarity conformed dimensions -- joins don't shift grain.
    "CLARITY_SER":       {"label": "dim",                "category": "dim",  "level": None},
    "CLARITY_DEP":       {"label": "dim",                "category": "dim",  "level": None},
    "CLARITY_LOC":       {"label": "dim",                "category": "dim",  "level": None},
    "CLARITY_PRC":       {"label": "dim",                "category": "dim",  "level": None},
    "CLARITY_EDG":       {"label": "dim",                "category": "dim",  "level": None},
    "CLARITY_MEDICATION": {"label": "dim",               "category": "dim",  "level": None},

    # Hospital fact family -- per-account is the cohort grain when
    # the community centers on inpatient stays.
    "HSP_ACCOUNT":       {"label": "cohort",             "category": "fact", "level": 0},
    "HSP_ADMIT_DX":      {"label": "↑ per dx",           "category": "fact", "level": +1},

    # Orders -- per-order grain when the community centers on orders.
    "ORDER_PROC":        {"label": "cohort",             "category": "fact", "level": 0},
    "ORDER_MED":         {"label": "cohort",             "category": "fact", "level": 0},
    "ORDER_RESULTS":     {"label": "↑ per result",       "category": "fact", "level": +1},

    # Referrals.
    "REFERRAL":          {"label": "cohort",             "category": "fact", "level": 0},
    "RFL_HX_ACT":        {"label": "↑ per status change","category": "fact", "level": +1},

    # Claims.
    "CLM_CLAIM":         {"label": "cohort",             "category": "fact", "level": 0},
    "CLM_CLAIM_LINE":    {"label": "↑ per line",         "category": "fact", "level": +1},
}


def _classify_table_grain(
    table_name: str, table_grain: dict[str, dict] | None = None,
) -> dict:
    """Return the grain classification for one table.

    Falls back to the ZC_* / CLARITY_* prefix rules when the table
    isn't named in the explicit dict. Unknown tables get "?" / None.
    """
    grain = table_grain if table_grain is not None else CLARITY_TABLE_GRAIN
    bare = table_name.upper().split(".")[-1]  # drop schema if present
    if bare in grain:
        return grain[bare]
    # Prefix-based fallback. Catches tables we haven't enumerated.
    if bare.startswith("ZC_"):
        return {"label": "code", "category": "code", "level": None}
    if bare.startswith("CLARITY_"):
        return {"label": "dim", "category": "dim", "level": None}
    # Unknown -- emit a "?" so the modeler sees we didn't classify it.
    return {"label": "?", "category": "unknown", "level": None}


# ---------------------------------------------------------------------------
# Matrix construction helpers (ported from mock_view_matrix.py).
# ---------------------------------------------------------------------------


def _build_matrix(
    view_data: dict[str, dict],
    feature_key: str,
    view_short_names: list[str],
    view_full_names: list[str],
) -> tuple[list[str], dict[str, dict[str, bool]]]:
    """Build a `feature -> view_short -> bool` matrix from one feature axis.

    feature_key is "tables" / "filters" / "base_columns" -- the field on
    each view's dict whose list-of-strings we matrix-ize.

    Returns (feature_order, membership). feature_order is descending by
    coverage (densest first), ties broken alphabetically.
    """
    # Count how many views light up each feature.
    feature_counts: dict[str, int] = {}
    for vn in view_full_names:
        for f in view_data.get(vn, {}).get(feature_key, []):
            feature_counts[f] = feature_counts.get(f, 0) + 1

    # Densest first; ties broken alphabetically.
    feature_order = sorted(
        feature_counts.keys(),
        key=lambda f: (-feature_counts[f], f),
    )

    # Build the membership map.
    membership: dict[str, dict[str, bool]] = {f: {} for f in feature_order}
    for vn, short in zip(view_full_names, view_short_names):
        v_features = set(view_data.get(vn, {}).get(feature_key, []))
        for f in feature_order:
            membership[f][short] = f in v_features

    return feature_order, membership


def _per_view_alignment_score(
    feature_order: list[str],
    membership: dict[str, dict[str, bool]],
    view_short_names: list[str],
    dense_threshold: float = 0.5,
) -> dict[str, float]:
    """Per view: fraction of dense rows this view participates in.

    A row is "dense" if at least `dense_threshold` fraction of views
    light it up. Low alignment = structural outlier candidate.
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


def _view_grain_change(
    view_tables: list[str], table_grain: dict[str, dict] | None = None,
) -> int:
    """Signed grain-change count for one view.

    Walks FACT tables only (dims and codes don't shift grain).

      - output_level = max(fact levels joined)
      - if output_level > 0: returns +(count of finer-grain facts)
      - if output_level == 0: returns 0 (at cohort grain)
      - if output_level < 0: returns the offset (categorical
        different-cohort, magnitude not count)

    Asymmetric encoding intentional: finer-grain joins COMPOUND
    (each one multiplies rows), so counting is the right magnitude.
    Coarser anchor is CATEGORICAL (different question entirely),
    so reporting the offset captures it without over-counting.
    """
    fact_levels = []
    for t in view_tables:
        info = _classify_table_grain(t, table_grain)
        if info.get("category") == "fact" and info.get("level") is not None:
            fact_levels.append(info["level"])
    if not fact_levels:
        return 0
    output_level = max(fact_levels)
    if output_level > 0:
        return sum(1 for L in fact_levels if L > 0)
    return output_level  # 0 or negative


def _render_matrix_md(
    title: str,
    subtitle: str,
    feature_order: list[str],
    membership: dict[str, dict[str, bool]],
    view_short_names: list[str],
    feature_col_label: str,
    alignment_scores: dict[str, float],
    *,
    feature_grain_fn: callable | None = None,
    per_view_grain_change: dict[str, int] | None = None,
    dense_threshold: float = 0.5,
) -> str:
    """Render one matrix to a pipe-table markdown block.

    feature_grain_fn (optional): callable(feature_name) -> dict with
    keys {"label": str, "level": int|None}. If provided, inserts a
    `grain` column between feature and view columns; used only on the
    table matrix.

    per_view_grain_change (optional): signed integer per view footer;
    rendered when feature_grain_fn is also provided.
    """
    lines: list[str] = []
    lines.append(f"## {title}")
    lines.append("")
    lines.append(subtitle)
    lines.append("")

    show_grain = feature_grain_fn is not None
    header_cells = [feature_col_label]
    if show_grain:
        header_cells.append("grain")
    header_cells += view_short_names + ["coverage"]
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("|" + "|".join(["---"] * len(header_cells)) + "|")

    n_views = len(view_short_names)
    dense_count_threshold = max(1, int(dense_threshold * n_views))

    for feature in feature_order:
        row: list[str] = []
        n_hits = sum(membership[feature].values())
        is_dense = n_hits >= dense_count_threshold
        feature_label = f"**{feature}**" if is_dense else feature
        row.append(feature_label)
        if show_grain:
            grain_info = feature_grain_fn(feature)
            grain_label = grain_info.get("label", "?")
            level = grain_info.get("level")
            # Bold any fact whose grain differs from the cohort (either
            # finer or coarser) -- visual flag that the table can shift
            # the view's output grain.
            if level not in (None, 0):
                row.append(f"**{grain_label}**")
            else:
                row.append(grain_label)
        for short in view_short_names:
            row.append("✓" if membership[feature].get(short, False) else " ")
        coverage_cell = f"{n_hits}/{n_views}" + ("  ●" if is_dense else "")
        row.append(coverage_cell)
        lines.append("| " + " | ".join(row) + " |")

    # Footer rows --------------------------------------------------------
    score_row = ["**alignment** (% of dense rows used)"]
    if show_grain:
        score_row.append("")
    for short in view_short_names:
        s = alignment_scores.get(short, 0.0)
        score_row.append(f"**{int(round(s * 100))}%**")
    score_row.append(f"_dense = ≥ {int(dense_threshold * 100)}% coverage; ● marks dense_")
    lines.append("| " + " | ".join(score_row) + " |")

    if show_grain and per_view_grain_change is not None:
        changer_row = ["**grain-changers joined**", ""]
        for short in view_short_names:
            n = per_view_grain_change.get(short, 0)
            if n > 0:
                cell = f"**+{n}**"
            elif n < 0:
                cell = f"**{n}**"
            else:
                cell = "0"
            changer_row.append(cell)
        changer_row.append(
            "_+N = N finer-grain joins; -N = anchor N levels coarser than cohort_"
        )
        lines.append("| " + " | ".join(changer_row) + " |")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry point.
# ---------------------------------------------------------------------------


def write_community_matrix(
    community_index: int,
    top_table: str,
    primary_views: list[str],
    view_data: dict[str, dict],
    output_path: str | Path,
    *,
    table_grain: dict[str, dict] | None = None,
    dense_threshold: float = 0.5,
    max_views_in_matrix: int = 20,
) -> str:
    """Write one community's 3-matrix feature view as markdown.

    Parameters
    ----------
    community_index : int
        0-based index used in filenames and headings.
    top_table : str
        The community's representative table -- used in the heading.
    primary_views : list[str]
        Names of the views in this community (already filtered to
        community-primary members by the caller). Order is preserved
        for the columns of the matrix.
    view_data : dict[str, dict]
        For each view name -> {"tables": list[str], "filters": list[str],
        "base_columns": list[str]}. The caller (orchestrator) builds
        this from the corpus + analysis; the renderer is decoupled.
    output_path : str | Path
        Destination file.
    table_grain : dict[str, dict], optional
        Override for the Clarity grain classification. Defaults to the
        module-level CLARITY_TABLE_GRAIN.
    dense_threshold : float, default 0.5
        Coverage fraction at or above which a row is "dense" (bolded).
    max_views_in_matrix : int, default 20
        Caps the number of views shown as columns. For larger
        communities the matrix would be too wide to read; the modeler
        can still drill into per-view details via the modeling spec.

    Returns
    -------
    str -- path to the written file.
    """
    if not primary_views:
        # Edge case: a community with no primary views. Write a stub.
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            f"# Community {community_index} -- {top_table} (no primary views)\n",
            encoding="utf-8",
        )
        return str(out)

    # Cap the view set if oversized. Truncation note appended later.
    truncated = False
    if len(primary_views) > max_views_in_matrix:
        primary_views_shown = primary_views[:max_views_in_matrix]
        truncated = True
    else:
        primary_views_shown = primary_views

    # Short names for matrix columns: R1, R2, ... -- the legend at the
    # bottom maps these to full view names.
    view_short_names = [f"R{i + 1}" for i in range(len(primary_views_shown))]

    # Three matrices ------------------------------------------------------
    table_order, table_membership = _build_matrix(
        view_data, "tables", view_short_names, primary_views_shown,
    )
    table_scores = _per_view_alignment_score(
        table_order, table_membership, view_short_names, dense_threshold,
    )
    grain_change = {
        short: _view_grain_change(
            view_data.get(vn, {}).get("tables", []), table_grain,
        )
        for short, vn in zip(view_short_names, primary_views_shown)
    }
    tables_md = _render_matrix_md(
        title="1. Table matrix  (structural shape)",
        subtitle=(
            "Which tables does each view touch? Includes tables used in "
            "any scope -- main, CTEs, subqueries. The `grain` column "
            "shows each table's row-cardinality relative to the community "
            "cohort: `dim` / `code` joins don't shift grain; `↑ per X` "
            "joins push the output finer; `↓ per Y` facts mean the view's "
            "anchor is coarser than cohort. The **grain-changers** footer "
            "reports a signed tally per view: +N = N finer-grain joins; "
            "-N = anchor N levels coarser. Zero = at cohort grain."
        ),
        feature_order=table_order,
        membership=table_membership,
        view_short_names=view_short_names,
        feature_col_label="table",
        alignment_scores=table_scores,
        feature_grain_fn=lambda t: _classify_table_grain(t, table_grain),
        per_view_grain_change=grain_change,
        dense_threshold=dense_threshold,
    )

    filter_order, filter_membership = _build_matrix(
        view_data, "filters", view_short_names, primary_views_shown,
    )
    filter_scores = _per_view_alignment_score(
        filter_order, filter_membership, view_short_names, dense_threshold,
    )
    filters_md = _render_matrix_md(
        title="2. Filter / cohort matrix",
        subtitle=(
            "Each row is a cohort-defining filter from any scope (WHERE, "
            "HAVING, JOIN ON, CTE filter, subquery filter). Filters do "
            "NOT vote on similarity -- a unified model can parameterize "
            "any of these. The matrix is **parameterization evidence**: "
            "dense rows are candidates to push down into the model; "
            "view-unique rows stay above. The alignment footer shows how "
            "much of the dense common ground each view participates in."
        ),
        feature_order=filter_order,
        membership=filter_membership,
        view_short_names=view_short_names,
        feature_col_label="filter / cohort definition",
        alignment_scores=filter_scores,
        dense_threshold=dense_threshold,
    )

    column_order, column_membership = _build_matrix(
        view_data, "base_columns", view_short_names, primary_views_shown,
    )
    column_scores = _per_view_alignment_score(
        column_order, column_membership, view_short_names, dense_threshold,
    )
    columns_md = _render_matrix_md(
        title="3. Base column matrix  (semantic data)",
        subtitle=(
            "Each row is a `TABLE.COLUMN` pair the view references "
            "anywhere -- SELECT, calculated-column derivation, filter "
            "predicate, join condition, CTE/subquery body. Traces "
            "calculated columns back to underlying data, so views with "
            "different output names but the same base columns surface "
            "as semantic twins. Alignment footer quantifies the overlap."
        ),
        feature_order=column_order,
        membership=column_membership,
        view_short_names=view_short_names,
        feature_col_label="base column (TABLE.COLUMN)",
        alignment_scores=column_scores,
        dense_threshold=dense_threshold,
    )

    # Header + legend + interpretation -----------------------------------
    legend_lines = ["## View legend", ""]
    for short, vn in zip(view_short_names, primary_views_shown):
        legend_lines.append(f"- **{short}** = `{vn}`")
    if truncated:
        legend_lines.append("")
        legend_lines.append(
            f"_Note: this community has {len(primary_views)} views; "
            f"showing the first {max_views_in_matrix} as matrix columns. "
            f"See the modeling spec for the full member list._"
        )
    legend_md = "\n".join(legend_lines)

    intro = (
        f"# Community {community_index} -- {top_table} feature matrix\n"
        f"\n"
        f"{len(primary_views)} primary view(s). Three matrices, ordered "
        f"structural -> filters -> base columns. Table matrix is the "
        f"determining axis for similarity; filter matrix is "
        f"parameterization evidence (not a similarity vote); base column "
        f"matrix surfaces semantic overlap that output-name comparison "
        f"misses.\n"
        f"\n"
        f"Each matrix shows:\n"
        f"\n"
        f"  - **rows** sorted by coverage descending. Dense rows "
        f"(>= {int(dense_threshold * 100)}% coverage) are **bolded** "
        f"and marked with a `●` -- they're the community's common "
        f"ground.\n"
        f"  - a **`coverage`** column on the right -- how many views use "
        f"that row.\n"
        f"  - an **`alignment`** footer -- how much of the dense common "
        f"ground each view participates in. Low alignment = structural "
        f"outlier signal, quantified.\n"
        f"  - on the **table matrix only**, a **`grain-changers joined`** "
        f"footer -- signed integer per view indicating finer-grain joins "
        f"(positive) or a coarser anchor than cohort (negative).\n"
    )

    interpretation = (
        "\n"
        "## How to read this\n"
        "\n"
        "Three independent axes of evidence (tables, filters, base "
        "columns). When all three say the same thing, the conclusion "
        "is strong; when they disagree, it's a steward conversation. "
        "The table matrix is the determining axis -- if structure says "
        "no, no scoring of filters or columns can rescue the pair.\n"
        "\n"
        "Look for: views with low alignment scores AND non-zero "
        "grain-changers (outlier candidates -- different cohort or "
        "different grain); near-twin pairs with similar alignment AND "
        "zero grain-changers (consolidation candidates); base-column "
        "overlap that surface-name comparison would miss (semantic "
        "twins under different output names).\n"
    )

    full = "\n".join([
        intro,
        tables_md, "",
        filters_md, "",
        columns_md, "",
        legend_md,
        interpretation,
    ])

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(full, encoding="utf-8")
    return str(out)


# ---------------------------------------------------------------------------
# Helper for the orchestrator: build the view_data dict from corpus + graph.
# ---------------------------------------------------------------------------


def build_view_data(
    views: list[dict],
    view_to_tables_map: dict[str, set[str]],
) -> dict[str, dict]:
    """Build the {view_name -> {"tables", "filters", "base_columns"}} dict.

    Walks each view's scopes and collects:
      - tables       : union of `view_to_tables_map[view_name]` (already
                       computed by p30_analyze.view_membership.view_to_tables)
      - filters      : every filter's english (or expression fallback)
                       across all scopes
      - base_columns : every "TABLE.COLUMN" pair referenced by any column
                       in any scope -- pairs ColumnV1.base_columns with
                       ColumnV1.base_tables for proper TABLE.COLUMN form.

    Parameters
    ----------
    views : list of ViewV1 dicts (business views, post-filter).
    view_to_tables_map : as returned by p30_analyze.view_membership.view_to_tables.

    Returns
    -------
    dict view_name -> dict with three keys.
    """
    result: dict[str, dict] = {}
    for view in views:
        view_name = view.get("view_name")
        if not view_name:
            continue

        # Tables: from the graph (already covers all scopes + joins).
        tables = sorted(view_to_tables_map.get(view_name, set()))

        # Filters: english-readable form, deduplicated by english key.
        filters_seen: set[str] = set()
        filters: list[str] = []
        for scope in view.get("scopes") or []:
            for f in scope.get("filters") or []:
                key = (f.get("english") or f.get("expression") or "").strip()
                if not key or key in filters_seen:
                    continue
                filters_seen.add(key)
                filters.append(key)

        # Base columns: pair each base_column with its base_table to
        # produce "TABLE.COLUMN" entries. base_columns and base_tables
        # are parallel tuples on ColumnV1.
        base_cols_seen: set[str] = set()
        base_cols: list[str] = []
        for scope in view.get("scopes") or []:
            for col in scope.get("columns") or []:
                bcs = col.get("base_columns") or ()
                bts = col.get("base_tables") or ()
                # If parallel-aligned, use the pair; otherwise emit the
                # column with "?" table marker.
                if len(bcs) == len(bts):
                    pairs = zip(bts, bcs)
                else:
                    pairs = [("?", bc) for bc in bcs]
                for table, column in pairs:
                    if not column:
                        continue
                    # Strip schema prefix from table (PATIENT.PAT_ID
                    # not Clarity.dbo.PATIENT.PAT_ID).
                    table_bare = (table or "?").split(".")[-1]
                    pair = f"{table_bare}.{column}"
                    if pair in base_cols_seen:
                        continue
                    base_cols_seen.add(pair)
                    base_cols.append(pair)

        result[view_name] = {
            "tables": tables,
            "filters": filters,
            "base_columns": base_cols,
        }
    return result
