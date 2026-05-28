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

import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Input-data hygiene filters (defensive guards against extractor noise).
#
# When the corpus extractor captures CTE definitions, JOIN ON keys, or
# placeholder filters as "tables" or "filters", they leak into the matrix
# as noise rows. These filters drop the obvious junk so the matrix shows
# only signal. The DEEPER fix belongs in the extractor; these guards are
# the renderer-side safety net.
# ---------------------------------------------------------------------------

# Tokens that, if found inside a "table" string, mean it's not a real
# table identifier -- almost certainly a CTE definition fragment or a
# SQL-expression that got captured as a table by mistake.
_NON_TABLE_TOKENS = (
    " WHERE ", " AND ", " OR ", " AS ",
    "CROSS APPLY", "OUTER APPLY",
    "=", "(", ")", "<", ">",
)


def _is_real_table_name(name: str) -> bool:
    """Return True if `name` looks like a real table identifier.

    Filters CTE-definition fragments and SQL operators that got captured
    as 'tables' by the corpus extractor:

      - 'DAY_OF_MONTH = 1) AS DD'   -- CTE definition
      - 'CROSS APPLY DateDim'        -- T-SQL syntax
      - 'MYPT_ID WHERE 1 = 1'        -- inline filter
      - 'HSP_ACCOUNT_ID IS NULL'     -- inline filter (IS NULL form)

    Two checks layered:

      1. Embedded whitespace in the bare identifier -- Clarity/BI tables
         use underscore-separated names; a space means we're looking at
         a SQL expression captured by mistake.
      2. SQL operators / keywords anywhere in the original string --
         catch-all for `=`, `<>`, `WHERE`, `AND`, etc.
    """
    if not name or name.strip() in ("?", ""):
        return False
    # Strip schema prefix and bracket quoting to get the bare identifier.
    bare = name.split(".")[-1].strip("[]").strip()
    # Real healthcare/BI table identifiers are single words. Embedded
    # whitespace is the strongest signal that we're looking at a SQL
    # expression rather than a table name.
    if " " in bare or "\t" in bare:
        return False
    upper = " " + name.upper() + " "  # pad so token-bounded matches work
    for tok in _NON_TABLE_TOKENS:
        if tok in upper:
            return False
    return True


# Pattern for tautology filters like "1 = 1" / "1=1" / " ( 1 = 1 ) ".
_TAUTOLOGY_RE = re.compile(r"^\s*\(?\s*1\s*=\s*1\s*\)?\s*$")


# Pattern for an "identifier-looking" expression in the english-translated
# filter: starts with a letter, contains only word chars / spaces / dots
# / brackets. No quotes, no numbers, no parens, no operators -- those
# signal a literal or function call.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z][\w \.\[\]]*$")


def _looks_like_identifier(s: str) -> bool:
    """True if `s` looks like a column-reference identifier (as the
    english translator renders them) rather than a literal value.

    A literal value would be a quoted string, number, NULL, function
    call, or arithmetic expression -- none of which match the
    identifier pattern. Joined column references render as title-case
    phrases like `Patient Identifier`, `Mypt Identifier`,
    `Ua Who Accessed`.
    """
    s = s.strip().strip("()").strip()
    if not s:
        return False
    if s[0] in ("'", '"'):
        return False  # quoted literal
    if s.upper() in ("NULL", "TRUE", "FALSE"):
        return False
    return bool(_IDENTIFIER_RE.fullmatch(s))


def _is_join_key_leg(leg: str) -> bool:
    """One leg is a JOIN ON-style `<identifier> = <identifier>`.

    Heuristic: both sides of the `=` look like column-reference
    identifiers (no literals, no function calls). Captures both
      - self-equality (`X = X`, JOIN keys that translate to identical
        sides), and
      - cross-identifier (`Ua Who Accessed = Mypt Identifier`, JOIN
        keys whose columns translate to different phrases).

    A genuine cohort predicate has at least one side that's a literal,
    so it doesn't match this pattern. Risk of false positives: filters
    that compare two columns of the SAME row (e.g.
    `Created Date = Modified Date`) get dropped. In the matrix-display
    context, the modeler/steward is hunting for cohort definitions,
    not these rare same-row column equalities; the tradeoff favors
    aggressive cleanup.
    """
    leg = leg.strip()
    if _TAUTOLOGY_RE.match(leg):
        return True
    parts = leg.split("=", 1)
    if len(parts) != 2:
        return False
    lhs = parts[0].strip().strip("()").strip()
    rhs = parts[1].strip().strip("()").strip()
    if not lhs or not rhs:
        return False
    # Self-equality (already a join key) or cross-identifier (also a
    # join key, just translated to different phrases) -- same behavior.
    return _looks_like_identifier(lhs) and _looks_like_identifier(rhs)


# Kept as a back-compat alias; new code should use _is_join_key_leg.
_is_self_equality_leg = _is_join_key_leg


_AND_OR_SPLIT_RE = re.compile(r"(\s+(?:and|or)\s+)", re.IGNORECASE)
_CONNECTOR_RE = re.compile(r"\s+(?:and|or)\s+", re.IGNORECASE)
_LEADING_CONNECTOR_RE = re.compile(r"^\s*(?:and|or)\s+", re.IGNORECASE)
_TRAILING_CONNECTOR_RE = re.compile(r"\s+(?:and|or)\s*$", re.IGNORECASE)


def _clean_filter(key: str) -> str | None:
    """Drop self-equality legs from a filter expression, return cleaned
    english (or None if entirely noise).

    Removes:
      - Tautologies (`1 = 1`) -- developer-pasted placeholder.
      - Self-equality (`X = X`) -- JOIN ON keys where the column name
        matches across the join, rendered by the english translator
        with identical lhs/rhs.

    Decomposes BOTH `and` AND `or` compounds. So
        `Patient Identifier = Patient Identifier and Is Valid Patient Yn = 'Y'`
    becomes
        `Is Valid Patient Yn = 'Y'`,
    and
        `(X = X) or (Coverage Type C = 2)`
    becomes
        `(Coverage Type C = 2)`.

    Note: decomposing OR is a display cleanup, not a semantically
    precise rewrite. In raw SQL, `a.X = b.X` is a real join predicate
    that filters out unmatched rows -- the english translator
    silently collapses both sides to one identifier and we lose that
    information. For the matrix display purpose (steward / modeler
    pattern-spotting), dropping these legs is the right tradeoff:
    they look like noise to a human reviewer.
    """
    if not key:
        return None
    stripped = key.strip()
    if _TAUTOLOGY_RE.match(stripped):
        return None

    # Tokenize: alternating [leg, connector, leg, connector, ...].
    # _AND_OR_SPLIT_RE captures the connector so we keep it for rejoining.
    parts = _AND_OR_SPLIT_RE.split(stripped)

    # Drop each self-equality leg. Also drop the connector adjacent to
    # it so we don't end up with `Y and  and Z` or leading `or X`.
    kept: list[str] = []
    for p in parts:
        is_connector = bool(_CONNECTOR_RE.fullmatch(p))
        if is_connector:
            # Only keep this connector if the previous kept item is a
            # leg (not another connector and not the start).
            if kept and not _CONNECTOR_RE.fullmatch(kept[-1]):
                kept.append(p)
            # else: drop -- we just dropped a leg before this connector.
        else:
            if _is_self_equality_leg(p):
                # Drop this leg. If the previous kept item is a
                # connector, drop it too (the connector now has no
                # right-hand operand).
                if kept and _CONNECTOR_RE.fullmatch(kept[-1]):
                    kept.pop()
            else:
                kept.append(p)

    result = "".join(kept).strip()
    # Tidy: leading or trailing orphan connectors.
    result = _LEADING_CONNECTOR_RE.sub("", result)
    result = _TRAILING_CONNECTOR_RE.sub("", result)
    if not result:
        return None
    return result


def _is_real_filter(key: str) -> bool:
    """Back-compat: True iff `_clean_filter(key)` returns non-None."""
    return _clean_filter(key) is not None


def _parse_base_column_ref(bc: str, bt: str) -> tuple[str, str] | None:
    """Parse a base-column lineage entry into (bare_table, column).

    The corpus encodes ColumnV1.base_columns entries in scope-qualified
    form. Three shapes:

      - "table:TABLENAME.COLNAME"   -- final base-table reference.
        Return ("TABLENAME", "COLNAME"); the parallel base_tables entry
        is redundant in this case.

      - "cte:scope_name.COLNAME"    -- scope-internal lineage; this
        column is computed inside a CTE, not pulled from a base table.
        Returns None so the base-column matrix excludes it -- these
        aren't substrate references.

      - "COLNAME" (bare)            -- legacy/parser-incomplete shape.
        Fall back to the parallel base_tables[i] for the table name;
        return None if base_tables is "?" / empty.

    The matrix consumes only the (TABLE, COLUMN) tuples this returns.
    """
    if not bc:
        return None
    if bc.startswith("cte:"):
        # Scope-internal lineage; not a base reference.
        return None
    if bc.startswith("table:"):
        rest = bc[len("table:"):]
        if "." not in rest:
            return None
        table, col = rest.split(".", 1)
        return (table, col)
    # Bare column-name shape; use base_tables fallback if available.
    if bt and bt != "?":
        bare_table = bt.split(".")[-1]
        return (bare_table, bc)
    return None


def _is_unresolved_view_reference(name: str) -> bool:
    """Heuristic: `V_*` prefix names that survived view-expansion are
    typically foundation views NOT in the corpus -- we can't see
    through them to base tables, so they're an opaque indirection
    layer the matrix should hide.

    The check is bypassed if the name has an explicit entry in
    CLARITY_TABLE_GRAIN -- author intent overrides the heuristic.
    `F_*` is intentionally NOT dropped (BI convention: F_ prefix = fact
    table, not foundation view).
    """
    bare = name.strip().split(".")[-1].strip("[]").upper()
    if bare in CLARITY_TABLE_GRAIN:
        return False
    return bare.startswith("V_")


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
    # ===================================================================
    # ADD NEW DOMAIN ENTRIES HERE -- one row per table, in alphabetical
    # order within each section. Categories: "fact", "dim", "code".
    # Levels are relative to community cohort (typically encounter):
    #   None  -- dim or code; join doesn't shift grain.
    #   0     -- cohort grain.
    #   +N    -- finer than cohort by N levels.
    #   -N    -- coarser than cohort by N levels.
    # See wiki/concepts/clarity-table-families.md for taxonomy notes.
    # ===================================================================

    # Universal date dimension (every healthcare warehouse has this).
    "DATE_DIMENSION":    {"label": "dim",                "category": "dim",  "level": None},

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


def _render_aligned_pipe_table(
    header: list[str], rows: list[list[str]],
) -> list[str]:
    """Render a pipe-table where every column is padded to its max width.

    GitHub renders pipe-tables aligned regardless of padding; this just
    makes the RAW markdown text readable when viewed in a plain editor
    (Fabric notebook output, VS Code without preview, etc.). The width
    is computed on raw character count, including markdown decorations
    like `**bold**`, since we're aligning the source text.
    """
    n_cols = len(header)
    widths = [len(h) for h in header]
    for row in rows:
        for i, cell in enumerate(row[:n_cols]):
            if len(cell) > widths[i]:
                widths[i] = len(cell)

    def fmt(cells: list[str]) -> str:
        padded = [c.ljust(widths[i]) for i, c in enumerate(cells)]
        return "| " + " | ".join(padded) + " |"

    out = [fmt(header)]
    # Separator row: at least 3 dashes per column, padded to width.
    sep_cells = ["-" * max(3, widths[i]) for i in range(n_cols)]
    out.append("| " + " | ".join(sep_cells) + " |")
    for row in rows:
        out.append(fmt(row))
    return out


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
    """Render one matrix to a column-aligned pipe-table markdown block.

    feature_grain_fn (optional): callable(feature_name) -> dict with
    keys {"label": str, "level": int|None}. If provided, inserts a
    `grain` column between feature and view columns; used only on the
    table matrix.

    per_view_grain_change (optional): signed integer per view footer;
    rendered when feature_grain_fn is also provided.
    """
    lines: list[str] = [f"## {title}", "", subtitle, ""]

    show_grain = feature_grain_fn is not None
    header_cells = [feature_col_label]
    if show_grain:
        header_cells.append("grain")
    header_cells += view_short_names + ["coverage"]

    n_views = len(view_short_names)
    dense_count_threshold = max(1, int(dense_threshold * n_views))

    body_rows: list[list[str]] = []
    for feature in feature_order:
        row: list[str] = []
        n_hits = sum(membership[feature].values())
        is_dense = n_hits >= dense_count_threshold
        row.append(f"**{feature}**" if is_dense else feature)
        if show_grain:
            grain_info = feature_grain_fn(feature)
            grain_label = grain_info.get("label", "?")
            level = grain_info.get("level")
            # Bold any fact whose grain differs from the cohort (either
            # finer or coarser) -- visual flag that the table can shift
            # the view's output grain.
            row.append(f"**{grain_label}**" if level not in (None, 0) else grain_label)
        for short in view_short_names:
            row.append("✓" if membership[feature].get(short, False) else " ")
        row.append(f"{n_hits}/{n_views}" + ("  ●" if is_dense else ""))
        body_rows.append(row)

    # Footer: alignment row.
    score_row = ["**alignment** (% of dense rows used)"]
    if show_grain:
        score_row.append("")
    for short in view_short_names:
        score_row.append(f"**{int(round(alignment_scores.get(short, 0.0) * 100))}%**")
    score_row.append(
        f"_dense = ≥ {int(dense_threshold * 100)}% coverage; ● marks dense_"
    )
    body_rows.append(score_row)

    # Footer: grain-changers row (table matrix only).
    if show_grain and per_view_grain_change is not None:
        changer_row = ["**grain-changers joined**", ""]
        for short in view_short_names:
            n = per_view_grain_change.get(short, 0)
            if n > 0:
                changer_row.append(f"**+{n}**")
            elif n < 0:
                changer_row.append(f"**{n}**")
            else:
                changer_row.append("0")
        changer_row.append(
            "_+N = N finer-grain joins; -N = anchor N levels coarser than cohort_"
        )
        body_rows.append(changer_row)

    lines.extend(_render_aligned_pipe_table(header_cells, body_rows))
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


# Shop-specific aliases / non-table identifiers the corpus extractor
# captured as tables by mistake. Names listed here are dropped from the
# matrix regardless of any other heuristic. Edit this set as new false
# positives surface in customer corpora; if it grows past ~20 entries,
# move it to a per-corpus config file.
DEFAULT_TABLE_SKIP_LIST: set[str] = {
    # SQL aliases caught masquerading as tables (Yang's MyChart corpus,
    # 2026-05-24). Likely originate from `FROM PAT_ENC PAC` patterns
    # where the extractor recorded the alias as a separate table.
    "PAC",
}


def build_view_data(
    views: list[dict],
    view_to_tables_map: dict[str, set[str]],
    *,
    table_skip_list: set[str] | None = None,
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
        # The graph encodes table nodes with a "table::" prefix on the
        # node ID -- strip it. Also filter out entries that aren't real
        # table identifiers (CTE-definition fragments, SQL operators)
        # that leaked through as table nodes; see _is_real_table_name.
        skip = table_skip_list if table_skip_list is not None else DEFAULT_TABLE_SKIP_LIST
        raw_tables = view_to_tables_map.get(view_name, set())
        clean_tables: set[str] = set()
        for t in raw_tables:
            bare = t[len("table::"):] if t.startswith("table::") else t
            if not _is_real_table_name(bare):
                continue
            # Drop V_* foundation-view references that survived
            # expansion (means they weren't in the corpus). The user
            # can extract those views later or add explicit entries to
            # CLARITY_TABLE_GRAIN to override the heuristic.
            if _is_unresolved_view_reference(bare):
                continue
            # Shop-specific skip list -- captures SQL aliases like PAC
            # that the extractor recorded as tables. Compare on the
            # bare identifier (post-schema-stripping), uppercased.
            bare_upper = bare.split(".")[-1].strip("[]").upper()
            if bare_upper in {s.upper() for s in skip}:
                continue
            clean_tables.add(bare)
        tables = sorted(clean_tables)

        # Filters: english form, decomposed to drop self-equality legs.
        # `_clean_filter` strips `1 = 1` tautologies AND self-equality
        # JOIN ON legs out of compound `AND` chains, preserving the
        # real predicates -- so the filter matrix doesn't drown in
        # `Patient Identifier = Patient Identifier` noise. The synthetic
        # group_by / order_by FilterV1 entries (added so their column
        # refs flow into the base-column matrix) are skipped here --
        # they aren't cohort-defining predicates.
        filters_seen: set[str] = set()
        filters: list[str] = []
        for scope in view.get("scopes") or []:
            for f in scope.get("filters") or []:
                if (f.get("kind") or "") in ("group_by", "order_by"):
                    continue
                key = (f.get("english") or f.get("expression") or "").strip()
                cleaned = _clean_filter(key)
                if not cleaned or cleaned in filters_seen:
                    continue
                filters_seen.add(cleaned)
                filters.append(cleaned)

        # Base columns. Three sources are merged onto the same axis:
        #   (1) SELECT-output lineage via ColumnV1.base_columns
        #       (scope-qualified `table:T.C` / `cte:scope.C`)
        #   (2) FilterV1.columns -- refs inside WHERE / HAVING / JOIN ON
        #       / GROUP BY / ORDER BY predicates
        #   (3) JoinV1.columns   -- refs inside JOIN ON predicates
        #
        # Each ref ends up as a (table, alias, column) triple so self-
        # joins disambiguate by alias. Row label is `TABLE(alias).COLUMN`
        # when alias differs from table; just `TABLE.COLUMN` otherwise.
        # Apply the same V_* unresolved-view drop and skip-list as the
        # table matrix so the three matrices stay consistent.
        skip = table_skip_list if table_skip_list is not None else DEFAULT_TABLE_SKIP_LIST
        skip_upper = {s.upper() for s in skip}
        base_cols_seen: set[tuple[str, str, str]] = set()
        base_cols: list[str] = []

        def _accept_triple(table_name: str, alias: str, column_name: str) -> None:
            if not table_name or not column_name:
                return
            if not _is_real_table_name(table_name):
                return
            if _is_unresolved_view_reference(table_name):
                return
            bare_upper = table_name.split(".")[-1].strip("[]").upper()
            if bare_upper in skip_upper:
                return
            alias_norm = alias or table_name
            key = (table_name, alias_norm, column_name)
            if key in base_cols_seen:
                return
            base_cols_seen.add(key)
            if alias_norm and alias_norm != table_name:
                base_cols.append(f"{table_name}({alias_norm}).{column_name}")
            else:
                base_cols.append(f"{table_name}.{column_name}")

        for scope in view.get("scopes") or []:
            # (1) SELECT-output lineage.
            for col in scope.get("columns") or []:
                bcs = col.get("base_columns") or ()
                bts = col.get("base_tables") or ()
                if len(bcs) == len(bts):
                    pairs_iter = list(zip(bts, bcs))
                else:
                    pairs_iter = [("?", bc) for bc in bcs]
                for bt, bc in pairs_iter:
                    parsed = _parse_base_column_ref(bc, bt)
                    if parsed is None:
                        continue
                    table_name, column_name = parsed
                    # SELECT-output lineage doesn't carry the SQL alias.
                    # Use the table name as alias so the label collapses
                    # to TABLE.COLUMN (no parens) -- and dedupes against
                    # any filter/join ref that DID carry the same alias.
                    _accept_triple(table_name, table_name, column_name)

            # (2) Filter / GROUP BY / ORDER BY column refs.
            for f in scope.get("filters") or []:
                for cref in f.get("columns") or []:
                    _accept_triple(
                        cref.get("table") or "",
                        cref.get("table_alias") or "",
                        cref.get("column") or "",
                    )

            # (3) JOIN ON column refs.
            for j in scope.get("joins") or []:
                for cref in j.get("columns") or []:
                    _accept_triple(
                        cref.get("table") or "",
                        cref.get("table_alias") or "",
                        cref.get("column") or "",
                    )

        result[view_name] = {
            "tables": tables,
            "filters": filters,
            "base_columns": base_cols,
        }
    return result
