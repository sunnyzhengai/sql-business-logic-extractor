"""The Term entity and a per-view extractor.

A Term is the comparison unit for governance. One Term = one
transformed/contextualized output column from a SQL view, carrying
both the NAME signal (canonical tokens) and the LOGIC signal
(resolved expression + structural fingerprint, when available).

Inclusion rule (per the user's spec): drop a column ONLY if all three
are true:
  - it's a passthrough (column_type == "passthrough"),
  - no view-level filters were applied,
  - the alias name is purely structural (e.g., PAT_ID, ROW_NUM)
    -- defined as: tokenizing the name leaves zero non-stop tokens.
Anything else IS a Term -- even bare passthroughs whose alias has
descriptive content, or where the surrounding query has filters
defining the row population.

Logic signal:
  - resolved_expression: the post-resolver SQL fragment (already in
    the technical lineage)
  - logic_fingerprint: re-uses tools/similar_logic_grouper/fingerprint.py.
    Dropped here as a string only; the bucket-comparison step in
    Phase 3 is what hashes / clusters.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Iterable

from .synonyms import SynonymDict, load_default_synonyms
from .tokenizer import name_to_canonical_tokens


# Names ending in these suffixes are treated as STRUCTURAL identifiers
# (foreign keys, surrogate keys, line numbers) rather than business
# terms -- even if their other tokens canonicalize to meaningful nouns
# like `patient`. So `PAT_ID` is structural even though `PAT` ->
# `patient`. Without this, every join-key column would clutter the
# governance worklist.
_STRUCTURAL_SUFFIX_RE = re.compile(
    r"_(ID|KEY|NUM|NO|SEQ|LINE|ROW)$", re.IGNORECASE
)


@dataclass(frozen=True)
class Term:
    """One governance-comparison unit.

    Frozen + hashable so callers can put Terms in sets / dict keys
    when clustering. The `name_tokens` and other tuple/frozenset
    fields preserve immutability.

    `to_dict()` is the canonical JSON-friendly export used by the
    corpus-level batch tool (Phase 3 input).
    """

    # Provenance ------------------------------------------------------
    view_name: str                       # filename stem
    column_name: str                     # column alias (output name)

    # Name signal -----------------------------------------------------
    raw_name: str                        # original column alias text
    name_tokens: frozenset[str]          # canonical-token bag

    # Logic signal ----------------------------------------------------
    column_type: str                     # passthrough / calculated / window / case / etc.
    resolved_expression: str             # full resolved SQL fragment

    # Context that defines the row-set the column is computed over.
    base_tables: tuple[str, ...] = ()
    base_columns: tuple[str, ...] = ()
    filters: tuple[str, ...] = ()        # view-level and per-column filters merged
    author_notes: tuple[str, ...] = ()   # comments attached to this column

    # Metadata for human triage of the inclusion decision.
    is_passthrough: bool = False
    has_filters: bool = False
    name_is_structural: bool = False

    def to_dict(self) -> dict:
        """JSON-friendly representation. Frozensets / tuples become lists."""
        d = asdict(self)
        d["name_tokens"] = sorted(self.name_tokens)
        d["base_tables"] = list(self.base_tables)
        d["base_columns"] = list(self.base_columns)
        d["filters"] = list(self.filters)
        d["author_notes"] = list(self.author_notes)
        return d


def _column_should_be_term(
    raw_name: str,
    name_tokens: frozenset[str],
    column_type: str,
    has_filters: bool,
) -> tuple[bool, bool]:
    """Apply the inclusion rule. Returns (include, name_is_structural).

    name_is_structural is True when EITHER:
      - tokenization left zero meaningful tokens (e.g. ROW_NUM ->
        all stop tokens), OR
      - the raw name ends with a structural suffix (_ID/_KEY/_NUM/_NO/
        _SEQ/_LINE/_ROW), which signals the column is a join key /
        index rather than a business attribute -- even when the other
        tokens like `PAT` would otherwise canonicalize to meaningful
        nouns.
    """
    name_is_structural = (
        len(name_tokens) == 0
        or _STRUCTURAL_SUFFIX_RE.search(raw_name) is not None
    )
    is_passthrough = column_type == "passthrough"
    # Drop ONLY if all three boring conditions hold.
    if is_passthrough and not has_filters and name_is_structural:
        return False, name_is_structural
    return True, name_is_structural


def extract_terms(
    view_name: str,
    column_translations: Iterable[dict],
    *,
    query_filters: Iterable[str] = (),
    synonyms: SynonymDict | None = None,
) -> list[Term]:
    """Build Terms from one view's per-column translations.

    `column_translations` is the same shape used by Tool 3 / 4 batch:
    each dict has at least column_name, column_type, resolved_expression,
    base_tables, base_columns, filters, author_notes (all optional;
    missing fields default to empty).

    `query_filters` are view-level WHERE/JOIN-ON predicates that apply
    to every column in the view. They get merged with each column's
    own per-column filters into the Term's `filters` tuple.

    `synonyms` defaults to the project's default healthcare dictionary.
    """
    if synonyms is None:
        synonyms = load_default_synonyms()

    view_filters = tuple(f for f in query_filters if f)
    out: list[Term] = []

    for col in column_translations:
        col_name = (col.get("column_name") or col.get("name") or "").strip()
        if not col_name:
            continue
        col_type = col.get("column_type") or col.get("type") or "unknown"

        # Filters: per-column entries plus view-level filters.
        per_col_filters = col.get("filters") or []
        # Some pipelines store filters as list[dict{expression}] vs list[str];
        # normalize to strings.
        per_col_strs: list[str] = []
        for f in per_col_filters:
            if isinstance(f, dict):
                expr = f.get("expression") or ""
                if expr:
                    per_col_strs.append(expr)
            elif isinstance(f, str):
                if f:
                    per_col_strs.append(f)
        merged_filters = tuple(view_filters) + tuple(per_col_strs)
        has_filters = bool(merged_filters)

        # Name -> canonical tokens.
        name_tokens = name_to_canonical_tokens(col_name, synonyms)

        include, structural = _column_should_be_term(
            col_name, name_tokens, col_type, has_filters
        )
        if not include:
            continue

        # Author notes: comment_attachment may have populated this either
        # as a list[str] field (Tool 3 in-memory shape) or as a "; "-
        # joined CSV string. Handle both.
        author_notes_raw = col.get("author_notes", []) or []
        if isinstance(author_notes_raw, str):
            author_notes = tuple(p for p in author_notes_raw.split(" | ") if p)
        else:
            author_notes = tuple(author_notes_raw)

        out.append(Term(
            view_name=view_name,
            column_name=col_name,
            raw_name=col_name,
            name_tokens=name_tokens,
            column_type=col_type,
            resolved_expression=col.get("resolved_expression") or "",
            base_tables=tuple(col.get("base_tables", []) or []),
            base_columns=tuple(col.get("base_columns", []) or []),
            filters=merged_filters,
            author_notes=author_notes,
            is_passthrough=(col_type == "passthrough"),
            has_filters=has_filters,
            name_is_structural=structural,
        ))

    return out
