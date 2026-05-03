"""Tokenize a column / view name into canonical tokens for similarity.

Pipeline:
    raw name
      -> split on CamelCase boundaries + underscores + spaces + dots/slashes
      -> lowercase
      -> drop stop tokens (structural / SQL-only words like _YN, _C, ID, FLAG)
      -> expand each surviving token via the synonym dict
      -> deduplicate to a frozenset

The result is a small canonical token bag suitable for Jaccard similarity
against another column's bag.

Examples (with default synonym dict):

    name_to_canonical_tokens("IS_PREGNANT_YN")     -> {"pregnant"}
    name_to_canonical_tokens("PregnancyFlag")      -> {"pregnant"}
    name_to_canonical_tokens("[Pregnant Patient]") -> {"pregnant", "patient"}
    name_to_canonical_tokens("PT_DX_DATE")         -> {"patient", "diagnosis", "date"}
    name_to_canonical_tokens("PAT_ID")             -> {"patient"}    # id stripped
    name_to_canonical_tokens("ROW_NUM")            -> set()          # all stop
"""

from __future__ import annotations

import re

from .synonyms import SynonymDict, load_default_synonyms


# Words that carry no business meaning -- structural / SQL-only.
# Removed from token bags before similarity comparison so noise doesn't
# inflate Jaccard scores.
STOP_TOKENS: frozenset[str] = frozenset({
    # Identity / structural
    "id", "key", "num", "no", "row", "line", "seq", "ord",
    # Boolean-flag suffixes
    "yn", "flag", "ind", "indicator", "is", "has",
    # Clarity code-suffix
    "c",
    # Generic articles / prepositions
    "a", "an", "the", "of", "in", "on", "at", "by", "for", "to",
    # SQL-table-context noise
    "table", "view", "column", "value", "val",
})


# CamelCase boundary: lowercase letter followed by uppercase. Also splits
# acronym->word boundaries: HTTPRequest -> HTTP Request.
_CAMEL_BOUNDARY_RE = re.compile(
    r"(?<=[a-z0-9])(?=[A-Z])"        # lowercase->uppercase
    r"|(?<=[A-Z])(?=[A-Z][a-z])"     # acronym->word
)

# Other split points: anything that's not a letter or digit.
_NON_ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")


def tokenize(name: str) -> list[str]:
    """Split a raw name into lowercase tokens. Does NOT drop stop words
    or apply synonyms -- that's `canonicalize_tokens`'s job. This stage
    is purely structural splitting.
    """
    if not name:
        return []
    # First pass: split on CamelCase boundaries (preserving the chars).
    s = _CAMEL_BOUNDARY_RE.sub(" ", name)
    # Second pass: replace all non-alphanumerics with spaces, then split.
    s = _NON_ALNUM_RE.sub(" ", s)
    return [t.lower() for t in s.split() if t]


def canonicalize_tokens(
    tokens: list[str],
    synonyms: SynonymDict | None = None,
    *,
    drop_stop: bool = True,
) -> frozenset[str]:
    """Filter stop tokens and expand each survivor via the synonym dict.

    Returns a frozenset (deduplicated, hashable, suitable as a dict key
    for clustering by token bag).
    """
    if synonyms is None:
        synonyms = load_default_synonyms()
    out: set[str] = set()
    for tok in tokens:
        if not tok:
            continue
        if drop_stop and tok in STOP_TOKENS:
            continue
        # Single-character non-alpha (numeric) tokens carry no meaning.
        if len(tok) == 1 and not tok.isalpha():
            continue
        out.add(synonyms.expand(tok))
    return frozenset(out)


def name_to_canonical_tokens(
    name: str,
    synonyms: SynonymDict | None = None,
) -> frozenset[str]:
    """End-to-end: raw column/view name -> canonical token bag.

    This is the function callers in the term extractor and the
    governance-bucketing report use to compute name similarity.
    """
    return canonicalize_tokens(tokenize(name), synonyms)
