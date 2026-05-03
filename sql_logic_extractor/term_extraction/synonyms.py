"""Loads the healthcare synonym dictionary and provides canonical-form
expansion for raw tokens.

The dictionary is HITL-curated YAML at data/dictionaries/healthcare_synonyms.yaml.
This module exists to:
  - Read the YAML once at startup.
  - Provide a `SynonymDict` object with one cheap method: `expand(token)`
    -> canonical form (or the input unchanged if no entry exists).
  - Cache the default-dictionary load globally so callers don't reload.

Format of the YAML:

    synonyms:
      - canonical: pregnant
        variants: [preg, pregnancy, gestational, ...]
      - canonical: patient
        variants: [pt, member, ...]

The expand() lookup is case-insensitive on input. The CANONICAL form
returned is exactly as written in the YAML (we preserve the curator's
chosen casing for downstream readability, though everything is
lowercased internally for matching).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import yaml


_DEFAULT_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "dictionaries" / "healthcare_synonyms.yaml"
)


@dataclass(frozen=True)
class SynonymDict:
    """Bidirectional lookup: variant -> canonical.

    Frozen so callers can pass it through pipelines without worrying
    about mutation. Construct via `load_default_synonyms()` or
    `load_synonyms_from_yaml(path)`.
    """
    # Lowercased variant -> lowercased canonical
    _variant_to_canonical: dict[str, str] = field(default_factory=dict)
    # Lowercased canonical -> set of lowercased variants (incl. canonical itself)
    _canonical_to_variants: dict[str, frozenset[str]] = field(default_factory=dict)

    def expand(self, token: str) -> str:
        """Return the canonical form of `token`, or `token` lowercased
        unchanged if no entry exists. Case-insensitive."""
        if not token:
            return token
        return self._variant_to_canonical.get(token.lower(), token.lower())

    def is_known(self, token: str) -> bool:
        """True iff `token` matches any canonical or variant in the dict."""
        return token.lower() in self._variant_to_canonical

    def canonicals(self) -> frozenset[str]:
        """All canonical forms in the dictionary."""
        return frozenset(self._canonical_to_variants.keys())

    def variants_of(self, canonical: str) -> frozenset[str]:
        """All variants (incl. the canonical itself) that map to a canonical."""
        return self._canonical_to_variants.get(canonical.lower(), frozenset())


def load_synonyms_from_yaml(path: str | Path) -> SynonymDict:
    """Build a SynonymDict from a YAML file. Raises on malformed input.

    The YAML must have a top-level `synonyms:` key whose value is a
    list of `{canonical, variants}` records. Empty / missing variants
    lists are tolerated (canonical maps to itself only).
    """
    p = Path(path)
    with p.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    entries = data.get("synonyms", [])
    if not isinstance(entries, list):
        raise ValueError(f"{p}: top-level 'synonyms' must be a list")

    v2c: dict[str, str] = {}
    c2v: dict[str, set[str]] = {}

    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"{p}: entry {i} is not a dict")
        canonical = entry.get("canonical")
        if not canonical or not isinstance(canonical, str):
            raise ValueError(f"{p}: entry {i} missing valid 'canonical'")
        canonical_lower = canonical.lower()
        # Canonical maps to itself.
        v2c[canonical_lower] = canonical_lower
        c2v.setdefault(canonical_lower, set()).add(canonical_lower)
        for variant in entry.get("variants", []) or []:
            if not isinstance(variant, str):
                raise ValueError(f"{p}: entry {i} variant must be string")
            variant_lower = variant.lower()
            v2c[variant_lower] = canonical_lower
            c2v[canonical_lower].add(variant_lower)

    return SynonymDict(
        _variant_to_canonical=v2c,
        _canonical_to_variants={k: frozenset(v) for k, v in c2v.items()},
    )


@lru_cache(maxsize=1)
def load_default_synonyms() -> SynonymDict:
    """Load and cache the project's default synonym dictionary."""
    return load_synonyms_from_yaml(_DEFAULT_PATH)
