"""Term extraction -- the unit of governance comparison.

A `Term` is one transformed/contextualized output column from a SQL
view, with both its NAME signal (canonicalized tokens after synonym
expansion) and its LOGIC signal (resolved expression + structural
fingerprint). Terms are the input to the cross-view governance report
that surfaces:

  A. same name + same logic   -- canonical definitions to register
  B. same name + diff logic   -- divergent definitions to reconcile
  C. diff name + same logic   -- naming inconsistencies to standardize

Phases 1+2 of this module:

  Phase 1 -- synonyms.py + tokenizer.py: load the curated healthcare
             synonym dictionary; tokenize a column name into canonical
             tokens.
  Phase 2 -- term.py: Term dataclass + extractor that walks a parsed
             view and emits Terms for every column that qualifies
             (anything transformed OR with descriptive alias OR with
             filter context applied).

Phase 3 (next) builds the pairwise A/B/C bucketing report.
"""

from .synonyms import SynonymDict, load_default_synonyms
from .term import Term, extract_terms
from .tokenizer import canonicalize_tokens, name_to_canonical_tokens, tokenize

__all__ = [
    "SynonymDict",
    "Term",
    "canonicalize_tokens",
    "extract_terms",
    "load_default_synonyms",
    "name_to_canonical_tokens",
    "tokenize",
]
