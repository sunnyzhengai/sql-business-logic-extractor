"""Term Resolver — layered resolution for unknown medical/clinical terms.

Layer 1: Learned terms glossary (instant lookup)
Layer 2: Pattern recognition (suffix + verb-frame, no LLM)
Layer 3: LLM with tools (stubbed in mock — returns None)
Layer 4: Ask user (returns category=None, caller presents UI)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

DATA_DIR = Path(__file__).resolve().parent / "data"


@dataclass
class TermResolution:
    """Result of resolving an unknown term."""
    term: str
    category: Optional[str]       # diagnosis, medication, procedure, encounter, referral, billing, None
    confidence: str               # "known", "pattern", "llm", "unknown"
    source: str                   # which layer resolved it
    details: dict = field(default_factory=dict)  # extra info (tables, route, icd10, etc.)


# ---------------------------------------------------------------------------
# Layer 1: Learned terms glossary
# ---------------------------------------------------------------------------

def _load_learned_terms(path: Path | None = None) -> dict:
    path = path or DATA_DIR / "learned_terms.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _search_learned_terms(term: str, learned: dict) -> Optional[TermResolution]:
    """Check if the term (or any alias) is in the learned glossary."""
    term_lower = term.lower().strip()

    # Direct key match
    for key, entry in learned.items():
        if term_lower == key or term_lower == entry.get("term", "").lower():
            return TermResolution(
                term=term,
                category=entry["category"],
                confidence="known",
                source=f"learned_terms:{key}",
                details=entry,
            )

    # Alias match
    for key, entry in learned.items():
        aliases = [a.lower() for a in entry.get("aliases", [])]
        if term_lower in aliases:
            return TermResolution(
                term=term,
                category=entry["category"],
                confidence="known",
                source=f"learned_terms:{key} (alias)",
                details=entry,
            )

    return None


# ---------------------------------------------------------------------------
# Layer 2: Pattern recognition
# ---------------------------------------------------------------------------

# Suffix patterns → category
SUFFIX_PATTERNS = [
    # Diagnosis
    (r".*\bdisease\b", "diagnosis"),
    (r".*\bsyndrome\b", "diagnosis"),
    (r".*\bdisorder\b", "diagnosis"),
    (r".*itis\b", "diagnosis"),         # bronchitis, appendicitis
    (r".*oma\b", "diagnosis"),          # melanoma, carcinoma
    (r".*emia\b", "diagnosis"),         # anemia, septicemia
    (r".*pathy\b", "diagnosis"),        # neuropathy, retinopathy
    (r".*osis\b", "diagnosis"),         # stenosis, thrombosis
    (r".*ia\b", "diagnosis"),           # pneumonia, anemia (overlap OK)
    # Medication
    (r".*\bmg\b", "medication"),        # metformin 500mg
    (r".*\btablet\b", "medication"),
    (r".*\bcapsule\b", "medication"),
    (r".*mab\b", "medication"),         # adalimumab, trastuzumab
    (r".*nib\b", "medication"),         # imatinib
    (r".*statin\b", "medication"),      # atorvastatin
    (r".*pril\b", "medication"),        # lisinopril
    (r".*olol\b", "medication"),        # metoprolol
    (r".*sartan\b", "medication"),      # losartan
    # Procedure
    (r".*ectomy\b", "procedure"),       # appendectomy, cholecystectomy
    (r".*oscopy\b", "procedure"),       # colonoscopy, endoscopy
    (r".*plasty\b", "procedure"),       # angioplasty
    (r".*otomy\b", "procedure"),        # tracheotomy
    (r".*graphy\b", "procedure"),       # mammography, echocardiography
]

# Verb-frame context patterns (applied to surrounding text)
VERB_FRAME_PATTERNS = [
    # "patients with X" / "diagnosed with X" → diagnosis
    (r"(?:patients?|people|individuals?)\s+(?:with|who\s+have|suffering\s+from)\s+(.+)", "diagnosis"),
    (r"diagnosed\s+with\s+(.+)", "diagnosis"),
    (r"(.+)\s+patients?", "diagnosis"),       # "diabetic patients", "asthma patients"
    # "taking X" / "on X" / "prescribed X" → medication
    (r"(?:taking|on|prescribed|receiving)\s+(.+)", "medication"),
    # "had X" / "underwent X" → procedure
    (r"(?:had|underwent|received|scheduled\s+for)\s+(?:a\s+)?(.+)", "procedure"),
    # "visited X" / "seen in X" → encounter/department
    (r"(?:visited|seen\s+in|went\s+to|been\s+to)\s+(?:the\s+)?(.+)", "encounter"),
    # "referred to X" → referral
    (r"referred\s+to\s+(.+)", "referral"),
]


def _pattern_match(term: str, context: str = "") -> Optional[TermResolution]:
    """Try suffix and verb-frame pattern matching."""
    term_lower = term.lower().strip()
    full_text = (context or term).lower().strip()

    # Try suffix patterns on the term itself
    for pattern, category in SUFFIX_PATTERNS:
        if re.match(pattern, term_lower, re.IGNORECASE):
            return TermResolution(
                term=term,
                category=category,
                confidence="pattern",
                source=f"suffix:{pattern}",
            )

    # Try verb-frame patterns on the full context
    for pattern, category in VERB_FRAME_PATTERNS:
        m = re.search(pattern, full_text, re.IGNORECASE)
        if m:
            return TermResolution(
                term=term,
                category=category,
                confidence="pattern",
                source=f"verb_frame:{pattern[:30]}...",
            )

    return None


# ---------------------------------------------------------------------------
# Layer 3: LLM with tools (stubbed)
# ---------------------------------------------------------------------------

def _llm_resolve(term: str, context: str = "") -> Optional[TermResolution]:
    """Placeholder for LLM-based resolution. Returns None in mock."""
    # In production: call LLM with tools (search_glossary, search_icd10, web_search)
    # For mock: skip
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_term(
    term: str,
    context: str = "",
    learned_terms: dict | None = None,
) -> TermResolution:
    """Resolve an unknown term through the layered fallback chain.

    Parameters
    ----------
    term    : the unknown term to resolve (e.g., "Addison's disease")
    context : the full question text for verb-frame matching
    learned_terms : pre-loaded glossary dict (loads from file if None)

    Returns
    -------
    TermResolution with category (or None if all layers fail)
    """
    # Layer 1: Learned terms
    if learned_terms is None:
        learned_terms = _load_learned_terms()

    result = _search_learned_terms(term, learned_terms)
    if result:
        return result

    # Layer 2: Pattern recognition
    result = _pattern_match(term, context)
    if result:
        return result

    # Layer 3: LLM (stubbed)
    result = _llm_resolve(term, context)
    if result:
        return result

    # Layer 4: Unknown — caller should ask the user
    return TermResolution(
        term=term,
        category=None,
        confidence="unknown",
        source="all_layers_exhausted",
    )


def expand_synonyms(text: str, synonyms_path: Path | None = None) -> str:
    """Expand abbreviations/variants in text using healthcare_synonyms.yaml.

    Returns text with variants replaced by their canonical forms,
    plus original text appended (so both match in TF-IDF).
    """
    syn_path = synonyms_path or Path("data/dictionaries/healthcare_synonyms.yaml")
    if not syn_path.exists():
        return text

    with open(syn_path) as f:
        data = yaml.safe_load(f)

    # Build variant → canonical lookup
    variant_to_canonical = {}
    for entry in data.get("synonyms", []):
        canonical = entry["canonical"]
        for variant in entry.get("variants", []):
            variant_to_canonical[variant.lower()] = canonical

    # Tokenize and expand
    tokens = re.findall(r'\b\w+\b', text.lower())
    expanded_tokens = []
    for token in tokens:
        if token in variant_to_canonical:
            expanded_tokens.append(variant_to_canonical[token])
        expanded_tokens.append(token)  # keep original too

    return " ".join(expanded_tokens) + " " + text.lower()
