"""Definition Searcher — TF-IDF search over the Business Definition Glossary.

Level 2 of the cascade. Finds reusable building blocks that can be
combined to answer the user's question. Includes equivalence grouping
and strong/weak classification.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from tools.jit.term_resolver import expand_synonyms

DATA_DIR = Path(__file__).resolve().parent / "data"

STRONG_THRESHOLD = 0.15
WEAK_THRESHOLD = 0.05


@dataclass
class DefinitionHit:
    """A matching definition from the glossary."""
    definition_name: str
    label: str
    score: float
    strength: str               # "strong" or "weak"
    domain: str
    description: str
    tables: list[str]
    anchor_table: str
    characteristic_filters: list[dict]
    parameters: list[dict]
    source_scopes: list[str]
    sql_template: str
    validation_count: int
    usage_count: int
    full_entry: dict = field(repr=False)


@dataclass
class DefinitionGroup:
    """Group of equivalent definitions (same concept, different source)."""
    group_label: str
    definitions: list[DefinitionHit]
    is_equivalent: bool          # True if definitions are equivalent
    equivalence_reason: str      # e.g., "same tables + similar filters"


class DefinitionSearcher:
    """TF-IDF index over Business Definition Glossary entries."""

    def __init__(self, definitions: list[dict] | None = None,
                 glossary_dir: Path | None = None):
        if definitions is None:
            gdir = glossary_dir or DATA_DIR / "definition_glossary"
            definitions = []
            for yf in sorted(gdir.glob("*.yaml")):
                with open(yf) as f:
                    definitions.append(yaml.safe_load(f))

        self.definitions = definitions
        self._build_index()

    def _build_document(self, defn: dict) -> str:
        """Build searchable text from a definition entry.

        High-signal fields (label, filter english) are boosted.
        """
        bb = defn.get("backbone", {})

        # Humanize definition name
        name_human = defn.get("definition_name", "").replace("_", " ")
        label = defn.get("label", "")

        # High-signal: label, filter english, scope names — repeat 3x
        high_parts = [label, label, label, name_human, name_human]
        for f in bb.get("characteristic_filters", []):
            eng = f.get("english", "")
            high_parts.extend([eng, eng])
        for scope in defn.get("source_scopes", []):
            if "::" in scope:
                scope_name = scope.split("::")[-1].replace("cte:", "").replace("_", " ")
                high_parts.append(scope_name)

        # Medium-signal: description, domain
        medium_parts = [
            defn.get("description", ""),
            defn.get("domain", ""),
        ]

        # Low-signal: tables, expressions, parameters
        low_parts = [
            " ".join(bb.get("tables", [])),
            bb.get("anchor_table", ""),
        ]
        for f in bb.get("characteristic_filters", []):
            low_parts.append(f.get("expression", ""))
        for p in defn.get("parameters", []):
            low_parts.append(p.get("description", ""))

        text = " ".join(high_parts) + " " + " ".join(medium_parts) + " " + " ".join(low_parts)
        return expand_synonyms(text)

    def _build_index(self):
        """Build TF-IDF vectors for all definitions."""
        self.documents = [self._build_document(d) for d in self.definitions]
        self.vectorizer = TfidfVectorizer(
            stop_words="english",
            max_features=5000,
            ngram_range=(1, 2),
        )
        self.tfidf_matrix = self.vectorizer.fit_transform(self.documents)

    def search(self, question: str, top_k: int = 10,
               min_score: float = WEAK_THRESHOLD) -> list[DefinitionHit]:
        """Search for definitions matching the question.

        Returns ranked list of DefinitionHit, highest score first.
        """
        query_expanded = expand_synonyms(question)
        query_vec = self.vectorizer.transform([query_expanded])
        scores = cosine_similarity(query_vec, self.tfidf_matrix)[0]

        hits = []
        for i, score in enumerate(scores):
            if score >= min_score:
                d = self.definitions[i]
                bb = d.get("backbone", {})
                strength = "strong" if score >= STRONG_THRESHOLD else "weak"
                hits.append(DefinitionHit(
                    definition_name=d["definition_name"],
                    label=d.get("label", ""),
                    score=float(score),
                    strength=strength,
                    domain=d.get("domain", ""),
                    description=d.get("description", ""),
                    tables=bb.get("tables", []),
                    anchor_table=bb.get("anchor_table", ""),
                    characteristic_filters=bb.get("characteristic_filters", []),
                    parameters=d.get("parameters", []),
                    source_scopes=d.get("source_scopes", []),
                    sql_template=d.get("sql_template", ""),
                    validation_count=d.get("validation_count", 0),
                    usage_count=d.get("usage_count", 0),
                    full_entry=d,
                ))

        hits.sort(key=lambda h: h.score, reverse=True)
        return hits[:top_k]

    def search_grouped(self, question: str, top_k: int = 10,
                       min_score: float = WEAK_THRESHOLD) -> list[DefinitionGroup]:
        """Search and group by equivalence.

        Two definitions are equivalent if:
        - Same set of backbone tables
        - >70% overlap in filter expressions (token-level Jaccard)
        """
        hits = self.search(question, top_k=top_k * 2, min_score=min_score)
        if not hits:
            return []

        groups: list[DefinitionGroup] = []
        used = set()

        for hit in hits:
            if hit.definition_name in used:
                continue

            # Find equivalents
            equivalents = [hit]
            used.add(hit.definition_name)

            for other in hits:
                if other.definition_name in used:
                    continue
                if self._are_equivalent(hit, other):
                    equivalents.append(other)
                    used.add(other.definition_name)

            is_eq = len(equivalents) > 1
            reason = ""
            if is_eq:
                reason = f"same tables ({', '.join(hit.tables)}) + similar filters"

            groups.append(DefinitionGroup(
                group_label=hit.label,
                definitions=equivalents,
                is_equivalent=is_eq,
                equivalence_reason=reason,
            ))

        return groups[:top_k]

    def _are_equivalent(self, a: DefinitionHit, b: DefinitionHit) -> bool:
        """Check if two definitions define the same concept."""
        # Same backbone tables?
        if set(a.tables) != set(b.tables):
            return False

        # Filter expression overlap (token-level Jaccard)
        a_tokens = set()
        for f in a.characteristic_filters:
            a_tokens.update(f.get("expression", "").lower().split())
        b_tokens = set()
        for f in b.characteristic_filters:
            b_tokens.update(f.get("expression", "").lower().split())

        if not a_tokens and not b_tokens:
            return True
        if not a_tokens or not b_tokens:
            return False

        intersection = a_tokens & b_tokens
        union = a_tokens | b_tokens
        jaccard = len(intersection) / len(union)
        return jaccard > 0.7
