"""Technical Searcher — domain/table search over the Technical Glossary.

Level 3 of the cascade. Used when no report or definition matches.
Suggests domains → anchor tables → satellite tables for building
queries from scratch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from tools.jit.term_resolver import resolve_term, expand_synonyms

DATA_DIR = Path(__file__).resolve().parent / "data"


@dataclass
class DomainMatch:
    """A matching domain from the technical glossary."""
    domain_name: str
    description: str
    score: float                 # simple keyword match score
    anchor_tables: list[dict]
    relevance: str               # why this domain matched


@dataclass
class RouteMatch:
    """A matching route from the route catalog."""
    category: str
    route_name: str
    path: list[str]
    description: str
    filter_column: str


class TechnicalSearcher:
    """Search over the Technical Glossary for domain/table suggestions."""

    def __init__(self, tech_glossary: dict | None = None,
                 route_catalog: dict | None = None,
                 learned_terms: dict | None = None):
        if tech_glossary is None:
            with open(DATA_DIR / "technical_glossary.yaml") as f:
                tech_glossary = yaml.safe_load(f)
        if route_catalog is None:
            with open(DATA_DIR / "route_catalog.yaml") as f:
                route_catalog = yaml.safe_load(f)
        if learned_terms is None:
            terms_path = DATA_DIR / "learned_terms.yaml"
            if terms_path.exists():
                with open(terms_path) as f:
                    learned_terms = yaml.safe_load(f) or {}
            else:
                learned_terms = {}

        self.glossary = tech_glossary
        self.routes = route_catalog
        self.learned_terms = learned_terms

    def search_domains(self, question: str,
                       category_hint: str | None = None) -> list[DomainMatch]:
        """Find relevant domains by keyword matching.

        Parameters
        ----------
        question       : the user's question text
        category_hint  : if term resolver already identified a category
                         (e.g., "diagnosis"), prefer that domain
        """
        question_lower = question.lower()
        tokens = set(question_lower.split())

        matches = []
        for domain_name, domain in self.glossary.get("domains", {}).items():
            score = 0.0
            reasons = []

            # Direct domain name match
            if domain_name in question_lower:
                score += 0.5
                reasons.append(f"domain name '{domain_name}' in question")

            # Category hint match
            if category_hint and category_hint.lower() == domain_name:
                score += 0.8
                reasons.append(f"term resolver category = {category_hint}")

            # Domain description keyword overlap
            desc_tokens = set(domain.get("description", "").lower().split())
            overlap = tokens & desc_tokens
            if overlap:
                score += len(overlap) * 0.1
                reasons.append(f"keywords: {', '.join(overlap)}")

            # Anchor table name match
            for anchor in domain.get("anchor_tables", []):
                anchor_name = anchor.get("name", "").lower()
                if anchor_name in question_lower:
                    score += 0.4
                    reasons.append(f"table '{anchor['name']}' in question")

                # Anchor description overlap
                anchor_desc_tokens = set(anchor.get("description", "").lower().split())
                a_overlap = tokens & anchor_desc_tokens
                if a_overlap:
                    score += len(a_overlap) * 0.05
                    reasons.append(f"table desc keywords: {', '.join(a_overlap)}")

            if score > 0:
                matches.append(DomainMatch(
                    domain_name=domain_name,
                    description=domain.get("description", ""),
                    score=score,
                    anchor_tables=domain.get("anchor_tables", []),
                    relevance="; ".join(reasons),
                ))

        matches.sort(key=lambda m: m.score, reverse=True)
        return matches

    def get_routes_for_category(self, category: str) -> list[RouteMatch]:
        """Get known routes for a category (diagnosis, medication, etc.)."""
        cat_data = self.routes.get(category)
        if not cat_data:
            return []

        return [
            RouteMatch(
                category=category,
                route_name=r["name"],
                path=r["path"],
                description=r["description"],
                filter_column=r.get("filter_column", ""),
            )
            for r in cat_data.get("routes", [])
        ]

    def get_all_routes(self) -> dict[str, list[RouteMatch]]:
        """Get all routes organized by category."""
        result = {}
        for category in self.routes:
            routes = self.get_routes_for_category(category)
            if routes:
                result[category] = routes
        return result

    def get_dimensions(self) -> list[dict]:
        """Get domain-neutral dimension tables."""
        return self.glossary.get("dimensions", [])

    def suggest_for_unknown_term(self, term: str,
                                 context: str = "") -> dict:
        """Full suggestion for an unknown term.

        1. Run term resolver to get category
        2. If category found, return routes for that category
        3. If not, return all domains ranked by question relevance

        Returns dict with:
            resolution: TermResolution
            routes: list[RouteMatch] (if category known)
            domains: list[DomainMatch] (if category unknown)
        """
        resolution = resolve_term(term, context, self.learned_terms)

        if resolution.category:
            routes = self.get_routes_for_category(resolution.category)
            if not routes:
                # Try broader domain search
                domains = self.search_domains(context or term,
                                              category_hint=resolution.category)
                return {
                    "resolution": resolution,
                    "routes": [],
                    "domains": domains,
                }
            return {
                "resolution": resolution,
                "routes": routes,
                "domains": [],
            }

        # Category unknown — search domains
        domains = self.search_domains(context or term)
        return {
            "resolution": resolution,
            "routes": [],
            "domains": domains,
        }
