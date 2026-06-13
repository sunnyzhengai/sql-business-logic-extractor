"""Report Searcher — TF-IDF search over the Report Glossary.

Level 1 of the cascade. Finds existing reports that might answer
the user's question (possibly with parameter changes).
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


@dataclass
class ReportHit:
    """A matching report from the glossary."""
    report_name: str
    score: float
    description: str
    primary_purpose: str
    tables_used: list[str]
    domains: list[str]
    parameters: list[dict]
    inline_comments: list[str]
    sql_path: str
    full_entry: dict = field(repr=False)


class ReportSearcher:
    """TF-IDF index over Report Glossary entries."""

    def __init__(self, reports: list[dict] | None = None,
                 glossary_dir: Path | None = None):
        if reports is None:
            gdir = glossary_dir or DATA_DIR / "report_glossary"
            reports = []
            for yf in sorted(gdir.glob("*.yaml")):
                with open(yf) as f:
                    reports.append(yaml.safe_load(f))

        self.reports = reports
        self._build_index()

    def _build_document(self, report: dict) -> str:
        """Build a searchable text document from a report entry.

        High-signal fields (report_name, primary_purpose) are repeated
        to boost their TF-IDF weight over lower-signal fields.
        """
        # Humanize report name: VW_DIABETIC_COHORT → "diabetic cohort"
        name_human = (report.get("report_name", "")
                      .replace("VW_", "").replace("_", " ").lower())
        purpose = report.get("primary_purpose", "")

        # High-signal: repeat 3x
        high = f"{name_human} {name_human} {name_human} {purpose} {purpose} {purpose}"

        # Medium-signal
        medium = " ".join([
            report.get("description", ""),
            " ".join(report.get("key_metrics", [])),
            " ".join(report.get("domains", [])),
        ])

        # Low-signal
        low = " ".join([
            " ".join(report.get("tables_used", [])),
            " ".join(report.get("inline_comments", [])),
        ])
        for p in report.get("parameters", []):
            low += " " + p.get("description", "") + " " + p.get("name", "")

        text = f"{high} {medium} {low}"
        return expand_synonyms(text)

    def _build_index(self):
        """Build TF-IDF vectors for all reports."""
        self.documents = [self._build_document(r) for r in self.reports]
        self.vectorizer = TfidfVectorizer(
            stop_words="english",
            max_features=5000,
            ngram_range=(1, 2),
        )
        self.tfidf_matrix = self.vectorizer.fit_transform(self.documents)

    def search(self, question: str, top_k: int = 5,
               min_score: float = 0.05) -> list[ReportHit]:
        """Search for reports matching the question.

        Uses TF-IDF cosine similarity + a purpose-alignment bonus.
        The bonus rewards reports whose primary_purpose directly
        overlaps with the query (word-level Jaccard).
        """
        query_expanded = expand_synonyms(question)
        query_vec = self.vectorizer.transform([query_expanded])
        tfidf_scores = cosine_similarity(query_vec, self.tfidf_matrix)[0]

        query_tokens = set(question.lower().split())

        hits = []
        for i, tfidf_score in enumerate(tfidf_scores):
            r = self.reports[i]

            # Purpose-alignment bonus: Jaccard overlap of query tokens
            # with primary_purpose tokens. Rewards direct concept match.
            purpose_tokens = set(r.get("primary_purpose", "").lower().split())
            name_tokens = set(r.get("report_name", "").replace("VW_", "")
                              .replace("_", " ").lower().split())
            combined = purpose_tokens | name_tokens

            if combined and query_tokens:
                overlap = query_tokens & combined
                jaccard = len(overlap) / len(query_tokens | combined)
                bonus = jaccard * 0.3  # up to 0.3 bonus
            else:
                bonus = 0.0

            final_score = float(tfidf_score) + bonus

            if final_score >= min_score:
                hits.append(ReportHit(
                    report_name=r["report_name"],
                    score=final_score,
                    description=r.get("description", ""),
                    primary_purpose=r.get("primary_purpose", ""),
                    tables_used=r.get("tables_used", []),
                    domains=r.get("domains", []),
                    parameters=r.get("parameters", []),
                    inline_comments=r.get("inline_comments", []),
                    sql_path=r.get("source_sql_path", ""),
                    full_entry=r,
                ))

        hits.sort(key=lambda h: h.score, reverse=True)
        return hits[:top_k]

    def get_report_sql(self, report_name: str,
                       sql_dir: Path | None = None) -> Optional[str]:
        """Load the SQL for a specific report."""
        sql_dir = sql_dir or DATA_DIR / "report_sql"
        sql_path = sql_dir / f"{report_name}.sql"
        if sql_path.exists():
            return sql_path.read_text()
        return None
