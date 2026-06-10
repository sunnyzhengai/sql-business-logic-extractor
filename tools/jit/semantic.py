"""JIT Phase 2: semantic retrieval + LLM synthesis.

TF-IDF based retrieval over view descriptions and column definitions,
with an LLM synthesis step that produces grounded answers with citations.

No embedding API calls needed -- TF-IDF is computed locally from the
corpus text. The only LLM call is the final synthesis step (~3-5s).

Usage::

    from tools.jit.semantic import SemanticIndex, synthesize_answer
    idx = SemanticIndex(views)
    hits = idx.search("how is denied referral defined?", top_k=5)
    answer = synthesize_answer("how is denied referral defined?", hits, llm_client)
"""

from __future__ import annotations

import re
from typing import Optional


class SemanticIndex:
    """TF-IDF based retrieval over view descriptions and column definitions.

    Each view becomes one document combining:
      - view_name
      - business_description + technical_description
      - column names + definitions
      - filter English translations
      - key_metrics

    The TF-IDF vectorizer captures domain terms (table names, column
    names, business concepts) and cosine similarity finds relevant views
    for any natural-language query.
    """

    def __init__(self, views: list[dict]):
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity

        self._views = views
        self._view_names: list[str] = []
        self._documents: list[str] = []
        self._cosine_similarity = cosine_similarity

        for view in views:
            vname = view.get("view_name", "")
            self._view_names.append(vname)
            self._documents.append(_build_document(view))

        # Build TF-IDF matrix
        self._vectorizer = TfidfVectorizer(
            # Keep SQL-style tokens (underscores, dots) and business terms
            token_pattern=r"(?u)\b[A-Za-z_][A-Za-z0-9_.]*\b",
            max_df=0.95,       # drop terms in >95% of docs (noise)
            min_df=1,          # keep even single-occurrence terms
            ngram_range=(1, 2),  # unigrams + bigrams for phrases
            sublinear_tf=True,   # dampen term frequency
        )
        self._tfidf_matrix = self._vectorizer.fit_transform(self._documents)

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        """Find the top-k most relevant views for a query.

        Returns a list of dicts with:
          - view_name: str
          - score: float (0-1 cosine similarity)
          - view: dict (the full ViewV1 dict for context assembly)
        """
        query_vec = self._vectorizer.transform([query])
        scores = self._cosine_similarity(query_vec, self._tfidf_matrix).flatten()

        # Get top-k indices by score (descending)
        top_indices = scores.argsort()[::-1][:top_k]

        results = []
        for idx in top_indices:
            score = float(scores[idx])
            if score <= 0:
                break  # no point returning zero-similarity results
            results.append({
                "view_name": self._view_names[idx],
                "score": score,
                "view": self._views[idx],
            })
        return results


def _build_document(view: dict) -> str:
    """Build a single searchable document from a ViewV1 dict.

    Combines all textual signals into one string that TF-IDF can index.
    Business terms, table names, column names, filter translations --
    everything a user might search for.
    """
    parts = []
    vname = view.get("view_name", "")
    parts.append(vname)

    report = view.get("report") or {}
    if report.get("business_description"):
        parts.append(report["business_description"])
    if report.get("technical_description"):
        parts.append(report["technical_description"])
    if report.get("primary_purpose"):
        parts.append(report["primary_purpose"])
    for metric in report.get("key_metrics") or []:
        parts.append(metric)

    for scope in view.get("scopes") or []:
        # Table names
        for table in scope.get("reads_from_tables") or []:
            parts.append(table)
        # Column names + definitions
        for col in scope.get("columns") or []:
            name = col.get("column_name", "")
            defn = (col.get("business_description")
                    or col.get("technical_description") or "")
            if name:
                parts.append(name)
            if defn:
                parts.append(defn)
        # Filter translations
        for filt in scope.get("filters") or []:
            english = filt.get("english") or ""
            expr = filt.get("expression") or ""
            if english:
                parts.append(english)
            if expr:
                parts.append(expr)

    return " ".join(parts)


# ---------------------------------------------------------------------------
# LLM Synthesizer -- grounded answers with citations
# ---------------------------------------------------------------------------

_SYNTHESIZE_SYSTEM_PROMPT = """You answer questions about SQL views and stored procedures based ONLY on the provided context.

CRITICAL RULES:
1. GROUNDED: answer ONLY from the context below. If the answer is not in the context, say "I could not find information about that in the ingested views."
2. CITE SOURCES: every claim must reference the view it comes from using [VIEW_NAME] notation. For column-level claims use [VIEW_NAME::COLUMN_NAME].
3. COMPARE when relevant: if multiple views address the question differently, note the differences.
4. BUSINESS LANGUAGE: translate SQL terms into business language. Do not dump raw SQL.
5. FILTERS MATTER: when a view filters data (e.g., "denied referrals only"), always mention the filter -- it defines the business slice.
6. Be CONCISE but COMPLETE: 2-4 paragraphs. Don't pad with filler.

Output format:
Start with a direct answer, then supporting detail with citations, then a "Sources" section listing the views referenced."""


def synthesize_answer(question: str, retrieved_views: list[dict],
                       llm_client, table_scores: dict | None = None) -> str:
    """Produce a grounded answer with citations from retrieved views.

    Parameters
    ----------
    question        : the user's natural-language question
    retrieved_views : list of dicts from SemanticIndex.search(), each with
                      view_name, score, and the full view dict
    llm_client      : provider-neutral LLM adapter with complete_json()
    table_scores    : optional table importance dict for context

    Returns
    -------
    Markdown-formatted answer string with citations.
    """
    if not retrieved_views:
        return ("I could not find information about that in the ingested views.\n\n"
                "Try asking about a specific table, column, or view name.")

    # Build context for the LLM
    context_parts = []
    for i, hit in enumerate(retrieved_views):
        view = hit["view"]
        vname = hit["view_name"]
        score = hit["score"]
        report = view.get("report") or {}

        parts = [f"## View {i+1}: {vname} (relevance: {score:.0%})"]

        if report.get("primary_purpose"):
            parts.append(f"**Purpose:** {report['primary_purpose']}")
        if report.get("business_description"):
            parts.append(f"**Description:** {report['business_description']}")

        # Tables with importance
        tables = set()
        for scope in view.get("scopes") or []:
            for t in scope.get("reads_from_tables") or []:
                bare = t.split(".")[-1].strip()
                if bare and ":" not in bare:
                    tables.add(bare)
        if tables:
            table_lines = []
            for t in sorted(tables):
                if table_scores:
                    sc, role = table_scores.get(t.upper(), (0.0, ""))
                    table_lines.append(f"{t} ({role})" if role else t)
                else:
                    table_lines.append(t)
            parts.append(f"**Tables:** {', '.join(table_lines)}")

        # Columns (main scope only, cap at 10)
        main_cols = []
        for scope in view.get("scopes") or []:
            if scope.get("id") == "main":
                for col in scope.get("columns") or []:
                    name = col.get("column_name", "")
                    defn = (col.get("business_description")
                            or col.get("technical_description") or "")
                    main_cols.append(f"- {name}: {defn}" if defn else f"- {name}")
        if main_cols:
            parts.append("**Columns:**")
            parts.extend(main_cols[:10])
            if len(main_cols) > 10:
                parts.append(f"- ... and {len(main_cols) - 10} more")

        # Filters
        filter_lines = []
        for scope in view.get("scopes") or []:
            for filt in scope.get("filters") or []:
                english = filt.get("english") or filt.get("expression") or ""
                if english:
                    filter_lines.append(f"- {english}")
        if filter_lines:
            parts.append("**Filters:**")
            parts.extend(filter_lines[:5])

        context_parts.append("\n".join(parts))

    context = "\n\n---\n\n".join(context_parts)
    user_prompt = (
        f"Question: {question}\n\n"
        f"Context (retrieved views, ranked by relevance):\n\n{context}"
    )

    try:
        result = llm_client.complete_json(
            _SYNTHESIZE_SYSTEM_PROMPT,
            user_prompt + '\n\nOutput JSON: {"answer": "your markdown answer with citations"}',
        )
        return (result.get("answer") or "").strip()
    except Exception as e:
        # Fall back to a simple context dump if LLM fails
        fallback = [f"*LLM synthesis failed ({type(e).__name__}). Showing raw results:*\n"]
        for hit in retrieved_views:
            report = hit["view"].get("report") or {}
            fallback.append(f"### {hit['view_name']} ({hit['score']:.0%} match)")
            fallback.append(report.get("business_description", "(no description)"))
            fallback.append("")
        return "\n".join(fallback)
