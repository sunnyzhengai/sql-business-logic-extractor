"""Router — intent classifier and cascade controller.

Classifies question intent and orchestrates the L1→L2→L3→L4 cascade.
Each level returns results; the user decides whether to accept or escalate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class IntentClassification:
    """Result of classifying a question's intent."""
    intent: str         # "report", "data", "concept", "structural"
    confidence: float   # 0-1
    signals: list[str]  # what triggered this classification


# ---------------------------------------------------------------------------
# Intent detection patterns
# ---------------------------------------------------------------------------

DATA_SIGNALS = [
    r"\bhow\s+many\b",
    r"\bwhat\s+percent",
    r"\bwhat\s+percentage\b",
    r"\bcount\s+of\b",
    r"\bhow\s+much\b",
    r"\btotal\s+number\b",
    r"\bwhat\s+is\s+the\s+rate\b",
    r"\bwhat\s+proportion\b",
    r"\bgive\s+me\s+the\s+number\b",
    r"\bshow\s+me\s+(?:the\s+)?(?:number|count|total)\b",
]

STRUCTURAL_SIGNALS = [
    r"\bwhat\s+does\s+\w+\s+do\b",
    r"\bwhat\s+tables?\s+does\b",
    r"\bwhich\s+(?:views?|reports?)\s+(?:use|contain|have)\b",
    r"\bdescribe\s+",
    r"\bexplain\s+",
    r"\bshow\s+me\s+(?:the\s+)?(?:report|view|definition)\b",
    r"\bwhat\s+is\s+(?:in\s+)?VW_",
]

REPORT_SIGNALS = [
    r"\bshow\s+me\s+(?:the\s+)?(?:\w+\s+)?report\b",
    r"\brun\s+(?:the\s+)?(?:\w+\s+)?report\b",
    r"\bis\s+there\s+a\s+report\s+(?:for|about|that)\b",
    r"\bexisting\s+report\b",
    r"\bVW_\w+",
]


def classify_intent(question: str,
                    known_view_names: set[str] | None = None,
                    known_table_names: set[str] | None = None) -> IntentClassification:
    """Classify the question's intent.

    Returns one of:
    - "report"     : user is asking about/for a specific report
    - "data"       : user wants quantitative data (counts, percentages)
    - "structural" : user wants to understand structure (tables, columns, logic)
    - "concept"    : user is asking about a business concept (neither data nor structural)
    """
    question_lower = question.lower()
    question_upper = question.upper()
    signals = []

    # Check for known view/table names
    if known_view_names:
        for vn in known_view_names:
            if vn.upper() in question_upper:
                signals.append(f"mentions view: {vn}")

    if known_table_names:
        for tn in known_table_names:
            if tn.upper() in question_upper and len(tn) > 3:  # skip short names
                signals.append(f"mentions table: {tn}")

    # Check report signals
    report_score = 0
    for pattern in REPORT_SIGNALS:
        if re.search(pattern, question_lower):
            report_score += 1
            signals.append(f"report signal: {pattern[:30]}")

    # Check data signals
    data_score = 0
    for pattern in DATA_SIGNALS:
        if re.search(pattern, question_lower):
            data_score += 1
            signals.append(f"data signal: {pattern[:30]}")

    # Check structural signals
    structural_score = 0
    for pattern in STRUCTURAL_SIGNALS:
        if re.search(pattern, question_lower):
            structural_score += 1
            signals.append(f"structural signal: {pattern[:30]}")

    # Decision logic
    if report_score > 0 and data_score == 0 and structural_score == 0:
        return IntentClassification("report", min(report_score * 0.3, 1.0), signals)

    if structural_score > 0 and data_score == 0:
        return IntentClassification("structural", min(structural_score * 0.3, 1.0), signals)

    if data_score > 0:
        return IntentClassification("data", min(data_score * 0.3, 1.0), signals)

    # Default: concept (business question without quantitative/structural signals)
    if not signals:
        signals.append("no specific signals detected → default to concept")
    return IntentClassification("concept", 0.3, signals)


@dataclass
class CascadeResult:
    """Result from one level of the cascade."""
    level: int                   # 1, 2, 3, or 4
    level_name: str              # "report", "definition", "technical", "build"
    matches: list                # ReportHit, DefinitionHit, DomainMatch, etc.
    has_matches: bool
    user_action: Optional[str] = None  # set by UI: "accept", "escalate", "handoff"


@dataclass
class CascadeState:
    """Tracks the full cascade state across levels."""
    question: str
    intent: IntentClassification
    levels: list[CascadeResult] = field(default_factory=list)
    current_level: int = 1
    final_answer: Optional[str] = None
    is_complete: bool = False

    @property
    def current_level_name(self) -> str:
        names = {1: "report", 2: "definition", 3: "technical", 4: "build"}
        return names.get(self.current_level, "unknown")

    def escalate(self):
        """Move to the next level."""
        if self.current_level < 4:
            self.current_level += 1
        else:
            self.is_complete = True
            self.final_answer = "handoff_to_human"

    def accept(self, answer: str):
        """Mark cascade as complete with an answer."""
        self.is_complete = True
        self.final_answer = answer
