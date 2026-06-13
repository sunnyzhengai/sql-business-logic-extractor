"""Quantifier and date range extraction from natural language questions.

Extracts:
- Thresholds: "more than 3" → (">", 3)
- Date ranges: "last year" → ("2025-01-01", "2025-12-31")
- Both are returned as pre-fill suggestions for user confirmation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional


TODAY = date.today()


@dataclass
class QuantifierMatch:
    """An extracted numeric threshold."""
    operator: str          # ">", ">=", "<", "<=", "="
    value: int | float
    raw_text: str          # the matched text ("more than 3 times")
    position: int          # char offset in original question


@dataclass
class DateRangeMatch:
    """An extracted date range."""
    start_date: str        # ISO format
    end_date: str          # ISO format
    range_type: str        # "calendar_year", "trailing", "from_date", "specific_year"
    raw_text: str          # the matched text ("last year")
    position: int          # char offset in original question


# ---------------------------------------------------------------------------
# Quantifier extraction
# ---------------------------------------------------------------------------

QUANTIFIER_PATTERNS = [
    # "more than N" / "greater than N"
    (r"(?:more|greater)\s+than\s+(\d+)", ">"),
    # "over N"
    (r"over\s+(\d+)", ">"),
    # "at least N"
    (r"at\s+least\s+(\d+)", ">="),
    # "N or more"
    (r"(\d+)\s+or\s+more", ">="),
    # "fewer than N" / "less than N"
    (r"(?:fewer|less)\s+than\s+(\d+)", "<"),
    # "under N"
    (r"under\s+(\d+)", "<"),
    # "at most N"
    (r"at\s+most\s+(\d+)", "<="),
    # "N or fewer"
    (r"(\d+)\s+or\s+fewer", "<="),
    # "> N" / ">= N" / "< N" / "<= N" (literal operators)
    (r">=\s*(\d+)", ">="),
    (r">\s*(\d+)", ">"),
    (r"<=\s*(\d+)", "<="),
    (r"<\s*(\d+)", "<"),
    # "exactly N"
    (r"exactly\s+(\d+)", "="),
]


def extract_quantifiers(text: str) -> list[QuantifierMatch]:
    """Extract numeric thresholds from question text."""
    results = []
    text_lower = text.lower()
    for pattern, operator in QUANTIFIER_PATTERNS:
        for m in re.finditer(pattern, text_lower):
            value = int(m.group(1))
            results.append(QuantifierMatch(
                operator=operator,
                value=value,
                raw_text=m.group(0),
                position=m.start(),
            ))
    # Deduplicate by position (overlapping patterns)
    seen_positions = set()
    deduped = []
    for r in sorted(results, key=lambda x: x.position):
        if r.position not in seen_positions:
            deduped.append(r)
            seen_positions.add(r.position)
    return deduped


# ---------------------------------------------------------------------------
# Date range extraction
# ---------------------------------------------------------------------------

def _year_range(year: int) -> tuple[str, str]:
    return f"{year}-01-01", f"{year}-12-31"


def _trailing_months(n: int, ref: date = TODAY) -> tuple[str, str]:
    start = ref - timedelta(days=n * 30)  # approximate
    return start.isoformat(), ref.isoformat()


DATE_PATTERNS: list[tuple[str, str, object]] = []  # populated in function


def extract_date_ranges(text: str, reference_date: date = TODAY) -> list[DateRangeMatch]:
    """Extract date ranges from question text.

    Examples:
        "last year" → calendar year (current year - 1)
        "last 6 months" → trailing 6 months from today
        "in 2024" → calendar year 2024
        "since January" → from Jan 1 of current year to today
        "past year" → trailing 12 months
    """
    results = []
    text_lower = text.lower()

    # "last year" / "past year" → calendar year before current
    for m in re.finditer(r"\b(?:last|past|previous)\s+year\b", text_lower):
        year = reference_date.year - 1
        start, end = _year_range(year)
        results.append(DateRangeMatch(
            start_date=start, end_date=end,
            range_type="calendar_year",
            raw_text=m.group(0), position=m.start(),
        ))

    # "last N months" / "past N months"
    for m in re.finditer(r"\b(?:last|past|previous)\s+(\d+)\s+months?\b", text_lower):
        n = int(m.group(1))
        start, end = _trailing_months(n, reference_date)
        results.append(DateRangeMatch(
            start_date=start, end_date=end,
            range_type="trailing",
            raw_text=m.group(0), position=m.start(),
        ))

    # "in YYYY"
    for m in re.finditer(r"\bin\s+(20\d{2})\b", text_lower):
        year = int(m.group(1))
        start, end = _year_range(year)
        results.append(DateRangeMatch(
            start_date=start, end_date=end,
            range_type="specific_year",
            raw_text=m.group(0), position=m.start(),
        ))

    # "since MONTH" (e.g., "since January")
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    for m in re.finditer(r"\bsince\s+(\w+)\b", text_lower):
        month_name = m.group(1).lower()
        if month_name in months:
            month_num = months[month_name]
            year = reference_date.year
            # If the month is in the future, use last year
            if month_num > reference_date.month:
                year -= 1
            start = f"{year}-{month_num:02d}-01"
            results.append(DateRangeMatch(
                start_date=start, end_date=reference_date.isoformat(),
                range_type="from_date",
                raw_text=m.group(0), position=m.start(),
            ))

    # "this year"
    for m in re.finditer(r"\bthis\s+year\b", text_lower):
        start, _ = _year_range(reference_date.year)
        results.append(DateRangeMatch(
            start_date=start, end_date=reference_date.isoformat(),
            range_type="calendar_year",
            raw_text=m.group(0), position=m.start(),
        ))

    # Deduplicate by position
    seen = set()
    deduped = []
    for r in sorted(results, key=lambda x: x.position):
        if r.position not in seen:
            deduped.append(r)
            seen.add(r.position)
    return deduped


# ---------------------------------------------------------------------------
# Combined extraction
# ---------------------------------------------------------------------------

@dataclass
class ExtractionResult:
    """Combined quantifier + date extraction result."""
    quantifiers: list[QuantifierMatch]
    date_ranges: list[DateRangeMatch]

    @property
    def has_extractions(self) -> bool:
        return bool(self.quantifiers or self.date_ranges)

    def summary(self) -> str:
        parts = []
        for q in self.quantifiers:
            parts.append(f"threshold: {q.operator} {q.value} (from '{q.raw_text}')")
        for d in self.date_ranges:
            parts.append(f"dates: {d.start_date} to {d.end_date} "
                         f"({d.range_type}, from '{d.raw_text}')")
        return "; ".join(parts) if parts else "no quantifiers or dates detected"


def extract_all(text: str, reference_date: date = TODAY) -> ExtractionResult:
    """Extract both quantifiers and date ranges from question text."""
    return ExtractionResult(
        quantifiers=extract_quantifiers(text),
        date_ranges=extract_date_ranges(text, reference_date),
    )
