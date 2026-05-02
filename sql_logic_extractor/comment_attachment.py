"""Comment-to-AST binding + downstream surfacing helpers.

Two operations on top of comments.extract_comments:

  attach_to_columns(sql, columns):
    For each column dict, set 'author_notes' = list of comment strings
    that the parser attached to the column's projection node. Uses
    sqlglot's native Expression.comments attachment (the lexer attaches
    each comment to the nearest token; the parser preserves it on the
    resulting AST node).

  extract_view_level_notes(sql):
    Comments that appear BEFORE the first WITH/SELECT keyword. These
    describe the view as a whole (top-of-file headers, doc blocks)
    rather than any specific column. Tool 4 surfaces these in
    business_description so the author's voice goes first.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from .comments import extract_comments


def extract_view_level_notes(sql: str) -> list[str]:
    """Return author comments that appear ABOVE the first WITH/SELECT.

    These are typically header doc blocks describing the view's purpose,
    section separators above the body, or audit lines (Author / Date /
    Revision). For Tool 4's business_description the author's own words
    are higher fidelity than any engineered template -- prepend them.
    """
    _, comments = extract_comments(sql)
    if not comments:
        return []

    # Find the first body-starting keyword OUTSIDE comments / strings.
    # extract_comments() above already returned the comment-stripped SQL
    # in its first return value, but we discarded it. Re-extract.
    stripped, _ = extract_comments(sql)
    upper = stripped.upper()
    body_start_line: int | None = None
    for keyword in ("WITH", "SELECT"):
        # Word-boundary match so we don't hit columns named WITH_X / SELECT_Y.
        idx = -1
        for i in range(len(upper)):
            if upper[i:i + len(keyword)] == keyword:
                # Check word boundaries.
                left_ok = i == 0 or not upper[i - 1].isalnum() and upper[i - 1] != "_"
                right_ok = (i + len(keyword) == len(upper)
                              or not upper[i + len(keyword)].isalnum()
                              and upper[i + len(keyword)] != "_")
                if left_ok and right_ok:
                    idx = i
                    break
        if idx >= 0:
            line = stripped[:idx].count("\n") + 1
            if body_start_line is None or line < body_start_line:
                body_start_line = line

    if body_start_line is None:
        # No body keyword found -- return all comments.
        return [c.text for c in comments if c.text]

    return [c.text for c in comments if c.line <= body_start_line and c.text]


def attach_to_columns(sql: str, columns: list[dict],
                       dialect: str = "tsql") -> list[dict]:
    """Mutate `columns` in place: each dict gets `author_notes: list[str]`
    populated from comments sqlglot attached to that column's projection
    node. Returns the same list for fluent use.

    If the SQL doesn't parse, every column still gets `author_notes = []`
    -- the field is always present, never KeyError.
    """
    # Default: empty notes on every column.
    for c in columns:
        c.setdefault("author_notes", [])

    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        return columns

    notes_by_alias: dict[str, list[str]] = {}
    for select in tree.find_all(exp.Select):
        for proj in select.expressions:
            alias = (proj.alias_or_name or "").upper()
            if not alias:
                continue
            collected: list[str] = []
            # Comments directly on the projection node.
            if proj.comments:
                collected.extend(proj.comments)
            # And on every descendant (catches /* label */ on inner literals).
            for descendant in proj.walk():
                if descendant is proj:
                    continue
                node_comments = getattr(descendant, "comments", None)
                if node_comments:
                    collected.extend(node_comments)
            if collected:
                # Dedupe while preserving order.
                seen: set[str] = set()
                deduped = []
                for c in collected:
                    if c not in seen:
                        seen.add(c)
                        deduped.append(c.strip())
                notes_by_alias[alias] = [n for n in deduped if n]

    for col in columns:
        name = (col.get("column_name") or col.get("name") or "").upper()
        if name in notes_by_alias:
            col["author_notes"] = notes_by_alias[name]

    return columns
