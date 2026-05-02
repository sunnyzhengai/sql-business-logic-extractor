"""SQL comment extraction + intent classification.

Comments in source SQL carry the author's native-language semantics --
section headers, inline labels (`STATUS_C = 5 /* Denied */`), CTE docs,
audit lines, TODOs. We extract them as first-class entities BEFORE
preprocessing strips whitespace, classify their intent, and (in later
steps) bind them back to AST nodes so downstream tools (Tool 3 English
definitions, Tool 4 business descriptions) can surface them.

This module ships step 1 of the comment-as-data pipeline:

  - `Comment` dataclass: the entity with position + kind + intent
  - `extract_comments(sql)` -> (stripped_sql, [Comment, ...])
    Replaces each comment with whitespace of EQUAL byte length so
    line/col offsets in the stripped SQL match the source -- critical
    for re-anchoring comments to AST nodes after parsing succeeds.
  - `classify_intent(comment)` -> assigns one of: label, doc,
    section_header, audit, todo, unclassified

Steps 2-4 (intent-aware binding to AST, propagation through resolver,
surfacing in Tool 3/4) build on this foundation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


CommentKind = Literal["line", "block"]
CommentIntent = Literal[
    "label",            # short, adjacent to a literal: /* Denied */, /* Managed Care */
    "doc",              # multi-line block: paragraph documenting a CTE / section
    "section_header",   # separator line: -- ====== Adjustment Overpayment claims ======
    "audit",            # Author: / Modified Date: / Revision: lines
    "todo",             # TODO / FIXME / HACK / XXX
    "unclassified",     # anything else
]


@dataclass(frozen=True)
class Comment:
    """One SQL comment in the source.

    text:    the comment body WITHOUT delimiters (`-- foo` -> `foo`)
    line:    1-based source line of the comment's opening character
    col:     1-based source column of the comment's opening character
    kind:    'line' (`--`) or 'block' (`/* */`)
    intent:  classification per CommentIntent
    raw:     the comment as it appeared in source, INCLUDING delimiters
             (preserved for round-trip / debugging)
    """
    text: str
    line: int
    col: int
    kind: CommentKind
    intent: CommentIntent
    raw: str


# ---------------------------------------------------------------------------
# Tokenizer-aware extraction
# ---------------------------------------------------------------------------
#
# A naive `re.findall(r"--.*$|/\*.*?\*/", sql)` would mis-match comment-like
# substrings inside string literals: `'I -- am a string'` would falsely yield
# `-- am a string'` as a comment.
#
# The token-alternation regex below handles SQL lexical structure precisely:
# strings, bracket-quoted identifiers, double-quoted identifiers, and comments
# are each their own atomic token. The first matching alternative wins, so by
# placing string/identifier patterns FIRST we ensure comment markers inside
# them aren't picked up.

_TOKEN_RE = re.compile(
    r"'(?:[^']|'')*'"           # T-SQL single-quoted string ('' escapes ')
    r"|\"(?:[^\"]|\"\")*\""     # double-quoted (identifier or ANSI string)
    r"|\[[^\]]*\]"               # bracket-quoted identifier
    r"|--[^\n]*"                 # line comment
    r"|/\*[\s\S]*?\*/"           # block comment (non-greedy, multi-line)
    r"|[^\s'\"\[\-/]+"           # bare identifier/keyword run (perf optimization)
    r"|.",                       # any single char fallback
    re.DOTALL,
)


def extract_comments(sql: str) -> tuple[str, list[Comment]]:
    """Extract every SQL comment; return (stripped_sql, comments).

    The stripped_sql is the input with each comment replaced by an
    equal-LENGTH whitespace run (newlines preserved for block comments
    that span lines). This means line/col coordinates in the stripped
    SQL match the source EXACTLY -- essential for binding comments
    back to AST nodes by position after parsing.
    """
    out_chars: list[str] = []
    comments: list[Comment] = []
    line = 1
    col = 1

    for m in _TOKEN_RE.finditer(sql):
        tok = m.group(0)
        # Save position of the FIRST char of this token before consuming.
        tok_line, tok_col = line, col

        if tok.startswith("--"):
            comments.append(Comment(
                text=tok[2:].strip(),
                line=tok_line, col=tok_col,
                kind="line",
                intent=classify_intent_raw(kind="line", body=tok[2:]),
                raw=tok,
            ))
            # Replace with same-length spaces (no newlines: -- comments don't
            # contain newlines; they end at EOL).
            out_chars.append(" " * len(tok))
        elif tok.startswith("/*") and tok.endswith("*/"):
            body = tok[2:-2]
            comments.append(Comment(
                text=body.strip(),
                line=tok_line, col=tok_col,
                kind="block",
                intent=classify_intent_raw(kind="block", body=body),
                raw=tok,
            ))
            # Replace with whitespace, preserving newlines so line numbers
            # downstream match.
            out_chars.append("".join(c if c == "\n" else " " for c in tok))
        else:
            out_chars.append(tok)

        # Advance line/col tracker by what was consumed.
        for c in tok:
            if c == "\n":
                line += 1
                col = 1
            else:
                col += 1

    return "".join(out_chars), comments


# ---------------------------------------------------------------------------
# Intent classification (regex / keyword based)
# ---------------------------------------------------------------------------

_SECTION_HEADER_RE = re.compile(r"^\s*([=\-*#~_]){4,}", re.MULTILINE)
_AUDIT_KEYS_RE = re.compile(
    r"\b(?:Author|Created\s*By|Modified\s*By|Modified\s*Date|"
    r"Created\s*Date|Updated\s*Date|Date|Revision|Version|Rev|"
    r"Change\s*Log|Changelog|History|Ticket|Jira)\s*[:=]",
    re.IGNORECASE,
)
# Intentionally NOT including NOTE -- too common in plain English ("trailing
# note about something") to be reliable. Real NOTE annotations almost always
# appear as "NOTE:" -- callers can add that pattern if they need it.
_TODO_RE = re.compile(r"\b(?:TODO|FIXME|HACK|XXX|BUG)\b", re.IGNORECASE)
# A "label" is a SHORT block comment (one line, < 40 chars) typical of
# inline annotations like `STATUS_C = 5 /* Denied */`.
_LABEL_MAX_LEN = 40


def classify_intent_raw(kind: CommentKind, body: str) -> CommentIntent:
    """Assign a CommentIntent based on shape + keyword heuristics.

    Order of checks matters -- earlier checks win. Definitions:
      - audit:          contains audit keys (Author:, Modified Date:, ...)
      - todo:           contains TODO/FIXME/HACK/XXX/BUG/NOTE token
      - section_header: separator-style (>= 4 repeated separator chars)
      - label:          short single-line block comment (likely enum)
      - doc:            multi-line block comment >= 2 lines
      - unclassified:   anything else
    """
    text = body.strip()
    if not text:
        return "unclassified"

    if _AUDIT_KEYS_RE.search(text):
        return "audit"
    if _TODO_RE.search(text):
        return "todo"
    if _SECTION_HEADER_RE.search(text):
        return "section_header"
    if kind == "block" and "\n" not in text and len(text) <= _LABEL_MAX_LEN:
        return "label"
    if kind == "block" and text.count("\n") >= 1:
        return "doc"
    return "unclassified"


def classify_intent(comment: Comment) -> CommentIntent:
    """Convenience wrapper for the dataclass form."""
    return classify_intent_raw(comment.kind, comment.text)
