#!/usr/bin/env python3
"""Walk a folder of views; for any that fail the registry, write a
proposed-rule markdown file under parsing_rules/proposed/.

Notebook usage:

    from tools.auto_propose_rule.batch import propose_rules
    propose_rules(input_dir='/lakehouse/default/Files/views')

CLI:

    python -m tools.auto_propose_rule.batch <input_dir>

The proposals are MARKDOWN, intended for human review. Workflow:

    1. preflight_check produces a CSV with status=unknown_failure rows.
    2. auto_propose_rule writes one .md per failing view.
    3. Human opens each .md, validates the suggested hypothesis, and
       (if accepted) creates the fixture pair + appends a Rule(...)
       entry to rules.py.
    4. Re-run preflight -- the view should now be 'needs_rule'.
"""

import argparse
import re
import sys
from pathlib import Path

import sqlglot

from sql_logic_extractor.resolve import preprocess_ssms
from sql_logic_extractor.parsing_rules import apply_all
from tools.auto_propose_rule.hypotheses import HYPOTHESES, Hypothesis


_PROPOSED_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "sql_logic_extractor" / "parsing_rules" / "proposed"
)


# ---------- redaction (matches diagnose_parse_failure) ---------------------

_STRING_LIT_RE = re.compile(r"'(?:[^']|'')*'")
_NUMBER_LIT_RE = re.compile(r"\b\d+\b")


def _redact(text: str) -> str:
    text = _STRING_LIT_RE.sub("'***'", text)
    text = _NUMBER_LIT_RE.sub("N", text)
    return text


_LINE_COL_RE = re.compile(r"[Ll]ine\s+(\d+),\s*[Cc]ol(?:umn)?\s*:?\s*(\d+)")


def _extract_line_col(error_msg: str) -> tuple[int, int] | None:
    m = _LINE_COL_RE.search(error_msg or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


# ---------- file reader ----------------------------------------------------

def _read_sql_file(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le")[1:]
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be")[1:]
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8")[1:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-16-le", errors="replace")


# ---------- core: parse + hypothesis sweep ---------------------------------

def _parses(sql: str) -> bool:
    try:
        sqlglot.parse_one(sql, dialect="tsql")
        return True
    except Exception:
        return False


def _try_hypotheses(clean_sql: str) -> Hypothesis | None:
    """Return the first hypothesis that unblocks the parse, or None."""
    for h in HYPOTHESES:
        candidate = h.transform(clean_sql)
        if candidate == clean_sql:
            continue  # transform was a no-op on this view
        if _parses(candidate):
            return h
    return None


def _context_window(sql: str, line: int, col: int, window: int = 5) -> str:
    lines = sql.split("\n")
    start = max(0, line - 1 - window)
    end = min(len(lines), line + window)
    out = []
    for i in range(start, end):
        marker = ">>" if i == line - 1 else "  "
        out.append(f"{marker} {i + 1:4d}: {_redact(lines[i])}")
        if i == line - 1 and col > 0:
            out.append(f"        {' ' * (col - 1)}^")
    return "\n".join(out)


# ---------- proposal markdown ---------------------------------------------

def _write_hypothesis_proposal(view_path: Path, h: Hypothesis,
                                 sample_before: str, sample_after: str) -> Path:
    out_path = _PROPOSED_DIR / f"{view_path.stem}.md"
    content = f"""# Proposed parsing rule: `{h.rule_id}`

**Source view:** `{view_path.name}`
**Status:** auto-proposed via hypothesis sweep -- HUMAN REVIEW REQUIRED
**Hypothesis fired:** YES (parse succeeds after this transform)

## What construct this rule targets

{h.description}

## Suggested rule entry

Add to `sql_logic_extractor/parsing_rules/rules.py`:

```python
Rule(
    id="{h.rule_id}",
    description=(
        # Edit this to match your team's documentation style.
        "{h.description}"
    ),
    pattern=r"{h.suggested_pattern}",
    replacement=r"",  # Edit if non-empty replacement is appropriate.
    flags={h.suggested_flags},
),
```

## Fixture pair to create

Path: `sql_logic_extractor/parsing_rules/fixtures/{h.rule_id}/`

`input.sql` (redacted excerpt of the failing construct):

```sql
{_redact(sample_before)[:500]}
```

`expected_clean.sql` (post-rule output -- redacted excerpt):

```sql
{_redact(sample_after)[:500]}
```

## Validation

1. Create the fixture pair above.
2. Append the Rule entry to `rules.py`.
3. Run: `pytest tests/parsing_rules/`
4. Run: `python -m tools.preflight_check.batch <views_dir>` and confirm
   this view's status moves from `unknown_failure` to `needs_rule`.
"""
    _PROPOSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content)
    return out_path


def _write_token_isolation_proposal(view_path: Path, clean_sql: str,
                                       error_msg: str) -> Path:
    """When NO hypothesis fires, write a 'human must investigate' proposal
    with the redacted context window around the failing token."""
    out_path = _PROPOSED_DIR / f"{view_path.stem}.md"
    lc = _extract_line_col(error_msg)
    if lc:
        ctx = _context_window(clean_sql, *lc, window=5)
        loc = f"line {lc[0]}, col {lc[1]}"
    else:
        ctx = "(no line/col in error message)"
        loc = "unknown"
    content = f"""# Proposed parsing rule: NEEDS HUMAN INVESTIGATION

**Source view:** `{view_path.name}`
**Status:** no canned hypothesis unblocked the parse -- this is a NEW
T-SQL construct sqlglot can't handle. The construct must be identified
manually below, then a Rule + fixture pair authored.

## sqlglot error

```
{_redact(error_msg)[:500]}
```

Failing position: {loc}

## Redacted context window (5 lines around the failing line)

```
{ctx}
```

## Next steps

1. Identify the offending T-SQL construct from the context window.
2. Decide if it's:
   - **A drop**: the construct is irrelevant to column extraction
     (e.g. PRINT, SET, table hints) -- write a strip rule.
   - **A rewrite**: the construct has a sqlglot-parseable equivalent
     -- write a substitution rule.
   - **An sqlglot bug**: file an upstream issue. May need a sqlglot
     pin or workaround in the meantime.
3. Author a fixture pair under
   `sql_logic_extractor/parsing_rules/fixtures/<new_rule_id>/`
4. Append a `Rule(...)` to `parsing_rules/rules.py`.
5. Re-run preflight; this view should move to `needs_rule`.
"""
    _PROPOSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content)
    return out_path


# ---------- batch entry point ----------------------------------------------

def propose_rules(input_dir: str, *, dialect: str = "tsql") -> int:
    in_dir = Path(input_dir)
    if not in_dir.is_dir():
        print(f"Error: {in_dir} is not a directory", file=sys.stderr)
        return 1

    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"Error: no .sql files in {in_dir}", file=sys.stderr)
        return 1

    n_clean = 0
    n_hypothesis = 0
    n_unknown = 0
    proposals: list[Path] = []

    for path in sql_files:
        try:
            sql = _read_sql_file(path)
            # Apply registry first; this is the same path preprocess_ssms takes.
            sql_after_rules, _fired = apply_all(sql)
            try:
                clean, _ = preprocess_ssms(sql)
            except Exception:
                clean = sql_after_rules

            if _parses(clean):
                n_clean += 1
                continue

            # The view fails. Try the hypothesis sweep.
            h = _try_hypotheses(clean)
            if h:
                n_hypothesis += 1
                sample_after = h.transform(clean)
                proposals.append(_write_hypothesis_proposal(
                    path, h, sample_before=clean, sample_after=sample_after))
                continue

            # Fully unknown -- write the token-isolation proposal.
            n_unknown += 1
            try:
                sqlglot.parse_one(clean, dialect=dialect)
            except Exception as e:
                proposals.append(_write_token_isolation_proposal(
                    path, clean, f"{type(e).__name__}: {e}"))
        except Exception as e:
            print(f"  ERROR processing {path.name}: {type(e).__name__}: {e}",
                  file=sys.stderr)

    print(f"\nAuto-propose: scanned {len(sql_files)} views.")
    print(f"  clean (no proposal):              {n_clean}")
    print(f"  hypothesis fired (proposal made): {n_hypothesis}")
    print(f"  no hypothesis (human review):     {n_unknown}")
    if proposals:
        print(f"\nWrote {len(proposals)} proposal(s) to {_PROPOSED_DIR}/")
        for p in proposals[:5]:
            print(f"  - {p.name}")
        if len(proposals) > 5:
            print(f"  ...and {len(proposals) - 5} more")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Auto-propose parsing rules for unknown_failure views."
    )
    parser.add_argument("input_dir", help="Folder containing view *.sql files")
    parser.add_argument("-d", "--dialect", default="tsql")
    args = parser.parse_args()
    return propose_rules(args.input_dir, dialect=args.dialect)


def _is_notebook() -> bool:
    return "ipykernel" in sys.argv[0] or "ipykernel" in " ".join(sys.argv[1:])


if __name__ == "__main__":
    if _is_notebook():
        print("Notebook environment detected -- call propose_rules("
              "input_dir=...) from a cell.")
    else:
        sys.exit(main())
