"""AST fingerprinting for cross-view business-logic deduplication.

The fingerprint of a SQL expression is a canonical form that ignores:
- table aliases (CVG vs C vs Coverage all collapse to a placeholder)
- column-name ORDER inside commutative operators (AND/OR/+)
- whitespace and quoting variations

But preserves:
- expression structure (which operators are nested how)
- literal values (5, 'Denied', etc. -- semantically meaningful)
- function names (COALESCE, IIF, CASE, etc.)
- column-name TYPE (we use the bare column name without table qualifier)

Two columns from two different views with the same fingerprint are very
likely the same business term implemented twice. The fingerprint is a
hash of the canonical AST -- equality, not similarity (we use the
fingerprint as a dict key for cluster discovery).
"""

import hashlib
from typing import Any

import sqlglot
from sqlglot import exp


def _normalize_node(node: exp.Expression) -> Any:
    """Walk a sqlglot AST and produce a hashable canonical tree.

    Rules:
    - Column refs: keep the bare column name; drop the table alias.
      `CVG.STATUS_C` and `C.STATUS_C` both become `STATUS_C`.
    - Literals: keep the value verbatim (`5`, `'Denied'`).
    - Functions: keep the function name + canonical arg list.
    - Commutative ops (AND, OR, ADD, MUL, EQ, NEQ): sort children so
      `A AND B` and `B AND A` produce the same fingerprint.
    - Other expressions: keep type + canonical children in source order.
    """
    if isinstance(node, exp.Column):
        return ("col", (node.name or "").upper())
    if isinstance(node, exp.Literal):
        return ("lit", node.this, node.is_string)
    if isinstance(node, exp.Boolean):
        return ("bool", bool(node.this))
    if isinstance(node, exp.Null):
        return ("null",)

    type_name = type(node).__name__

    children = []
    for arg in node.args.values():
        if isinstance(arg, exp.Expression):
            children.append(_normalize_node(arg))
        elif isinstance(arg, list):
            child_list = [_normalize_node(c) for c in arg if isinstance(c, exp.Expression)]
            if type_name in _COMMUTATIVE_OPS:
                child_list = sorted(child_list, key=lambda x: repr(x))
            children.append(("list", tuple(child_list)))
        elif arg is None:
            continue
        else:
            children.append(("scalar", str(arg)))

    if type_name in _COMMUTATIVE_OPS and len(children) >= 2:
        children = sorted(children, key=lambda x: repr(x))

    return (type_name, tuple(children))


# sqlglot Expression subclasses whose argument order doesn't affect semantics.
_COMMUTATIVE_OPS = frozenset({
    "And", "Or", "Add", "Mul",
    "EQ", "NEQ", "Is",  # equality is symmetric
})


def fingerprint(sql_expression: str, dialect: str = "tsql") -> str | None:
    """Return a stable hex fingerprint for a SQL expression. Returns None
    if the expression doesn't parse (so callers can skip it without a
    crash)."""
    if not sql_expression or not sql_expression.strip():
        return None
    try:
        node = sqlglot.parse_one(sql_expression, dialect=dialect)
    except Exception:
        return None
    canonical = _normalize_node(node)
    blob = repr(canonical).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]
