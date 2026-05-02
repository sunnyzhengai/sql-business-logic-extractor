"""Tests for the AST fingerprinting in tools/similar_logic_grouper.

Asserts the public contract:
- alias-equivalent expressions produce the SAME fingerprint
- semantically different expressions produce DIFFERENT fingerprints
- commutative-arg reordering doesn't change the fingerprint
- malformed input returns None instead of crashing
"""

from tools.similar_logic_grouper.fingerprint import fingerprint


def test_alias_normalization_same_fingerprint():
    """`CVG.STATUS_C = 5` and `C.STATUS_C = 5` are the same business term."""
    a = fingerprint("CVG.STATUS_C = 5")
    b = fingerprint("C.STATUS_C = 5")
    assert a == b
    assert a is not None


def test_commutative_and_same_fingerprint():
    """`A AND B` and `B AND A` are semantically identical."""
    a = fingerprint("X.STATUS = 'Denied' AND X.YEAR = 2025")
    b = fingerprint("X.YEAR = 2025 AND X.STATUS = 'Denied'")
    assert a == b


def test_different_literals_different_fingerprint():
    """Status = 5 (Denied) vs Status = 1 (Active) is NOT the same definition."""
    denied = fingerprint("X.STATUS_C = 5")
    active = fingerprint("X.STATUS_C = 1")
    assert denied != active


def test_different_columns_different_fingerprint():
    """COVERAGE_TYPE_C = 2 vs COVERAGE_ID = 2 are different definitions."""
    a = fingerprint("X.COVERAGE_TYPE_C = 2")
    b = fingerprint("X.COVERAGE_ID = 2")
    assert a != b


def test_function_preserved_in_fingerprint():
    """COALESCE(a, b) and a are different shapes."""
    a = fingerprint("COALESCE(X.A, X.B)")
    b = fingerprint("X.A")
    assert a != b


def test_invalid_sql_returns_none():
    """Malformed SQL doesn't crash the grouper."""
    assert fingerprint(") not even close to SQL (") is None
    assert fingerprint("") is None
    assert fingerprint("   ") is None
