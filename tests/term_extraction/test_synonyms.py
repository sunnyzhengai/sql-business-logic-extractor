"""Tests for sql_logic_extractor.term_extraction.synonyms."""

from sql_logic_extractor.term_extraction.synonyms import (
    SynonymDict,
    load_default_synonyms,
    load_synonyms_from_yaml,
)


def test_default_synonyms_loads():
    syn = load_default_synonyms()
    assert isinstance(syn, SynonymDict)
    # Spot-check a few entries we know are seeded.
    assert syn.expand("preg") == "pregnant"
    assert syn.expand("pregnancy") == "pregnant"
    assert syn.expand("pt") == "patient"
    assert syn.expand("dx") == "diagnosis"


def test_canonical_maps_to_itself():
    syn = load_default_synonyms()
    assert syn.expand("pregnant") == "pregnant"
    assert syn.expand("patient") == "patient"


def test_unknown_token_returns_lowercase_input():
    syn = load_default_synonyms()
    assert syn.expand("xyzzy") == "xyzzy"
    assert syn.expand("PAT_NAME") == "pat_name"   # not split here -- lowercase only


def test_is_known_distinguishes_canonical_and_variants():
    syn = load_default_synonyms()
    assert syn.is_known("preg")
    assert syn.is_known("pregnant")
    assert not syn.is_known("xyzzy")


def test_canonicals_returns_seeded_set():
    syn = load_default_synonyms()
    cs = syn.canonicals()
    assert "pregnant" in cs
    assert "patient" in cs
    assert "encounter" in cs


def test_load_from_custom_yaml(tmp_path):
    yaml_text = """
synonyms:
  - canonical: foo
    variants: [f, fooz]
  - canonical: bar
"""
    p = tmp_path / "syn.yaml"
    p.write_text(yaml_text)
    syn = load_synonyms_from_yaml(p)
    assert syn.expand("f") == "foo"
    assert syn.expand("fooz") == "foo"
    assert syn.expand("bar") == "bar"   # canonical with no variants
    assert syn.expand("unknown") == "unknown"


def test_yaml_missing_canonical_raises(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("synonyms:\n  - variants: [a, b]\n")
    import pytest
    with pytest.raises(ValueError):
        load_synonyms_from_yaml(p)
