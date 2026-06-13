"""Tests for the mock database and glossaries.

Verifies:
- Data invariants (correlated patient counts)
- SQL views execute correctly against mock data
- Glossary files are valid YAML with expected structure
- Learned terms are pre-populated
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


@pytest.fixture(scope="module")
def mock_db():
    """Create a fresh mock DB in a temp file for testing."""
    from tools.jit.mock.mock_db import create_mock_db

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    conn = create_mock_db(db_path)
    yield conn
    conn.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture(scope="module")
def glossary_dir():
    """Use the generated glossaries in tools/jit/data/."""
    # Ensure they exist (generate if needed)
    if not (DATA_DIR / "report_glossary").exists():
        from tools.jit.mock.mock_reports import generate_report_glossary
        generate_report_glossary(DATA_DIR)
    if not (DATA_DIR / "definition_glossary").exists():
        from tools.jit.mock.mock_definitions import generate_definition_glossary
        generate_definition_glossary(DATA_DIR)
    if not (DATA_DIR / "technical_glossary.yaml").exists():
        from tools.jit.mock.mock_technical import generate_technical_glossary
        generate_technical_glossary(DATA_DIR)
    return DATA_DIR


# ---------------------------------------------------------------------------
# Database invariant tests
# ---------------------------------------------------------------------------

class TestMockDbInvariants:
    """Verify the correlated data produces expected cascade."""

    def test_patient_count(self, mock_db):
        cur = mock_db.cursor()
        cur.execute("SELECT COUNT(*) FROM PATIENT")
        assert cur.fetchone()[0] == 500

    def test_diabetic_patient_count(self, mock_db):
        cur = mock_db.cursor()
        cur.execute("""
            SELECT COUNT(DISTINCT p.PAT_ID)
            FROM PATIENT p
            JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
            JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
            WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
              AND pl.RESOLVED_DATE IS NULL
        """)
        assert cur.fetchone()[0] == 90

    def test_diabetic_ed_high_utilizers(self, mock_db):
        cur = mock_db.cursor()
        cur.execute("""
            WITH diabetic_pats AS (
                SELECT DISTINCT p.PAT_ID
                FROM PATIENT p
                JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
                JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
                WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
                  AND pl.RESOLVED_DATE IS NULL
            )
            SELECT COUNT(*) FROM (
                SELECT enc.PAT_ID
                FROM PAT_ENC enc
                JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
                WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
                  AND enc.CONTACT_DATE >= '2025-06-11'
                  AND enc.PAT_ID IN (SELECT PAT_ID FROM diabetic_pats)
                GROUP BY enc.PAT_ID
                HAVING COUNT(*) > 3
            )
        """)
        count = cur.fetchone()[0]
        assert count == 45, f"Expected 45 diabetic ED high-utilizers, got {count}"

    def test_diabetic_ed_high_pcp_noshow(self, mock_db):
        """The full cascade should produce ~20 patients."""
        cur = mock_db.cursor()
        cur.execute("""
            WITH diabetic_pats AS (
                SELECT DISTINCT p.PAT_ID
                FROM PATIENT p
                JOIN PROBLEM_LIST pl ON pl.PAT_ID = p.PAT_ID
                JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
                WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
                  AND pl.RESOLVED_DATE IS NULL
            ),
            er_high AS (
                SELECT enc.PAT_ID
                FROM PAT_ENC enc
                JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
                WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
                  AND enc.CONTACT_DATE >= '2025-06-11'
                  AND enc.PAT_ID IN (SELECT PAT_ID FROM diabetic_pats)
                GROUP BY enc.PAT_ID
                HAVING COUNT(*) > 3
            ),
            missed_pcp AS (
                SELECT DISTINCT enc.PAT_ID
                FROM PAT_ENC enc
                JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
                WHERE dep.SPECIALTY = 'Family Medicine'
                  AND enc.APPT_STATUS_C = 4
                  AND enc.CONTACT_DATE >= '2025-12-11'
                  AND enc.PAT_ID IN (SELECT PAT_ID FROM er_high)
            )
            SELECT COUNT(*) FROM missed_pcp
        """)
        count = cur.fetchone()[0]
        # Allow range because of random PCP assignments and 5% noise
        assert 15 <= count <= 25, f"Expected ~20 PCP no-shows, got {count}"

    def test_nondiabetic_ed_high_contrast(self, mock_db):
        """Non-diabetic patients with 4+ ED visits should exist (for contrast)."""
        cur = mock_db.cursor()
        cur.execute("""
            WITH diabetic_pats AS (
                SELECT DISTINCT pl.PAT_ID FROM PROBLEM_LIST pl
                JOIN CLARITY_EDG edg ON edg.DX_ID = pl.DX_ID
                WHERE edg.CURRENT_ICD10_LIST LIKE 'E11%'
                  AND pl.RESOLVED_DATE IS NULL
            )
            SELECT COUNT(*) FROM (
                SELECT enc.PAT_ID
                FROM PAT_ENC enc
                JOIN CLARITY_DEP dep ON dep.DEPARTMENT_ID = enc.DEPARTMENT_ID
                WHERE dep.DEPARTMENT_NAME LIKE '%Emergency%'
                  AND enc.CONTACT_DATE >= '2025-06-11'
                  AND enc.PAT_ID NOT IN (SELECT PAT_ID FROM diabetic_pats)
                GROUP BY enc.PAT_ID
                HAVING COUNT(*) > 3
            )
        """)
        count = cur.fetchone()[0]
        assert count == 60, f"Expected 60 non-diabetic ED high-utilizers, got {count}"

    def test_lookup_tables_populated(self, mock_db):
        cur = mock_db.cursor()
        cur.execute("SELECT COUNT(*) FROM ZC_APPT_STATUS")
        assert cur.fetchone()[0] == 6
        cur.execute("SELECT COUNT(*) FROM CLARITY_EDG")
        assert cur.fetchone()[0] == 200
        cur.execute("SELECT COUNT(*) FROM CLARITY_DEP")
        assert cur.fetchone()[0] == 20
        cur.execute("SELECT COUNT(*) FROM CLARITY_SER")
        assert cur.fetchone()[0] == 50


# ---------------------------------------------------------------------------
# SQL view tests
# ---------------------------------------------------------------------------

class TestSqlViews:
    """Verify all 8 report SQL views execute against the mock DB."""

    def test_all_views_execute(self, mock_db):
        cur = mock_db.cursor()
        sql_dir = DATA_DIR / "report_sql"
        if not sql_dir.exists():
            pytest.skip("report_sql not generated yet")

        for sql_file in sorted(sql_dir.glob("*.sql")):
            name = sql_file.stem
            sql = sql_file.read_text()
            # Drop view if exists, then create
            cur.execute(f"DROP VIEW IF EXISTS {name}")
            cur.executescript(sql)
            cur.execute(f"SELECT COUNT(*) FROM {name}")
            count = cur.fetchone()[0]
            assert count > 0, f"{name} returned 0 rows"

    def test_diabetic_cohort_view_count(self, mock_db):
        cur = mock_db.cursor()
        sql_file = DATA_DIR / "report_sql" / "VW_DIABETIC_COHORT.sql"
        if not sql_file.exists():
            pytest.skip("SQL file not generated")
        cur.execute("DROP VIEW IF EXISTS VW_DIABETIC_COHORT")
        cur.executescript(sql_file.read_text())
        cur.execute("SELECT COUNT(*) FROM VW_DIABETIC_COHORT")
        assert cur.fetchone()[0] == 90


# ---------------------------------------------------------------------------
# Glossary structure tests
# ---------------------------------------------------------------------------

class TestReportGlossary:
    """Verify report glossary YAML files have expected structure."""

    def test_report_count(self, glossary_dir):
        reports = list((glossary_dir / "report_glossary").glob("*.yaml"))
        assert len(reports) == 8

    def test_report_has_required_fields(self, glossary_dir):
        required = {"report_name", "description", "primary_purpose",
                     "tables_used", "domains"}
        for yaml_path in (glossary_dir / "report_glossary").glob("*.yaml"):
            with open(yaml_path) as f:
                report = yaml.safe_load(f)
            missing = required - set(report.keys())
            assert not missing, f"{yaml_path.name} missing: {missing}"


class TestDefinitionGlossary:
    """Verify definition glossary YAML files have expected structure."""

    def test_definition_count(self, glossary_dir):
        defs = list((glossary_dir / "definition_glossary").glob("*.yaml"))
        assert len(defs) == 12

    def test_definition_has_backbone(self, glossary_dir):
        for yaml_path in (glossary_dir / "definition_glossary").glob("*.yaml"):
            with open(yaml_path) as f:
                defn = yaml.safe_load(f)
            assert "backbone" in defn, f"{yaml_path.name} missing backbone"
            bb = defn["backbone"]
            assert "anchor_table" in bb
            assert "tables" in bb
            assert isinstance(bb["tables"], list)

    def test_definition_has_provenance(self, glossary_dir):
        for yaml_path in (glossary_dir / "definition_glossary").glob("*.yaml"):
            with open(yaml_path) as f:
                defn = yaml.safe_load(f)
            assert "source_reports" in defn
            assert "source_scopes" in defn


class TestTechnicalGlossary:
    """Verify technical glossary structure."""

    def test_technical_glossary_exists(self, glossary_dir):
        path = glossary_dir / "technical_glossary.yaml"
        assert path.exists()
        with open(path) as f:
            tech = yaml.safe_load(f)
        assert "domains" in tech
        assert "dimensions" in tech

    def test_domains_have_anchors(self, glossary_dir):
        with open(glossary_dir / "technical_glossary.yaml") as f:
            tech = yaml.safe_load(f)
        for domain_name, domain in tech["domains"].items():
            assert "anchor_tables" in domain, f"{domain_name} missing anchor_tables"
            assert len(domain["anchor_tables"]) > 0


class TestLearnedTerms:
    """Verify learned terms pre-population."""

    def test_learned_terms_populated(self, glossary_dir):
        path = glossary_dir / "learned_terms.yaml"
        assert path.exists()
        with open(path) as f:
            terms = yaml.safe_load(f)
        assert len(terms) >= 5, f"Expected 5+ terms, got {len(terms)}"

    def test_diabetes_term_present(self, glossary_dir):
        with open(glossary_dir / "learned_terms.yaml") as f:
            terms = yaml.safe_load(f)
        assert "diabetes" in terms
        assert terms["diabetes"]["category"] == "diagnosis"
        assert "E11" in terms["diabetes"].get("icd10", "")

    def test_terms_have_aliases(self, glossary_dir):
        with open(glossary_dir / "learned_terms.yaml") as f:
            terms = yaml.safe_load(f)
        for key, term in terms.items():
            assert "aliases" in term, f"{key} missing aliases"
            assert len(term["aliases"]) > 0


class TestRouteCatalog:
    """Verify route catalog structure."""

    def test_route_catalog_exists(self, glossary_dir):
        path = glossary_dir / "route_catalog.yaml"
        assert path.exists()
        with open(path) as f:
            routes = yaml.safe_load(f)
        assert "diagnosis" in routes
        assert len(routes["diagnosis"]["routes"]) == 4  # 4 diagnosis routes

    def test_routes_have_paths(self, glossary_dir):
        with open(glossary_dir / "route_catalog.yaml") as f:
            routes = yaml.safe_load(f)
        for cat_name, cat in routes.items():
            for route in cat["routes"]:
                assert "path" in route, f"{cat_name}:{route['name']} missing path"
                assert len(route["path"]) >= 2


class TestDbExecutor:
    """Verify db_executor works correctly."""

    def test_execute_sql(self, mock_db):
        from tools.jit.mock.db_executor import execute_sql
        result = execute_sql(mock_db, "SELECT COUNT(*) AS cnt FROM PATIENT")
        assert result["columns"] == ["cnt"]
        assert result["rows"][0][0] == 500
        assert result["row_count"] == 1

    def test_execute_count(self, mock_db):
        from tools.jit.mock.db_executor import execute_count
        count = execute_count(mock_db, "SELECT COUNT(*) FROM PATIENT")
        assert count == 500
