"""Boot — wire all glossaries, indexes, DB connection, and tools.

Called once at Streamlit app startup via @st.cache_resource.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import yaml

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


@dataclass
class AppContext:
    """All shared resources for the assistant."""
    db_conn: sqlite3.Connection
    report_searcher: object
    defn_searcher: object
    tech_searcher: object
    learned_terms: dict
    fk_graph: object  # networkx DiGraph or None


def boot(data_dir: Path | None = None) -> AppContext:
    """Initialize all indexes and connections."""
    data_dir = data_dir or DATA_DIR

    # Ensure mock data exists
    db_path = data_dir / "mock.db"
    if not db_path.exists():
        from tools.jit.mock.mock_db import create_mock_db
        create_mock_db(db_path)
    if not (data_dir / "report_glossary").exists():
        from tools.jit.mock.mock_reports import generate_report_glossary
        generate_report_glossary(data_dir)
    if not (data_dir / "definition_glossary").exists():
        from tools.jit.mock.mock_definitions import generate_definition_glossary
        generate_definition_glossary(data_dir)
    if not (data_dir / "technical_glossary.yaml").exists():
        from tools.jit.mock.mock_technical import generate_technical_glossary
        generate_technical_glossary(data_dir)

    # DB connection — WAL mode for concurrent reads
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    # Search tools
    from tools.jit.search_reports import ReportSearcher
    from tools.jit.search_definitions import DefinitionSearcher
    from tools.jit.search_technical import TechnicalSearcher

    report_searcher = ReportSearcher(glossary_dir=data_dir / "report_glossary")
    defn_searcher = DefinitionSearcher(glossary_dir=data_dir / "definition_glossary")
    tech_searcher = TechnicalSearcher()

    # Learned terms
    terms_path = data_dir / "learned_terms.yaml"
    with open(terms_path) as f:
        learned_terms = yaml.safe_load(f) or {}

    # FK graph (optional — try to load from clarity_schema.yaml)
    fk_graph = None
    schema_path = Path("data/schemas/clarity_schema.yaml")
    if schema_path.exists():
        try:
            from tools.jit.query_graph import build_fk_graph
            fk_graph = build_fk_graph(schema_path)
        except Exception:
            pass

    return AppContext(
        db_conn=conn,
        report_searcher=report_searcher,
        defn_searcher=defn_searcher,
        tech_searcher=tech_searcher,
        learned_terms=learned_terms,
        fk_graph=fk_graph,
    )
