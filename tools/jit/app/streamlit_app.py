"""SQL Business Logic Assistant — Conversational Graph Builder.

Run:
    streamlit run tools/jit/app/streamlit_app.py

Architecture:
  - LLM: classifies intent + extracts concepts (NL understanding only)
  - Graph: decides tables, joins, columns (structural truth)
  - Schema: provides column names and descriptions
  - Definitions/Terms: map concepts to known SQL patterns
"""

from __future__ import annotations

import re
import sys
import sqlite3
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[3])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import streamlit as st
import pandas as pd
import yaml

from tools.jit.app.boot import boot
from tools.jit.app.query_graph_ui import (
    GraphTable, build_graph_sql, execute_graph_counts,
)
from tools.jit.app.route_explorer import (
    get_routes_for_question, preview_route_counts, _infer_join_column,
)
from tools.jit.app.llm_interpreter import (
    classify_and_extract, build_definitions_summary, build_terms_summary,
)
from tools.jit.mock.db_executor import execute_sql, execute_count
from tools.jit.term_resolver import expand_synonyms
from tools.jit.flywheel import validate_definition

# ---------------------------------------------------------------------------
# Config + boot
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Business Logic Assistant", layout="wide",
                   initial_sidebar_state="expanded")
st.markdown("<style>.block-container{padding:1rem 2rem;max-width:1400px}</style>",
            unsafe_allow_html=True)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


@st.cache_resource(ttl=60)
def _boot():
    return boot()

ctx = _boot()


def get_db():
    c = sqlite3.connect(str(DATA_DIR / "mock.db"), check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=5000")
    return c

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

for k, v in {"messages": [], "graph_tables": [], "pending_proposals": []}.items():
    if k not in st.session_state:
        st.session_state[k] = v


def reset():
    st.session_state["messages"] = []
    st.session_state["graph_tables"] = []
    st.session_state["pending_proposals"] = []

# ---------------------------------------------------------------------------
# Graph-driven execution: concept → tables, filter → column
# ---------------------------------------------------------------------------

def resolve_concept_to_proposals(concepts: list[str],
                                  graph_tables: list[GraphTable]) -> list[dict]:
    """Use the FK graph + definitions + routes to turn NL concepts into proposals.

    The LLM identified concepts like "diabetes", "ER visits".
    This function finds the RIGHT tables and joins using structural data.
    """
    existing = {gt.name for gt in graph_tables}
    tokens = set()
    for c in concepts:
        tokens.update(expand_synonyms(c).lower().split())

    proposals = []

    # 1. Search known routes via learned terms
    route_groups = get_routes_for_question(tokens, existing, ctx.learned_terms)
    for group in route_groups:
        # Find the filter for this category from learned terms
        filter_expr, filter_english = "", ""
        for key, term in ctx.learned_terms.items():
            if term.get("category") == group["category"]:
                aliases = {a.lower() for a in term.get("aliases", [])}
                aliases.add(term.get("term", "").lower())
                if tokens & aliases:
                    for ft, fe in term.get("filters", {}).items():
                        filter_expr = f"{ft}.{fe}" if "." not in fe else fe
                        filter_english = f"{term.get('term', key)} ({fe})"
                    break

        conn = get_db()
        for route in preview_route_counts(group["routes"], conn, filter_expr):
            path = route.get("path", [])
            count = route.get("preview_count")
            new_tables = []
            for pi, tname in enumerate(path):
                if tname in existing:
                    continue
                jf = path[pi - 1] if pi > 0 else None
                jc = _infer_join_column(jf, tname) if jf else ""
                filters = []
                if pi == len(path) - 1 and filter_expr:
                    filters.append({"expression": filter_expr,
                                    "english": filter_english, "active": True})
                new_tables.append(GraphTable(
                    name=tname, join_from=jf, join_column=jc, pk_column=jc,
                    join_direction="child_to_parent", filters=filters, is_root=False))

            if new_tables:
                cs = f"{count:,}" if count else "?"
                proposals.append({
                    "description": f"{route['name']}: {' → '.join(path)} ({cs})",
                    "detail": route.get("description", ""),
                    "tables_to_add": new_tables,
                    "source": route["name"], "count": count,
                })
        conn.close()

    # 2. Fallback: search business definitions
    if not proposals:
        from tools.jit.search_definitions import DefinitionSearcher
        for hit in DefinitionSearcher().search(" ".join(concepts), min_score=0.1)[:3]:
            bb = hit.full_entry.get("backbone", {})
            new_tables = []
            for tname in bb.get("tables", []):
                if tname in existing:
                    continue
                jf, jc = None, ""
                if ctx.fk_graph:
                    for gt in graph_tables:
                        tu, gu = tname.upper(), gt.name.upper()
                        if ctx.fk_graph.has_edge(tu, gu):
                            jf, jc = gt.name, ctx.fk_graph.edges[tu, gu].get("fk_column", "")
                            break
                        elif ctx.fk_graph.has_edge(gu, tu):
                            jf, jc = gt.name, ctx.fk_graph.edges[gu, tu].get("fk_column", "")
                            break
                if jf:
                    filters = [{"expression": f["expression"],
                                "english": f.get("english", ""), "active": True}
                               for f in bb.get("characteristic_filters", [])
                               if tname.upper() + "." in f.get("expression", "").upper()]
                    new_tables.append(GraphTable(
                        name=tname, join_from=jf, join_column=jc, pk_column=jc,
                        join_direction="child_to_parent", filters=filters, is_root=False))
            if new_tables:
                proposals.append({
                    "description": hit.label, "detail": hit.description[:80],
                    "tables_to_add": new_tables, "source": hit.definition_name,
                    "defn_name": hit.definition_name,
                })

    return proposals


def resolve_filter_to_column(filter_desc: str, filter_type: str,
                              filter_value: str, table_hint: str,
                              graph_tables: list[GraphTable]) -> dict | None:
    """Use the schema to find the right column for a filter.

    The LLM said "filter by appointment date, this year".
    This function finds the actual column from the schema.
    """
    conn = get_db()

    # Find target table — prefer table_hint, else search graph
    target = None
    if table_hint:
        target = next((gt for gt in graph_tables
                        if gt.name.upper() == table_hint.upper()), None)

    # Search columns across all graph tables for the best match
    best = None
    best_score = 0.0
    desc_tokens = set(expand_synonyms(filter_desc).lower().split())

    for gt in graph_tables:
        try:
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({gt.name})")
            cols = [(row[1], row[2]) for row in cur.fetchall()]
        except Exception:
            continue

        # Also get descriptions from schema YAML if available
        schema_path = Path("data/schemas/clarity_schema.yaml")
        col_descs = {}
        if schema_path.exists():
            with open(schema_path) as f:
                schema = yaml.safe_load(f)
            for table in schema.get("tables", []):
                if table.get("name", "").upper() == gt.name.upper():
                    for c in table.get("columns", []):
                        col_descs[c["name"].upper()] = c.get("description", "")

        for col_name, col_type in cols:
            col_tokens = set(col_name.lower().replace("_", " ").split())
            col_desc_text = col_descs.get(col_name.upper(), "").lower()
            col_desc_tokens = set(col_desc_text.split())
            all_tokens = col_tokens | col_desc_tokens

            overlap = desc_tokens & all_tokens
            score = len(overlap) / max(len(desc_tokens), 1) if desc_tokens else 0

            # Type bonus: if filter_type=date and column name has date/time
            if filter_type == "date" and any(h in col_name.lower()
                                              for h in ["date", "time", "dttm"]):
                score += 0.3

            if filter_type == "status" and "status" in col_name.lower():
                score += 0.3

            if target and gt.name == target.name:
                score += 0.2  # prefer the hinted table

            if score > best_score:
                best_score = score
                best = {"table": gt.name, "column": col_name}

    conn.close()

    if not best or best_score < 0.1:
        return None

    # Build the SQL expression
    table, column = best["table"], best["column"]
    expr = ""
    english = filter_desc

    if filter_type == "date":
        from tools.jit.quantifier_extractor import extract_date_ranges
        from datetime import date
        ranges = extract_date_ranges(filter_value or filter_desc,
                                      reference_date=date.today())
        if ranges:
            dr = ranges[0]
            expr = (f"{table}.{column} >= '{dr.start_date}' "
                    f"AND {table}.{column} <= '{dr.end_date}'")
            english = f"{column}: {dr.start_date} to {dr.end_date}"
        else:
            expr = f"-- date filter on {table}.{column}: {filter_value}"

    elif filter_type == "threshold":
        from tools.jit.quantifier_extractor import extract_quantifiers
        quants = extract_quantifiers(filter_value or filter_desc)
        if quants:
            q = quants[0]
            expr = f"{table}.{column} {q.operator} {q.value}"
            english = f"{column} {q.operator} {q.value}"

    elif filter_type == "status":
        # Search learned terms for status mappings
        for key, term in ctx.learned_terms.items():
            if filter_value and filter_value.lower() in [
                a.lower() for a in term.get("aliases", [])] + [term.get("term", "").lower()]:
                for ft, fe in term.get("filters", {}).items():
                    if ft.upper() == table.upper():
                        expr = f"{ft}.{fe}" if "." not in fe else fe
                        english = f"{term.get('term', key)}"
                        break
        if not expr:
            expr = f"-- status filter on {table}.{column}: {filter_value}"

    elif filter_type == "value" and filter_value:
        expr = f"{table}.{column} = '{filter_value}'"
        english = f"{column} = {filter_value}"

    if not expr:
        expr = f"-- filter: {filter_desc}"

    return {"table": table, "column": column, "expression": expr, "english": english}


def apply_proposal(proposal: dict, graph_tables: list[GraphTable]):
    """Add proposal's tables to graph (merge if table exists)."""
    for t in proposal.get("tables_to_add", []):
        existing = next((gt for gt in graph_tables if gt.name == t.name), None)
        if existing:
            for f in t.filters:
                if not any(ef["expression"] == f["expression"] for ef in existing.filters):
                    existing.filters.append(f)
        else:
            graph_tables.append(t)
    conn = get_db()
    execute_graph_counts(graph_tables, conn)
    conn.close()
    if proposal.get("defn_name"):
        validate_definition(proposal["defn_name"], user_id="sunny")


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### Browse")
    cat = st.selectbox("Catalog", ["Reports", "Definitions", "Schema"],
                        label_visibility="collapsed")
    bq = (st.text_input("Filter", placeholder="Type to filter...",
                         label_visibility="collapsed", key="bf") or "").lower()

    if cat == "Reports":
        for r in ctx.report_searcher.reports:
            if bq and bq not in r["report_name"].lower():
                continue
            with st.expander(r["report_name"]):
                st.caption(r.get("primary_purpose", ""))
                st.write(r.get("description", ""))
    elif cat == "Definitions":
        from tools.jit.mock.mock_definitions import load_definition_glossary
        for d in load_definition_glossary():
            lbl = d.get("label", d["definition_name"])
            if bq and bq not in lbl.lower():
                continue
            with st.expander(lbl):
                st.write(d.get("description", ""))
    elif cat == "Schema":
        from tools.jit.mock.mock_technical import load_technical_glossary
        for dn, dom in load_technical_glossary().get("domains", {}).items():
            if bq and bq not in dn:
                continue
            with st.expander(dn.title()):
                for a in dom.get("anchor_tables", []):
                    st.markdown(f"**{a['name']}** — {a.get('description', '')[:60]}")

    if st.session_state["graph_tables"]:
        st.markdown("---")
        st.markdown("### Actions")
        if st.button("Show SQL", use_container_width=True):
            st.code(build_graph_sql(st.session_state["graph_tables"]), language="sql")
        if st.button("Save as definition", use_container_width=True):
            st.session_state["_show_save"] = True
        if st.button("Start over", use_container_width=True):
            reset()
            st.cache_resource.clear()
            st.rerun()

        if st.session_state.get("_show_save"):
            from tools.jit.flywheel import create_definition_from_graph, save_to_user_library
            sl = st.text_input("Name", key="sv_l")
            sd = st.text_area("Description", height=60, key="sv_d")
            dm = st.selectbox("Domain", ["diagnosis", "encounters", "medications",
                "procedures", "billing", "referrals"], key="sv_dm")
            if st.button("Save", type="primary", key="sv_go") and sl.strip():
                gt = st.session_state["graph_tables"]
                nd = create_definition_from_graph(gt, sl.strip(), sd.strip(), dm, "sunny")
                conn = get_db()
                fc = execute_count(conn, build_graph_sql(gt))
                conn.close()
                save_to_user_library(sl.strip(), [nd["definition_name"]],
                    build_graph_sql(gt), fc, "sunny")
                st.session_state["_show_save"] = False
                st.success(f"Saved **{sl}**")

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

st.markdown("## Business Logic Assistant")

graph_tables = st.session_state["graph_tables"]

# Conversation
for msg in st.session_state["messages"]:
    with st.chat_message("user" if msg["role"] == "user" else "assistant"):
        st.write(msg["text"])

# Pending proposals (HITL)
if st.session_state["pending_proposals"]:
    with st.chat_message("assistant"):
        st.write("Which approach should I use?")
        for pi, prop in enumerate(st.session_state["pending_proposals"]):
            with st.container(border=True):
                ci, ca = st.columns([5, 1])
                with ci:
                    st.markdown(f"**{prop['description']}**")
                    if prop.get("detail"):
                        st.caption(prop["detail"])
                with ca:
                    if st.button("Use", key=f"ap_{pi}", type="primary",
                                 use_container_width=True):
                        apply_proposal(prop, graph_tables)
                        st.session_state["graph_tables"] = graph_tables
                        st.session_state["messages"].append(
                            {"role": "system", "text": f"Added: {prop['description']}"})
                        st.session_state["pending_proposals"] = []
                        st.rerun()
        if st.button("None of these"):
            st.session_state["pending_proposals"] = []
            st.session_state["messages"].append(
                {"role": "system", "text": "OK — try describing it differently."})
            st.rerun()

# ---------------------------------------------------------------------------
# Graph + detail + sample data
# ---------------------------------------------------------------------------

if graph_tables:
    from streamlit_agraph import agraph, Node, Edge, Config

    conn = get_db()
    if any(t.row_count is None for t in graph_tables):
        execute_graph_counts(graph_tables, conn)
        st.session_state["graph_tables"] = graph_tables

    final_sql = build_graph_sql(graph_tables)
    try:
        final_count = execute_count(conn, final_sql)
    except Exception:
        final_count = None

    col_graph, col_detail = st.columns([3, 2])

    with col_graph:
        nodes, edges = [], []
        for t in graph_tables:
            cnt = f"{t.row_count:,}" if t.row_count is not None else "?"
            nf = sum(1 for f in t.filters if f.get("active"))
            lbl = f"{t.name}\n{cnt}"
            if nf:
                lbl += f"\n[{nf} filter{'s' if nf > 1 else ''}]"
            nodes.append(Node(id=t.name, label=lbl, size=28,
                color="#3b82f6" if t.is_root else "#93c5fd", shape="box",
                font={"color": "#1e293b", "size": 11}))
            if t.join_from:
                edges.append(Edge(source=t.join_from, target=t.name,
                    title=t.join_column or "", color="#475569", width=2))

        clicked = agraph(nodes=nodes, edges=edges,
            config=Config(width=550, height=350, directed=True,
                          physics=True, hierarchical=False))

    with col_detail:
        active = re.sub(r'^r\d+_', '', clicked) if clicked else (
            graph_tables[0].name if graph_tables else None)
        t = next((t for t in graph_tables if t.name == active), None) if active else None

        if t:
            st.markdown(f"### {t.name}")
            st.metric("Rows (filtered)", f"{t.row_count:,}" if t.row_count else "?")

            if t.join_from:
                st.caption(f"Joined from {t.join_from}")
                jt = st.radio("Join", ["JOIN", "LEFT JOIN"],
                    index=["JOIN", "LEFT JOIN"].index(
                        t.join_type if t.join_type in ["JOIN", "LEFT JOIN"] else "JOIN"),
                    horizontal=True, key=f"jt_{t.name}")
                if jt != (t.join_type or "JOIN"):
                    t.join_type = jt
                    execute_graph_counts(graph_tables, conn)
                    st.session_state["graph_tables"] = graph_tables
                    st.rerun()

            if t.filters:
                st.markdown("**Filters:**")
                changed = False
                for fi, f in enumerate(t.filters):
                    v = st.checkbox(f.get("english", f["expression"]),
                                     value=f.get("active", True), key=f"f_{t.name}_{fi}")
                    if v != f.get("active", True):
                        f["active"] = v
                        changed = True
                if changed:
                    execute_graph_counts(graph_tables, conn)
                    st.session_state["graph_tables"] = graph_tables
                    st.rerun()

            st.caption("Use chat to add filters: e.g., 'filter to this year'")

    # Sample data — always visible
    if len(graph_tables) >= 2:
        TABLE_COLS = {
            "PATIENT": ["PAT_ID", "PAT_NAME"], "PROBLEM_LIST": ["PROBLEM_LIST_ID", "RESOLVED_DATE"],
            "CLARITY_EDG": ["DX_NAME", "CURRENT_ICD10_LIST"],
            "PAT_ENC": ["PAT_ENC_CSN_ID", "CONTACT_DATE", "APPT_STATUS_C"],
            "PAT_ENC_DX": ["LINE", "DX_ID"], "PAT_ENC_HSP": ["HOSP_ADMSN_TIME", "HOSP_DISCH_TIME"],
            "HSP_ACCOUNT": ["HSP_ACCOUNT_ID", "TOT_CHGS"],
            "HSP_ACCT_DX_LIST": ["LINE", "DX_ID"], "CLARITY_DEP": ["DEPARTMENT_NAME", "SPECIALTY"],
            "ORDER_MED": ["ORDER_MED_ID", "MEDICATION_ID"], "REFERRAL": ["REFERRAL_ID", "ENTRY_DATE"],
        }
        parts = []
        for gt in graph_tables:
            for c in TABLE_COLS.get(gt.name, []):
                parts.append(f"{gt.name}.{c} AS [{gt.name}.{c}]")
        base = build_graph_sql(graph_tables)
        sql = base.replace("SELECT COUNT(*)", "SELECT " + ", ".join(parts or ["*"]), 1)
        sql += "\nORDER BY PATIENT.PAT_ID\nLIMIT 10"
        try:
            res = execute_sql(conn, sql)
            df = pd.DataFrame(res["rows"], columns=res["columns"])
            fc_str = f"{final_count:,}" if final_count else "?"
            st.caption(f"Sample data ({fc_str} total rows)")
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(str(e))

    conn.close()

# ---------------------------------------------------------------------------
# Chat input
# ---------------------------------------------------------------------------

user_input = st.chat_input(
    "What would you like to know?" if not graph_tables else "Add more detail...")

if user_input:
    st.session_state["messages"].append({"role": "user", "text": user_input})

    # Initialize graph with PATIENT if empty
    if not graph_tables:
        conn = get_db()
        graph_tables.append(GraphTable(
            name="PATIENT", join_from=None, join_column="", pk_column="",
            join_direction="", filters=[], is_root=True))
        execute_graph_counts(graph_tables, conn)
        conn.close()
        st.session_state["graph_tables"] = graph_tables

    # Ask LLM to classify intent + extract concepts
    graph_state = [{"name": gt.name, "join_from": gt.join_from,
                     "filters": [f["english"] for f in gt.filters if f.get("active")],
                     "row_count": gt.row_count} for gt in graph_tables]

    llm_result = classify_and_extract(
        user_input, graph_state,
        build_definitions_summary(), build_terms_summary())

    action = llm_result.get("action", "question")
    explanation = llm_result.get("explanation", "")
    concepts = llm_result.get("concepts", [])

    if action == "add_concept" and concepts:
        # Graph-driven: find tables/routes for these concepts
        proposals = resolve_concept_to_proposals(concepts, graph_tables)
        if proposals:
            st.session_state["messages"].append(
                {"role": "system", "text": explanation})
            st.session_state["pending_proposals"] = proposals
        else:
            st.session_state["messages"].append(
                {"role": "system",
                 "text": f"{explanation}\n\nI couldn't find matching tables for "
                         f"{', '.join(concepts)}. Try describing differently."})

    elif action == "add_filter":
        # Schema-driven: find the right column
        filt = resolve_filter_to_column(
            llm_result.get("filter_description", ""),
            llm_result.get("filter_type"),
            llm_result.get("filter_value", ""),
            llm_result.get("table_hint", ""),
            graph_tables)

        if filt and not filt["expression"].startswith("--"):
            target = next((gt for gt in graph_tables
                            if gt.name == filt["table"]), None)
            if target:
                target.filters.append({"expression": filt["expression"],
                                        "english": filt["english"], "active": True})
                conn = get_db()
                execute_graph_counts(graph_tables, conn)
                conn.close()
                st.session_state["graph_tables"] = graph_tables
                st.session_state["messages"].append(
                    {"role": "system",
                     "text": f"{explanation}\n\nAdded filter to **{filt['table']}**: {filt['english']}"})
            else:
                st.session_state["messages"].append(
                    {"role": "system", "text": f"{explanation}\n\nTable not in query yet."})
        else:
            st.session_state["messages"].append(
                {"role": "system",
                 "text": f"{explanation}\n\nI couldn't determine the exact column. "
                         f"Try mentioning the table or column name."})

    elif action == "remove":
        table_hint = llm_result.get("table_hint", "")
        removed = next((gt for gt in graph_tables
                         if gt.name.upper() == table_hint.upper() and not gt.is_root), None)
        if removed:
            graph_tables.remove(removed)
            deps = [gt for gt in graph_tables if gt.join_from == removed.name]
            for d in deps:
                graph_tables.remove(d)
            conn = get_db()
            execute_graph_counts(graph_tables, conn)
            conn.close()
            st.session_state["graph_tables"] = graph_tables
            st.session_state["messages"].append(
                {"role": "system", "text": f"Removed **{removed.name}**."})
        else:
            st.session_state["messages"].append(
                {"role": "system", "text": f"{explanation}\n\nSpecify which table to remove."})

    else:  # question or unclear
        st.session_state["messages"].append(
            {"role": "system", "text": explanation})

    st.rerun()
