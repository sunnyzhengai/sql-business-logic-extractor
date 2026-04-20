#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- Neo4j Chat (Streamlit Web App)

A web-based chat interface for querying Neo4j lineage data using natural language.

Usage:
    streamlit run neo4j_chat_app.py

    # Or with custom port
    streamlit run neo4j_chat_app.py --server.port 8502

Requires:
    pip install streamlit neo4j openai
"""

import os
import streamlit as st
from neo4j import GraphDatabase
from openai import OpenAI

# Page config
st.set_page_config(
    page_title="SQL Lineage Chat",
    page_icon="🔍",
    layout="wide"
)

# Schema description for the LLM
GRAPH_SCHEMA = """
Neo4j Graph Schema for SQL Lineage Data:

NODES:
- (:Table {tableId, name})
  - Source database tables (e.g., HSP_ACCOUNT, PATIENT)

- (:Column {columnId, name, table})
  - Source columns from tables
  - columnId format: "TABLE_NAME.COLUMN_NAME"

- (:Report {reportId, name})
  - SQL reports/queries that produce output

- (:OutputColumn {outputColumnId, name, report, expression, businessDefinition, businessDomain, columnType, filterCount})
  - Output columns produced by reports
  - outputColumnId format: "report_name:column_name"
  - columnType: "passthrough", "calculated", "case", "aggregation", etc.
  - expression: the SQL/technical expression that defines this output
  - businessDefinition: plain English description of what this column means
  - businessDomain: business area (e.g., "Hospital Metrics", "Financial")

RELATIONSHIPS:
- (Column)-[:BELONGS_TO]->(Table)
  - Links columns to their parent tables

- (OutputColumn)-[:DERIVED_FROM {transformation}]->(Column)
  - Links output columns to their source columns
  - transformation: type of transformation applied

- (Report)-[:OUTPUTS]->(OutputColumn)
  - Links reports to the columns they produce

COMMON QUERIES:
- Find outputs from a table: Table <- Column <- OutputColumn <- Report
- Find conflicting definitions: OutputColumns with same name but different reports
- Trace lineage: Follow DERIVED_FROM relationships backward
"""

SYSTEM_PROMPT = f"""You are a helpful assistant that converts natural language questions into Neo4j Cypher queries.

{GRAPH_SCHEMA}

Rules:
1. Return ONLY the Cypher query, no explanations
2. Use MATCH patterns that follow the schema exactly
3. For "show me everything" or overview questions, limit results to avoid overwhelming output
4. Property names are case-sensitive: use tableId, columnId, reportId, outputColumnId
5. Table and column names in the data are UPPERCASE (e.g., 'HSP_ACCOUNT', 'PATIENT')
6. Report names are lowercase with underscores (e.g., 'report_billing', 'report_quality')

Examples:
Q: "Show me all tables"
A: MATCH (t:Table) RETURN t.name AS table_name ORDER BY t.name

Q: "What columns come from the PATIENT table?"
A: MATCH (c:Column)-[:BELONGS_TO]->(t:Table {{name: 'PATIENT'}}) RETURN c.name AS column_name

Q: "Which reports use the HSP_ACCOUNT table?"
A: MATCH (t:Table {{name: 'HSP_ACCOUNT'}})<-[:BELONGS_TO]-(c:Column)<-[:DERIVED_FROM]-(o:OutputColumn)<-[:OUTPUTS]-(r:Report) RETURN DISTINCT r.name AS report

Q: "Show me conflicting definitions"
A: MATCH (o1:OutputColumn)<-[:OUTPUTS]-(r1:Report), (o2:OutputColumn)<-[:OUTPUTS]-(r2:Report) WHERE o1.name = o2.name AND r1 <> r2 AND r1.name < r2.name RETURN o1.name AS column_name, r1.name AS report_1, o1.expression AS definition_1, r2.name AS report_2, o2.expression AS definition_2 ORDER BY o1.name

Q: "What is length_of_stay derived from?"
A: MATCH (o:OutputColumn)-[:DERIVED_FROM]->(c:Column) WHERE o.name = 'length_of_stay' RETURN o.report AS report, o.name AS column, o.businessDefinition AS business_definition, o.expression AS technical_definition, collect(c.columnId) AS source_columns

Q: "What is length_of_stay"
A: MATCH (o:OutputColumn) WHERE o.name = 'length_of_stay' RETURN o.report AS report, o.name AS column, o.businessDefinition AS business_definition, o.expression AS technical_definition ORDER BY o.report

Q: "Show me all output columns"
A: MATCH (o:OutputColumn) RETURN o.name AS column, o.report AS report, o.businessDomain AS domain, o.businessDefinition AS business_definition ORDER BY o.name LIMIT 50

Q: "Compare definitions of length_of_stay across reports"
A: MATCH (o:OutputColumn) WHERE o.name = 'length_of_stay' RETURN o.report AS report, o.businessDefinition AS business_definition, o.expression AS technical_definition ORDER BY o.report

IMPORTANT: Always return specific properties with readable aliases, never return raw nodes (n) or (o). Use aliases like "AS column", "AS report", "AS business_definition", "AS technical_definition". When showing definitions, include BOTH businessDefinition and expression.
"""


@st.cache_resource
def get_neo4j_driver(uri: str, user: str, password: str):
    """Get cached Neo4j driver."""
    return GraphDatabase.driver(uri, auth=(user, password))


def nl_to_cypher(client: OpenAI, question: str) -> str:
    """Convert natural language question to Cypher query."""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question}
        ],
        temperature=0,
        max_tokens=500
    )

    cypher = response.choices[0].message.content.strip()
    # Remove markdown code blocks if present
    if cypher.startswith("```"):
        lines = cypher.split("\n")
        cypher = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    return cypher


def serialize_neo4j_value(value):
    """Convert Neo4j types to clean, readable Python types."""
    if hasattr(value, 'labels'):  # It's a Node - extract key properties only
        props = dict(value.items())
        # Return a clean summary string for nodes
        if 'name' in props:
            return props['name']
        elif 'expression' in props:
            return props.get('name', '') or props.get('expression', str(props))
        return str(props)
    elif hasattr(value, 'type'):  # Relationship
        return value.type
    elif isinstance(value, list):
        return [serialize_neo4j_value(v) for v in value]
    elif hasattr(value, 'items'):  # dict-like
        return dict(value)
    else:
        return value


def execute_cypher(driver, cypher: str) -> list:
    """Execute Cypher query and return results."""
    with driver.session() as session:
        result = session.run(cypher)
        results = []
        for record in result:
            row = {}
            for key, value in record.items():
                row[key] = serialize_neo4j_value(value)
            results.append(row)
        return results


def format_results_for_display(results: list) -> list:
    """Format results for better display - flatten nested dicts, truncate long strings."""
    formatted = []
    for row in results:
        new_row = {}
        for key, value in row.items():
            if isinstance(value, dict):
                # Flatten dict to string
                new_row[key] = ", ".join(f"{k}: {v}" for k, v in value.items())
            elif isinstance(value, list):
                # Join list items
                new_row[key] = ", ".join(str(v) for v in value)
            elif isinstance(value, str) and len(value) > 200:
                # Truncate long expressions but keep them readable
                new_row[key] = value
            else:
                new_row[key] = value
        formatted.append(new_row)
    return formatted


def get_graph_stats(driver) -> dict:
    """Get basic graph statistics."""
    stats = {}
    with driver.session() as session:
        for label in ['Table', 'Column', 'Report', 'OutputColumn']:
            result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
            stats[label] = result.single()['count']
    return stats


# Sidebar configuration
with st.sidebar:
    st.header("⚙️ Configuration")

    neo4j_uri = st.text_input("Neo4j URI", value="bolt://localhost:7687")
    neo4j_user = st.text_input("Neo4j User", value="neo4j")
    neo4j_password = st.text_input("Neo4j Password", value="password", type="password")

    st.divider()

    openai_key = st.text_input(
        "OpenAI API Key",
        value=os.environ.get('OPENAI_API_KEY', ''),
        type="password"
    )

    st.divider()

    # Test connection button
    if st.button("🔌 Test Connection"):
        try:
            driver = get_neo4j_driver(neo4j_uri, neo4j_user, neo4j_password)
            stats = get_graph_stats(driver)
            st.success("Connected!")
            st.json(stats)
        except Exception as e:
            st.error(f"Connection failed: {e}")

    st.divider()

    st.markdown("""
    ### 💡 Example Questions
    - Show me all tables
    - Which reports use PATIENT?
    - What is length_of_stay derived from?
    - Show conflicting definitions
    - How many columns per report?
    - What's the lineage of readmission_flag?
    """)

# Main content
st.title("🔍 SQL Lineage Chat")
st.markdown("Ask questions about your SQL lineage data in natural language.")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "cypher" in message:
            with st.expander("View Cypher Query"):
                st.code(message["cypher"], language="cypher")
        if "dataframe" in message:
            st.dataframe(message["dataframe"], use_container_width=True)

# Chat input
if prompt := st.chat_input("Ask a question about your SQL lineage..."):
    # Check for API key
    if not openai_key:
        st.error("Please enter your OpenAI API key in the sidebar.")
        st.stop()

    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generate response
    with st.chat_message("assistant"):
        try:
            # Initialize clients
            client = OpenAI(api_key=openai_key)
            driver = get_neo4j_driver(neo4j_uri, neo4j_user, neo4j_password)

            # Check for direct Cypher
            if prompt.lower().startswith("cypher:"):
                cypher = prompt[7:].strip()
                st.info("Running direct Cypher query...")
            else:
                # Convert NL to Cypher
                with st.spinner("Translating to Cypher..."):
                    cypher = nl_to_cypher(client, prompt)

            # Show the Cypher query
            with st.expander("View Cypher Query", expanded=True):
                st.code(cypher, language="cypher")

            # Execute query
            with st.spinner("Querying Neo4j..."):
                results = execute_cypher(driver, cypher)

            # Display results
            if results:
                import pandas as pd
                # Format results for cleaner display
                formatted_results = format_results_for_display(results)
                df = pd.DataFrame(formatted_results)

                # Rename columns to be more readable (remove underscores, title case)
                df.columns = [col.replace('_', ' ').title() for col in df.columns]

                st.success(f"Found {len(results)} result(s)")

                # Use a styled dataframe with scrolling and word wrap
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    height=min(400, 50 + len(df) * 35),  # Dynamic height with max
                )

                # Save to session state
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"Found {len(results)} result(s)",
                    "cypher": cypher,
                    "dataframe": df
                })
            else:
                st.warning("No results found.")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": "No results found.",
                    "cypher": cypher
                })

        except Exception as e:
            error_msg = f"Error: {str(e)}"
            st.error(error_msg)
            st.session_state.messages.append({
                "role": "assistant",
                "content": error_msg
            })

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: gray; font-size: 0.8em;">
    SQL Business Logic Extractor | Neo4j Lineage Explorer
</div>
""", unsafe_allow_html=True)
