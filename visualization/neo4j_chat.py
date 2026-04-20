#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- Neo4j Natural Language Chat

A simple chatbox that converts natural language questions to Cypher queries
and displays results from Neo4j.

Usage:
    python3 neo4j_chat.py

    # With custom Neo4j connection
    python3 neo4j_chat.py --uri bolt://localhost:7687 --user neo4j --password password

Requires:
    pip install neo4j openai
"""

import os
import sys
from neo4j import GraphDatabase

# Try to load OpenAI
try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


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

- (:OutputColumn {outputColumnId, name, report, expression, columnType, filterCount})
  - Output columns produced by reports
  - outputColumnId format: "report_name:column_name"
  - columnType: "passthrough", "calculated", "case", "aggregation", etc.
  - expression: the SQL expression that defines this output

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
A: MATCH (t:Table) RETURN t.name ORDER BY t.name

Q: "What columns come from the PATIENT table?"
A: MATCH (c:Column)-[:BELONGS_TO]->(t:Table {{name: 'PATIENT'}}) RETURN c.name

Q: "Which reports use the HSP_ACCOUNT table?"
A: MATCH (t:Table {{name: 'HSP_ACCOUNT'}})<-[:BELONGS_TO]-(c:Column)<-[:DERIVED_FROM]-(o:OutputColumn)<-[:OUTPUTS]-(r:Report) RETURN DISTINCT r.name

Q: "Show me conflicting definitions"
A: MATCH (o1:OutputColumn)<-[:OUTPUTS]-(r1:Report), (o2:OutputColumn)<-[:OUTPUTS]-(r2:Report) WHERE o1.name = o2.name AND r1 <> r2 RETURN o1.name, r1.name, r2.name, o1.expression, o2.expression

Q: "What is length_of_stay derived from?"
A: MATCH (o:OutputColumn)-[:DERIVED_FROM]->(c:Column) WHERE o.name = 'length_of_stay' RETURN o.report, c.columnId, o.expression
"""


class Neo4jChat:
    def __init__(self, uri: str, user: str, password: str, api_key: str = None):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.api_key = api_key or os.environ.get('OPENAI_API_KEY')

        if not self.api_key:
            raise ValueError("OpenAI API key required. Set OPENAI_API_KEY env var or pass --api-key")

        if not HAS_OPENAI:
            raise ImportError("openai package not installed. Run: pip install openai")

        self.client = OpenAI(api_key=self.api_key)

    def close(self):
        self.driver.close()

    def nl_to_cypher(self, question: str) -> str:
        """Convert natural language question to Cypher query."""
        response = self.client.chat.completions.create(
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

    def execute_cypher(self, cypher: str) -> list:
        """Execute Cypher query and return results."""
        with self.driver.session() as session:
            result = session.run(cypher)
            return [dict(record) for record in result]

    def ask(self, question: str) -> dict:
        """Ask a natural language question and get results."""
        cypher = self.nl_to_cypher(question)

        try:
            results = self.execute_cypher(cypher)
            return {
                "question": question,
                "cypher": cypher,
                "results": results,
                "error": None
            }
        except Exception as e:
            return {
                "question": question,
                "cypher": cypher,
                "results": [],
                "error": str(e)
            }

    def format_results(self, results: list, max_rows: int = 20) -> str:
        """Format results as a readable table."""
        if not results:
            return "  (no results)"

        # Get all keys
        keys = list(results[0].keys())

        # Calculate column widths
        widths = {k: len(k) for k in keys}
        for row in results[:max_rows]:
            for k in keys:
                val = str(row.get(k, ''))[:50]  # Truncate long values
                widths[k] = max(widths[k], len(val))

        # Build table
        lines = []

        # Header
        header = " | ".join(k.ljust(widths[k]) for k in keys)
        lines.append(f"  {header}")
        lines.append("  " + "-+-".join("-" * widths[k] for k in keys))

        # Rows
        for row in results[:max_rows]:
            line = " | ".join(str(row.get(k, ''))[:50].ljust(widths[k]) for k in keys)
            lines.append(f"  {line}")

        if len(results) > max_rows:
            lines.append(f"  ... and {len(results) - max_rows} more rows")

        return "\n".join(lines)


def run_chat(uri: str, user: str, password: str, api_key: str = None):
    """Run interactive chat session."""
    print("=" * 60)
    print("Neo4j Natural Language Chat")
    print("=" * 60)
    print("\nAsk questions about your SQL lineage data in plain English.")
    print("Type 'quit' or 'exit' to end the session.")
    print("Type 'cypher: <query>' to run raw Cypher directly.")
    print("\nExample questions:")
    print("  - Show me all tables")
    print("  - Which reports use the PATIENT table?")
    print("  - What is length_of_stay derived from?")
    print("  - Show me conflicting definitions")
    print("  - How many output columns does each report have?")
    print()

    try:
        chat = Neo4jChat(uri, user, password, api_key)
    except Exception as e:
        print(f"Error connecting: {e}")
        return

    try:
        while True:
            try:
                question = input("\n> ").strip()
            except EOFError:
                break

            if not question:
                continue

            if question.lower() in ('quit', 'exit', 'q'):
                print("Goodbye!")
                break

            # Direct Cypher mode
            if question.lower().startswith('cypher:'):
                cypher = question[7:].strip()
                print(f"\n[Cypher] {cypher}")
                try:
                    results = chat.execute_cypher(cypher)
                    print(f"\n{len(results)} result(s):")
                    print(chat.format_results(results))
                except Exception as e:
                    print(f"\nError: {e}")
                continue

            # Natural language mode
            print("\nThinking...")
            response = chat.ask(question)

            print(f"\n[Cypher] {response['cypher']}")

            if response['error']:
                print(f"\nError: {response['error']}")
            else:
                print(f"\n{len(response['results'])} result(s):")
                print(chat.format_results(response['results']))

    finally:
        chat.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Natural language chat interface for Neo4j lineage data"
    )
    parser.add_argument("--uri", default="bolt://localhost:7687",
                        help="Neo4j bolt URI (default: bolt://localhost:7687)")
    parser.add_argument("--user", default="neo4j",
                        help="Neo4j username (default: neo4j)")
    parser.add_argument("--password", default="password",
                        help="Neo4j password (default: password)")
    parser.add_argument("--api-key",
                        help="OpenAI API key (or set OPENAI_API_KEY env var)")

    args = parser.parse_args()

    run_chat(args.uri, args.user, args.password, args.api_key)


if __name__ == "__main__":
    main()
