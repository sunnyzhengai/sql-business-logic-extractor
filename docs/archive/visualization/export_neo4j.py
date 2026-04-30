#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL Business Logic Extractor -- Neo4j Export

Exports L3 lineage data to Neo4j CSV format for graph visualization.

Generates:
  - tables.csv: Table nodes
  - columns.csv: Column nodes
  - reports.csv: Report nodes
  - output_columns.csv: Output column nodes (with expressions)
  - belongs_to.csv: Column → Table relationships
  - derived_from.csv: OutputColumn → Column relationships
  - outputs.csv: Report → OutputColumn relationships

Usage:
    python3 export_neo4j.py ./l3_json_folder/ --output ./neo4j_import/

    # Then import to Neo4j:
    neo4j-admin database import full \\
      --nodes=Table=neo4j_import/tables.csv \\
      --nodes=Column=neo4j_import/columns.csv \\
      --nodes=Report=neo4j_import/reports.csv \\
      --nodes=OutputColumn=neo4j_import/output_columns.csv \\
      --relationships=BELONGS_TO=neo4j_import/belongs_to.csv \\
      --relationships=DERIVED_FROM=neo4j_import/derived_from.csv \\
      --relationships=OUTPUTS=neo4j_import/outputs.csv \\
      neo4j
"""

import csv
import glob
import json
import os
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class GraphData:
    """Collected graph data from all L3 files."""
    tables: dict = field(default_factory=dict)  # table_name -> {description}
    columns: dict = field(default_factory=dict)  # table.column -> {table, name}
    reports: dict = field(default_factory=dict)  # report_name -> {name}
    output_columns: list = field(default_factory=list)  # [{id, name, report, expression, ...}]
    derived_from: list = field(default_factory=list)  # [{start, end, transformation}]
    belongs_to: list = field(default_factory=list)  # [{start, end}]
    outputs: list = field(default_factory=list)  # [{start, end}]


def parse_l3_file(l3_path: str, graph: GraphData):
    """Parse a single L3 or L4 JSON file and add to graph data.

    L4 files contain business definitions (english_definition) in addition to technical definitions.
    """
    with open(l3_path, 'r') as f:
        data = json.load(f)

    # Extract report name from filename
    filename = os.path.basename(l3_path)
    # Handle _L3.json, _L4.json, _L5.json naming
    report_name = filename.replace('_L3.json', '').replace('_L4.json', '').replace('_L5.json', '').replace('.json', '')

    # Add report node
    graph.reports[report_name] = {'name': report_name}

    # Process each column
    for col in data.get('columns', []):
        # L4 format uses column_name, L3 uses name
        col_name = col.get('column_name') or col.get('name', 'unknown')
        col_type = col.get('column_type') or col.get('type', 'unknown')

        # Skip passthrough columns - they're just direct column references, not interesting
        if col_type == 'passthrough':
            continue

        # L4 format has technical_definition nested, L3 has it flat
        tech_def = col.get('technical_definition', {})
        expression = tech_def.get('resolved_expression') or col.get('resolved_expression', '')
        base_tables = tech_def.get('base_tables') or col.get('base_tables', [])
        base_columns = tech_def.get('base_columns') or col.get('base_columns', [])
        filters = tech_def.get('filters') or col.get('filters', [])

        # Business definition (L4 only)
        business_definition = col.get('english_definition', '')
        business_domain = col.get('business_domain', '')

        # Create unique ID for output column
        output_col_id = f"{report_name}:{col_name}"

        # Add output column node
        graph.output_columns.append({
            'id': output_col_id,
            'name': col_name,
            'report': report_name,
            'expression': expression[:500] if expression else '',  # Truncate long expressions
            'column_type': col_type,
            'filter_count': len(filters),
            'business_definition': business_definition[:500] if business_definition else '',
            'business_domain': business_domain,
        })

        # Add Report → OutputColumn relationship
        graph.outputs.append({
            'start': report_name,
            'end': output_col_id,
        })

        # Process base tables
        for table in base_tables:
            table_upper = table.upper()
            if table_upper not in graph.tables:
                graph.tables[table_upper] = {'name': table_upper}

        # Process base columns and create relationships
        for base_col in base_columns:
            if '.' in base_col:
                table, column = base_col.split('.', 1)
                table_upper = table.upper()
                col_id = f"{table_upper}.{column.upper()}"

                # Add table if not exists
                if table_upper not in graph.tables:
                    graph.tables[table_upper] = {'name': table_upper}

                # Add column node
                if col_id not in graph.columns:
                    graph.columns[col_id] = {
                        'name': column.upper(),
                        'table': table_upper,
                    }

                    # Add Column → Table relationship
                    graph.belongs_to.append({
                        'start': col_id,
                        'end': table_upper,
                    })

                # Add OutputColumn → Column relationship
                graph.derived_from.append({
                    'start': output_col_id,
                    'end': col_id,
                    'transformation': col_type,
                })


def write_neo4j_csvs(graph: GraphData, output_dir: str):
    """Write graph data to Neo4j CSV format."""
    os.makedirs(output_dir, exist_ok=True)

    # 1. tables.csv
    tables_path = os.path.join(output_dir, 'tables.csv')
    with open(tables_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['tableId:ID(Table)', 'name', ':LABEL'])
        for table_id, data in sorted(graph.tables.items()):
            writer.writerow([table_id, data['name'], 'Table'])
    print(f"  Written: {tables_path} ({len(graph.tables)} tables)")

    # 2. columns.csv
    columns_path = os.path.join(output_dir, 'columns.csv')
    with open(columns_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['columnId:ID(Column)', 'name', 'table', ':LABEL'])
        for col_id, data in sorted(graph.columns.items()):
            writer.writerow([col_id, data['name'], data['table'], 'Column'])
    print(f"  Written: {columns_path} ({len(graph.columns)} columns)")

    # 3. reports.csv
    reports_path = os.path.join(output_dir, 'reports.csv')
    with open(reports_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['reportId:ID(Report)', 'name', ':LABEL'])
        for report_id, data in sorted(graph.reports.items()):
            writer.writerow([report_id, data['name'], 'Report'])
    print(f"  Written: {reports_path} ({len(graph.reports)} reports)")

    # 4. output_columns.csv
    output_cols_path = os.path.join(output_dir, 'output_columns.csv')
    with open(output_cols_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['outputColumnId:ID(OutputColumn)', 'name', 'report', 'expression', 'businessDefinition', 'businessDomain', 'columnType', 'filterCount:int', ':LABEL'])
        for oc in graph.output_columns:
            # Escape expression for CSV
            expr = oc['expression'].replace('"', '""') if oc['expression'] else ''
            biz_def = oc.get('business_definition', '').replace('"', '""') if oc.get('business_definition') else ''
            writer.writerow([
                oc['id'],
                oc['name'],
                oc['report'],
                expr,
                biz_def,
                oc.get('business_domain', ''),
                oc['column_type'],
                oc['filter_count'],
                'OutputColumn'
            ])
    print(f"  Written: {output_cols_path} ({len(graph.output_columns)} output columns)")

    # 5. belongs_to.csv (Column → Table)
    belongs_to_path = os.path.join(output_dir, 'belongs_to.csv')
    with open(belongs_to_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([':START_ID(Column)', ':END_ID(Table)', ':TYPE'])
        for rel in graph.belongs_to:
            writer.writerow([rel['start'], rel['end'], 'BELONGS_TO'])
    print(f"  Written: {belongs_to_path} ({len(graph.belongs_to)} relationships)")

    # 6. derived_from.csv (OutputColumn → Column)
    derived_from_path = os.path.join(output_dir, 'derived_from.csv')
    with open(derived_from_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([':START_ID(OutputColumn)', ':END_ID(Column)', 'transformation', ':TYPE'])
        for rel in graph.derived_from:
            writer.writerow([rel['start'], rel['end'], rel['transformation'], 'DERIVED_FROM'])
    print(f"  Written: {derived_from_path} ({len(graph.derived_from)} relationships)")

    # 7. outputs.csv (Report → OutputColumn)
    outputs_path = os.path.join(output_dir, 'outputs.csv')
    with open(outputs_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([':START_ID(Report)', ':END_ID(OutputColumn)', ':TYPE'])
        for rel in graph.outputs:
            writer.writerow([rel['start'], rel['end'], 'OUTPUTS'])
    print(f"  Written: {outputs_path} ({len(graph.outputs)} relationships)")


def export_to_neo4j(input_path: str, output_dir: str):
    """Main export function.

    Args:
        input_path: Path to folder containing L3 JSON files, or single L3 JSON file
        output_dir: Output directory for Neo4j CSV files
    """
    # Find L3/L4 JSON files (prefer L4 since they have business definitions)
    if os.path.isfile(input_path):
        l3_files = [input_path]
    else:
        # First look for L4 files (have business definitions)
        l3_files = glob.glob(os.path.join(input_path, '*_L4.json'))
        l3_files += glob.glob(os.path.join(input_path, 'details', '*_L4.json'))

        # Fall back to L3/L5 if no L4 files found
        if not l3_files:
            l3_files = glob.glob(os.path.join(input_path, '*_L3.json'))
            l3_files += glob.glob(os.path.join(input_path, '*_L5.json'))
            l3_files += glob.glob(os.path.join(input_path, 'details', '*_L3.json'))
            l3_files += glob.glob(os.path.join(input_path, 'details', '*_L5.json'))

    if not l3_files:
        print(f"No L3/L4/L5 JSON files found in {input_path}")
        return

    print(f"Found {len(l3_files)} lineage files")

    # Collect graph data
    graph = GraphData()

    for l3_file in sorted(l3_files):
        filename = os.path.basename(l3_file)
        print(f"  Parsing: {filename}")
        parse_l3_file(l3_file, graph)

    # Write CSVs
    print(f"\nWriting Neo4j CSVs to: {output_dir}")
    write_neo4j_csvs(graph, output_dir)

    # Print summary
    print(f"\n" + "=" * 60)
    print("EXPORT COMPLETE")
    print("=" * 60)
    print(f"\nGraph Statistics:")
    print(f"  Tables:         {len(graph.tables)}")
    print(f"  Columns:        {len(graph.columns)}")
    print(f"  Reports:        {len(graph.reports)}")
    print(f"  Output Columns: {len(graph.output_columns)}")
    print(f"  Relationships:  {len(graph.belongs_to) + len(graph.derived_from) + len(graph.outputs)}")

    print(f"\nTo import into Neo4j:")
    print(f"""
  # Option 1: neo4j-admin import (for new database)
  neo4j-admin database import full \\
    --nodes=Table={output_dir}/tables.csv \\
    --nodes=Column={output_dir}/columns.csv \\
    --nodes=Report={output_dir}/reports.csv \\
    --nodes=OutputColumn={output_dir}/output_columns.csv \\
    --relationships={output_dir}/belongs_to.csv \\
    --relationships={output_dir}/derived_from.csv \\
    --relationships={output_dir}/outputs.csv \\
    neo4j

  # Option 2: LOAD CSV in Cypher (for existing database)
  # See generated cypher_import.txt for commands
""")

    # Also generate Cypher LOAD CSV commands
    cypher_path = os.path.join(output_dir, 'cypher_import.txt')
    with open(cypher_path, 'w') as f:
        f.write("// Neo4j Cypher commands to import CSV files\n")
        f.write("// Run these in Neo4j Browser or cypher-shell\n\n")

        f.write("// 1. Create constraints (run once)\n")
        f.write("CREATE CONSTRAINT table_id IF NOT EXISTS FOR (t:Table) REQUIRE t.tableId IS UNIQUE;\n")
        f.write("CREATE CONSTRAINT column_id IF NOT EXISTS FOR (c:Column) REQUIRE c.columnId IS UNIQUE;\n")
        f.write("CREATE CONSTRAINT report_id IF NOT EXISTS FOR (r:Report) REQUIRE r.reportId IS UNIQUE;\n")
        f.write("CREATE CONSTRAINT output_column_id IF NOT EXISTS FOR (o:OutputColumn) REQUIRE o.outputColumnId IS UNIQUE;\n\n")

        f.write("// 2. Load Tables\n")
        f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{output_dir}/tables.csv' AS row\n")
        f.write("CREATE (t:Table {tableId: row.`tableId:ID(Table)`, name: row.name});\n\n")

        f.write("// 3. Load Columns\n")
        f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{output_dir}/columns.csv' AS row\n")
        f.write("CREATE (c:Column {columnId: row.`columnId:ID(Column)`, name: row.name, table: row.table});\n\n")

        f.write("// 4. Load Reports\n")
        f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{output_dir}/reports.csv' AS row\n")
        f.write("CREATE (r:Report {reportId: row.`reportId:ID(Report)`, name: row.name});\n\n")

        f.write("// 5. Load Output Columns\n")
        f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{output_dir}/output_columns.csv' AS row\n")
        f.write("CREATE (o:OutputColumn {outputColumnId: row.`outputColumnId:ID(OutputColumn)`, name: row.name, report: row.report, expression: row.expression, columnType: row.columnType, filterCount: toInteger(row.`filterCount:int`)});\n\n")

        f.write("// 6. Create BELONGS_TO relationships (Column -> Table)\n")
        f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{output_dir}/belongs_to.csv' AS row\n")
        f.write("MATCH (c:Column {columnId: row.`:START_ID(Column)`})\n")
        f.write("MATCH (t:Table {tableId: row.`:END_ID(Table)`})\n")
        f.write("CREATE (c)-[:BELONGS_TO]->(t);\n\n")

        f.write("// 7. Create DERIVED_FROM relationships (OutputColumn -> Column)\n")
        f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{output_dir}/derived_from.csv' AS row\n")
        f.write("MATCH (o:OutputColumn {outputColumnId: row.`:START_ID(OutputColumn)`})\n")
        f.write("MATCH (c:Column {columnId: row.`:END_ID(Column)`})\n")
        f.write("CREATE (o)-[:DERIVED_FROM {transformation: row.transformation}]->(c);\n\n")

        f.write("// 8. Create OUTPUTS relationships (Report -> OutputColumn)\n")
        f.write(f"LOAD CSV WITH HEADERS FROM 'file:///{output_dir}/outputs.csv' AS row\n")
        f.write("MATCH (r:Report {reportId: row.`:START_ID(Report)`})\n")
        f.write("MATCH (o:OutputColumn {outputColumnId: row.`:END_ID(OutputColumn)`})\n")
        f.write("CREATE (r)-[:OUTPUTS]->(o);\n\n")

        f.write("// Sample queries:\n")
        f.write("// Find all outputs derived from a specific table\n")
        f.write("// MATCH (t:Table {name: 'PAT_ENC_HSP'})<-[:BELONGS_TO]-(c:Column)<-[:DERIVED_FROM]-(o:OutputColumn)<-[:OUTPUTS]-(r:Report)\n")
        f.write("// RETURN r.name, o.name, c.name;\n\n")

        f.write("// Find conflicting definitions (same output name from different reports)\n")
        f.write("// MATCH (o1:OutputColumn)<-[:OUTPUTS]-(r1:Report)\n")
        f.write("// MATCH (o2:OutputColumn)<-[:OUTPUTS]-(r2:Report)\n")
        f.write("// WHERE o1.name = o2.name AND r1 <> r2\n")
        f.write("// RETURN o1.name, r1.name, r2.name, o1.expression, o2.expression;\n")

    print(f"  Cypher import commands: {cypher_path}")


def import_to_neo4j_direct(graph: GraphData, uri: str, user: str, password: str, clear_existing: bool = False):
    """Import graph data directly to Neo4j using the Python driver.

    Args:
        graph: GraphData object with collected data
        uri: Neo4j bolt URI (e.g., bolt://localhost:7687)
        user: Neo4j username
        password: Neo4j password
        clear_existing: If True, delete existing lineage data before import
    """
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("ERROR: neo4j package not installed. Run: pip install neo4j")
        return False

    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with driver.session() as session:
            # Optionally clear existing data
            if clear_existing:
                print("  Clearing existing lineage data...")
                session.run("MATCH (n:Table) DETACH DELETE n")
                session.run("MATCH (n:Column) DETACH DELETE n")
                session.run("MATCH (n:Report) DETACH DELETE n")
                session.run("MATCH (n:OutputColumn) DETACH DELETE n")

            # Create constraints (idempotent)
            print("  Creating constraints...")
            constraints = [
                "CREATE CONSTRAINT table_id IF NOT EXISTS FOR (t:Table) REQUIRE t.tableId IS UNIQUE",
                "CREATE CONSTRAINT column_id IF NOT EXISTS FOR (c:Column) REQUIRE c.columnId IS UNIQUE",
                "CREATE CONSTRAINT report_id IF NOT EXISTS FOR (r:Report) REQUIRE r.reportId IS UNIQUE",
                "CREATE CONSTRAINT output_column_id IF NOT EXISTS FOR (o:OutputColumn) REQUIRE o.outputColumnId IS UNIQUE",
            ]
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception:
                    pass  # Constraint may already exist

            # Import Tables
            print(f"  Importing {len(graph.tables)} tables...")
            for table_id, data in graph.tables.items():
                session.run(
                    "MERGE (t:Table {tableId: $id}) SET t.name = $name",
                    id=table_id, name=data['name']
                )

            # Import Columns
            print(f"  Importing {len(graph.columns)} columns...")
            for col_id, data in graph.columns.items():
                session.run(
                    "MERGE (c:Column {columnId: $id}) SET c.name = $name, c.table = $table",
                    id=col_id, name=data['name'], table=data['table']
                )

            # Import Reports
            print(f"  Importing {len(graph.reports)} reports...")
            for report_id, data in graph.reports.items():
                session.run(
                    "MERGE (r:Report {reportId: $id}) SET r.name = $name",
                    id=report_id, name=data['name']
                )

            # Import Output Columns
            print(f"  Importing {len(graph.output_columns)} output columns...")
            for oc in graph.output_columns:
                session.run(
                    """MERGE (o:OutputColumn {outputColumnId: $id})
                       SET o.name = $name, o.report = $report, o.expression = $expression,
                           o.businessDefinition = $businessDefinition, o.businessDomain = $businessDomain,
                           o.columnType = $columnType, o.filterCount = $filterCount""",
                    id=oc['id'], name=oc['name'], report=oc['report'],
                    expression=oc['expression'][:500] if oc['expression'] else '',
                    businessDefinition=oc.get('business_definition', '')[:500] if oc.get('business_definition') else '',
                    businessDomain=oc.get('business_domain', ''),
                    columnType=oc['column_type'], filterCount=oc['filter_count']
                )

            # Create BELONGS_TO relationships (Column -> Table)
            print(f"  Creating {len(graph.belongs_to)} BELONGS_TO relationships...")
            for rel in graph.belongs_to:
                session.run(
                    """MATCH (c:Column {columnId: $start})
                       MATCH (t:Table {tableId: $end})
                       MERGE (c)-[:BELONGS_TO]->(t)""",
                    start=rel['start'], end=rel['end']
                )

            # Create DERIVED_FROM relationships (OutputColumn -> Column)
            print(f"  Creating {len(graph.derived_from)} DERIVED_FROM relationships...")
            for rel in graph.derived_from:
                session.run(
                    """MATCH (o:OutputColumn {outputColumnId: $start})
                       MATCH (c:Column {columnId: $end})
                       MERGE (o)-[r:DERIVED_FROM]->(c)
                       SET r.transformation = $transformation""",
                    start=rel['start'], end=rel['end'], transformation=rel['transformation']
                )

            # Create OUTPUTS relationships (Report -> OutputColumn)
            print(f"  Creating {len(graph.outputs)} OUTPUTS relationships...")
            for rel in graph.outputs:
                session.run(
                    """MATCH (r:Report {reportId: $start})
                       MATCH (o:OutputColumn {outputColumnId: $end})
                       MERGE (r)-[:OUTPUTS]->(o)""",
                    start=rel['start'], end=rel['end']
                )

            print("  Import complete!")
            return True

    finally:
        driver.close()


def export_neo4j_direct(input_path: str, uri: str, user: str, password: str, clear_existing: bool = False):
    """Parse L3/L4 files and import directly to Neo4j.

    Args:
        input_path: Path to folder containing L3/L4 JSON files, or single JSON file
        uri: Neo4j bolt URI
        user: Neo4j username
        password: Neo4j password
        clear_existing: If True, delete existing lineage data before import
    """
    # Find L3/L4 JSON files (prefer L4 since they have business definitions)
    if os.path.isfile(input_path):
        l3_files = [input_path]
    else:
        # First look for L4 files (have business definitions)
        l3_files = glob.glob(os.path.join(input_path, '*_L4.json'))
        l3_files += glob.glob(os.path.join(input_path, 'details', '*_L4.json'))

        # Fall back to L3/L5 if no L4 files found
        if not l3_files:
            l3_files = glob.glob(os.path.join(input_path, '*_L3.json'))
            l3_files += glob.glob(os.path.join(input_path, '*_L5.json'))
            l3_files += glob.glob(os.path.join(input_path, 'details', '*_L3.json'))
            l3_files += glob.glob(os.path.join(input_path, 'details', '*_L5.json'))

    if not l3_files:
        print(f"No L3/L4/L5 JSON files found in {input_path}")
        return

    print(f"Found {len(l3_files)} lineage files")

    # Collect graph data
    graph = GraphData()

    for l3_file in sorted(l3_files):
        filename = os.path.basename(l3_file)
        print(f"  Parsing: {filename}")
        parse_l3_file(l3_file, graph)

    # Import to Neo4j
    print(f"\nImporting to Neo4j at {uri}...")
    import_to_neo4j_direct(graph, uri, user, password, clear_existing)

    # Print summary
    print(f"\n" + "=" * 60)
    print("IMPORT COMPLETE")
    print("=" * 60)
    print(f"\nGraph Statistics:")
    print(f"  Tables:         {len(graph.tables)}")
    print(f"  Columns:        {len(graph.columns)}")
    print(f"  Reports:        {len(graph.reports)}")
    print(f"  Output Columns: {len(graph.output_columns)}")
    print(f"  Relationships:  {len(graph.belongs_to) + len(graph.derived_from) + len(graph.outputs)}")

    print(f"\nSample queries to try in Neo4j Browser:")
    print("""
  // View all reports and their outputs
  MATCH (r:Report)-[:OUTPUTS]->(o:OutputColumn)
  RETURN r.name, collect(o.name) AS outputs

  // Find all outputs derived from a specific table
  MATCH (t:Table {name: 'HSP_ACCOUNT'})<-[:BELONGS_TO]-(c:Column)<-[:DERIVED_FROM]-(o:OutputColumn)<-[:OUTPUTS]-(r:Report)
  RETURN r.name AS report, o.name AS output, c.name AS source_column

  // Find conflicting definitions (same output name from different reports)
  MATCH (o1:OutputColumn)<-[:OUTPUTS]-(r1:Report)
  MATCH (o2:OutputColumn)<-[:OUTPUTS]-(r2:Report)
  WHERE o1.name = o2.name AND r1 <> r2
  RETURN o1.name, r1.name, r2.name, o1.expression, o2.expression
""")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Export L3 lineage data to Neo4j (CSV files or direct import)"
    )
    parser.add_argument("input", help="Folder containing L3 JSON files, or single L3 JSON file")
    parser.add_argument("--output", "-o", default="neo4j_import",
                        help="Output directory for Neo4j CSV files (default: neo4j_import)")
    parser.add_argument("--direct", action="store_true",
                        help="Import directly to Neo4j instead of generating CSV files")
    parser.add_argument("--uri", default="bolt://localhost:7687",
                        help="Neo4j bolt URI (default: bolt://localhost:7687)")
    parser.add_argument("--user", default="neo4j",
                        help="Neo4j username (default: neo4j)")
    parser.add_argument("--password", default="password",
                        help="Neo4j password (default: password)")
    parser.add_argument("--clear", action="store_true",
                        help="Clear existing lineage data before import (only with --direct)")

    args = parser.parse_args()

    if args.direct:
        export_neo4j_direct(args.input, args.uri, args.user, args.password, args.clear)
    else:
        export_to_neo4j(args.input, args.output)


if __name__ == "__main__":
    main()
