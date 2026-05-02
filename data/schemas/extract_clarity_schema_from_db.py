"""Connect to Epic Clarity from a Fabric notebook and extract a schema YAML
in the format Tool 3 (business_logic_extractor) consumes.

This file is a notebook-shaped Python script. Each `# %%` block below is
intended as ONE Fabric notebook cell -- copy-paste each into a separate
cell in your Fabric notebook, run them in order. Cell 1 is a connection
test; if it fails the rest can't proceed.

Sibling utility to data/schemas/csv_to_schema.py (which converts a CSV
metadata export into the same schema format).

Background:
    Tool 3 (business_logic_extractor) produces English definitions for
    each output column of a SQL view. Without a schema, definitions are
    mechanical abbreviation expansions (e.g. CVG_EFF_DT -> "Cvg Eff Dt").
    With a schema YAML carrying real Clarity descriptions, definitions
    become "Coverage effective date" or whatever the data dictionary says.

Most likely failure mode:
    Fabric notebooks run in Microsoft's cloud; Clarity is on-prem. The
    bridge is an On-Premises Data Gateway. If Cell 1 fails with a network
    timeout, that's the issue -- ask your Fabric admin to set one up.
    Fallback: run the metadata query in SSMS yourself, export to CSV,
    feed through csv_to_schema.py.
"""


# %% [Cell 1: Test connectivity to Clarity]

# === EDIT THESE ===
CLARITY_SERVER = "your-clarity-sqlserver-hostname-or-ip"   # e.g. "clarity-prod.your-org.local"
CLARITY_DATABASE = "Clarity"                                # the Clarity database name
CLARITY_AUTH = "windows"                                    # "windows" or "sql"
CLARITY_USER = ""                                            # only if CLARITY_AUTH == "sql"
CLARITY_PASSWORD = ""                                        # only if CLARITY_AUTH == "sql"
# ====================

# %pip install pyodbc                                         # uncomment on first run
import pyodbc

drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
print(f"Available ODBC drivers: {drivers}")
if not drivers:
    print("No SQL Server ODBC driver found. Fabric notebooks usually have one,")
    print("but if not, ask your admin to add the Microsoft ODBC driver.")
    raise SystemExit
driver = drivers[-1]   # latest version typically

if CLARITY_AUTH == "windows":
    conn_str = (f"DRIVER={{{driver}}};"
                f"SERVER={CLARITY_SERVER};DATABASE={CLARITY_DATABASE};"
                f"Trusted_Connection=yes;")
else:
    conn_str = (f"DRIVER={{{driver}}};"
                f"SERVER={CLARITY_SERVER};DATABASE={CLARITY_DATABASE};"
                f"UID={CLARITY_USER};PWD={CLARITY_PASSWORD};")

masked = conn_str.replace(CLARITY_PASSWORD, "****") if CLARITY_PASSWORD else conn_str
print(f"\nConnection string (password masked): {masked}")
print("\nAttempting connection...")
try:
    conn = pyodbc.connect(conn_str, timeout=15)
    cur = conn.cursor()
    cur.execute("SELECT @@VERSION")
    version = cur.fetchone()[0]
    print(f"Connected. Server reports:\n  {version[:200]}")
    cur.execute("SELECT COUNT(*) FROM sys.tables WHERE name LIKE 'CLARITY[_]%'")
    n = cur.fetchone()[0]
    print(f"\nFound {n} CLARITY_* metadata tables.")
    if n == 0:
        print("Warning: no CLARITY_TBL / CLARITY_COL tables found in this database.")
        print("You may be connected to a different DB, or your account may lack permission.")
    conn.close()
except pyodbc.OperationalError as e:
    print(f"\nFAILED to connect: {e}")
    print("\nMost common causes:")
    print("  1. Fabric notebook can't reach your on-prem Clarity server.")
    print("     -> Set up an On-Premises Data Gateway in your Fabric workspace.")
    print("     -> Confirm with your IT / Fabric admin.")
    print("  2. Wrong server name. Try the FQDN or IP from your usual SSMS connection.")
    print("  3. Firewall blocks port 1433 (default SQL Server port).")
    print("  4. Auth: 'windows' only works if Fabric is on the same domain.")
    print("     If not, use SQL auth with a service account.")
except Exception as e:
    print(f"\nUnexpected error: {type(e).__name__}: {e}")


# %% [Cell 2: Extract metadata for tables your views reference]

import pandas as pd

# Read the table list from Tool 1's manifest (so we only query metadata for
# tables your views actually touch, not all of Clarity's thousands).
manifest_path = '/lakehouse/default/Files/outputs/column_lineage_extractor.csv'
try:
    manifest = pd.read_csv(manifest_path)
    tables_needed = sorted(manifest['referenced_table'].dropna().unique().tolist())
    tables_needed = [t for t in tables_needed if t and t.strip()]
    print(f"Read {len(tables_needed)} unique tables from {manifest_path}")
except FileNotFoundError:
    # No manifest yet -- hard-code the list (edit this for your view set):
    tables_needed = [
        'COVERAGE', 'COVERAGE_MEMBER_LIST', 'PATIENT', 'PLAN_GRP_BEN_PLAN',
        'CLARITY_EPP', 'CLARITY_LOB', 'VALID_PATIENT', 'CVG_LOC_PCP',
        'CLARITY_LOC', 'CLARITY_SER', 'ZC_SUBSC_RACE', 'CVG_SUBSCR_ADDR',
        'ZC_TAX_STATE',
    ]
    print(f"No manifest at {manifest_path}; using hard-coded list of "
          f"{len(tables_needed)} tables. Edit this cell to extend.")

print(f"First few: {', '.join(tables_needed[:10])}"
      f"{'...' if len(tables_needed) > 10 else ''}")

# Build a parameterized IN clause
placeholders = ", ".join(["?"] * len(tables_needed))
sql = f"""
SELECT
    c.TABLE_NAME,
    t.DESCRIPTION              AS TABLE_DESCRIPTION,
    c.COLUMN_NAME,
    c.DESCRIPTION              AS COLUMN_DESCRIPTION,
    c.DATA_TYPE,
    c.LENGTH,
    c.IS_NULLABLE,
    c.IS_PRIMARY_KEY,
    c.ITEM_NAMES               AS INI_ITEM
FROM CLARITY_COL c
LEFT JOIN CLARITY_TBL t ON c.TABLE_NAME = t.TABLE_NAME
WHERE c.TABLE_NAME IN ({placeholders})
ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
"""

conn = pyodbc.connect(conn_str)
df = pd.read_sql(sql, conn, params=tables_needed)
conn.close()
print(f"\nFetched {len(df)} column-level metadata rows.")
print(df.head(10))

# If `c.DESCRIPTION` isn't the right column name for your Clarity install,
# inspect with: pyodbc.connect(conn_str).cursor().columns(table='CLARITY_COL').fetchall()
# Common alternatives: DESCRIPT, COLUMN_DESC.


# %% [Cell 3: Write the schema YAML in Tool 3's format]

import yaml
from pathlib import Path

SCHEMA_OUT = '/lakehouse/default/Files/schemas/clarity_schema_from_db.yaml'
Path(SCHEMA_OUT).parent.mkdir(parents=True, exist_ok=True)

# Group rows by table -- output format matches the existing
# data/schemas/clarity_schema.yaml shape: top-level `tables:` list, each
# table has name + description + columns list.
tables_dict = {}
for _, row in df.iterrows():
    t = row['TABLE_NAME']
    if t not in tables_dict:
        tables_dict[t] = {
            'name': t,
            'data_source': 'Epic Clarity',
            'description': (row['TABLE_DESCRIPTION'] or '').strip(),
            'columns': [],
        }
    col_entry = {
        'name': row['COLUMN_NAME'],
        'type': (f"{row['DATA_TYPE']}({row['LENGTH']})"
                  if row['LENGTH'] else row['DATA_TYPE']),
        'description': (row['COLUMN_DESCRIPTION'] or '').strip(),
    }
    if row.get('INI_ITEM'):
        col_entry['ini_item'] = row['INI_ITEM']
    if row.get('IS_PRIMARY_KEY') == 1:
        col_entry['primary_key'] = True
    tables_dict[t]['columns'].append(col_entry)

schema = {'tables': list(tables_dict.values())}

with open(SCHEMA_OUT, 'w') as f:
    yaml.safe_dump(schema, f, sort_keys=False, allow_unicode=True,
                    default_flow_style=False)

print(f"Wrote schema to: {SCHEMA_OUT}")
print(f"  Tables:  {len(schema['tables'])}")
print(f"  Columns: {sum(len(t['columns']) for t in schema['tables'])}")

with open(SCHEMA_OUT) as f:
    print("\n--- First 20 lines ---")
    for line in f.readlines()[:20]:
        print(line, end='')


# %% [Cell 4: Re-run Tool 3 with the new schema, see if English improves]

import sys
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

from tools.business_logic_extractor.batch import build_business_logic
build_business_logic(
    input_dir='/lakehouse/default/Files/test_one_view',
    schema_path='/lakehouse/default/Files/schemas/clarity_schema_from_db.yaml',
    output_csv='/lakehouse/default/Files/outputs/business_logic_extractor_with_schema.csv',
    use_llm=False,
    dialect='tsql',
)

import pandas as pd
df = pd.read_csv('/lakehouse/default/Files/outputs/business_logic_extractor_with_schema.csv')
print(df[['column_name', 'english_definition']].head(20))

# For columns the schema covers, you should now see real definitions like
# "Coverage effective date" instead of "Cvg Eff Dt".
# For columns NOT in the schema, the english_definition falls back to
# mechanical abbreviation expansion -- those are your data-dictionary backlog.
