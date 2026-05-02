"""Fabric notebook script -- run all 4 SQL Logic Extractor tools on a folder of views.

Each `# %%` block is ONE notebook cell. Copy each into a separate cell.
Notebook lives in the Workspace; attach lakehouse SZ_SQL_Logic so paths
under /lakehouse/default/Files/ resolve.

Expected lakehouse layout (create these folders via Files -> New subfolder):
    /lakehouse/default/Files/
        views/                              <- drop your .sql view files here
        schemas/clarity_schema.json         <- from csv_to_schema.py (optional)
        sql-business-logic-extractor/       <- the unzipped repo (see Cell 2)
        outputs/                            <- generated CSVs land here
"""


# %% [Cell 1: install Python deps]

# Run once per kernel session. Fabric pip can be slow -- be patient.
%pip install sqlglot pyyaml


# %% [Cell 2: get the repo onto the lakehouse]
#
# If raw.githubusercontent.com is blocked at your org (it is at many),
# do this manually from your laptop:
#
#   1. Open https://github.com/sunnyzhengai/sql-business-logic-extractor
#   2. Click the green "Code" button -> "Download ZIP"
#   3. Unzip locally; you'll get a folder `sql-business-logic-extractor-main/`
#   4. In the Fabric Lakehouse Files explorer, upload the WHOLE folder
#      (rename it to `sql-business-logic-extractor` to match the path below)
#      Files -> right-click -> Upload -> Upload folder
#
# After upload, verify:

import os
REPO_ROOT = '/lakehouse/default/Files/sql-business-logic-extractor'
print(f"Repo present? {os.path.isdir(REPO_ROOT)}")
print(f"  sql_logic_extractor/ ? {os.path.isdir(REPO_ROOT + '/sql_logic_extractor')}")
print(f"  tools/                ? {os.path.isdir(REPO_ROOT + '/tools')}")


# %% [Cell 3: add the repo to sys.path so imports work]

import sys
REPO_ROOT = '/lakehouse/default/Files/sql-business-logic-extractor'
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Drop any cached modules from a previous run (so re-uploaded files actually
# reload). Safe to run repeatedly.
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

from tools.batch_all import run_all
print("Imports OK.")


# %% [Cell 4: run all four tools, single resolver pass per view]

run_all(
    input_dir='/lakehouse/default/Files/views',
    output_dir='/lakehouse/default/Files/outputs',
    schema_path='/lakehouse/default/Files/schemas/clarity_schema.json',  # set None if you don't have one yet
    use_llm=False,
    dialect='tsql',
)

# Outputs land in /lakehouse/default/Files/outputs/ as:
#   column_lineage_extractor.csv       (Tool 1)
#   technical_logic_extractor.csv      (Tool 2)
#   business_logic_extractor.csv       (Tool 3)  <- has english_definition
#   report_description_generator.csv   (Tool 4)


# %% [Cell 5: peek at Tool 3 output to see the schema's effect]

import pandas as pd

df = pd.read_csv('/lakehouse/default/Files/outputs/business_logic_extractor.csv')
print(f"Rows: {len(df)}")
print(df[['view_name', 'column_name', 'english_definition']].head(30))


# %% [Cell 6 (optional): re-run a single view with LLM mode]
# Uses Gemini; requires an API key set in the environment. Leave commented
# out unless you've configured one.
#
# import os
# os.environ['GOOGLE_API_KEY'] = '...'
# run_all(
#     input_dir='/lakehouse/default/Files/test_one_view',
#     output_dir='/lakehouse/default/Files/outputs_llm',
#     schema_path='/lakehouse/default/Files/schemas/clarity_schema.json',
#     use_llm=True,
#     dialect='tsql',
# )
