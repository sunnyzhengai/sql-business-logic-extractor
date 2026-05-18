"""Fabric notebook -- run the graph-pivot pipeline end to end.

This notebook walks through the five pipeline phases + the validation
diagnostic introduced by the 2026-05 restructure. Each `# %%` block is
ONE notebook cell -- copy them into separate cells in your Fabric
notebook, or open this file as a notebook directly if your editor
recognizes the `# %%` cell markers.

Pipeline (run in order):
  Step 1  p10_extract           SQL files -> corpus.jsonl
  Step 2  p20_index.term_extract  corpus -> terms.json + terms.csv
  Step 3  p40_synthesize.cohort_extract  corpus -> cohorts.md / .json
  Step 4  p40_synthesize.dataset_extract corpus -> datasets.md / .json
  Step 5  operate.validate_graph_pivot   corpus -> graph.html + communities.md
                                                   + per-community HTMLs
                                                   + validation_report.md

You can run Step 5 alone -- it exercises the whole production pipeline
(graph build -> projection -> bridges -> communities -> primary-community
-> per-community HTMLs -> markdown summary -> verdict). Steps 2-4 are
standalone deliverables (terms, cohorts, datasets) that some audiences
will want in isolation.

Notebook lives in your Fabric workspace; attach the lakehouse where you
put the views + the cloned repo so paths under /lakehouse/default/Files/
resolve.

Expected lakehouse layout (REPO_ROOT directly under Files/):
    /lakehouse/default/Files/
        tools/                              <- this repo's tools/ folder
        sql_logic_extractor/                <- this repo's parser engine
        data/                               <- this repo's data/
        views/                              <- drop your .sql view files here
        schemas/clarity_schema.json         <- optional, from csv_to_schema.py
        outputs/                            <- generated artifacts land here

If you uploaded the repo as a wrapped folder (e.g., the GitHub ZIP
unzips to `sql-business-logic-extractor-main/`), see Cell 3 for how to
adjust REPO_ROOT to point at the wrapping folder instead.
"""


# %% [Cell 1: install Python dependencies]

# Run once per kernel session. Fabric pip can be slow -- be patient.
# - sqlglot:   SQL parser, used by p10_extract
# - pyyaml:    schema loader, used by p40_synthesize.cohort_extract
# - pyvis:     interactive HTML rendering, used by p50_present + validate_graph_pivot

%pip install sqlglot pyyaml pyvis


# %% [Cell 2: verify the repo is uploaded]
#
# If raw.githubusercontent.com is blocked at your org, upload manually:
#   1. Open https://github.com/sunnyzhengai/sql-business-logic-extractor
#   2. Click "Code" -> "Download ZIP"
#   3. Unzip locally
#   4. In Fabric Lakehouse Files explorer: upload the contents.
#
# Two layouts are common:
#
#   (a) Repo CONTENTS at the lakehouse Files/ root -- tools/, sql_logic_extractor/,
#       data/, etc. sit directly under /lakehouse/default/Files/. This is what
#       the notebook uses by default (simpler; no extra path segment).
#
#   (b) Repo wrapped in a subfolder -- if you uploaded the ZIP-unzipped folder
#       as-is, it may sit at /lakehouse/default/Files/sql-business-logic-extractor/
#       (or .../sql-business-logic-extractor-main/ if you didn't rename it).
#       In that case, change REPO_ROOT in Cell 3 to include the subfolder.
#
# This cell lists what's at the Files/ root so you can see which layout you have.

import os

LAKEHOUSE_FILES = '/lakehouse/default/Files'
print(f"Contents of {LAKEHOUSE_FILES}:")
for name in sorted(os.listdir(LAKEHOUSE_FILES)):
    marker = " (DIR)" if os.path.isdir(f"{LAKEHOUSE_FILES}/{name}") else ""
    print(f"  {name}{marker}")

# Quick diagnostic: do we see `tools` at the root, or only nested in a wrapper folder?
direct = os.path.isdir(f"{LAKEHOUSE_FILES}/tools")
wrapped = os.path.isdir(f"{LAKEHOUSE_FILES}/sql-business-logic-extractor/tools")
print()
print(f"tools/ at Files/ root?               {direct}    <- layout (a)")
print(f"tools/ at .../sql-business-logic-extractor/? {wrapped}    <- layout (b)")


# %% [Cell 3: add the repo to sys.path so imports work]

import sys

# Default: layout (a) -- repo contents at the lakehouse Files/ root.
REPO_ROOT = '/lakehouse/default/Files'

# If you have layout (b) instead (repo wrapped in a subfolder), use one of:
# REPO_ROOT = '/lakehouse/default/Files/sql-business-logic-extractor'
# REPO_ROOT = '/lakehouse/default/Files/sql-business-logic-extractor-main'

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Drop any cached modules from a previous run (so re-uploaded files actually
# reload). Safe to run repeatedly. We sweep the whole `tools.` and
# `sql_logic_extractor.` namespaces, plus shared/.
for mod in list(sys.modules):
    if mod.startswith('tools') or mod.startswith('sql_logic_extractor'):
        del sys.modules[mod]

# Sanity-check that the new structure imports cleanly.
from tools.p10_extract.batch import extract_corpus
from tools.p20_index.term_extraction import extract_corpus_terms
from tools.p40_synthesize.cohort_extract import extract_cohorts
from tools.p40_synthesize.dataset_extract import extract_datasets
from tools.operate.validate_graph_pivot import run_validation
print("Imports OK -- pipeline modules resolved.")


# %% [Cell 4: Step 1 -- parse SQL files into corpus.jsonl]
#
# Produces /lakehouse/default/Files/outputs/corpus.jsonl, which every
# downstream step consumes. Skip this cell if you already have a corpus.jsonl
# from a previous run (the file format is stable across phases of the restructure).

extract_corpus(
    input_dir='/lakehouse/default/Files/views',
    output_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    schema_path='/lakehouse/default/Files/schemas/clarity_schema.json',  # set to None if you don't have one
)
# Watch /lakehouse/default/Files/outputs/corpus_progress.txt mid-run for
# a per-view timing log (tail-able from the Fabric file viewer).


# %% [Cell 5: Step 2 -- extract terms (lexical anchors)]
#
# Walks the views and emits Term records (one per qualifying output column).
# Used downstream by p30_analyze for naming-collision discovery -- but also
# a standalone deliverable: "what business terms appear across this corpus?"

extract_corpus_terms(
    input_dir='/lakehouse/default/Files/views',
    output_path='/lakehouse/default/Files/outputs/terms.json',
    # Add all_scopes=True to also get CTE-internal terms (default: main only).
)
# Outputs: terms.json (full records) + terms.csv (flattened for spreadsheet review).


# %% [Cell 6: Step 3 -- render cohorts in English]
#
# For each scope of each view, produces a population-level English
# description ("Active members with at least one outpatient encounter
# in 2024"). The steward-facing artifact for "what population does this
# view define?"

extract_cohorts(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/cohorts',
)
# Outputs: cohorts.md (steward-readable) + cohorts.json (structured).


# %% [Cell 7: Step 4 -- render datasets (per-scope dataflow chains)]
#
# Each view as a chain of "datasets" -- one per scope (CTE / derived /
# subquery / main). Each dataset has English column descriptions + filters.
# Complements cohorts: cohorts = "WHO", datasets = "WHAT FLOWS WHERE".

extract_datasets(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/datasets',
)
# Outputs: datasets.md (one section per view) + datasets.json (structured).


# %% [Cell 8: Step 5 -- run the validation diagnostic (the orchestrator)]
#
# This is the BIG one. Loads corpus -> builds graph -> projects to tables ->
# auto-detects bridge tables (PATIENT, CLARITY_*, etc.) -> runs Louvain
# community detection -> assigns each view to a primary community ->
# identifies cross-domain views -> emits per-community HTMLs + markdown
# summary + verdict.
#
# Exercises every production pipeline phase end-to-end. Output is the
# governance evidence pack stewards will work from.

result = run_validation(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/graph_pivot_validation',
    resolution=1.0,           # try 0.5 for fewer / broader communities;
                              # try 1.5 for more / finer communities.
    bridge_percentile=90.0,   # top 10% by degree get classified as bridges.
    exclude_patterns=None,    # defaults: collibra / metadata / catalog / ingest +
                              # views reading from sys.* / INFORMATION_SCHEMA
)
print(f"\nSummary:")
print(f"  total views ingested:       {result['n_views_total']}")
print(f"  business views (kept):      {result['n_views_business']}")
print(f"  infrastructure (excluded):  {result['n_views_excluded']}")
print(f"  distinct tables:            {result['n_tables']}")
print(f"  bridge tables:              {result['n_bridge_tables']}")
print(f"  communities found:          {result['n_communities']}")
print(f"  cross-domain views:         {result['n_cross_domain_views']}")


# %% [Cell 9: peek at the validation report]

print(open('/lakehouse/default/Files/outputs/graph_pivot_validation/validation_report.md').read())


# %% [Cell 10 (optional): re-run at a different Louvain resolution to compare]
#
# Lower resolution (0.5) -> fewer, larger communities -- closer to "Epic module" level.
# Higher resolution (1.5) -> more, smaller communities -- closer to "sub-domain" level.
# Useful when the default 1.0 result looks too fine or too coarse for your steward audience.

result_broad = run_validation(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/graph_pivot_broad',
    resolution=0.5,
)
print(f"Broad: {result_broad['n_communities']} communities "
      f"(vs default: {result['n_communities']})")


# %% [Cell 11 (optional): operations sidecar tools]
#
# These do not participate in the pipeline -- they are run on demand by
# BI devs / admins to triage parser health, identify slow views, etc.

# Parser-health triage: classify each .sql as clean / needs_rule / unknown_failure.
# Run BEFORE Step 1 on a new corpus to know what fraction is processable.
from tools.operate.preflight_check import preflight

preflight(
    input_dir='/lakehouse/default/Files/views',
    output_dir='/lakehouse/default/Files/outputs/preflight',
)

# Inventory manifest: emits used-table / used-ZC / used-column lists from
# corpus.jsonl. Paste-ready SQL VALUES clauses for narrowing the SSMS
# metadata extracts (extract_clarity_metadata.sql, extract_zc_values.sql).
from tools.operate.inventory_manifest import build_inventory_manifest

build_inventory_manifest(
    corpus_path='/lakehouse/default/Files/outputs/corpus.jsonl',
    output_dir='/lakehouse/default/Files/outputs/inventory',
)
