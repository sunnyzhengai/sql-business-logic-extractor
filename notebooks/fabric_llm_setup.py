"""One-time Fabric setup to enable LLM-mode view/proc descriptions.

Run this in a Fabric notebook to wire up the provider-neutral LLM adapter
(Azure OpenAI / OpenAI / Gemini) and prove the whole chain works -- code
imports, the vendor SDK is installed, and the API key authenticates -- with
ONE tiny live call before you point it at a real view.

The file is split into two cells. Paste each into its own Fabric cell:

  Cell 1 (%pip): installs the dependencies. Run it ALONE and let it finish;
                 Fabric may restart the Python session right after, so
                 anything below a %pip in the same cell would not run.

  Cell 2: edits the 3 constants, adds the repo to the import path, sets the
          provider env vars, imports the package, and does a smoke-test call.

DATA SAFETY: this path is metadata-only -- it sends table/column names,
descriptions, and query logic, never patient rows. See the wiki concept page
wiki/concepts/metadata-only-no-phi.md for the governance posture.
"""


# %% [Cell 1: install dependencies -- run ALONE, let it finish/restart]
# (This is a notebook magic; paste it into its own Fabric cell.)
#
#     %pip install sqlglot openai pyyaml python-dotenv
#
# Use %pip (not !pip): %pip installs into the whole notebook session, !pip
# only installs on the driver node and the import below would then fail.


# %% [Cell 2: setup + smoke test -- edit the 3 constants, then run]

# ============================================================
# EDIT THESE 3 THINGS
# ============================================================
REPO_DIR = "/lakehouse/default/Files/<your_repo_folder>"  # folder holding sql_logic_extractor/ and tools/
PROVIDER = "openai"                                        # "openai" (personal key) | "azure" (work) | "gemini"
API_KEY = "sk-...PASTE_YOUR_KEY..."                        # the key set up for you
# ============================================================

import os
import sys

# 1) Make the repo importable. No `pip install -e` needed -- adding REPO_DIR
#    to sys.path sidesteps editable-install quirks on the Lakehouse mount.
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

# 2) Point the provider-neutral adapter at your chosen backend via env vars.
#    make_llm_client() reads SLE_LLM_PROVIDER (or auto-detects from whichever
#    credentials are present) and pulls the key from the matching env var.
os.environ["SLE_LLM_PROVIDER"] = PROVIDER
if PROVIDER == "openai":
    os.environ["OPENAI_API_KEY"] = API_KEY
    os.environ.setdefault("OPENAI_MODEL", "gpt-4o-mini")  # cheap + good for descriptions
elif PROVIDER == "azure":
    os.environ["AZURE_OPENAI_API_KEY"] = API_KEY
    # EDIT both of these for your Azure resource:
    os.environ["AZURE_OPENAI_ENDPOINT"] = "https://<your-resource>.openai.azure.com/"
    # NOTE: deployment NAME from the Azure portal, NOT a public model id like 'gpt-4o'.
    os.environ["AZURE_OPENAI_DEPLOYMENT"] = "<your-deployment-name>"
elif PROVIDER == "gemini":
    os.environ["GEMINI_API_KEY"] = API_KEY
else:
    raise ValueError(f"Unknown PROVIDER {PROVIDER!r}: use 'openai', 'azure', or 'gemini'.")

# 3) Confirm the code imports -- this proves REPO_DIR points at the right folder.
import sql_logic_extractor  # noqa: E402
import tools  # noqa: E402

print(f"✓ repo importable from {REPO_DIR}")

# 4) Build the client and make ONE tiny live call to prove the key works.
from sql_logic_extractor.llm_client import make_llm_client  # noqa: E402

client = make_llm_client()  # reads SLE_LLM_PROVIDER + the key from env
result = client.complete_json(
    "You output JSON only.",
    'Reply with this JSON exactly: {"status": "ok"}',
)
print(f"✓ {PROVIDER} LLM responded: {result}")
print("\nAll set -- ready to run a real view/proc description.")


# %% [Cell 3 (optional): the payoff -- engineered vs LLM, side by side]
# Once Cell 2 prints {'status': 'ok'}, run the demo on a real view. Paste
# into its own cell and point it at one of your .sql files:
#
#     !python3 {REPO_DIR}/notebooks/demo_llm_vs_engineered.py {REPO_DIR}/<path-to-a-view>.sql
