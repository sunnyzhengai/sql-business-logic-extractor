# %% Cell 1: Force reload + verify file is updated
# Run this FIRST after kernel restart, before any other cells.
import importlib
import sql_logic_extractor.proc_normalize as _pn
importlib.reload(_pn)

# Verify the fixes are loaded
print(f"Has _strip_bare_header: {hasattr(_pn, '_strip_bare_header')}")
print(f"Has _SAFE_CMD_PREFIXES check: hasattr check not possible, checking source...")

# Check the actual file on disk
with open("/lakehouse/default/Files/sql_logic_extractor/proc_normalize.py") as f:
    content = f.read()
print(f"File size: {len(content)} chars")
print(f"Contains _strip_bare_header: {'_strip_bare_header' in content}")
print(f"Contains SAFE_CMD_PREFIXES: {'SAFE_CMD_PREFIXES' in content}")

# Re-import with fresh code
from sql_logic_extractor.proc_normalize import select_into_to_cte, ProcNotViewShaped
print("\nReload complete — now run diagnose_skipped_files.py cells")
