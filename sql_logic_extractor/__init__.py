"""SQL Business Logic Extractor -- the engine that powers the 4 tools.

Required files (Tools 1-4):
    __init__.py, license.py, products.py
    extract.py (L1), normalize.py (L2), resolve.py (L3)
    translate.py (L4 -- needed for Tool 3 engineered mode), patterns/

Optional / archived submodules (compare.py, batch.py, collibra.py, metadata.py)
have been moved to docs/archive/sql_logic_extractor_extras/ -- they are not
used by Tools 1-4. The try/except blocks below quietly skip them if the
files happen to be present in some other deployment."""

from .extract import SQLBusinessLogicExtractor, to_dict
from .normalize import (
    BusinessLogicNormalizer,
    extract_definitions,
    definitions_to_dict,
    BusinessDefinition,
)
from .resolve import resolve_query, resolved_to_dict, ResolvedQuery

# The 4 commercial product functions (Tools 1-4) and their license module.
# Public entry points always go through these; CLI/HTTP wrappers are thin
# adapters in the tools/ subpackages.
from .products import (
    extract_columns,
    extract_technical_lineage,
    extract_business_logic,
    generate_report_description,
    ColumnIdentifier,
    ColumnInventory,
    TechnicalLineage,
    BusinessLogic,
    ReportDescription,
)
from .license import LicenseError, require_feature, current_license

# Optional L4+ surfaces -- present only when the corresponding submodules
# AND their dependencies (yaml, openpyxl, google-genai, etc.) are installed.
# A failure here doesn't block the L1-L3 lineage path.
try:
    from .compare import BusinessLogicComparator, report_to_dict
except ImportError:
    pass
try:
    from .translate import translate_query, translate_resolved
except ImportError:
    pass
try:
    from .collibra import export_collibra, CollibraConfig, glossary_csv, lineage_json, dictionary_csv
except ImportError:
    pass
try:
    from .batch import batch_process, BatchResult
except ImportError:
    pass

__version__ = "0.1.0"
