"""SQL Business Logic Extractor -- parse, normalize, compare, resolve, and translate SQL."""

from .extract import SQLBusinessLogicExtractor, to_dict
from .normalize import (
    BusinessLogicNormalizer,
    extract_definitions,
    definitions_to_dict,
    BusinessDefinition,
)
from .compare import BusinessLogicComparator, report_to_dict
from .resolve import resolve_query, resolved_to_dict, ResolvedQuery
from .translate import translate_query, translate_resolved

__version__ = "0.1.0"

__all__ = [
    "SQLBusinessLogicExtractor",
    "to_dict",
    "BusinessLogicNormalizer",
    "extract_definitions",
    "definitions_to_dict",
    "BusinessDefinition",
    "BusinessLogicComparator",
    "report_to_dict",
    "resolve_query",
    "resolved_to_dict",
    "ResolvedQuery",
    "translate_query",
    "translate_resolved",
]
