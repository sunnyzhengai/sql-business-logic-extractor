"""Base-case patterns: column references, literals, aliases, star, identifiers."""

import re

from sqlglot import exp

from .base import Context, Translation
from .registry import register


# Ported from offline_translate.py. Fallback expansion applied when a column
# is not found in the schema -- produces a best-guess English name from the
# conventional Epic-Clarity naming fragments.
_ABBREVIATIONS = {
    "ADMSN": "Admission", "ADM": "Admission", "DISCH": "Discharge",
    "PAT": "Patient", "ENC": "Encounter", "HSP": "Hospital", "HOSP": "Hospital",
    "ACCT": "Account", "DX": "Diagnosis", "PROC": "Procedure", "MED": "Medication",
    "ORD": "Order", "DEPT": "Department", "LOC": "Location",
    "SER": "Service/Provider", "PROV": "Provider", "APPT": "Appointment",
    "SCHED": "Scheduled", "CSN": "Contact Serial Number",
    "MRN": "Medical Record Number", "DOB": "Date of Birth",
    "LOS": "Length of Stay", "ED": "Emergency Department", "IP": "Inpatient",
    "OP": "Outpatient", "OBS": "Observation", "ICU": "Intensive Care Unit",
    "ADT": "Admit/Discharge/Transfer", "HX": "History", "TX": "Treatment",
    "RX": "Prescription", "PX": "Procedure", "FIN": "Financial",
    "INS": "Insurance", "AUTH": "Authorization", "REF": "Referral",
    "XFER": "Transfer", "TRANS": "Transaction", "AMT": "Amount",
    "QTY": "Quantity", "CNT": "Count", "NUM": "Number", "DT": "Date",
    "TM": "Time", "DTTM": "Date/Time", "YR": "Year", "MTH": "Month",
    "STAT": "Status", "CAT": "Category", "CLS": "Class", "TYP": "Type",
    "CD": "Code", "ID": "Identifier", "DESC": "Description", "NM": "Name",
    "ADDR": "Address", "PH": "Phone", "FAX": "Fax", "ZIP": "ZIP Code",
    "ST": "State", "CTY": "City", "CNTRY": "Country",
    "SUBSC": "Subscriber", "SUBSCR": "Subscriber",
    "CVG": "Coverage", "CVGEPT": "Coverage Patient",
    "BEN": "Benefit", "EPP": "Employer/Plan/Payor", "EPT": "Employer/Plan",
}


def _expand_abbreviations(name: str) -> str:
    parts = name.upper().split("_")
    return " ".join(_ABBREVIATIONS.get(p, p.title()) for p in parts)


def _table_index(schema: dict) -> dict:
    """Build a cached {TABLE_NAME: {COL_NAME: {description, short_description,
    ini, item}}} index.

    Supports the Clarity-metadata-derived schema shape (with ini/item on
    each column) and the hand-curated shape (description only). Cached on
    the schema dict itself to amortize across lookups.

    `short_description` is the user-curated concise label sourced from
    the `short_description` column in clarity_metadata.csv. When present
    it's preferred over `description` for column English translation.
    """
    idx = schema.get("__table_index__")
    if idx is not None:
        return idx
    idx = {}
    for t in schema.get("tables", []):
        tname = (t.get("name") or "").upper()
        cols = {}
        for c in t.get("columns", []) or []:
            cname = (c.get("name") or "").upper()
            entry = {
                "description": c.get("description") or c.get("name"),
                "short_description": c.get("short_description"),
                "ini": c.get("ini"),
                "item": c.get("item"),
            }
            cols[cname] = entry
        idx[tname] = cols
    schema["__table_index__"] = idx
    return idx


def _clean_description(desc: str) -> str:
    """Normalize schema descriptions for readable prose embedding.
    - Strip trailing punctuation (periods, whitespace).
    - Sentence-case ALL-CAPS descriptions (auto-extracted Clarity metadata).
    Leaves well-cased descriptions alone."""
    if not desc:
        return desc
    s = desc.rstrip(". \t")
    if s and any(c.isalpha() for c in s) and not any(c.islower() for c in s):
        # All caps -> sentence case: first letter up, rest lower
        s = s[0] + s[1:].lower()
    return s


def _lookup_column(schema: dict, table: str | None, column: str) -> dict | None:
    """Return the schema entry {description, ini, item} for a column, or None."""
    if not schema:
        return None
    idx = _table_index(schema)
    col_upper = column.upper()
    if table:
        cols = idx.get(table.upper())
        if cols and col_upper in cols:
            return cols[col_upper]
    for cols in idx.values():
        if col_upper in cols:
            return cols[col_upper]
    return None


def _ini_item_key(entry: dict) -> str | None:
    ini = entry.get("ini")
    item = entry.get("item")
    if ini and item:
        return f"{ini}.{item}"
    return None


_ZC_TABLE_RE = re.compile(r"^ZC_(.+)$", re.IGNORECASE)


def _zc_domain_english(table: str | None) -> str | None:
    """For a ZC_<X> table reference, return a humanized "<X>" string.

    ZC_* tables in Epic Clarity are pure code/lookup tables with a
    consistent shape: (`<X>_C` numeric code, `NAME` text label). When a
    query projects `Z.NAME` from such a table, the column's effective
    meaning is the table's domain (e.g., ZC_APPT_STATUS.NAME means
    "Appointment Status"). This helper turns the table name into that
    humanized domain string.

    Returns None if `table` doesn't match the ZC_<X> shape.
    """
    if not table:
        return None
    m = _ZC_TABLE_RE.match(table.strip())
    if not m:
        return None
    body = m.group(1)
    # Snake-case-aware tokenize; expand abbreviations (APPT -> Appointment etc.)
    # using the same helper used elsewhere in this module.
    parts = [p for p in body.split("_") if p]
    expanded = " ".join(_expand_abbreviations(p) for p in parts)
    # Title-case any leftover acronyms cleanly; preserve already-good casing.
    if expanded and any(c.isalpha() for c in expanded):
        return expanded.strip().title() if expanded.isupper() else expanded.strip()
    return None


@register(name="column_ref", node_class=exp.Column, category="passthrough", priority=10)
def column_ref(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    col_name = node.name
    table = node.table or None
    ref = f"{table}.{col_name}" if table else col_name

    # ZC.NAME shortcut: when a column is `ZC_<X>.NAME`, the effective
    # meaning is the table's domain (e.g., ZC_APPT_STATUS.NAME -> "Appointment
    # Status"). Skip the schema lookup entirely -- ZC tables are
    # structurally predictable and the table-name carries the semantics.
    if (col_name or "").upper() == "NAME":
        zc_english = _zc_domain_english(table)
        if zc_english:
            return Translation(
                english=zc_english,
                category="passthrough",
                base_columns=[ref],
                base_tables=[table] if table else [],
            )

    entry = _lookup_column(ctx.schema, table, col_name)
    if entry is not None:
        # Prefer short_description (user-curated, concise) over the
        # full description (often verbose Clarity reference text).
        chosen = (entry.get("short_description") or entry.get("description"))
        if chosen:
            ini_key = _ini_item_key(entry)
            return Translation(
                english=_clean_description(chosen),
                category="passthrough",
                base_columns=[ref],
                base_tables=[table] if table else [],
                ini_items=[ini_key] if ini_key else [],
            )
    # Fall back to abbreviation expansion; still flag as unknown so the
    # schema-authoring backlog is visible.
    expanded = _expand_abbreviations(col_name)
    return Translation(
        english=expanded,
        category="passthrough",
        base_columns=[ref],
        base_tables=[table] if table else [],
        unknown_columns=[ref],
    )


@register(name="literal", node_class=exp.Literal, category="literal", priority=10)
def literal(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    val = node.name
    if node.args.get("is_string"):
        return Translation(english=f"'{val}'", category="literal", subcategory="string")
    return Translation(english=str(val), category="literal", subcategory="numeric")


@register(name="alias_unwrap", node_class=exp.Alias, category="passthrough", priority=10)
def alias_unwrap(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    # The alias itself carries no semantic content -- pass through the inner
    # translation.
    inner = children.get("this")
    if inner is None:
        return Translation(english=node.alias_or_name or "(alias)", category="unknown")
    return inner


@register(name="star", node_class=exp.Star, category="passthrough", priority=10)
def star(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    return Translation(english="all rows", category="passthrough", subcategory="star")


@register(name="identifier_passthrough", node_class=exp.Identifier, category="passthrough", priority=50)
def identifier(ctx: Context, node: exp.Expression, children: dict[str, Translation]) -> Translation:
    # Bare identifiers (unit literals like YEAR, DAY in DATEDIFF) reach here
    # only when they're not wrapped in Literal. Surface the raw name.
    return Translation(english=node.name, category="literal", subcategory="identifier")
