"""Capture pattern classifier -- label views by clinical data patterns.

Instead of clustering views by co-occurrence (Louvain), this classifies
each view by which **domain-specific anchor tables** it touches. The
anchor table is the table that carries the clinical meaning -- not
PATIENT (everyone joins PATIENT), not ZC lookups (decorative), but
the table that defines *what kind of clinical data* the view is about.

Each capture pattern is defined by one or more **signature tables**.
If a view touches any signature table for a pattern, it gets that label.
A view can have multiple labels (e.g., a view that joins PAT_ENC_DX
and ORDER_MED is both "encounter_diagnosis" and "medication").

The patterns are derived from how clinical data enters Epic Clarity.
They are NOT medical knowledge -- they are data architecture patterns.

Usage::

    from tools.jit.pattern_classifier import classify_view, classify_corpus

    patterns = classify_view(view_dict)
    # [CapturePattern(name="encounter_diagnosis",
    #                 anchor_table="PAT_ENC_DX", ...)]

    all_patterns = classify_corpus(views)
    # {"encounter_diagnosis": [view1, view2, ...], ...}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from tools.shared.table_names import bare_table_name


@dataclass(frozen=True)
class CapturePattern:
    """A clinical data capture pattern identified in a view."""
    name: str                    # e.g., "encounter_diagnosis"
    label: str                   # human-readable, e.g., "Encounter Diagnosis"
    anchor_table: str            # the signature table that triggered this match
    description: str             # what this pattern captures


# ---------------------------------------------------------------------------
# Pattern registry -- each pattern defined by its signature tables
# ---------------------------------------------------------------------------
# The key insight: these are data architecture patterns, not medical concepts.
# "encounter_diagnosis" means "this view joins through the table where
# encounter-level diagnoses are stored" -- it doesn't know what diabetes is.
#
# Order matters: more specific patterns should come first when displaying,
# but matching is independent (a view can match multiple patterns).

_PATTERN_DEFINITIONS: list[dict] = [
    {
        "name": "encounter_diagnosis",
        "label": "Encounter Diagnosis",
        "description": "Diagnoses recorded at outpatient/ED encounters (ICD codes via CLARITY_EDG)",
        "signature_tables": ["PAT_ENC_DX"],
    },
    {
        "name": "problem_list",
        "label": "Problem List",
        "description": "Active/resolved problems on the patient's problem list",
        "signature_tables": ["PROBLEM_LIST"],
    },
    {
        "name": "admission_diagnosis",
        "label": "Admission Diagnosis",
        "description": "Diagnoses recorded at hospital admission",
        "signature_tables": ["HSP_ADMIT_DIAG"],
    },
    {
        "name": "discharge_diagnosis",
        "label": "Discharge Diagnosis",
        "description": "Final billing diagnoses on the hospital account",
        "signature_tables": ["HSP_ACCT_DX_LIST"],
    },
    {
        "name": "surgery_diagnosis",
        "label": "Surgery Diagnosis",
        "description": "Pre-op or post-op diagnoses on surgery cases",
        "signature_tables": ["OR_CASE_DX_CODE"],
    },
    {
        "name": "lab_result",
        "label": "Lab / Test Result",
        "description": "Laboratory or diagnostic test results (values, flags, ranges)",
        "signature_tables": ["ORDER_RESULTS"],
    },
    {
        "name": "medication",
        "label": "Medication",
        "description": "Medication orders and prescriptions",
        "signature_tables": ["ORDER_MED", "CLARITY_MEDICATION"],
    },
    {
        "name": "procedure",
        "label": "Procedure",
        "description": "Procedure orders (labs, imaging, surgeries ordered)",
        "signature_tables": ["ORDER_PROC", "CLARITY_EAP"],
    },
    {
        "name": "surgery_case",
        "label": "Surgery Case",
        "description": "Surgical case records (OR scheduling, room, date)",
        "signature_tables": ["OR_CASE"],
    },
    {
        "name": "hospitalization",
        "label": "Hospitalization",
        "description": "Inpatient hospital stays (admission, discharge, ADT class)",
        "signature_tables": ["PAT_ENC_HSP"],
    },
    {
        "name": "hospital_account",
        "label": "Hospital Account / Billing",
        "description": "Hospital account charges, payments, and financial data",
        "signature_tables": ["HSP_ACCOUNT", "HSP_ACCT_SBO"],
    },
    {
        "name": "referral",
        "label": "Referral",
        "description": "Referral orders, tracking, and status",
        "signature_tables": ["REFERRAL"],
    },
    {
        "name": "immunization",
        "label": "Immunization",
        "description": "Patient immunization/vaccination records",
        "signature_tables": ["PAT_IMMUNIZATION", "IMMUNE", "CLARITY_IMMUNZATN"],
    },
    {
        "name": "encounter",
        "label": "Encounter / Visit",
        "description": "Patient encounters, appointments, and visits",
        "signature_tables": ["PAT_ENC"],
    },
    {
        "name": "scheduling",
        "label": "Scheduling / Access",
        "description": "Appointment scheduling, availability, and access metrics",
        "signature_tables": ["F_SCHED_APPT", "AVAILABILITY", "ACCESS_PROV"],
    },
    {
        "name": "ed_event",
        "label": "ED Event",
        "description": "Emergency department event tracking (arrival, triage, etc.)",
        "signature_tables": ["ED_IEV_PAT_INFO", "ED_IEV_EVENT_INFO"],
    },
    {
        "name": "billing_transaction",
        "label": "Professional Billing",
        "description": "Professional billing transactions (charges, payments, AR)",
        "signature_tables": ["ARPB_TRANSACTIONS"],
    },
    {
        "name": "pcp_assignment",
        "label": "PCP Assignment",
        "description": "Patient primary care provider assignments",
        "signature_tables": ["PAT_PCP"],
    },
    {
        "name": "diagnosis_master",
        "label": "Diagnosis Lookup",
        "description": "Diagnosis code definitions (ICD-10, DX names) without encounter context",
        "signature_tables": ["CLARITY_EDG"],
    },
    {
        "name": "value_set",
        "label": "Value Set / Quality Measure",
        "description": "Standard code sets for quality measures (NIH, CMS, etc.)",
        "signature_tables": ["VALUESET", "VALUESET_ITEMS"],
    },
    {
        "name": "demographics",
        "label": "Demographics",
        "description": "Patient demographic information only (age, sex, address)",
        # PATIENT alone, without any other clinical join -- detected specially
        "signature_tables": [],
    },
]

# Build lookup: table name (upper) -> list of pattern defs
_TABLE_TO_PATTERNS: dict[str, list[dict]] = {}
for _pdef in _PATTERN_DEFINITIONS:
    for _tbl in _pdef["signature_tables"]:
        _TABLE_TO_PATTERNS.setdefault(_tbl.upper(), []).append(_pdef)


def classify_view(view: dict) -> list[CapturePattern]:
    """Classify a single view by its capture patterns.

    Returns a list of CapturePattern objects, one per matched pattern.
    A view can match multiple patterns (e.g., encounter_diagnosis +
    medication for a combined query). Returns empty list only if the
    view has no recognizable tables at all.

    The "demographics" pattern is assigned when a view touches PATIENT
    but no other clinical domain tables (it's purely demographic).
    The "encounter" pattern is suppressed when a more specific pattern
    already explains why PAT_ENC is joined (e.g., encounter_diagnosis
    already implies PAT_ENC).
    """
    # Collect all bare table names across all scopes
    tables: set[str] = set()
    for scope in view.get("scopes") or []:
        for t in scope.get("reads_from_tables") or []:
            bare = bare_table_name(t).upper()
            if bare and ":" not in bare:
                tables.add(bare)
        for join in scope.get("joins") or []:
            rt = bare_table_name(join.get("right_table") or "").upper()
            if rt and ":" not in rt:
                tables.add(rt)

    # Match against pattern signatures
    matched: list[CapturePattern] = []
    matched_names: set[str] = set()

    for tbl in tables:
        for pdef in _TABLE_TO_PATTERNS.get(tbl, []):
            if pdef["name"] not in matched_names:
                matched_names.add(pdef["name"])
                matched.append(CapturePattern(
                    name=pdef["name"],
                    label=pdef["label"],
                    anchor_table=tbl,
                    description=pdef["description"],
                ))

    # Suppress generic "encounter" if a more specific encounter-based
    # pattern already matched (encounter_diagnosis, hospitalization, etc.)
    # These patterns already imply PAT_ENC involvement.
    _encounter_implies = {
        "encounter_diagnosis", "admission_diagnosis", "discharge_diagnosis",
        "hospitalization", "lab_result", "medication", "procedure",
        "surgery_case", "surgery_diagnosis", "ed_event", "scheduling",
    }
    if "encounter" in matched_names and matched_names & _encounter_implies:
        matched = [p for p in matched if p.name != "encounter"]
        matched_names.discard("encounter")

    # Suppress generic "diagnosis_master" if a specific diagnosis source
    # already matched (encounter_dx, problem_list, etc. already join EDG)
    _dx_implies = {
        "encounter_diagnosis", "problem_list", "admission_diagnosis",
        "discharge_diagnosis", "surgery_diagnosis",
    }
    if "diagnosis_master" in matched_names and matched_names & _dx_implies:
        matched = [p for p in matched if p.name != "diagnosis_master"]
        matched_names.discard("diagnosis_master")

    # If nothing matched but PATIENT is present, it's demographics-only
    if not matched and "PATIENT" in tables:
        matched.append(CapturePattern(
            name="demographics",
            label="Demographics",
            anchor_table="PATIENT",
            description="Patient demographic information only (age, sex, address)",
        ))

    return matched


def classify_corpus(views: list[dict]) -> dict[str, list[dict]]:
    """Classify every view in a corpus and group by pattern.

    Returns a dict mapping pattern name -> list of view dicts that
    match that pattern. A view appears under every pattern it matches.

    Also returns an "_unclassified" key for views that matched no pattern.
    """
    groups: dict[str, list[dict]] = {}
    for view in views:
        patterns = classify_view(view)
        if not patterns:
            groups.setdefault("_unclassified", []).append(view)
        else:
            for p in patterns:
                groups.setdefault(p.name, []).append(view)
    return groups


def summarize_corpus_patterns(views: list[dict]) -> str:
    """Return a markdown summary of capture patterns across the corpus."""
    groups = classify_corpus(views)

    lines = ["# Capture Pattern Summary", ""]
    lines.append(f"Total views: {len(views)}")
    lines.append("")

    # Sort by count descending
    sorted_patterns = sorted(
        [(k, v) for k, v in groups.items() if k != "_unclassified"],
        key=lambda x: len(x[1]),
        reverse=True,
    )

    lines.append("| Pattern | Views | Example |")
    lines.append("|---|---|---|")
    for name, pattern_views in sorted_patterns:
        # Find the label from the registry
        label = name
        for pdef in _PATTERN_DEFINITIONS:
            if pdef["name"] == name:
                label = pdef["label"]
                break
        example = pattern_views[0].get("view_name", "") if pattern_views else ""
        lines.append(f"| {label} | {len(pattern_views)} | {example} |")

    unclassified = groups.get("_unclassified", [])
    if unclassified:
        lines.append(f"| _Unclassified_ | {len(unclassified)} | "
                     f"{unclassified[0].get('view_name', '')} |")
    lines.append("")
    return "\n".join(lines)
