"""Operations -- parser dev, diagnostics, system-health, and admin tools.

This folder is the OPERATE layer of the toolkit (see ARCHITECTURE.md
for the three-layer model: catalog -> govern -> visualize, with
operate as a sidecar to all three). The audience for these tools is
*BI developers and admins running the system*, not stewards
ratifying definitions.

Five categories of tool live here:

  1. Parse-health triage  (preflight_check.py, diagnose_parse_failure.py)
     "Which views parse cleanly? Why won't this one parse?"

  2. Parser maintenance  (auto_propose_rule.py, auto_propose_rule_hypotheses.py)
     "Generate candidate parsing-rule additions for human review."

  3. Performance audits  (timing_audit.py)
     "Which views take too long to parse / resolve?"

  4. Inventory + manifests  (inventory_manifest.py)
     "Which tables / ZC tables / columns does the corpus actually
     touch? Narrow downstream SSMS extracts to just those."

  5. Pipeline validation  (validate_graph_pivot.py, check_zc_lookups.py)
     "Did the analysis pipeline produce sensible output? If results
     look wrong, where is the breakage?"

All are standalone -- none participate in the pN0_* pipeline phases.
They are CALLED by humans on demand, not run automatically.

Historical note
---------------
This folder was previously `tools/diagnostics/` (narrow scope: small
sanity checks). It was renamed to `tools/operate/` as part of the
2026-05 codebase restructure when scattered ops tools (parser-dev,
timing, manifest) were consolidated here. The scope is broader than
"diagnosis" -- parser maintenance and inventory generation are real
operational work, not just triage.
"""
