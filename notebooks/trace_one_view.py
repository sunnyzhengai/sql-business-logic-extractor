"""Trace ONE view through every pipeline stage and print where the
UNION branch tables drop out.

The data path is:
  raw .sql
    -> preprocess_ssms              (resolve.py)
    -> SQLBusinessLogicExtractor.extract() -> QueryLogic
    -> to_dict(logic)              (extract.py)
    -> LineageResolver(logic).resolve_all_scoped()
                                   -> ResolvedScopeTree
    -> _build_scope_v1() per scope (p10_extract/batch.py)
                                   -> ScopeV1
    -> asdict + json.dumps         (corpus.jsonl)
    -> graph_builder reads corpus  (p20_index)
    -> view_to_tables walks graph  (p30_analyze)
    -> matrix renders              (p50_present)

This cell stops at the FIRST four stages and prints the CTE's
sources / reads_from_tables at each. The stage where the branch
tables drop out is where the bug lives.
"""


# %% [Cell: trace one view through extract -> resolve]

import sys
for mod in list(sys.modules):
    if mod.startswith('sql_logic_extractor') or mod.startswith('tools'):
        del sys.modules[mod]

from tools.shared.sql_loader import read_sql_robust, load_clean_sql
from sql_logic_extractor.extract import SQLBusinessLogicExtractor, to_dict
from sql_logic_extractor.resolve import LineageResolver


# ---- EDIT to point at YOUR failing view ---------------------------------
view_path = "/lakehouse/default/Files/data/mychart_views/v_cchp_member.sql"
# -------------------------------------------------------------------------


# Stage 1: load + preprocess
raw_sql = read_sql_robust(view_path)
clean_sql, _meta = load_clean_sql(view_path)
print(f"=== Stage 1: preprocess_ssms ===")
print(f"  raw: {len(raw_sql)} chars, clean: {len(clean_sql)} chars")
print()


# Stage 2: extract -> QueryLogic -> dict
logic_obj = SQLBusinessLogicExtractor(dialect="tsql").extract(clean_sql)
logic = to_dict(logic_obj)
print(f"=== Stage 2: extract.py -> logic dict ===")
print(f"  top-level sources: "
      f"{[s.get('name') for s in logic.get('sources') or []]}")
print(f"  CTEs:")
for cte in logic.get("ctes") or []:
    cn = cte.get("name") or "?"
    cl = cte.get("logic") or {}
    src_names = [s.get("name") for s in cl.get("sources") or []]
    set_ops = cl.get("set_operations") or []
    print(f"    cte:{cn}  sources={src_names}  set_ops={len(set_ops)}")
    for i, op in enumerate(set_ops):
        for j, br in enumerate(op.get("branches") or []):
            bs = [s.get("name") for s in br.get("sources") or []]
            print(f"      branch[{j}] sources={bs}")
print()


# Stage 3: LineageResolver -> ResolvedScopeTree
resolver = LineageResolver(logic)
tree = resolver.resolve_all_scoped()
print(f"=== Stage 3: resolve.py -> ResolvedScopeTree ===")
print(f"  scopes ({len(tree.scopes)}):")
for sc in tree.scopes:
    print(f"    {sc.id!r}  kind={sc.kind!r}  "
          f"reads_from_tables={list(sc.reads_from_tables or [])}  "
          f"reads_from_scopes={list(sc.reads_from_scopes or [])}")
print()


# Verdict.
print("=== Verdict ===")
print("Compare Stage 2 (extract.py CTE sources) vs Stage 3 (resolve.py")
print("CTE-scope reads_from_tables) for the failing CTE:")
print()
print("  - If Stage 2 has the UNION branch tables but Stage 3's")
print("    cte:<name>.reads_from_tables is EMPTY -> bug is in")
print("    resolve.py's _emit_scope_recursive. The extractor produces")
print("    the right data; the resolver isn't reading the merged")
print("    sources from the CTE's logic dict.")
print()
print("  - If Stage 2 ALSO has empty CTE sources -> the extractor")
print("    isn't running with the UNION fix. Check that extract.py")
print("    is the version with the 'Promote nested-union sources'")
print("    comment in _extract_set_operations.")
print()
print("  - If both stages have the tables -> bug is downstream")
print("    (batch.py serialization, graph builder, or matrix).")
print("    Run extract+validate on /tmp and re-inspect.")
