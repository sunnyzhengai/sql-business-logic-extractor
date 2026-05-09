"""Tool 17 -- in-notebook graph exploration of corpus.jsonl.

Three layers:

  build_view_graph(view_dict)
       One ViewV1 dict (one line of corpus.jsonl) -> networkx
       MultiDiGraph. Nodes per view/scope/column/table/filter; edges
       carry the relationship type (HAS_SCOPE, READS_FROM_TABLE,
       JOINS, CONTAINS_COLUMN, DERIVED_FROM, REFERENCES_TABLE,
       READS_FROM_SCOPE, HAS_FILTER).

  build_cluster_graph(view_dicts)
       Multiple views combined; tables are GLOBAL nodes (one node per
       bare table name), so cross-view connections through shared
       tables are visible -- valuable for "show me all views about
       PATIENT" queries.

  build_corpus_graph(corpus_path, view_filter=None)
       Whole corpus or a subset. Use sparingly past 1000 nodes total.

Plus three rendering helpers:

  render_pyvis(g, output_html_path)   interactive HTML, drag/zoom/click
  export_graphml(g, output_path)       for Gephi etc.
  render_inline(g, max_nodes=100)      matplotlib quick-look in notebook

Lazy imports keep `import tools.graph_explore` cheap when the user
only wants one rendering format.
"""
