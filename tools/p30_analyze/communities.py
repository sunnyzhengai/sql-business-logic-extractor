"""Louvain community detection on the table-projection graph.

Once the projection has been computed (`p30_analyze.projection`) and
bridge tables removed (`p30_analyze.bridges`), we run Louvain to
discover communities -- groups of tables that are densely connected to
each other and loosely connected to the rest.

The resulting communities are the structural basis for almost every
downstream finding:
  - "Which views belong to which subject area" (primary-community
     assignment, see `p30_analyze.primary_community`).
  - "Which views cross multiple subject areas" (cross-domain spans,
     same module).
  - Steward packets (p40_synthesize) are organized one-per-community.
  - The per-community HTMLs (p50_present) render one focused subgraph
     per community.

Resolution
----------
Louvain has a `resolution` parameter that trades off community size vs.
density:
  - resolution < 1.0 -> fewer, larger communities (more aggressive merging)
  - resolution = 1.0 -> default modularity optimization
  - resolution > 1.0 -> more, smaller communities (more aggressive splitting)

The right resolution depends on the corpus and the analytical question.
For the validation diagnostic we default to 1.0 and let the user sweep
manually (running with resolution=0.5 and resolution=1.5 for comparison
is a common workflow).

Determinism
-----------
Louvain is stochastic by default. We fix the random seed (seed=42) so
re-running on the same input produces the same community assignment.
This matters for end-to-end reproducibility and for tests.

Historical note
---------------
This module was previously `tools.operate.validate_graph_pivot.detect_table_communities`.
In Phase 2c of the 2026-05 restructure it moved here -- community
detection is the analytical core of p30_analyze, not specific to the
validation diagnostic.
"""

from __future__ import annotations


def detect_table_communities(table_g, resolution: float = 1.0) -> list[set]:
    """Run Louvain community detection on the weighted table graph.

    Parameters
    ----------
    table_g     : nx.Graph (typically from `projection.extract_table_projection`,
                  with bridges optionally removed via `bridges.project_without_bridges`).
                  Edges should carry a `weight` attribute.
    resolution  : 1.0 default. See module docstring for the trade-off.

    Returns
    -------
    communities : list of frozenset-like sets of node IDs. Each set is one
                  community. The list is sorted by community size (largest
                  first), so `communities[0]` is the biggest community.
    """
    from networkx.algorithms import community as nx_community

    # The seed is fixed so re-runs give the same partitioning. Louvain is
    # stochastic by default; a deterministic seed makes results reproducible
    # and tests stable.
    communities = nx_community.louvain_communities(
        table_g, weight="weight", resolution=resolution, seed=42,
    )
    # Sort communities by size (largest first) so any downstream report is
    # naturally ordered "biggest cluster first".
    communities.sort(key=len, reverse=True)
    return communities
