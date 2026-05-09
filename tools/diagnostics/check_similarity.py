#!/usr/bin/env python3
"""Diagnose unexpectedly large/small clusters in similarity output.

Most common scenario: user runs `similarity` and sees one giant L1
cluster covering most views, or sees mostly-singleton L2+ clusters.
Three things can cause this:

  - Module cache: the new code isn't actually loaded (rare after kernel
    restart, common with notebook hot-edits).
  - Genuine driver concentration: most views actually do drive from
    the same base table (typical in patient/coverage-centric corpora).
  - Bug in driver detection: `_leaf_driver` collapses to a common
    ancestor when it shouldn't.

The functions here distinguish the cases.

Notebook usage:

    from tools.diagnostics.check_similarity import (
        check_module_freshness,
        check_driver_distribution,
        diagnose_l1,
    )

    diagnose_l1(features_json='/lakehouse/.../similarity/features.json')

CLI:

    python -m tools.diagnostics.check_similarity <features.json>
"""

from __future__ import annotations

import argparse
import inspect
import json
import sys
from collections import Counter
from pathlib import Path


# ---------------------------------------------------------------------------
# CHECK 1 -- is the freshly-edited similarity code actually loaded?
# ---------------------------------------------------------------------------

def check_module_freshness() -> bool:
    """Drop cached `tools.similarity.*` modules, re-import, and confirm
    the latest fixes are in place. Returns True if both expected
    sentinels are found in the source on disk."""
    for name in list(sys.modules):
        if name.startswith("tools.similarity"):
            del sys.modules[name]
    import tools.similarity.signatures as S
    import tools.similarity.clusters as C

    sig_ok = 'col.set("table", None)' in inspect.getsource(S._canonicalize_filter)
    cluster_ok = "a.driver == b.driver" in inspect.getsource(C._l1_match)

    print("[Check 1] alias-stripping in _canonicalize_filter:", sig_ok)
    print("[Check 1] strict driver equality in _l1_match:    ", cluster_ok)
    if not (sig_ok and cluster_ok):
        print("[Check 1] => one or both fixes are MISSING from the file on disk")
        print("           OR the wrong version is loaded. Re-download")
        print("           tools/similarity/{signatures,clusters}.py and")
        print("           restart the kernel.")
        return False
    print("[Check 1] => both fixes present. Continue to check 2.")
    return True


# ---------------------------------------------------------------------------
# CHECK 2 -- driver distribution across the corpus
# ---------------------------------------------------------------------------

def check_driver_distribution(features_json: str | Path,
                                top_n: int = 20) -> None:
    """Read similarity's features.json, count distinct drivers, and
    show the top N by view count. If one driver dominates, that's not
    a bug -- it's a property of the corpus (e.g., patient-centric
    healthcare BI). If drivers are diverse but L1 is still one giant
    cluster, the problem is in driver detection or clustering."""
    p = Path(features_json)
    if not p.is_file():
        print(f"[Check 2] ERROR: features.json not found at {p}")
        return

    doc = json.loads(p.read_text())
    views = doc.get("views") or []
    drivers = Counter(v.get("driver", "") for v in views)

    n_total = len(views)
    n_empty = drivers.get("", 0)
    n_distinct = len(drivers)

    print(f"[Check 2] Total views:               {n_total}")
    print(f"[Check 2] Distinct drivers:          {n_distinct}")
    print(f"[Check 2] Views with empty driver:   {n_empty}")
    if n_total:
        top_driver, top_count = drivers.most_common(1)[0]
        pct = 100 * top_count / n_total
        print(f"[Check 2] Most common driver:        {top_driver or '(empty)'} "
              f"-- {top_count}/{n_total} views ({pct:.0f}%)")
        if pct >= 80:
            print(f"[Check 2] => Driver concentration is REAL. {pct:.0f}% of views")
            print(f"           drive from one table. The big L1 cluster is")
            print(f"           expected. Look at clusters_L2.md for the actionable")
            print(f"           findings (within-driver grain differences).")
        elif n_distinct >= 5:
            print(f"[Check 2] => Drivers are diverse ({n_distinct} distinct). If")
            print(f"           L1 still shows one giant cluster, that's a bug --")
            print(f"           tell us so we can investigate _leaf_driver chain.")
        else:
            print(f"[Check 2] => Mixed: review the top drivers below.")

    print(f"\n[Check 2] Top {top_n} drivers by view count:")
    for driver, n in drivers.most_common(top_n):
        print(f"  {(driver or '(empty)'):<45} {n:>4}")


# ---------------------------------------------------------------------------
# Combined entry-point
# ---------------------------------------------------------------------------

def diagnose_l1(features_json: str | Path) -> None:
    """Run both checks; emit a clear next-step recommendation."""
    print("=" * 60)
    fresh = check_module_freshness()
    print()
    if not fresh:
        print("=" * 60)
        print("Stop here. Resolve the module-freshness issue first;")
        print("re-run the diagnostic afterward.")
        return
    print("=" * 60)
    check_driver_distribution(features_json)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=("Diagnose why similarity output looks unexpected "
                      "(e.g., one giant L1 cluster, mostly singletons).")
    )
    parser.add_argument("features_json",
                          help="Path to similarity/features.json")
    parser.add_argument("--top-n", type=int, default=20,
                          help="Top N drivers to print (default: 20).")
    args = parser.parse_args()
    diagnose_l1(args.features_json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
