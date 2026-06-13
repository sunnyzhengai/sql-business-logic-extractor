#!/usr/bin/env python3
"""Phase 3 Demo — Launch the Streamlit UI.

Run:
    python3 -m tools.jit.mock.demo_phase3

This launches the Streamlit app and prints testing instructions.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main():
    app_path = Path(__file__).resolve().parents[1] / "app" / "streamlit_app.py"

    print()
    print("=" * 70)
    print("  Phase 3: Streamlit UI — HITL at Every Step")
    print("=" * 70)
    print()
    print("  Launching Streamlit app...")
    print(f"  App: {app_path}")
    print()
    print("  TEST SCENARIOS TO TRY:")
    print()
    print("  1. COMPLEX QUESTION (L1 → L2 cascade):")
    print('     "How many percent of diabetic patients who have been')
    print('      to the ER more than 3 times last year have missed')
    print('      their PCP visit in the last 6 months?"')
    print()
    print("     Expected flow:")
    print("     - L1: multiple partial matches, no single report → escalate")
    print("     - L2: 3 strong definitions found → select, sequence, approve")
    print("     - Step 1: 90 patients → Step 2: 45 → Step 3: ~22 → 24.4%")
    print()
    print("  2. SIMPLE REPORT MATCH (L1 success):")
    print('     "billing charges by department"')
    print()
    print("     Expected: VW_BILLING_SUMMARY matches, run and approve")
    print()
    print("  3. UNKNOWN TERM (L3 cascade):")
    print('     "patients with Addison\'s disease"')
    print()
    print("     Expected: L1/L2 no match → L3 pattern recognition")
    print("     → 'disease' = diagnosis → 4 routes shown")
    print()
    print("  4. REJECT & MODIFY (test the reject flow):")
    print('     Start with the complex question, approve steps 1-2,')
    print('     then click "Reject — wrong logic" on step 3.')
    print('     This escalates to L3 where you can build from scratch.')
    print()
    print("  5. HAND OFF (test the escape hatch):")
    print('     "patients with xyzzy condition"')
    print('     Click "Hand off to BI developer" at any level.')
    print()
    print("  WHAT TO LOOK FOR:")
    print("  - Every step waits for your click before proceeding")
    print("  - You can edit SQL before running")
    print("  - The funnel builds on the right as you approve steps")
    print("  - Table graph shows involved tables with FK edges")
    print()
    print("  Press Ctrl+C to stop the server.")
    print()
    print("-" * 70)
    print()

    # Launch streamlit
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(app_path),
        "--server.headless", "true",
    ])


if __name__ == "__main__":
    main()
