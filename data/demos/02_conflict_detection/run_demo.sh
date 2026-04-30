#!/bin/bash
# Demo 2: Conflict Detection - Integration Test
# Tests the full governance_extract.py pipeline

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Demo 2: Conflict Detection (Integration Test) ==="
echo ""
echo "Input: 4 SQL reports from different teams"
echo "  - report_finance.sql"
echo "  - report_quality.sql"
echo "  - report_operations.sql"
echo "  - report_billing.sql"
echo ""

# Check for API key
if [ -z "$OPENAI_API_KEY" ]; then
    echo "Note: OPENAI_API_KEY not set. Running without L4 translation."
    echo ""
    python3 "$ROOT_DIR/governance_extract.py" \
        "$SCRIPT_DIR/input/" \
        --output "$SCRIPT_DIR/output/governance_summary.xlsx" \
        --details-dir "$SCRIPT_DIR/output"
else
    echo "OPENAI_API_KEY detected. Running with L4 translation."
    echo ""
    python3 "$ROOT_DIR/governance_extract.py" \
        "$SCRIPT_DIR/input/" \
        --schema "$ROOT_DIR/clarity_schema.yaml" \
        --output "$SCRIPT_DIR/output/governance_summary.xlsx" \
        --details-dir "$SCRIPT_DIR/output"
fi

echo ""
echo "=== Demo Complete ==="
echo ""
echo "Output files:"
echo "  - output/governance_summary.xlsx (main deliverable)"
echo "  - output/details/*_L3.json (technical lineage)"
echo "  - output/details/*_L4.json (English definitions)"
