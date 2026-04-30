#!/bin/bash
# Demo 1: Basic Extraction - Component Test
# Tests L3 (resolve) and L4 (translate)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Demo 1: Basic Extraction (Component Test) ==="
echo ""
echo "Input: complex_referral_analytics.sql"
echo ""

# Step 1: L3 - Extract lineage
echo "Step 1: L3 - Resolving lineage..."
python3 "$ROOT_DIR/resolve.py" \
    "$SCRIPT_DIR/input/complex_referral_analytics.sql" \
    --output "$SCRIPT_DIR/output/L3_resolved_lineage"

echo ""
echo "Generated:"
echo "  - output/L3_resolved_lineage.json"
echo "  - output/L3_resolved_lineage.txt"

# Step 2: L4 - Generate English definitions (if OPENAI_API_KEY is set)
if [ -n "$OPENAI_API_KEY" ]; then
    echo ""
    echo "Step 2: L4 - Generating English definitions..."
    python3 "$ROOT_DIR/llm_translate.py" \
        "$SCRIPT_DIR/output/L3_resolved_lineage.json" \
        --schema "$ROOT_DIR/clarity_schema.yaml" \
        --output "$SCRIPT_DIR/output/L4_english_definitions"

    echo ""
    echo "Generated:"
    echo "  - output/L4_english_definitions.json"
    echo "  - output/L4_english_definitions.txt"
else
    echo ""
    echo "Step 2: Skipped (set OPENAI_API_KEY to generate English definitions)"
fi

echo ""
echo "=== Demo Complete ==="
echo "Review the output/ folder for results"
