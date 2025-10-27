#!/bin/bash
# Generate flamegraphs for all benchmarks using cargo-flamegraph
# Requires: cargo install flamegraph

set -e

echo "==========================================="
echo "Generating Flamegraphs for All Benchmarks"
echo "==========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if flamegraph is installed
if ! command -v flamegraph &> /dev/null; then
    echo -e "${YELLOW}cargo-flamegraph not found. Installing...${NC}"
    cargo install flamegraph
fi

# Check if running on macOS and DTrace is available
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo -e "${YELLOW}Note: On macOS, you may need to grant DTrace permissions${NC}"
    echo -e "${YELLOW}Run: sudo dtruss -c ls (to initialize permissions)${NC}"
    echo ""
fi

# Create flamegraphs directory
mkdir -p flamegraphs

# List of benchmarks
BENCHMARKS=("cdr_generation" "compression" "csv_writing" "end_to_end")

for bench in "${BENCHMARKS[@]}"; do
    echo -e "${BLUE}Generating flamegraph for: ${bench}${NC}"

    # Generate flamegraph
    cargo flamegraph --bench "$bench" --profile profiling -o "flamegraphs/${bench}_flamegraph.svg"

    echo -e "${GREEN}✓ Flamegraph saved: flamegraphs/${bench}_flamegraph.svg${NC}"
    echo ""
done

echo ""
echo -e "${GREEN}✓ All flamegraphs generated successfully!${NC}"
echo ""
echo "Flamegraphs location: flamegraphs/"
echo ""
echo "View flamegraphs by opening the .svg files in a browser:"
for bench in "${BENCHMARKS[@]}"; do
    echo "  - flamegraphs/${bench}_flamegraph.svg"
done
