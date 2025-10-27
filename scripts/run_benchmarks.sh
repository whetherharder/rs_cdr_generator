#!/bin/bash
# Run all benchmarks with Criterion
# Generates HTML reports in target/criterion/

set -e

echo "==================================="
echo "Running CDR Generator Benchmarks"
echo "==================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Build in release mode first
echo -e "${BLUE}Building project in release mode...${NC}"
cargo build --release

echo ""
echo -e "${GREEN}Running all benchmarks...${NC}"
echo ""

# Run all benchmarks
cargo bench

echo ""
echo -e "${GREEN}✓ Benchmarks completed!${NC}"
echo ""
echo "Reports generated in: target/criterion/"
echo "Open target/criterion/report/index.html to view results"
