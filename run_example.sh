#!/bin/bash
# Example script for running CDR generator with various configurations

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== CDR Generator - Example Run Script ===${NC}\n"

# Build the project first
echo -e "${YELLOW}Building project...${NC}"
cargo build --release
echo -e "${GREEN}✓ Build complete${NC}\n"

# Example 1: Quick test with 1,000 subscribers for 1 day
echo -e "${BLUE}Example 1: Quick test (1,000 subscribers, 1 day)${NC}"
./target/release/rs_cdr_generator \
    --subs 1000 \
    --start 2025-01-01 \
    --days 1 \
    --out out_example1 \
    --seed 42
echo -e "${GREEN}✓ Example 1 complete${NC}\n"

# Example 2: Using configuration file
echo -e "${BLUE}Example 2: Using config file (example_config.yaml)${NC}"
./target/release/rs_cdr_generator \
    --config example_config.yaml \
    --subs 10000 \
    --start 2025-01-01 \
    --days 3 \
    --out out_example2 \
    --seed 123
echo -e "${GREEN}✓ Example 2 complete${NC}\n"

# Example 3: Custom parameters
echo -e "${BLUE}Example 3: Custom parameters${NC}"
./target/release/rs_cdr_generator \
    --subs 5000 \
    --start 2025-01-15 \
    --days 7 \
    --out out_example3 \
    --seed 456 \
    --prefixes "31612,31613,31620" \
    --cells 1000 \
    --cell-center "52.37,4.895" \
    --cell-radius-km 25.0 \
    --workers 8 \
    --mo-share-call 0.6 \
    --mo-share-sms 0.5 \
    --cleanup-after-archive
echo -e "${GREEN}✓ Example 3 complete${NC}\n"

# Example 4: Large dataset (commented out by default)
# Uncomment to run with 100k subscribers
# echo -e "${BLUE}Example 4: Large dataset (100,000 subscribers, 30 days)${NC}"
# ./target/release/rs_cdr_generator \
#     --config example_config.yaml \
#     --start 2025-01-01 \
#     --days 30 \
#     --out out_example4 \
#     --seed 789 \
#     --cleanup-after-archive

echo -e "${GREEN}=== All examples completed successfully! ===${NC}\n"

# Show output directories
echo -e "${BLUE}Output directories created:${NC}"
ls -lh out_example* 2>/dev/null || echo "No output directories found"

echo ""
echo -e "${YELLOW}To analyze the generated data:${NC}"
echo "  tar -xzf out_example1/2025-01-01.tar.gz -C /tmp/"
echo "  head -20 /tmp/2025-01-01/cdr_*_shard000_part001.csv"
echo ""
echo -e "${YELLOW}To count unique subscribers in DATA events:${NC}"
echo "  cat /tmp/2025-01-01/cdr_*_shard*_part*.csv | awk -F';' 'NR>1 && \$1==\"DATA\" {print \$2}' | sort -u | wc -l"
