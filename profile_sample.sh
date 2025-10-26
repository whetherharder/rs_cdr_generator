#!/bin/bash
# macOS profiling using built-in 'sample' command (works without Xcode)
# Generates call tree and identifies hot functions

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=== CDR Generator Profiling (macOS sample) ==="
echo ""

# Default parameters
SUBS=${SUBS:-50000}
DAYS=${DAYS:-1}
START_DATE=${START_DATE:-2025-01-01}
DURATION=${DURATION:-30}  # seconds to sample
OUTPUT_DIR="profiling_results/sample_$(date +%Y%m%d_%H%M%S)"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --subs)
            SUBS="$2"
            shift 2
            ;;
        --days)
            DAYS="$2"
            shift 2
            ;;
        --start)
            START_DATE="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --config)
            CONFIG="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --subs N       Number of subscribers (default: 50000)"
            echo "  --days N       Number of days (default: 1)"
            echo "  --start DATE   Start date YYYY-MM-DD (default: 2025-01-01)"
            echo "  --duration N   Sampling duration in seconds (default: 30)"
            echo "  --output DIR   Output directory (default: profiling_results/sample_<timestamp>)"
            echo "  -h, --help     Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

mkdir -p "$OUTPUT_DIR"

echo "Configuration:"
echo "  Subscribers: ${SUBS}"
echo "  Days: ${DAYS}"
echo "  Start date: ${START_DATE}"
echo "  Sampling duration: ${DURATION}s"
echo "  Output directory: ${OUTPUT_DIR}"
echo ""

# Build with debug symbols
echo "Building with debug symbols..."
cargo build --release --quiet
echo -e "${GREEN}✓ Build complete${NC}"
echo ""

BINARY="./target/release/rs_cdr_generator"
TEST_OUTPUT="${OUTPUT_DIR}/test_output"
SAMPLE_OUTPUT="${OUTPUT_DIR}/sample_output.txt"

echo "Starting CDR generator in background..."
if [ -n "${CONFIG}" ]; then
    "${BINARY}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}" \
        --config "${CONFIG}" \
        --seed 42 &
else
    "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}" &
fi

PID=$!
echo "Process started with PID: ${PID}"
echo ""

# Wait a moment for process to start
sleep 2

# Check if process is still running
if ! ps -p $PID > /dev/null; then
    echo -e "${RED}Error: Process died immediately${NC}"
    exit 1
fi

echo "Profiling for ${DURATION} seconds..."
echo "This will sample the call stack every 1ms..."
echo ""

# Run sample command
sample "${PID}" "${DURATION}" 1 -file "${SAMPLE_OUTPUT}" 2>&1 | grep -v "^$" || true

# Wait for process to complete
wait $PID 2>/dev/null || true

echo ""
echo -e "${GREEN}✓ Profiling complete${NC}"
echo ""

# Clean up test output
rm -rf "${TEST_OUTPUT}" 2>/dev/null || true

# Parse the sample output to find hot functions
echo "Analyzing results..."
echo ""

# Extract top functions by weight
echo "Top 20 functions by sample count:"
echo "=================================="

# Parse the sample output for function weights
# The format shows symbol names with sample counts
grep -E "^\s+[0-9]+" "${SAMPLE_OUTPUT}" | \
    grep -v "^Analysis of sampling" | \
    sort -rn -k1 | \
    head -20 | \
    awk '{
        count = $1
        $1 = ""
        func = $0
        gsub(/^[ \t]+/, "", func)
        printf "%6s samples: %s\n", count, func
    }' || echo "Could not parse sample output"

echo ""
echo ""

# Look for Rust-specific symbols
echo "Rust functions (top 15):"
echo "========================"
grep -E "rs_cdr_generator::" "${SAMPLE_OUTPUT}" | \
    grep -E "^\s+[0-9]+" | \
    sort -rn -k1 | \
    head -15 || echo "No Rust functions found in output"

echo ""
echo ""

# Save metadata
cat > "${OUTPUT_DIR}/metadata.json" <<EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo 'unknown')",
  "git_branch": "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')",
  "subscribers": ${SUBS},
  "days": ${DAYS},
  "start_date": "${START_DATE}",
  "sample_duration": ${DURATION},
  "sample_file": "sample_output.txt"
}
EOF

echo -e "${GREEN}=== Profiling Complete ===${NC}"
echo ""
echo "Results saved to: ${OUTPUT_DIR}"
echo "  - ${SAMPLE_OUTPUT}"
echo "  - ${OUTPUT_DIR}/metadata.json"
echo ""
echo "To view full call tree:"
echo "  less ${SAMPLE_OUTPUT}"
echo ""
echo "To search for specific function:"
echo "  grep 'function_name' ${SAMPLE_OUTPUT}"
echo ""
