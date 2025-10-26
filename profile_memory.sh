#!/bin/bash
# Memory profiling script for CDR generator
# Analyzes memory usage, allocations, and potential leaks

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=== CDR Generator Memory Profiling ==="
echo ""

# Default parameters
SUBS=${SUBS:-50000}
DAYS=${DAYS:-1}
START_DATE=${START_DATE:-2025-01-01}
OUTPUT_DIR="profiling_results/memory_$(date +%Y%m%d_%H%M%S)"

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
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --subs N       Number of subscribers (default: 50000)"
            echo "  --days N       Number of days (default: 1)"
            echo "  --start DATE   Start date YYYY-MM-DD (default: 2025-01-01)"
            echo "  --output DIR   Output directory (default: profiling_results/memory_<timestamp>)"
            echo "  -h, --help     Show this help message"
            echo ""
            echo "Tools used (platform-specific):"
            echo "  macOS: Instruments (Leaks, Allocations)"
            echo "  Linux: valgrind (massif, memcheck), heaptrack"
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
echo "  Output directory: ${OUTPUT_DIR}"
echo ""

# Build with profiling profile
echo "Building with profiling symbols..."
cargo build --profile=profiling --quiet
echo -e "${GREEN}✓ Build complete${NC}"
echo ""

BINARY="./target/profiling/rs_cdr_generator"
TEST_OUTPUT="${OUTPUT_DIR}/test_output"

# Platform-specific memory profiling
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macOS detected - using Instruments"
    echo ""

    if ! command -v instruments &> /dev/null; then
        echo -e "${RED}Error: Instruments not found${NC}"
        echo "Install Xcode Command Line Tools: xcode-select --install"
        exit 1
    fi

    # Allocations profiling
    echo "Running Allocations profiler..."
    ALLOC_TRACE="${OUTPUT_DIR}/allocations.trace"

    instruments -t "Allocations" \
        -D "${ALLOC_TRACE}" \
        "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}"

    echo -e "${GREEN}✓ Allocations profiling complete${NC}"
    echo "  Trace: ${ALLOC_TRACE}"
    echo ""

    # Leaks profiling
    echo "Running Leaks profiler..."
    LEAKS_TRACE="${OUTPUT_DIR}/leaks.trace"

    instruments -t "Leaks" \
        -D "${LEAKS_TRACE}" \
        "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}_leaks"

    echo -e "${GREEN}✓ Leaks profiling complete${NC}"
    echo "  Trace: ${LEAKS_TRACE}"
    echo ""

    # Also use time -l for basic memory stats
    echo "Collecting basic memory statistics..."
    /usr/bin/time -l "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}_stats" \
        2> "${OUTPUT_DIR}/time_stats.txt"

    # Extract memory info
    MAX_RSS=$(grep "maximum resident set size" "${OUTPUT_DIR}/time_stats.txt" | awk '{print $1}')
    AVG_RSS=$(grep "average resident set size" "${OUTPUT_DIR}/time_stats.txt" | awk '{print $1}' || echo "N/A")

    echo -e "${GREEN}✓ Statistics collected${NC}"
    echo ""
    echo "Memory Statistics:"
    echo "  Maximum RSS: $(echo "scale=2; $MAX_RSS / 1024 / 1024" | bc) MB"
    if [ "$AVG_RSS" != "N/A" ]; then
        echo "  Average RSS: $(echo "scale=2; $AVG_RSS / 1024 / 1024" | bc) MB"
    fi
    echo ""

    cat "${OUTPUT_DIR}/time_stats.txt" >> "${OUTPUT_DIR}/memory_stats.txt"

    echo "Opening Instruments traces..."
    open "${ALLOC_TRACE}" 2>/dev/null || true
    open "${LEAKS_TRACE}" 2>/dev/null || true

elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "Linux detected"
    echo ""

    PROFILER_FOUND=false

    # Try heaptrack (best for Rust)
    if command -v heaptrack &> /dev/null; then
        echo "Using heaptrack for memory profiling..."
        PROFILER_FOUND=true

        HEAPTRACK_OUTPUT="${OUTPUT_DIR}/heaptrack_output"

        heaptrack \
            --output "${HEAPTRACK_OUTPUT}" \
            "${BINARY}" \
            --subs "${SUBS}" \
            --start "${START_DATE}" \
            --days "${DAYS}" \
            --out "${TEST_OUTPUT}"

        echo -e "${GREEN}✓ heaptrack profiling complete${NC}"
        echo ""

        # Generate report
        if command -v heaptrack_print &> /dev/null; then
            HEAPTRACK_FILE=$(ls -t ${HEAPTRACK_OUTPUT}.* | head -1)
            heaptrack_print "${HEAPTRACK_FILE}" > "${OUTPUT_DIR}/heaptrack_report.txt"
            echo "Report saved to: ${OUTPUT_DIR}/heaptrack_report.txt"
            echo ""
            echo "Top allocations:"
            head -50 "${OUTPUT_DIR}/heaptrack_report.txt"
            echo ""
        fi

        # Try to open GUI
        if command -v heaptrack_gui &> /dev/null; then
            echo "Opening heaptrack GUI..."
            heaptrack_gui "${HEAPTRACK_FILE}" &
        fi
    fi

    # Try valgrind massif
    if command -v valgrind &> /dev/null; then
        echo "Using valgrind massif for heap profiling..."
        PROFILER_FOUND=true

        MASSIF_OUT="${OUTPUT_DIR}/massif.out"

        valgrind \
            --tool=massif \
            --massif-out-file="${MASSIF_OUT}" \
            --detailed-freq=1 \
            --max-snapshots=100 \
            "${BINARY}" \
            --subs "${SUBS}" \
            --start "${START_DATE}" \
            --days "${DAYS}" \
            --out "${TEST_OUTPUT}_massif"

        echo -e "${GREEN}✓ massif profiling complete${NC}"
        echo ""

        # Generate report
        if command -v ms_print &> /dev/null; then
            ms_print "${MASSIF_OUT}" > "${OUTPUT_DIR}/massif_report.txt"
            echo "Massif report saved to: ${OUTPUT_DIR}/massif_report.txt"
            echo ""
            echo "Memory usage summary:"
            grep -A 20 "Peak" "${OUTPUT_DIR}/massif_report.txt" | head -25
            echo ""
        fi

        # Also run memcheck for leaks (optional, can be slow)
        if [ "${RUN_MEMCHECK:-0}" = "1" ]; then
            echo "Running valgrind memcheck (this may be slow)..."
            MEMCHECK_OUT="${OUTPUT_DIR}/memcheck.txt"

            valgrind \
                --tool=memcheck \
                --leak-check=full \
                --show-leak-kinds=all \
                --track-origins=yes \
                --log-file="${MEMCHECK_OUT}" \
                "${BINARY}" \
                --subs "${SUBS}" \
                --start "${START_DATE}" \
                --days "${DAYS}" \
                --out "${TEST_OUTPUT}_memcheck"

            echo -e "${GREEN}✓ memcheck complete${NC}"
            echo "Report: ${MEMCHECK_OUT}"
            echo ""
        fi
    fi

    # Basic /usr/bin/time stats
    echo "Collecting basic memory statistics with /usr/bin/time..."
    /usr/bin/time -v "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}_stats" \
        2> "${OUTPUT_DIR}/time_stats.txt"

    echo -e "${GREEN}✓ Statistics collected${NC}"
    echo ""
    echo "Memory Statistics:"
    grep -E "(Maximum resident|Average resident|Page size)" "${OUTPUT_DIR}/time_stats.txt"
    echo ""

    if [ "$PROFILER_FOUND" = false ]; then
        echo -e "${YELLOW}No memory profilers found${NC}"
        echo ""
        echo "Install profiling tools:"
        echo "  heaptrack: sudo apt-get install heaptrack (recommended for Rust)"
        echo "  valgrind: sudo apt-get install valgrind"
        echo ""
    fi

else
    echo -e "${RED}Unsupported platform: ${OSTYPE}${NC}"
    exit 1
fi

# Clean up test outputs
rm -rf "${TEST_OUTPUT}" "${TEST_OUTPUT}_leaks" "${TEST_OUTPUT}_stats" "${TEST_OUTPUT}_massif" "${TEST_OUTPUT}_memcheck" 2>/dev/null || true

# Save metadata
cat > "${OUTPUT_DIR}/metadata.json" <<EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "platform": "${OSTYPE}",
  "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo 'unknown')",
  "git_branch": "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')",
  "subscribers": ${SUBS},
  "days": ${DAYS},
  "start_date": "${START_DATE}"
}
EOF

echo -e "${GREEN}=== Memory Profiling Complete ===${NC}"
echo ""
echo "Results saved to: ${OUTPUT_DIR}"
echo ""
echo "What to look for:"
echo "  - Peak memory usage vs expected (should be O(subscribers))"
echo "  - Memory leaks (should be zero for Rust)"
echo "  - Large allocations (check Vec allocations, String formatting)"
echo "  - Excessive reallocations (Vec::push without capacity)"
echo ""
