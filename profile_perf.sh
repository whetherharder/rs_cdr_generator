#!/bin/bash
# Native profiling script using perf (Linux) or Instruments (macOS)
# Provides detailed CPU and system-level profiling

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=== CDR Generator Native Profiling ==="
echo ""

# Default parameters
SUBS=${SUBS:-50000}
DAYS=${DAYS:-1}
START_DATE=${START_DATE:-2025-01-01}
OUTPUT_DIR="profiling_results/native_$(date +%Y%m%d_%H%M%S)"

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
            echo "  --output DIR   Output directory (default: profiling_results/native_<timestamp>)"
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
echo "  Output directory: ${OUTPUT_DIR}"
echo ""

# Build with profiling profile
echo "Building with profiling symbols..."
cargo build --profile=profiling --quiet
echo -e "${GREEN}✓ Build complete${NC}"
echo ""

BINARY="./target/profiling/rs_cdr_generator"
TEST_OUTPUT="${OUTPUT_DIR}/test_output"

# Platform-specific profiling
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macOS detected - using Instruments"
    echo ""

    # Check if Instruments is available
    if ! command -v instruments &> /dev/null; then
        echo -e "${RED}Error: Instruments not found${NC}"
        echo "Instruments is part of Xcode Command Line Tools"
        echo "Install with: xcode-select --install"
        exit 1
    fi

    # Time Profiler
    echo "Running Time Profiler..."
    TRACE_FILE="${OUTPUT_DIR}/time_profile.trace"

    instruments -t "Time Profiler" \
        -D "${TRACE_FILE}" \
        "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}"

    echo -e "${GREEN}✓ Time profiling complete${NC}"
    echo "  Trace file: ${TRACE_FILE}"
    echo ""

    # Allocations (memory) profiling
    echo "Running Allocations profiler..."
    ALLOC_TRACE="${OUTPUT_DIR}/allocations.trace"

    instruments -t "Allocations" \
        -D "${ALLOC_TRACE}" \
        "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}_alloc"

    echo -e "${GREEN}✓ Allocations profiling complete${NC}"
    echo "  Trace file: ${ALLOC_TRACE}"
    echo ""

    echo "To view the results:"
    echo "  open ${TRACE_FILE}"
    echo "  open ${ALLOC_TRACE}"
    echo ""

    # Try to open traces
    echo "Opening Instruments traces..."
    open "${TRACE_FILE}" 2>/dev/null || true
    open "${ALLOC_TRACE}" 2>/dev/null || true

elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "Linux detected - using perf"
    echo ""

    # Check if perf is available
    if ! command -v perf &> /dev/null; then
        echo -e "${RED}Error: perf not found${NC}"
        echo "Install with:"
        echo "  Ubuntu/Debian: sudo apt-get install linux-tools-common linux-tools-generic"
        echo "  Fedora: sudo dnf install perf"
        exit 1
    fi

    # Check perf permissions
    PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")
    if [ "$PARANOID" -gt 1 ]; then
        echo -e "${YELLOW}Warning: perf_event_paranoid is set to ${PARANOID}${NC}"
        echo "For better profiling, run: sudo sysctl -w kernel.perf_event_paranoid=1"
        echo ""
    fi

    # Record perf data
    echo "Recording perf data..."
    PERF_DATA="${OUTPUT_DIR}/perf.data"

    perf record \
        -F 999 \
        --call-graph dwarf \
        -o "${PERF_DATA}" \
        -- "${BINARY}" \
        --subs "${SUBS}" \
        --start "${START_DATE}" \
        --days "${DAYS}" \
        --out "${TEST_OUTPUT}"

    echo -e "${GREEN}✓ Recording complete${NC}"
    echo ""

    # Generate perf report
    echo "Generating perf report..."
    PERF_REPORT="${OUTPUT_DIR}/perf_report.txt"

    perf report \
        -i "${PERF_DATA}" \
        --stdio \
        --sort=dso,symbol \
        --percent-limit=1 \
        > "${PERF_REPORT}"

    echo -e "${GREEN}✓ Report generated${NC}"
    echo "  Data file: ${PERF_DATA}"
    echo "  Report: ${PERF_REPORT}"
    echo ""

    # Generate annotated source
    echo "Generating annotated assembly..."
    ANNOTATE_DIR="${OUTPUT_DIR}/annotate"
    mkdir -p "${ANNOTATE_DIR}"

    # Get top functions
    TOP_FUNCS=$(perf report -i "${PERF_DATA}" --stdio --percent-limit=5 | \
                grep -E "^\s+[0-9]" | awk '{print $3}' | head -10)

    for func in $TOP_FUNCS; do
        if [ -n "$func" ]; then
            perf annotate \
                -i "${PERF_DATA}" \
                --stdio \
                "${func}" \
                > "${ANNOTATE_DIR}/${func}.txt" 2>/dev/null || true
        fi
    done

    echo -e "${GREEN}✓ Annotation complete${NC}"
    echo ""

    # Display summary
    echo "Top 20 functions by CPU time:"
    echo "-----------------------------"
    head -30 "${PERF_REPORT}" | tail -20
    echo ""

    # Generate flamegraph from perf data if flamegraph script is available
    if [ -d "/opt/flamegraph" ] || [ -d "$HOME/flamegraph" ]; then
        echo "Generating flamegraph from perf data..."

        FLAMEGRAPH_DIR="/opt/flamegraph"
        if [ ! -d "$FLAMEGRAPH_DIR" ]; then
            FLAMEGRAPH_DIR="$HOME/flamegraph"
        fi

        perf script -i "${PERF_DATA}" | \
            "${FLAMEGRAPH_DIR}/stackcollapse-perf.pl" | \
            "${FLAMEGRAPH_DIR}/flamegraph.pl" \
            > "${OUTPUT_DIR}/perf_flamegraph.svg"

        echo -e "${GREEN}✓ Flamegraph generated: ${OUTPUT_DIR}/perf_flamegraph.svg${NC}"
        echo ""

        # Try to open
        if command -v xdg-open &> /dev/null; then
            xdg-open "${OUTPUT_DIR}/perf_flamegraph.svg" 2>/dev/null || true
        fi
    else
        echo "Tip: Install FlameGraph tools for visualization:"
        echo "  git clone https://github.com/brendangregg/FlameGraph /opt/flamegraph"
        echo ""
    fi

else
    echo -e "${RED}Unsupported platform: ${OSTYPE}${NC}"
    exit 1
fi

# Clean up test output
rm -rf "${TEST_OUTPUT}" "${TEST_OUTPUT}_alloc" 2>/dev/null || true

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

echo -e "${GREEN}=== Profiling Complete ===${NC}"
echo ""
echo "Results saved to: ${OUTPUT_DIR}"
echo ""
