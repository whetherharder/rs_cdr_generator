#!/bin/bash
# Flamegraph profiling script for CDR generator
# Generates interactive flamegraphs to visualize CPU usage and identify bottlenecks

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=== CDR Generator Flamegraph Profiling ==="
echo ""

# Check if cargo-flamegraph is installed
if ! command -v cargo-flamegraph &> /dev/null && ! [ -f "$HOME/.cargo/bin/cargo-flamegraph" ]; then
    echo -e "${RED}Error: cargo-flamegraph is not installed${NC}"
    echo ""
    echo "Install it with:"
    echo "  cargo install flamegraph"
    echo ""
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        echo "On Linux, you may also need to install perf:"
        echo "  sudo apt-get install linux-tools-common linux-tools-generic"
        echo ""
        echo "And set perf permissions:"
        echo "  sudo sysctl -w kernel.perf_event_paranoid=1"
    fi
    exit 1
fi

# Default parameters
SUBS=${SUBS:-50000}
DAYS=${DAYS:-1}
START_DATE=${START_DATE:-2025-01-01}
OUTPUT_DIR="profiling_results/flamegraph_$(date +%Y%m%d_%H%M%S)"

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
            echo "  --output DIR   Output directory (default: profiling_results/flamegraph_<timestamp>)"
            echo "  -h, --help     Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --subs 100000 --days 1"
            echo "  $0 --subs 1000000"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

mkdir -p "$OUTPUT_DIR"
mkdir -p flamegraph_temp

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

# Run flamegraph profiling
echo "Running flamegraph profiling..."
echo "This will take some time depending on dataset size..."
echo ""

FLAMEGRAPH_OUTPUT="${OUTPUT_DIR}/flamegraph.svg"

# Set environment for better flamegraph output
export CARGO_PROFILE_PROFILING_DEBUG=true

# Run cargo-flamegraph
cargo flamegraph \
    --profile=profiling \
    --output="${FLAMEGRAPH_OUTPUT}" \
    -- \
    --subs "${SUBS}" \
    --start "${START_DATE}" \
    --days "${DAYS}" \
    --out flamegraph_temp

echo ""
echo -e "${GREEN}✓ Flamegraph generation complete${NC}"
echo ""

# Clean up temporary output
rm -rf flamegraph_temp

# Get flamegraph file size
FLAMEGRAPH_SIZE=$(du -h "${FLAMEGRAPH_OUTPUT}" | cut -f1)

# Save profiling metadata
cat > "${OUTPUT_DIR}/metadata.json" <<EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_commit": "$(git rev-parse HEAD 2>/dev/null || echo 'unknown')",
  "git_branch": "$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')",
  "subscribers": ${SUBS},
  "days": ${DAYS},
  "start_date": "${START_DATE}",
  "flamegraph_file": "flamegraph.svg",
  "flamegraph_size": "${FLAMEGRAPH_SIZE}"
}
EOF

echo "Results saved to: ${OUTPUT_DIR}"
echo "  - Flamegraph: ${FLAMEGRAPH_OUTPUT} (${FLAMEGRAPH_SIZE})"
echo "  - Metadata: ${OUTPUT_DIR}/metadata.json"
echo ""

# Try to open flamegraph in browser
echo "Opening flamegraph..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    open "${FLAMEGRAPH_OUTPUT}"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    if command -v xdg-open &> /dev/null; then
        xdg-open "${FLAMEGRAPH_OUTPUT}"
    else
        echo "Please open ${FLAMEGRAPH_OUTPUT} in your web browser"
    fi
fi

echo ""
echo -e "${GREEN}=== Profiling Complete ===${NC}"
echo ""
echo "How to read the flamegraph:"
echo "  - Width of boxes = CPU time spent in that function"
echo "  - Click on boxes to zoom in"
echo "  - Search (Ctrl+F) to find specific functions"
echo "  - Look for wide boxes at the top for optimization targets"
echo ""
echo "Common bottlenecks to look for:"
echo "  - worker_generate (main CDR generation)"
echo "  - write_row (I/O operations)"
echo "  - sample_poisson, sample_call_duration (RNG operations)"
echo "  - String formatting and allocation"
echo ""
