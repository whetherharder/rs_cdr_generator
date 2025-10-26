#!/bin/bash
# Enhanced benchmark script for CDR generator performance testing
# Supports large datasets (1M-10M subscribers), system metrics, and result tracking

set -e

echo "=== CDR Generator Performance Benchmark ==="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Build release version
echo "Building release binary..."
cargo build --release --quiet
echo -e "${GREEN}✓ Build complete${NC}"
echo ""

# Create results directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="benchmark_results/${TIMESTAMP}"
mkdir -p "${RESULTS_DIR}"

# Get system information
get_system_info() {
    echo "Collecting system information..."

    # Git commit
    GIT_COMMIT=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

    # CPU info
    if [[ "$OSTYPE" == "darwin"* ]]; then
        CPU_MODEL=$(sysctl -n machdep.cpu.brand_string)
        CPU_CORES=$(sysctl -n hw.ncpu)
        TOTAL_MEM=$(sysctl -n hw.memsize)
        OS_VERSION=$(sw_vers -productVersion)
    else
        CPU_MODEL=$(grep "model name" /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)
        CPU_CORES=$(nproc)
        TOTAL_MEM=$(grep MemTotal /proc/meminfo | awk '{print $2 * 1024}')
        OS_VERSION=$(uname -r)
    fi

    # Rust version
    RUST_VERSION=$(rustc --version)

    cat > "${RESULTS_DIR}/system_info.json" <<EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "git_commit": "${GIT_COMMIT}",
  "git_branch": "${GIT_BRANCH}",
  "cpu_model": "${CPU_MODEL}",
  "cpu_cores": ${CPU_CORES},
  "total_memory_bytes": ${TOTAL_MEM},
  "os_version": "${OS_VERSION}",
  "os_type": "${OSTYPE}",
  "rust_version": "${RUST_VERSION}"
}
EOF
}

get_system_info
echo -e "${GREEN}✓ System info collected${NC}"
echo ""

# Test configurations
# Format: subscribers:days:description:warmup_runs
declare -a CONFIGS=(
    "1000:1:Tiny dataset (1K):1"
    "10000:1:Small dataset (10K):1"
    "50000:1:Medium dataset (50K):1"
    "100000:1:Large dataset (100K):1"
    "500000:1:Very large dataset (500K):1"
    "1000000:1:Huge dataset (1M):0"
    "5000000:1:Extreme dataset (5M):0"
    "10000000:1:Maximum dataset (10M):0"
)

# Check if hyperfine is available for better benchmarking
USE_HYPERFINE=false
if command -v hyperfine &> /dev/null; then
    USE_HYPERFINE=true
    echo -e "${GREEN}Using hyperfine for precise benchmarking${NC}"
else
    echo -e "${YELLOW}Note: Install hyperfine for better statistics (cargo install hyperfine)${NC}"
fi
echo ""

# Results file
RESULTS_FILE="${RESULTS_DIR}/benchmark_results.txt"
JSON_RESULTS="${RESULTS_DIR}/benchmark_results.json"

echo "CDR Generator Benchmark Results" > $RESULTS_FILE
echo "Generated: $(date)" >> $RESULTS_FILE
cat "${RESULTS_DIR}/system_info.json" >> $RESULTS_FILE
echo "" >> $RESULTS_FILE

# Start JSON array
echo "[" > $JSON_RESULTS

FIRST_RESULT=true

# Run benchmarks
for config in "${CONFIGS[@]}"; do
    IFS=':' read -r subs days desc warmup <<< "$config"

    echo -e "${YELLOW}Testing: $desc ($subs subscribers, $days day(s))${NC}"

    # Clean up previous test
    rm -rf benchmark_test

    BENCHMARK_CMD="./target/release/rs_cdr_generator \
        --subs $subs \
        --start 2025-01-01 \
        --days $days \
        --out benchmark_test"

    if [ "$USE_HYPERFINE" = true ] && [ "$warmup" -gt 0 ]; then
        # Use hyperfine for detailed statistics
        echo "  Running with hyperfine..."

        HYPERFINE_OUTPUT="${RESULTS_DIR}/hyperfine_${subs}_subs.json"

        hyperfine \
            --warmup $warmup \
            --runs 3 \
            --export-json "$HYPERFINE_OUTPUT" \
            --prepare "rm -rf benchmark_test" \
            "$BENCHMARK_CMD" 2>&1 | grep -v "^$"

        # Extract timing from hyperfine JSON
        ELAPSED=$(python3 -c "import json; data=json.load(open('$HYPERFINE_OUTPUT')); print(f\"{data['results'][0]['mean']:.3f}\")")
        STDDEV=$(python3 -c "import json; data=json.load(open('$HYPERFINE_OUTPUT')); print(f\"{data['results'][0]['stddev']:.3f}\")")

        echo -e "  ${GREEN}✓ Complete: ${ELAPSED}s (±${STDDEV}s)${NC}"
    else
        # Use time measurement for very large datasets
        echo "  Running..."

        # Capture memory usage (macOS)
        if [[ "$OSTYPE" == "darwin"* ]]; then
            START_TIME=$(date +%s.%N)
            /usr/bin/time -l $BENCHMARK_CMD > /dev/null 2> "${RESULTS_DIR}/time_${subs}_subs.txt"
            END_TIME=$(date +%s.%N)

            MAX_MEM=$(grep "maximum resident set size" "${RESULTS_DIR}/time_${subs}_subs.txt" | awk '{print $1}')
        else
            START_TIME=$(date +%s.%N)
            /usr/bin/time -v $BENCHMARK_CMD > /dev/null 2> "${RESULTS_DIR}/time_${subs}_subs.txt"
            END_TIME=$(date +%s.%N)

            MAX_MEM=$(grep "Maximum resident set size" "${RESULTS_DIR}/time_${subs}_subs.txt" | awk '{print $6 * 1024}')
        fi

        ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
        STDDEV="0"

        echo -e "  ${GREEN}✓ Complete: ${ELAPSED}s${NC}"
    fi

    # Calculate throughput
    THROUGHPUT=$(echo "scale=0; $subs / $ELAPSED" | bc)

    # Get output directory size
    if [ -d "benchmark_test" ]; then
        OUTPUT_SIZE=$(du -sk benchmark_test | cut -f1)
    else
        OUTPUT_SIZE=0
    fi

    echo ""

    # Save to text results
    echo "[$desc]" >> $RESULTS_FILE
    echo "  Subscribers: $subs" >> $RESULTS_FILE
    echo "  Days: $days" >> $RESULTS_FILE
    echo "  Elapsed time: ${ELAPSED}s" >> $RESULTS_FILE
    if [ "$STDDEV" != "0" ]; then
        echo "  Std deviation: ${STDDEV}s" >> $RESULTS_FILE
    fi
    echo "  Throughput: ${THROUGHPUT} subs/sec" >> $RESULTS_FILE
    echo "  Output size: ${OUTPUT_SIZE} KB" >> $RESULTS_FILE
    if [ -n "$MAX_MEM" ]; then
        echo "  Max memory: ${MAX_MEM} bytes" >> $RESULTS_FILE
    fi
    echo "" >> $RESULTS_FILE

    # Add comma separator for JSON array (except first element)
    if [ "$FIRST_RESULT" = false ]; then
        echo "," >> $JSON_RESULTS
    fi
    FIRST_RESULT=false

    # Save to JSON results
    cat >> $JSON_RESULTS <<EOF
  {
    "description": "$desc",
    "subscribers": $subs,
    "days": $days,
    "elapsed_seconds": $ELAPSED,
    "stddev_seconds": $STDDEV,
    "throughput_subs_per_sec": $THROUGHPUT,
    "output_size_kb": $OUTPUT_SIZE,
    "max_memory_bytes": ${MAX_MEM:-0}
  }
EOF
done

# Close JSON array
echo "" >> $JSON_RESULTS
echo "]" >> $JSON_RESULTS

# Clean up
rm -rf benchmark_test

echo ""
echo -e "${GREEN}=== Benchmark Complete ===${NC}"
echo ""
echo "Results saved to: ${RESULTS_DIR}"
echo "  - ${RESULTS_FILE}"
echo "  - ${JSON_RESULTS}"
echo "  - ${RESULTS_DIR}/system_info.json"
echo ""

# Display summary
echo "Summary:"
cat $RESULTS_FILE

# Check for performance regressions
echo ""
echo "Checking for performance changes..."

# Find previous benchmark result
PREV_RESULT=$(find benchmark_results -name "benchmark_results.json" -type f | grep -v "$TIMESTAMP" | sort -r | head -1)

if [ -n "$PREV_RESULT" ]; then
    echo "Comparing with: $PREV_RESULT"

    # Simple comparison (can be enhanced with compare_benchmarks.py)
    echo "Run ./compare_benchmarks.py to see detailed comparison"
else
    echo "No previous results found for comparison"
fi

echo ""
echo -e "${GREEN}Done!${NC}"
