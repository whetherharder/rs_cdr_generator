#!/bin/bash
# Benchmark script for CDR generator performance testing

set -e

echo "=== CDR Generator Performance Benchmark ==="
echo ""

# Build release version
echo "Building release binary..."
cargo build --release --quiet
echo "✓ Build complete"
echo ""

# Test configurations
declare -a CONFIGS=(
    "1000:1:Small dataset"
    "10000:1:Medium dataset"
    "50000:1:Large dataset"
    "100000:1:Very large dataset"
)

# Results file
RESULTS_FILE="benchmark_results.txt"
echo "CDR Generator Benchmark Results" > $RESULTS_FILE
echo "Generated: $(date)" >> $RESULTS_FILE
echo "Host: $(uname -a)" >> $RESULTS_FILE
echo "CPUs: $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 'unknown')" >> $RESULTS_FILE
echo "" >> $RESULTS_FILE

# Run benchmarks
for config in "${CONFIGS[@]}"; do
    IFS=':' read -r subs days desc <<< "$config"

    echo "Testing: $desc ($subs subscribers, $days day(s))"

    # Clean up previous test
    rm -rf benchmark_test

    # Run with time measurement
    echo "  Running..."
    START_TIME=$(date +%s.%N)
    ./target/release/rs_cdr_generator \
        --subs $subs \
        --start 2025-01-01 \
        --days $days \
        --out benchmark_test \
        > /dev/null 2>&1
    END_TIME=$(date +%s.%N)

    ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)

    echo "  ✓ Complete: ${ELAPSED}s"
    echo ""

    # Save to results
    echo "[$desc]" >> $RESULTS_FILE
    echo "  Subscribers: $subs" >> $RESULTS_FILE
    echo "  Days: $days" >> $RESULTS_FILE
    echo "  Elapsed time: ${ELAPSED}s" >> $RESULTS_FILE
    echo "  Throughput: $(echo "scale=0; $subs / $ELAPSED" | bc) subs/sec" >> $RESULTS_FILE
    echo "" >> $RESULTS_FILE
done

# Clean up
rm -rf benchmark_test

echo "=== Benchmark Complete ==="
echo ""
echo "Results saved to: $RESULTS_FILE"
echo ""
cat $RESULTS_FILE
