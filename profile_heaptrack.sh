#!/bin/bash
# Heaptrack profiling script for rs_cdr_generator
# This script should be run INSIDE the Docker container

set -e

# Configuration
SUBS=${1:-1000000}  # Default: 1M subscribers
OUTPUT_DIR=${2:-prof_output}

echo "=== Heaptrack Memory Profiling ==="
echo "Subscribers: $SUBS"
echo "Output directory: $OUTPUT_DIR"
echo

# Clean previous profiling data
rm -rf $OUTPUT_DIR/*
mkdir -p $OUTPUT_DIR

echo "Starting CDR generation with heaptrack..."
echo

# Run with heaptrack
heaptrack --output $OUTPUT_DIR/heaptrack.out \
  ./target/profiling/rs_cdr_generator \
  --subs $SUBS \
  --start 2024-01-03 \
  --days 1 \
  --out $OUTPUT_DIR \
  --subscriber-db test_db_1m.arrow \
  --config test_config.yaml \
  --seed 42

echo
echo "=== Profiling Complete ===="
echo "Data file: $OUTPUT_DIR/heaptrack.out.*.gz"
echo
echo "To analyze results:"
echo "  # Print text report"
echo "  heaptrack_print $OUTPUT_DIR/heaptrack.out.*.gz"
echo
echo "  # Or copy .gz file to macOS and view with heaptrack GUI (if installed)"
echo
