#!/bin/bash
set -e

echo "=== CDR Generator Benchmark with redb ==="
echo ""

# Test 1K
echo "=== Benchmark: 1K subscribers (redb) ==="
hyperfine --runs 3 --warmup 1 \
  --prepare "rm -rf bench_test" \
  './target/release/rs_cdr_generator --start 2024-01-03 --days 1 --out bench_test --config bench_config_1k.yaml --seed 42'
echo ""

# Test 10K
echo "=== Benchmark: 10K subscribers (redb) ==="
hyperfine --runs 3 --warmup 1 \
  --prepare "rm -rf bench_test" \
  './target/release/rs_cdr_generator --start 2024-01-03 --days 1 --out bench_test --config bench_config_10k.yaml --seed 42'
echo ""

# Test 100K
echo "=== Benchmark: 100K subscribers (redb) ==="
hyperfine --runs 3 \
  --prepare "rm -rf bench_test" \
  './target/release/rs_cdr_generator --start 2024-01-03 --days 1 --out bench_test --config bench_config_100k.yaml --seed 42'
echo ""

# Test 1M
echo "=== Benchmark: 1M subscribers (redb) ==="
hyperfine --runs 3 \
  --prepare "rm -rf bench_test" \
  './target/release/rs_cdr_generator --start 2024-01-03 --days 1 --out bench_test --config bench_config_1m.yaml --seed 42'
echo ""

echo "=== All benchmarks complete ==="
