# Performance Profiling Guide

Comprehensive guide for profiling and optimizing the CDR Generator.

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Benchmarking](#benchmarking)
- [Profiling Tools](#profiling-tools)
- [Analyzing Results](#analyzing-results)
- [Common Bottlenecks](#common-bottlenecks)
- [Best Practices](#best-practices)

---

## Quick Start

### Run Full Benchmarks

```bash
./benchmark.sh
```

This will run benchmarks from 1K to 10M subscribers and save results to `benchmark_results/`.

### Generate Flamegraph

```bash
./profile_flamegraph.sh --subs 50000
```

Opens an interactive flamegraph showing CPU time distribution.

### Profile with Native Tools

```bash
# macOS
./profile_perf.sh --subs 50000

# Linux
./profile_perf.sh --subs 50000
```

### Compare Benchmark Results

```bash
# Compare two most recent runs
./compare_benchmarks.py --compare

# Show trends across all runs
./compare_benchmarks.py --trends
```

---

## Installation

### Core Requirements

```bash
# Install Rust profiling tools
cargo install flamegraph
cargo install hyperfine  # Optional but recommended
```

### Platform-Specific Tools

#### macOS

Instruments comes with Xcode Command Line Tools:

```bash
xcode-select --install
```

#### Linux (Ubuntu/Debian)

```bash
# perf tools
sudo apt-get install linux-tools-common linux-tools-generic

# Optional: heaptrack (best for memory profiling)
sudo apt-get install heaptrack

# Optional: valgrind
sudo apt-get install valgrind

# Set perf permissions
sudo sysctl -w kernel.perf_event_paranoid=1
```

#### Linux (Fedora/RHEL)

```bash
sudo dnf install perf
sudo dnf install valgrind
```

---

## Benchmarking

### Basic Usage

```bash
# Run all benchmarks (1K to 10M subscribers)
./benchmark.sh

# Results are saved to:
# benchmark_results/<timestamp>/
#   - benchmark_results.json  (machine-readable)
#   - benchmark_results.txt   (human-readable)
#   - system_info.json        (environment metadata)
```

### Customizing Benchmarks

Edit `benchmark.sh` to modify test configurations:

```bash
declare -a CONFIGS=(
    "1000:1:Tiny dataset (1K):1"
    "10000:1:Small dataset (10K):1"
    # subscribers:days:description:warmup_runs
)
```

### Using hyperfine

For more precise measurements on smaller datasets, install hyperfine:

```bash
cargo install hyperfine
```

The benchmark script will automatically detect and use it.

### Comparing Results

```bash
# Compare last two runs
./compare_benchmarks.py --compare

# Show trends across all runs
./compare_benchmarks.py --trends

# Show latest run only
./compare_benchmarks.py --latest

# Export to CSV for analysis
./compare_benchmarks.py --export-csv results.csv
```

---

## Profiling Tools

### 1. Criterion Benchmarks (Micro-benchmarks)

Best for: Measuring performance of individual functions

```bash
# Run all criterion benchmarks
cargo bench

# Run specific benchmark
cargo bench -- lognorm_params

# Results are saved to:
# target/criterion/
```

Criterion provides:
- Statistical analysis
- HTML reports
- Comparison with previous runs
- Baseline tracking

### 2. Flamegraph (CPU Profiling)

Best for: Identifying CPU bottlenecks and hot code paths

```bash
# Basic usage
./profile_flamegraph.sh --subs 50000

# Custom parameters
./profile_flamegraph.sh --subs 1000000 --days 7 --start 2025-01-01

# Results saved to:
# profiling_results/flamegraph_<timestamp>/
#   - flamegraph.svg (interactive visualization)
#   - metadata.json
```

**Reading Flamegraphs:**
- Width = CPU time spent in function
- Height = call stack depth
- Click to zoom in on specific functions
- Use Ctrl+F to search for functions
- Look for wide boxes at the top for optimization targets

### 3. Native Profiling (perf/Instruments)

Best for: System-level profiling, cache misses, branch mispredictions

#### macOS (Instruments)

```bash
./profile_perf.sh --subs 50000

# Opens Instruments with:
# - Time Profiler (CPU usage)
# - Allocations (memory allocations)
```

#### Linux (perf)

```bash
./profile_perf.sh --subs 50000

# Generates:
# - perf.data (raw profiling data)
# - perf_report.txt (text summary)
# - annotate/ (annotated source/assembly)
# - perf_flamegraph.svg (if FlameGraph tools installed)
```

**Advanced perf commands:**

```bash
# Record with specific events
perf record -e cycles,cache-misses ./target/release/rs_cdr_generator ...

# View report interactively
perf report -i profiling_results/native_<timestamp>/perf.data

# Annotate specific function
perf annotate -i perf.data worker_generate
```

### 4. Memory Profiling

Best for: Finding memory leaks, excessive allocations, memory usage patterns

```bash
./profile_memory.sh --subs 50000
```

#### macOS (Instruments)

Generates:
- Allocations trace (allocation patterns)
- Leaks trace (memory leaks - should be zero for Rust!)
- Basic memory statistics

#### Linux (heaptrack)

```bash
# Heaptrack provides the best memory analysis for Rust
./profile_memory.sh --subs 50000

# Opens GUI showing:
# - Peak memory usage
# - Allocation hotspots
# - Temporary allocations
# - Allocation flamegraph
```

#### Linux (valgrind)

```bash
# Massif (heap profiling)
./profile_memory.sh --subs 50000

# For detailed leak checking (slow):
RUN_MEMCHECK=1 ./profile_memory.sh --subs 10000
```

---

## Analyzing Results

### Identifying Bottlenecks

#### 1. CPU Bottlenecks

Look for in flamegraph/perf:
- `worker_generate` - main CDR generation loop
- `write_row` - I/O operations
- `sample_poisson`, `sample_call_duration` - RNG operations
- String formatting operations
- JSON/CSV serialization

#### 2. Memory Issues

Look for in memory profiler:
- Large `Vec` allocations without pre-allocation
- Excessive `String` allocations
- Temporary allocations in hot loops
- Memory growth during execution

#### 3. I/O Bottlenecks

Signs:
- High time in `write_row`, `flush`
- Difference between buffered/unbuffered writes
- File system operations

### Performance Metrics

**Good performance indicators:**
- Throughput: >10,000 subs/sec on modern hardware
- Memory: O(n) growth with subscriber count
- CPU scaling: Near-linear with core count (rayon parallelism)

**Warning signs:**
- Throughput degradation at scale
- Memory usage >> subscriber count * expected_size
- Poor CPU core utilization

---

## Common Bottlenecks

### 1. Random Number Generation

**Issue:** RNG can be a bottleneck in event generation

**Solutions:**
- Use faster RNG (rand's StdRng is already good)
- Pre-generate random values in batches
- Use lookup tables for distributions

**Profile:**
```bash
cargo bench -- poisson
cargo bench -- call_duration
```

### 2. String Formatting

**Issue:** Creating formatted strings for CSV output

**Solutions:**
- Pre-allocate string buffers
- Use `write!` instead of `format!`
- Consider using fixed-size buffers

**Look for in flamegraph:**
- `std::fmt::*` taking significant time
- `String::push_str` allocations

### 3. I/O Operations

**Issue:** Writing to files can be slow

**Already optimized:**
- Using `BufWriter` for buffering
- File rotation to avoid huge files
- Parallel generation per subscriber

**Further optimization:**
- Increase buffer size if needed
- Consider memory-mapped files for huge datasets

### 4. Vector Allocations

**Issue:** Reallocating vectors as they grow

**Solution:**
- Pre-allocate with `Vec::with_capacity`
- Reserve space before push operations

**Check in memory profiler:**
```bash
./profile_memory.sh --subs 50000
# Look for allocations in Vec::push, Vec::extend
```

### 5. Parallel Processing Overhead

**Issue:** Too much/too little parallelism

**Current approach:**
- Parallel per-subscriber generation (rayon)
- Single-threaded within subscriber

**Tuning:**
```bash
# Test with different worker counts
./target/release/rs_cdr_generator --workers 1 ...
./target/release/rs_cdr_generator --workers 4 ...
./target/release/rs_cdr_generator --workers 8 ...
```

---

## Best Practices

### 1. Measure Before Optimizing

Always profile before making changes:
```bash
# Baseline
./profile_flamegraph.sh --subs 100000
git checkout -b optimization/improve-rng

# Make changes, then compare
./profile_flamegraph.sh --subs 100000
```

### 2. Use Appropriate Tools

| Goal | Tool |
|------|------|
| Find slow functions | flamegraph, perf |
| Measure specific function | criterion bench |
| Check memory usage | heaptrack, Instruments |
| Compare runs | compare_benchmarks.py |
| Test at scale | benchmark.sh with large datasets |

### 3. Test at Scale

Performance characteristics can change with dataset size:

```bash
# Test multiple scales
./profile_flamegraph.sh --subs 10000    # Small
./profile_flamegraph.sh --subs 100000   # Medium
./profile_flamegraph.sh --subs 1000000  # Large
```

### 4. Track Performance Over Time

```bash
# Run benchmarks regularly
./benchmark.sh

# Compare with previous runs
./compare_benchmarks.py --trends

# Export for long-term tracking
./compare_benchmarks.py --export-csv historical_results.csv
```

### 5. Document Optimizations

When you find and fix a bottleneck:
1. Document the issue
2. Save before/after flamegraphs
3. Record performance improvement in commit message

Example:
```
git commit -m "perf: Pre-allocate event vectors

Reduces allocations in hot loop by ~40%.
Improves throughput by 15% for large datasets.

Before: 8,500 subs/sec
After: 9,800 subs/sec

Flamegraphs saved to docs/optimizations/
"
```

---

## Troubleshooting

### Flamegraph Not Working

**macOS:**
```bash
# Reinstall cargo-flamegraph
cargo install --force flamegraph

# Check DTrace permissions
sudo dtruss ls  # Should work
```

**Linux:**
```bash
# Check perf permissions
cat /proc/sys/kernel/perf_event_paranoid
# Should be 1 or less

# Fix permissions
sudo sysctl -w kernel.perf_event_paranoid=1

# Install perf tools
sudo apt-get install linux-tools-$(uname -r)
```

### Out of Memory

For very large datasets (5M+ subscribers):

```bash
# Monitor memory usage
./profile_memory.sh --subs 5000000

# If needed, reduce parallelism
./target/release/rs_cdr_generator --workers 2 --subs 5000000
```

### Slow Benchmarks

If benchmarks are taking too long:

```bash
# Edit benchmark.sh to reduce dataset sizes
# or skip large tests temporarily
```

---

## Resources

- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Flamegraph Guide](https://www.brendangregg.com/flamegraphs.html)
- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)
- [perf Examples](https://www.brendangregg.com/perf.html)

---

## Support

For questions or issues with profiling:
1. Check this guide
2. Review existing flamegraphs in `profiling_results/`
3. Compare benchmark results with `compare_benchmarks.py`
4. Open an issue with profiling data
