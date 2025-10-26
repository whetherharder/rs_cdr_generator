# Performance Profiling - Quick Start

Quick reference for performance testing and profiling of CDR Generator.

## 🚀 Quick Commands

```bash
# Full benchmark suite (1K to 10M subscribers)
./benchmark.sh

# Generate flamegraph (CPU profiling)
./profile_flamegraph.sh --subs 50000

# Memory profiling
./profile_memory.sh --subs 50000

# Native profiling (perf/Instruments)
./profile_perf.sh --subs 50000

# Compare benchmark results
./compare_benchmarks.py --compare

# Run criterion micro-benchmarks
cargo bench
```

## 📁 Files Created

### Scripts
- **`benchmark.sh`** - Full benchmark suite with metrics tracking
- **`profile_flamegraph.sh`** - CPU flamegraph generation
- **`profile_perf.sh`** - Native profiling (perf/Instruments)
- **`profile_memory.sh`** - Memory usage analysis
- **`compare_benchmarks.py`** - Compare and visualize results

### Configuration
- **`benches/cdr_benchmark.rs`** - Criterion micro-benchmarks
- **`Cargo.toml`** - Added profiling profiles and criterion dependency

### Documentation
- **`PROFILING.md`** - Complete profiling guide

### Results Directories (gitignored)
- `benchmark_results/` - Benchmark results with timestamps
- `profiling_results/` - Profiling outputs (flamegraphs, traces, etc.)

## 🎯 Common Use Cases

### 1. Quick Performance Check
```bash
./benchmark.sh
./compare_benchmarks.py --latest
```

### 2. Find CPU Bottlenecks
```bash
./profile_flamegraph.sh --subs 100000
# Opens interactive flamegraph in browser
```

### 3. Memory Analysis
```bash
./profile_memory.sh --subs 100000
# Check for memory leaks and allocation patterns
```

### 4. Compare Changes
```bash
# Before changes
./benchmark.sh

# Make code changes...

# After changes
./benchmark.sh
./compare_benchmarks.py --compare
```

### 5. Test at Scale
```bash
# Test with large dataset
./profile_flamegraph.sh --subs 1000000 --days 1
```

## 📊 Understanding Results

### Benchmark Output
- **Throughput**: subscribers/second (higher is better)
- **Memory**: Peak RSS usage
- **Output size**: Generated data size
- JSON format for machine processing

### Flamegraph
- **Width** = CPU time in function
- **Height** = Call stack depth
- Click to zoom, Ctrl+F to search
- Look for wide boxes at top

### Memory Profiling
- **Peak usage**: Maximum memory used
- **Allocations**: Number and size of allocations
- **Leaks**: Should be zero for Rust

## ⚡ Performance Targets

| Dataset | Expected Throughput | Expected Memory |
|---------|-------------------|-----------------|
| 10K subs | >20,000 subs/sec | <100 MB |
| 100K subs | >15,000 subs/sec | <500 MB |
| 1M subs | >10,000 subs/sec | <3 GB |
| 10M subs | >8,000 subs/sec | <20 GB |

*Targets are approximate and depend on hardware*

## 🔧 Installation

```bash
# Core tools
cargo install flamegraph
cargo install hyperfine  # optional but recommended

# macOS
xcode-select --install  # For Instruments

# Linux
sudo apt-get install linux-tools-common linux-tools-generic
sudo apt-get install heaptrack  # Best memory profiler for Rust
sudo sysctl -w kernel.perf_event_paranoid=1  # Allow perf
```

## 📚 More Information

See **[PROFILING.md](PROFILING.md)** for:
- Detailed tool explanations
- Advanced usage examples
- Analyzing specific bottlenecks
- Best practices
- Troubleshooting

## 🐛 Common Issues

**Flamegraph not working on Linux:**
```bash
sudo sysctl -w kernel.perf_event_paranoid=1
```

**Out of memory on large datasets:**
```bash
# Reduce parallelism
./target/release/rs_cdr_generator --workers 2 --subs 5000000
```

**Benchmarks too slow:**
Edit `benchmark.sh` to reduce dataset sizes or skip large tests.

---

**Happy profiling! 🚀**
