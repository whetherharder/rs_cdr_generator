# CDR Generator Benchmarks

This directory contains comprehensive microbenchmarks for the CDR Generator project using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Overview

The benchmarks measure performance of critical operations:
- **CDR Generation**: CALL, SMS, DATA event generation
- **Compression**: Gzip vs Zstd compression performance and ratios
- **CSV Writing**: Serialization and buffer optimization
- **End-to-End**: Complete pipeline throughput

## Running Benchmarks

### Quick Start

Run all benchmarks:
```bash
cargo bench
```

Run specific benchmark:
```bash
cargo bench --bench cdr_generation
cargo bench --bench compression
cargo bench --bench csv_writing
cargo bench --bench end_to_end
```

Use the convenience script:
```bash
./scripts/run_benchmarks.sh
```

### Viewing Results

After running benchmarks, open the HTML report:
```bash
open target/criterion/report/index.html
```

The report includes:
- Performance metrics (mean, median, std dev)
- Statistical comparison with previous runs
- Beautiful charts and graphs
- Regression detection

## Profiling with Flamegraphs

### Prerequisites

Install cargo-flamegraph:
```bash
cargo install flamegraph
```

On macOS, you may need to grant DTrace permissions:
```bash
sudo dtruss -c ls
```

### Generate Flamegraphs

Generate flamegraphs for all benchmarks:
```bash
./scripts/generate_flamegraphs.sh
```

Generate flamegraph for specific benchmark:
```bash
cargo flamegraph --bench cdr_generation --profile profiling -o flamegraphs/cdr_generation.svg
```

Flamegraphs are saved to `flamegraphs/` directory. Open the .svg files in a browser to analyze performance hotspots.

## Benchmark Details

### 1. CDR Generation (`cdr_generation.rs`)

Measures event generation performance for different event types:

**Benchmarks:**
- `call_generation` - CALL event generation (100, 1K, 10K events)
- `sms_generation` - SMS event generation (100, 1K, 10K events)
- `data_generation` - DATA session generation (100, 1K, 10K events)

**What it measures:**
- RNG performance
- Event field population
- Time calculations
- Disposition sampling

**Example:**
```bash
cargo bench --bench cdr_generation
```

### 2. Compression (`compression.rs`)

Compares compression algorithms on CDR data:

**Benchmarks:**
- `gzip_compression` - Gzip compression (100KB, 1MB, 10MB)
- `zstd_compression` - Zstd compression (100KB, 1MB, 10MB)
- `no_compression` - Baseline (100KB, 1MB, 10MB)
- `compression_ratio_comparison` - Ratio comparison

**What it measures:**
- Compression throughput (MB/s)
- Compression ratios
- CPU utilization

**Example:**
```bash
cargo bench --bench compression
```

### 3. CSV Writing (`csv_writing.rs`)

Measures CSV serialization performance:

**Benchmarks:**
- `csv_serialization` - Serialize EventRow to CSV (100, 1K, 10K events)
- `csv_buffer_sizes` - Buffer size optimization (8KB, 64KB, 256KB, 1MB)
- `event_row_clone` - Clone performance
- `event_row_reset` - Reset performance (pool reuse)

**What it measures:**
- CSV serialization overhead
- Buffer size impact
- EventRow operations

**Example:**
```bash
cargo bench --bench csv_writing
```

### 4. End-to-End (`end_to_end.rs`)

Measures complete pipeline throughput:

**Benchmarks:**
- `full_pipeline_call` - Generate + Serialize + Compress + Write (CALL events)
- `full_pipeline_mixed` - Mixed CALL/SMS/DATA events
- `event_pool_performance` - EventPool allocation performance
- `compression_comparison` - Compare None/Gzip/Zstd in pipeline

**What it measures:**
- Real-world throughput
- Memory allocations
- Pipeline bottlenecks

**Example:**
```bash
cargo bench --bench end_to_end
```

## Interpreting Results

### Criterion Output

```
cdr_generation/call_generation/1000
                        time:   [45.123 µs 45.456 µs 45.789 µs]
                        change: [-2.34% -1.23% +0.45%] (p = 0.23 > 0.05)
                        No change in performance detected.
```

- **time**: Mean execution time with confidence interval
- **change**: Performance change compared to previous baseline
- **p-value**: Statistical significance (< 0.05 means significant change)

### Flamegraph Analysis

Flamegraphs show CPU time distribution:
- **Width**: Proportion of CPU time
- **Color**: Random (for differentiation only)
- **Top frames**: Currently executing functions
- **Bottom frames**: Call stack

Look for:
- Wide bars = performance hotspots
- Unexpected call stacks
- Allocation patterns

## Best Practices

1. **Consistent Environment**:
   - Close other applications
   - Disable CPU frequency scaling
   - Run multiple times for confidence

2. **Baseline Comparison**:
   - Save baseline before changes: `cargo bench --save-baseline before`
   - After changes: `cargo bench --baseline before`

3. **Focus on Relevant Metrics**:
   - Throughput (events/sec) for generation
   - Compression ratio + speed tradeoff
   - Memory allocations (use flamegraph)

4. **Profile Before Optimizing**:
   - Always generate flamegraph first
   - Identify actual bottlenecks
   - Measure impact after changes

## Configuration

Benchmarks use `config.yaml` from project root. Ensure it exists before running benchmarks.

Criterion configuration in `Cargo.toml`:
```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[profile.bench]
inherits = "release"
debug = true

[profile.profiling]
inherits = "release"
debug = true
strip = false
```

## CI/CD Integration

To run benchmarks in CI without generating reports:
```bash
cargo bench --no-fail-fast -- --test
```

## Troubleshooting

### Benchmark fails to compile

Ensure all dependencies are available:
```bash
cargo clean
cargo build --release
cargo bench
```

### Flamegraph permission denied (macOS)

Grant DTrace permissions:
```bash
sudo dtruss -c ls
```

### Benchmarks show high variance

- Close other applications
- Run with isolated cores: `taskset -c 0,1 cargo bench`
- Increase sample size in benchmark code

## Further Reading

- [Criterion.rs User Guide](https://bheisler.github.io/criterion.rs/book/)
- [Flamegraph](https://www.brendangregg.com/flamegraphs.html)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
