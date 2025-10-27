# CDR Generator (Rust)

Unified CDR (Call Detail Record) generator for large-scale synthetic telecom datasets supporting CALL, SMS, and DATA events.

## Features

- **Multi-event support**: CALL, SMS, DATA session generation
- **High performance**: Parallel processing with optimized memory usage
- **Realistic data**: Diurnal patterns, seasonal variations, contact networks
- **Flexible compression**: Gzip and Zstd support with multi-threading
- **Database snapshots**: Subscriber state management with redb
- **Configurable**: YAML-based configuration for all parameters

## Quick Start

### Build

```bash
cargo build --release
```

### Run

```bash
./target/release/rs_cdr_generator generate \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --users 100000 \
  --workers 8 \
  --compression zstd
```

## Benchmarking

The project includes comprehensive microbenchmarks using [Criterion.rs](https://github.com/bheisler/criterion.rs).

### Run All Benchmarks

```bash
# Run all benchmarks
cargo bench

# Or use convenience script
./scripts/run_benchmarks.sh
```

### Run Specific Benchmark

```bash
cargo bench --bench cdr_generation
cargo bench --bench compression
cargo bench --bench csv_writing
cargo bench --bench end_to_end
```

### View Results

After running benchmarks, open the HTML report:

```bash
open target/criterion/report/index.html
```

## Profiling with Flamegraphs

### Prerequisites

Install cargo-flamegraph:

```bash
cargo install flamegraph
```

On macOS, grant DTrace permissions:

```bash
sudo dtruss -c ls
```

### Generate Flamegraphs

Generate flamegraphs for all benchmarks:

```bash
./scripts/generate_flamegraphs.sh
```

Or profile a specific benchmark:

```bash
cargo flamegraph --bench cdr_generation --profile profiling -o flamegraph.svg
```

Flamegraphs are saved to `flamegraphs/` directory. Open the .svg files in a browser to analyze performance hotspots.

### Available Benchmarks

1. **cdr_generation** - CDR event generation (CALL/SMS/DATA)
2. **compression** - Compression algorithm comparison (Gzip vs Zstd)
3. **csv_writing** - CSV serialization and buffer optimization
4. **end_to_end** - Full pipeline throughput and EventPool performance

See [benches/README.md](benches/README.md) for detailed benchmark documentation.

## Configuration

### Runtime (CLI)

Create a `config.yaml` file with your desired workload parameters:

```yaml
prefixes:
  - "7916"
  - "7917"

mccmnc_pool:
  - "25001"
  - "25002"

avg_calls_per_user: 5.0
avg_sms_per_user: 10.0
avg_data_sessions_per_user: 20.0

compression_type: zstd  # gzip, zstd, or none
```

### Benchmark presets

Ready-to-use YAML presets live in `benches/configs/`:
- `benchmark_micro.yaml` – fast Criterion iterations
- `benchmark_profiling.yaml` – balanced flamegraph runs
- `benchmark_throughput.yaml` – high-load throughput checks

Override the default preset with the `BENCH_CONFIG` environment variable:

```bash
BENCH_CONFIG=benches/configs/benchmark_throughput.yaml cargo bench
```

## Project Structure

```
rs_cdr_generator/
├── src/
│   ├── main.rs              - CLI entry point
│   ├── config.rs            - Configuration management
│   ├── generators.rs        - Event generation logic
│   ├── writer.rs            - CSV writer with rotation
│   ├── compression.rs       - Compression abstraction
│   └── subscriber_db_redb.rs - Subscriber database
├── benches/                 - Criterion benchmarks
│   ├── configs/             - Ready-to-use benchmark presets
│   │   ├── benchmark_micro.yaml
│   │   ├── benchmark_profiling.yaml
│   │   └── benchmark_throughput.yaml
│   ├── cdr_generation.rs
│   ├── compression.rs
│   ├── csv_writing.rs
│   ├── end_to_end.rs
│   └── README.md
└── scripts/                 - Utility scripts
    ├── run_benchmarks.sh
    └── generate_flamegraphs.sh
```

## Performance

Key optimizations:
- Event pool for zero-allocation event generation
- Pre-computed distributions (Poisson, LogNormal)
- Chunked subscriber loading for memory efficiency
- Multi-threaded Zstd compression
- Batched async I/O

See benchmark results for detailed performance metrics.

## License

[Your License Here]
