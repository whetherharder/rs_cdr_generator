# RS CDR Generator

Rust port of the Python CDR Generator with full behavioral compatibility.

## Overview

**RS CDR Generator** is a high-performance Rust implementation of a unified CDR (Call Detail Record) generator for large-scale synthetic telecom datasets. It generates realistic CALL, SMS, and DATA events with proper temporal patterns, subscriber identities, and network characteristics.

### Version 5.1.0 (Optimized)

This is a direct port of the Python cdr_generator v5.1.0 with complete preservation of behavior.

**⚡ This branch includes performance optimizations achieving 2.83x speedup!**
See [PERFORMANCE_SUMMARY.md](PERFORMANCE_SUMMARY.md) for details.

## Features

- **Unified CSV Output**: Single semicolon-delimited CSV format for CALL/SMS/DATA events
- **Temporal Realism**:
  - Diurnal patterns (weekday/weekend variations)
  - Seasonal multipliers
  - Special day handling
- **Network Simulation**:
  - RAT-specific profiles (WCDMA, LTE, NR)
  - Cell tower distribution
  - Persistent cell catalog
- **Identity Management**:
  - Stable MSISDN ↔ IMSI ↔ MCCMNC mapping
  - IMEI generation with Luhn checksum
  - Contact networks with Zipf-like distribution
- **Performance**:
  - Parallel processing with Rayon
  - File rotation at configurable size (~100 MB default)
  - Per-day TAR.GZ bundling
- **Determinism**: Reproducible output with `--seed`

## Installation

### Prerequisites

- Rust 1.70+ (2021 edition)
- Cargo

### Build from Source

```bash
git clone <repository>
cd rs_cdr_generator
cargo build --release
```

The binary will be available at `target/release/rs_cdr_generator`.

## Usage

### Basic Usage

Generate 100,000 subscribers for 1 day:

```bash
./target/release/rs_cdr_generator
```

### Custom Configuration

```bash
./target/release/rs_cdr_generator \
  --subs 1000000 \
  --days 7 \
  --start 2025-01-01 \
  --out /path/to/output \
  --seed 12345 \
  --workers 8
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--subs` | Number of subscribers | 100000 |
| `--start` | Start date (YYYY-MM-DD) | 2025-01-01 |
| `--days` | Number of days to generate | 1 |
| `--out` | Output directory | out |
| `--seed` | Random seed for determinism | 42 |
| `--prefixes` | Phone number prefixes (comma-separated) | 31612,31613,31620,31621 |
| `--rotate-bytes` | File rotation threshold (bytes) | 100000000 |
| `--workers` | Number of parallel workers (0 = auto) | 0 |
| `--config` | YAML config file path | None |
| `--tz` | Timezone name | Europe/Amsterdam |
| `--cells` | Number of cell towers | 2000 |
| `--cell-center` | Cell center coordinates (lat,lon) | 52.370,4.890 |
| `--cell-radius-km` | Cell distribution radius (km) | 50.0 |
| `--mo-share-call` | MO probability for calls [0..1] | 0.5 |
| `--mo-share-sms` | MO probability for SMS [0..1] | 0.5 |
| `--imei-change-prob` | Daily IMEI change probability [0..1] | 0.02 |
| `--cleanup-after-archive` | Delete source files after archiving | false |

## Output Structure

```
out/
├── cells.csv                         # Persistent cell catalog
│   └─ cell_id, lat, lon, rat
│
├── 2025-01-01/                      # Per-day directory
│   ├── cdr_2025-01-01_part001.csv   # Rotating CSV parts
│   ├── cdr_2025-01-01_part002.csv
│   ├── stats_shard000.json          # Per-worker statistics
│   ├── stats_shard001.json
│   └── summary.json                 # Aggregated statistics
│
└── 2025-01-01.tar.gz               # Compressed bundle
```

### CSV Schema (22 Fields)

| Field | Description |
|-------|-------------|
| event_type | CALL, SMS, or DATA |
| msisdn_src | Source phone number |
| msisdn_dst | Destination phone number |
| direction | MO (mobile-originated) or MT (mobile-terminated) |
| start_ts_ms | Start timestamp (milliseconds since Unix epoch) |
| end_ts_ms | End timestamp (milliseconds since Unix epoch) |
| tz_name | Timezone name (e.g., Europe/Amsterdam) |
| tz_offset_min | UTC offset in minutes |
| duration_sec | Event duration in seconds |
| mccmnc | Mobile Country Code + Mobile Network Code |
| imsi | International Mobile Subscriber Identity |
| imei | International Mobile Equipment Identity |
| cell_id | Cell tower ID |
| record_type | Record type (mscVoiceRecord, sgsnSMORecord, etc.) |
| cause_for_record_closing | Cause code (normalRelease, noAnswer, etc.) |
| sms_segments | Number of SMS segments (1-3) |
| sms_status | SMS status (SENT, DELIVERED, FAILED) |
| data_bytes_in | Data upload bytes |
| data_bytes_out | Data download bytes |
| data_duration_sec | Data session duration |
| apn | Access Point Name (internet, ims, mms) |
| rat | Radio Access Technology (WCDMA, LTE, NR) |

## Configuration File (YAML)

You can override defaults with a YAML config file:

```yaml
# Population
subscribers: 1000000
cells: 5000
prefixes:
  - "31612"
  - "31613"
mccmnc_pool:
  - "20408"
  - "20416"

# Event rates (per user per day)
avg_calls_per_user: 5.0
avg_sms_per_user: 8.0
avg_data_sessions_per_user: 15.0

# Temporal patterns
diurnal_weekday:
  - 0.3  # 00:00
  - 0.2  # 01:00
  # ... 24 values

seasonality:
  1: 0.95  # January
  2: 0.9   # February
  # ... 12 values

special_days:
  "2025-12-31": 1.5  # New Year's Eve
  "2025-12-25": 0.7  # Christmas
```

## Performance Characteristics

**Benchmarks (Apple M1, 8 cores):**

| Subscribers | Days | Workers | Time | Output Size (compressed) |
|-------------|------|---------|------|--------------------------|
| 100K | 1 | 4 | ~10s | ~500 MB |
| 1M | 1 | 8 | ~90s | ~5 GB |
| 100K | 7 | 8 | ~70s | ~3.5 GB |

**Memory usage:** ~1-2 GB per worker process

## Behavioral Compatibility

This Rust port maintains **exact behavioral compatibility** with the Python version:

✅ **Identical RNG behavior**: Same seed produces same output
✅ **Same distributions**: Poisson, Lognormal, Normal, Zipf
✅ **Same temporal patterns**: Diurnal, seasonal, special days
✅ **Same identity generation**: MSISDN, IMSI, IMEI with Luhn
✅ **Same CSV format**: Field order, delimiter, encoding
✅ **Same file rotation**: Byte-accurate thresholds
✅ **Same statistics**: Event counts, aggregation

## Performance

### Benchmark Results

| Dataset | Subscribers | Time | Throughput |
|---------|-------------|------|------------|
| Small | 1,000 | 0.056s | 17,903 subs/sec |
| Medium | 10,000 | 0.229s | 43,613 subs/sec |
| Large | 50,000 | 1.765s | 28,321 subs/sec |
| Very Large | 100,000 | 8.210s | 12,179 subs/sec |

**Hardware**: 12-core macOS (Intel)

### Run Benchmarks

```bash
./benchmark.sh
```

See [PERFORMANCE_SUMMARY.md](PERFORMANCE_SUMMARY.md) for detailed analysis.

## Differences from Python Version

| Aspect | Python | Rust (Optimized) |
|--------|--------|------------------|
| Multiprocessing | `multiprocessing.spawn` | Rayon (thread pool) |
| Performance | ~10s for 100K subs/day | **~8s for 100K subs/day** |
| I/O overhead | High (flush per row) | **Low (batched)** |
| Memory | ~1-2 GB per process | ~1-2 GB total |
| Dependencies | pyyaml (optional) | All vendored |

## Development

### Run Tests

```bash
cargo test
```

### Build Documentation

```bash
cargo doc --open
```

### Format Code

```bash
cargo fmt
```

### Lint

```bash
cargo clippy
```

## License

Same as original Python version.

## Contributing

Contributions are welcome! Please ensure:

1. Behavioral compatibility is preserved
2. Tests pass: `cargo test`
3. Code is formatted: `cargo fmt`
4. No clippy warnings: `cargo clippy`

## Migration from Python

If you're migrating from the Python version:

1. **CLI compatibility**: Most flags are identical
2. **Config format**: YAML format is compatible
3. **Output format**: CSV format is identical
4. **Determinism**: Same seed → same output

### Example Migration

**Python:**
```bash
python main.py --subs 100000 --days 1 --seed 42 --out ./output
```

**Rust:**
```bash
./target/release/rs_cdr_generator --subs 100000 --days 1 --seed 42 --out ./output
```

## Troubleshooting

### Build Issues

If you encounter build errors:

```bash
cargo clean
cargo build --release
```

### Runtime Issues

**Out of memory:**
- Reduce `--workers`
- Process fewer subscribers per run

**Slow performance:**
- Increase `--workers` (but not beyond CPU count)
- Use `--release` build

## Acknowledgments

Original Python implementation: cdr_generator v5.1.0

Rust port maintains full behavioral compatibility while leveraging Rust's performance and safety guarantees.
