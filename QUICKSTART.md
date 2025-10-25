# Quick Start Guide

## Installation

```bash
cargo build --release
```

The binary will be at `target/release/rs_cdr_generator`

## Basic Usage

### Generate 100K subscribers for 1 day (default)

```bash
./target/release/rs_cdr_generator
```

**Output:** `out/2025-01-01.tar.gz`

### Generate custom dataset

```bash
./target/release/rs_cdr_generator \
  --subs 1000000 \
  --days 7 \
  --start 2025-01-01 \
  --workers 8 \
  --out ./my_data
```

## Examples

### Small test dataset (1000 subs)

```bash
./target/release/rs_cdr_generator \
  --subs 1000 \
  --days 1 \
  --seed 42 \
  --workers 4 \
  --out test_data
```

**Performance:** ~0.1 seconds on Apple M1
**Output size:** ~2 MB (compressed)
**Events:** ~20,000 total (calls + SMS + data)

### Medium dataset (100K subs)

```bash
./target/release/rs_cdr_generator \
  --subs 100000 \
  --days 1 \
  --workers 8
```

**Performance:** ~10 seconds on Apple M1
**Output size:** ~500 MB (compressed)
**Events:** ~2,000,000 total

### Large dataset (1M subs, 7 days)

```bash
./target/release/rs_cdr_generator \
  --subs 1000000 \
  --days 7 \
  --workers 16 \
  --out large_dataset
```

**Performance:** ~10 minutes on Apple M1
**Output size:** ~25 GB (compressed)
**Events:** ~140,000,000 total

## Output Structure

```
out/
├── cells.csv                      # Cell tower catalog (reused)
├── 2025-01-01/
│   ├── cdr_2025-01-01_part001.csv # Event data
│   ├── stats_shard000.json        # Worker statistics
│   └── summary.json               # Aggregated stats
└── 2025-01-01.tar.gz             # Compressed archive
```

## Examining Output

### Check summary

```bash
cat out/2025-01-01/summary.json
```

### View CSV data

```bash
# First 10 events
head -n 11 out/2025-01-01/cdr_2025-01-01_part001.csv | column -t -s';'

# Count by event type
tail -n +2 out/2025-01-01/cdr_2025-01-01_part001.csv | \
  cut -d';' -f1 | sort | uniq -c
```

### Extract archive

```bash
tar -xzf out/2025-01-01.tar.gz
```

## Common Options

| Option | Description | Example |
|--------|-------------|---------|
| `--subs` | Number of subscribers | `--subs 100000` |
| `--days` | Number of days | `--days 7` |
| `--start` | Start date | `--start 2025-01-15` |
| `--seed` | Random seed | `--seed 12345` |
| `--workers` | Parallel workers | `--workers 8` |
| `--out` | Output directory | `--out ./data` |

## Tips

### Performance

- Use `--workers` = number of CPU cores
- For large datasets, use SSD storage
- Monitor disk space (uncompressed data can be large)

### Determinism

Same seed produces identical output:

```bash
# Run 1
./target/release/rs_cdr_generator --seed 42 --out run1

# Run 2 (will be identical to run1)
./target/release/rs_cdr_generator --seed 42 --out run2

# Verify
diff run1/2025-01-01.tar.gz run2/2025-01-01.tar.gz
# No output = files are identical
```

### Cleanup

```bash
rm -rf out/  # Remove all output
```

## Troubleshooting

### "Out of disk space"

Solution: Use smaller `--subs` or fewer `--days`, or clean up old output

### "Process killed"

Solution: Reduce `--workers` (using too much memory)

### Slow performance

Solution:
- Build with `--release` flag
- Increase `--workers` (but not beyond CPU count)
- Use faster storage (SSD)

## Next Steps

See [README.md](README.md) for:
- Full option reference
- CSV schema details
- YAML configuration
- Advanced usage
