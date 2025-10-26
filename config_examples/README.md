# Configuration Examples

This directory contains example configuration files for different use cases.

## Available Configurations

### 1. `small_test.yaml` - Quick Testing
**Recommended for:** Development, testing, quick validation

```bash
./target/release/rs_cdr_generator \
    --config config_examples/small_test.yaml \
    --start 2025-01-01 \
    --days 1 \
    --out out_small
```

**Specifications:**
- Subscribers: 1,000
- Cells: 500
- Coverage area: 25 km radius (Amsterdam)
- Expected output: ~20 MB per day
- Processing time: ~1-2 seconds

**Use cases:**
- Unit testing
- Quick verification
- Development iterations
- CI/CD pipelines

---

### 2. `medium_production.yaml` - Realistic Testing
**Recommended for:** Integration testing, realistic simulations

```bash
./target/release/rs_cdr_generator \
    --config config_examples/medium_production.yaml \
    --start 2025-01-01 \
    --days 7 \
    --out out_medium \
    --cleanup-after-archive
```

**Specifications:**
- Subscribers: 50,000
- Cells: 2,000
- Coverage area: 50 km radius
- Expected output: ~1 GB per day
- Processing time: ~30-60 seconds per day

**Use cases:**
- Integration testing
- Performance benchmarking
- Realistic production simulation
- Training datasets

**Features:**
- Includes special days (holidays)
- Realistic activity patterns
- Multiple operators (3 MCCMNC)

---

### 3. `large_scale.yaml` - Production Scale
**Recommended for:** Large-scale testing, production datasets

```bash
./target/release/rs_cdr_generator \
    --config config_examples/large_scale.yaml \
    --start 2025-01-01 \
    --days 30 \
    --out out_large \
    --workers 16 \
    --cleanup-after-archive
```

**Specifications:**
- Subscribers: 1,000,000
- Cells: 5,000
- Coverage area: 100 km radius (Netherlands)
- Expected output: ~20 GB per day
- Processing time: ~5-10 minutes per day

**Use cases:**
- Production-scale testing
- Performance stress testing
- Big data pipeline testing
- ML/AI training datasets

**Recommendations:**
- Use SSD for output
- At least 32 GB RAM
- 16+ CPU cores recommended
- Enable `--cleanup-after-archive`

---

## Configuration Parameters Guide

### Population Settings

```yaml
subscribers: 1000        # Total number of subscribers
cells: 500              # Number of cell towers
prefixes:               # Phone number prefixes (without country code)
  - "31612"
  - "31613"
mccmnc_pool:           # Mobile Country Code + Mobile Network Code
  - "20408"            # Operator 1
  - "20416"            # Operator 2
```

### Geographic Settings

```yaml
center_lat: 52.37      # Latitude of coverage center
center_lon: 4.895      # Longitude of coverage center
radius_km: 50.0        # Coverage radius in kilometers
```

### Event Rates

```yaml
avg_calls_per_user: 3.5          # Average calls per subscriber per day
avg_sms_per_user: 5.2            # Average SMS per subscriber per day
avg_data_sessions_per_user: 12.0 # Average data sessions per subscriber per day
```

### Traffic Direction

```yaml
mo_share_call: 0.5     # Probability of Mobile-Originated calls (0.0-1.0)
mo_share_sms: 0.5      # Probability of Mobile-Originated SMS (0.0-1.0)
```

### Call Dispositions

```yaml
call_dispositions:
  ANSWERED: 0.82       # Percentage of answered calls
  NO ANSWER: 0.12      # Percentage of unanswered calls
  BUSY: 0.04           # Percentage of busy signals
  FAILED: 0.015        # Percentage of failed calls
  CONGESTION: 0.005    # Percentage of congestion events
# Must sum to 1.0
```

### Temporal Patterns

#### Diurnal (Hourly) Patterns

```yaml
diurnal_weekday:       # 24 hourly multipliers for weekdays
  - 0.3                # 00:00 - low activity
  - 1.6                # 18:00 - peak activity
  # ... (24 values total)

diurnal_weekend:       # 24 hourly multipliers for weekends
  # Usually different pattern than weekdays
```

#### Seasonal (Monthly) Patterns

```yaml
seasonality:
  1: 0.95              # January - 95% of baseline
  7: 1.2               # July - 120% of baseline (summer peak)
  # ... (12 months)
```

#### Special Days

```yaml
special_days:
  "2025-12-31": 1.5    # New Year's Eve - 150% activity
  "2025-12-25": 0.7    # Christmas - 70% activity
```

### File Management

```yaml
rotate_bytes: 100000000  # File rotation size (~100 MB)
workers: 0               # Number of parallel workers (0 = auto-detect)
tz_name: "Europe/Amsterdam"  # Timezone for timestamps
```

---

## Quick Start Examples

### Minimal Command
```bash
# Uses default configuration
./target/release/rs_cdr_generator \
    --subs 1000 \
    --start 2025-01-01 \
    --days 1 \
    --out out_minimal
```

### With Custom Config
```bash
# Override specific parameters
./target/release/rs_cdr_generator \
    --config config_examples/medium_production.yaml \
    --subs 25000 \
    --days 14 \
    --out out_custom
```

### Full Production Example
```bash
# Large scale with cleanup
./target/release/rs_cdr_generator \
    --config config_examples/large_scale.yaml \
    --start 2025-01-01 \
    --days 30 \
    --out /data/cdr_output \
    --workers 24 \
    --cleanup-after-archive
```

---

## Output Structure

Each run creates the following structure:

```
out/
├── cells.csv                          # Cell tower catalog (reused)
├── 2025-01-01/
│   ├── cdr_2025-01-01_shard000_part001.csv
│   ├── cdr_2025-01-01_shard001_part001.csv
│   ├── ...
│   ├── stats_shard000.json
│   ├── stats_shard001.json
│   └── summary.json
├── 2025-01-01.tar.gz                  # Archived day
├── 2025-01-02/
│   └── ...
└── 2025-01-02.tar.gz
```

### CSV Format

Semicolon-delimited with the following columns:

```
event_type;msisdn_src;msisdn_dst;direction;start_ts_ms;end_ts_ms;
tz_name;tz_offset_min;duration_sec;mccmnc;imsi;imei;cell_id;
record_type;cause_for_record_closing;sms_segments;sms_status;
data_bytes_in;data_bytes_out;data_duration_sec;apn;rat
```

---

## Performance Tips

### For Small Datasets (< 10k subs)
- Use default workers (auto-detect)
- No need for cleanup
- Fine for regular HDD

### For Medium Datasets (10k - 100k subs)
- Explicitly set workers based on CPU cores
- Enable cleanup if disk space limited
- SSD recommended for faster I/O

### For Large Datasets (> 100k subs)
- Use `--workers` matching CPU cores
- Always use `--cleanup-after-archive`
- SSD required
- Monitor RAM usage (may need 16-32 GB)
- Consider splitting into smaller date ranges

### Optimization Flags

The release build includes:
- LTO (Link Time Optimization)
- Single codegen unit
- Maximum optimization level

Build with:
```bash
cargo build --release
```

---

## Validation

After generation, validate your data:

```bash
# Extract and check one day
tar -xzf out/2025-01-01.tar.gz -C /tmp/

# Count total events
cat /tmp/2025-01-01/cdr_*.csv | wc -l

# Count unique subscribers in DATA events
cat /tmp/2025-01-01/cdr_*.csv | \
    awk -F';' 'NR>1 && $1=="DATA" {print $2}' | \
    sort -u | wc -l

# View summary
cat /tmp/2025-01-01/summary.json
```

---

## Creating Custom Configurations

1. Copy an existing config:
   ```bash
   cp config_examples/small_test.yaml my_config.yaml
   ```

2. Edit parameters as needed

3. Validate with a test run:
   ```bash
   ./target/release/rs_cdr_generator \
       --config my_config.yaml \
       --subs 100 \
       --days 1 \
       --out out_test
   ```

4. Check output and adjust

---

## Troubleshooting

### "Too many open files" error
Increase system limits:
```bash
ulimit -n 4096
```

### Out of memory
- Reduce number of subscribers
- Reduce number of workers
- Use `--cleanup-after-archive`

### Slow performance
- Use SSD for output
- Increase `--workers`
- Check CPU/RAM usage with `top` or `htop`

### Unexpected event counts
- Remember: Poisson distribution adds variability (±20%)
- Run integration tests: `cargo test`
- Check configuration multipliers (diurnal, seasonal)
