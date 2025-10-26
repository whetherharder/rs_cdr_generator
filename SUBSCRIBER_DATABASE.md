# Subscriber Database Feature

## Overview

The Subscriber Database feature allows you to generate and use a pre-defined database of subscribers with historical changes to their identities (IMSI, MSISDN, IMEI). This enables more realistic CDR generation that reflects real-world scenarios such as:

- Users changing devices (IMEI changes)
- Users changing SIM cards (IMSI changes)
- Phone numbers being released and reassigned to different subscribers
- New subscribers joining the network over time

## Quick Start

### 1. Generate a Subscriber Database

```bash
# Generate a database with 1,000 subscribers and 365 days of history
./target/release/rs_cdr_generator \
  --generate-db subscribers.csv \
  --db-size 1000 \
  --db-history-days 365 \
  --db-device-change-rate 0.15 \
  --db-number-release-rate 0.05 \
  --db-cooldown-days 90
```

**Parameters:**
- `--db-size` - Initial number of subscribers (default: 10,000)
- `--db-history-days` - Period of history in days (default: 365)
- `--db-device-change-rate` - Annual probability of device change, 0.0-1.0 (default: 0.15 = 15% per year)
- `--db-number-release-rate` - Annual probability of number release, 0.0-1.0 (default: 0.05 = 5% per year)
- `--db-cooldown-days` - Days before a released number can be reassigned (default: 90)

### 2. Validate a Subscriber Database

```bash
./target/release/rs_cdr_generator \
  --subscriber-db subscribers.csv \
  --validate-db
```

This will:
- Check chronological order of events
- Verify no MSISDN conflicts (same number assigned to multiple subscribers simultaneously)
- Validate IMSI/MSISDN/IMEI formats
- Display statistics (event count, unique IMSI count)

### 3. Generate CDR with Subscriber Database

```bash
./target/release/rs_cdr_generator \
  --subscriber-db subscribers.csv \
  --start 2025-01-01 \
  --days 7 \
  --out ./output
```

**Note:** Currently, the subscriber database is loaded and validated, but CDR generation integration with historical snapshots is pending implementation.

## CSV Format

The subscriber database is stored as a CSV file with the following format:

```csv
timestamp_ms,event_type,imsi,msisdn,imei,mccmnc
1704067200000,NEW_SUBSCRIBER,204081234567890,31612345678,123456789012345,20408
1704153600000,CHANGE_DEVICE,204081234567890,31612345678,987654321098765,20408
1704240000000,RELEASE_NUMBER,204081234567890,31612345678,,20408
1704326400000,ASSIGN_NUMBER,204082345678901,31612345678,111222333444555,20408
```

### Event Types

1. **NEW_SUBSCRIBER** - New subscriber joins the network
   - All fields (IMSI, MSISDN, IMEI) are populated

2. **CHANGE_DEVICE** - Subscriber changes device
   - IMSI and MSISDN remain the same
   - IMEI changes to new device

3. **CHANGE_SIM** - Subscriber changes SIM card
   - MSISDN remains the same
   - IMSI changes (new SIM card)
   - IMEI may also change

4. **RELEASE_NUMBER** - Phone number is released
   - MSISDN is freed
   - IMEI field is empty

5. **ASSIGN_NUMBER** - Phone number assigned to different subscriber
   - Previously released MSISDN assigned to new IMSI
   - New IMEI for the new subscriber

## Configuration via YAML

You can also specify subscriber database parameters in a YAML configuration file:

```yaml
# Subscriber database configuration
db_size: 10000
db_history_days: 365
db_device_change_rate: 0.15
db_number_release_rate: 0.05
db_cooldown_days: 90
```

Then use:

```bash
./target/release/rs_cdr_generator \
  --config my_config.yaml \
  --generate-db subscribers.csv
```

## Examples

### Example 1: Small Test Database

Generate a small database for testing:

```bash
./target/release/rs_cdr_generator \
  --generate-db test_db.csv \
  --db-size 100 \
  --db-history-days 30 \
  --seed 42
```

### Example 2: Large Production Database

Generate a large database with realistic parameters:

```bash
./target/release/rs_cdr_generator \
  --generate-db production_db.csv \
  --db-size 100000 \
  --db-history-days 730 \
  --db-device-change-rate 0.20 \
  --db-number-release-rate 0.08 \
  --db-cooldown-days 120
```

### Example 3: Validate and Use

```bash
# Step 1: Validate the database
./target/release/rs_cdr_generator \
  --subscriber-db production_db.csv \
  --validate-db

# Step 2: Generate CDR using the database
./target/release/rs_cdr_generator \
  --subscriber-db production_db.csv \
  --start 2025-01-01 \
  --days 30 \
  --out ./cdr_output
```

## Statistics and Insights

After generating a database, you'll see statistics like:

```
Generated 10234 events
Active subscribers: 9876
Released numbers in cooldown: 45
```

After validation:

```
✓ Database validation passed!
  Events: 10234
  Unique IMSI: 10050
```

## Behavioral Patterns

The generator creates realistic patterns:

1. **Device Changes**: Modeled with exponential distribution over time
2. **Number Releases**: Small percentage of users release their numbers each year
3. **Cooldown Period**: Released numbers wait 90 days (configurable) before reassignment
4. **New Subscribers**: Occasionally new subscribers join with completely new identities

## Technical Details

### Snapshot Generation

When a subscriber database is loaded, it builds "snapshots" - pre-computed states of each subscriber at different points in time. This allows for efficient lookups:

```rust
db.get_snapshot_at("204081234567890", timestamp_ms) -> Option<SubscriberSnapshot>
```

### Validation Rules

The validator checks:
- Events are in chronological order
- No MSISDN is assigned to multiple IMSI simultaneously
- IMSI format: 14-15 digits
- MSISDN format: 8-15 digits
- IMEI format: 15 digits with valid Luhn checksum

## Future Enhancements

The following features are planned:

1. **Integration with CDR Generation**: Use subscriber snapshots to generate CDR with historically accurate IMSI/MSISDN/IMEI mappings
2. **Query Interface**: Query subscriber state at any point in time
3. **Export to SQLite**: For more complex queries and analysis
4. **Roaming Scenarios**: Support for subscribers changing networks (MCCMNC changes)
5. **Churn Modeling**: More sophisticated subscriber lifecycle modeling

## Troubleshooting

### Issue: Validation fails with "MSISDN conflict"

This means the same phone number was assigned to multiple subscribers at the same time. This shouldn't happen with the generator, but might occur if you manually edit the CSV.

**Solution**: Check the CSV file for overlapping assignments of the same MSISDN.

### Issue: "Invalid IMSI length"

IMSI must be 14-15 digits.

**Solution**: Verify the MCCMNC pool in your configuration contains 5-digit codes.

### Issue: Database generation is slow

For very large databases (> 100,000 subscribers with > 365 days history), generation can take a few minutes.

**Solution**: This is normal. Consider using `--seed` for reproducibility and generating the database once, then reusing it.

## Contributing

When adding new event types or modifying the database schema, ensure:

1. Validation logic is updated
2. Tests are added
3. This documentation is updated
4. Backward compatibility is maintained for existing CSV files

## License

Same as the main rs_cdr_generator project.
