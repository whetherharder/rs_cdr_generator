# Integration Tests for CDR Generator

This directory contains integration tests that validate the correctness of CDR event generation.

## Test Suite: `event_counts_test.rs`

### Test 1: `test_event_generation_counts`

**Purpose:** Validates that the generator produces the correct number and distribution of events.

**What it tests:**
- Generates events for 1,000 subscribers across 4 worker shards
- Verifies total event counts for CALL, SMS, and DATA match expected averages (within ±20% tolerance due to Poisson distribution)
- Validates that unique `src_msisdn` counts are correct:
  - DATA events should have ~1,000 unique subscribers (99%+ of total)
  - No more unique DATA subscribers than specified with `--subs`
  - MO (Mobile Originated) CALL and SMS events have appropriate unique subscriber counts

**Expected behavior:**
```
Subscribers: 1000
Expected events (approximate):
  - CALL events: ~3,500 (3.5 per subscriber)
  - SMS events: ~5,200 (5.2 per subscriber)
  - DATA events: ~12,000 (12.0 per subscriber)

Unique subscribers should match --subs parameter (±1% for DATA due to Poisson)
```

### Test 2: `test_no_duplicate_subscribers_across_shards`

**Purpose:** Ensures that parallel worker shards don't overlap subscribers.

**What it tests:**
- Generates events for 500 subscribers across 4 shards
- Collects unique `src_msisdn` from DATA events per shard
- Verifies that no subscriber appears in multiple shards
- Validates proper subscriber partitioning across workers

**Expected behavior:**
```
Each shard should have ~125 unique subscribers (500 / 4)
No subscriber overlap between any two shards
```

## Running the Tests

```bash
# Run all tests
cargo test

# Run only integration tests
cargo test --test event_counts_test

# Run with detailed output
cargo test --test event_counts_test -- --nocapture

# Run tests sequentially with output
cargo test --test event_counts_test -- --nocapture --test-threads=1
```

## Test Output Example

```
=== Event Generation Test Results ===
Subscribers: 1000
Workers: 4

Expected averages per subscriber:
  CALL events: 3.5
  SMS events: 5.2
  DATA events: 12

Expected totals (approximate):
  CALL events: ~3500
  SMS events: ~5200
  DATA events: ~12000

Actual results:
  CALL events: 3477
  SMS events: 5187
  DATA events: 11955

Unique subscribers (src_msisdn):
  In DATA events: 1000
  In MO CALL events: 808
  In MO SMS events: 928
  Overall: 1005

✅ All validation checks passed!
```

## Why These Tests Matter

### Bug Detection
These tests caught a critical race condition bug where multiple workers were overwriting each other's output files, causing:
- Loss of data from 11 out of 12 workers
- Only ~8,333 unique subscribers instead of 100,000
- Incorrect event distribution

### Validation Points
1. **Event Count Accuracy**: Ensures Poisson distribution sampling works correctly
2. **Subscriber Uniqueness**: Verifies no duplicate or missing subscribers
3. **Shard Isolation**: Confirms parallel workers operate independently
4. **File Naming**: Validates unique file names per shard (bug fix verification)

## Dependencies

The tests use:
- `tempfile`: For creating temporary test directories
- Standard library: `HashMap`, `HashSet` for data validation
- Project modules: `generators`, `config`, `cells`, `timezone_utils`
