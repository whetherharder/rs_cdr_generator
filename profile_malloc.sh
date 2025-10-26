#!/bin/bash
# Memory profiling script using malloc_history on macOS
# Shows stack traces with symbols for all memory allocations

set -e

# Configuration
SUBS=${1:-1000000}  # Default: 1M subscribers
SNAPSHOTS=${2:-3}    # Number of snapshots to take
INTERVAL=${3:-60}    # Seconds between snapshots

echo "=== malloc_history Memory Profiling ==="
echo "Subscribers: $SUBS"
echo "Snapshots: $SNAPSHOTS"
echo "Interval: ${INTERVAL}s"
echo

# Clean previous profiling data
rm -rf prof_output
mkdir -p prof_output

# Enable malloc stack logging
export MallocStackLogging=1
export MallocStackLoggingNoCompact=1

echo "Starting CDR generation with malloc logging enabled..."
./target/profiling/rs_cdr_generator \
  --subs $SUBS \
  --start 2024-01-03 \
  --days 1 \
  --out prof_output \
  --subscriber-db test_db_1m.arrow \
  --config test_config.yaml \
  --seed 42 &

PID=$!
echo "Process PID: $PID"
echo

# Take snapshots
for i in $(seq 1 $SNAPSHOTS); do
  sleep $INTERVAL

  echo "=== Snapshot $i/$SNAPSHOTS ==="
  echo "Taking malloc_history snapshot..."

  # Check if process is still running
  if ! ps -p $PID > /dev/null; then
    echo "Process finished before snapshot $i"
    break
  fi

  # Get memory info
  echo "Memory stats:"
  ps -o pid,rss,vsz -p $PID

  # Take detailed malloc snapshot (by size)
  malloc_history $PID -allBySize > prof_output/malloc_snapshot_${i}_bysize.txt 2>&1 || true

  # Take detailed malloc snapshot (by count)
  malloc_history $PID -allByCount > prof_output/malloc_snapshot_${i}_bycount.txt 2>&1 || true

  # Also get vmmap for comparison
  vmmap $PID > prof_output/vmmap_snapshot_${i}.txt 2>&1 || true

  echo "Snapshot $i saved to prof_output/"
  echo
done

# Wait a bit more then kill
echo "Waiting 10 more seconds before killing process..."
sleep 10

if ps -p $PID > /dev/null; then
  echo "Killing process $PID..."
  kill $PID
  wait $PID 2>/dev/null || true
fi

echo
echo "=== Profiling Complete ==="
echo "Results saved in prof_output/"
echo
echo "Analyze with:"
echo "  less prof_output/malloc_snapshot_1_bysize.txt"
echo "  less prof_output/malloc_snapshot_1_bycount.txt"
echo "  less prof_output/vmmap_snapshot_1.txt"
