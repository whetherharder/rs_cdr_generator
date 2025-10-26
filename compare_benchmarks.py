#!/usr/bin/env python3
"""
Compare benchmark results across multiple runs
Visualizes performance trends and detects regressions
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import argparse


class BenchmarkComparator:
    def __init__(self, results_dir: str = "benchmark_results"):
        self.results_dir = Path(results_dir)
        self.runs = []

    def load_all_results(self):
        """Load all benchmark results from the results directory"""
        if not self.results_dir.exists():
            print(f"Error: Results directory '{self.results_dir}' not found")
            return False

        # Find all benchmark result directories
        run_dirs = sorted([d for d in self.results_dir.iterdir() if d.is_dir()])

        if not run_dirs:
            print(f"No benchmark results found in {self.results_dir}")
            return False

        for run_dir in run_dirs:
            results_file = run_dir / "benchmark_results.json"
            system_info_file = run_dir / "system_info.json"

            if results_file.exists():
                try:
                    with open(results_file) as f:
                        results = json.load(f)

                    system_info = {}
                    if system_info_file.exists():
                        with open(system_info_file) as f:
                            system_info = json.load(f)

                    self.runs.append({
                        "timestamp": run_dir.name,
                        "results": results,
                        "system_info": system_info,
                        "path": run_dir
                    })
                except json.JSONDecodeError as e:
                    print(f"Warning: Could not parse {results_file}: {e}")
                    continue

        print(f"Loaded {len(self.runs)} benchmark run(s)")
        return True

    def compare_latest_two(self):
        """Compare the two most recent benchmark runs"""
        if len(self.runs) < 2:
            print("Need at least 2 benchmark runs for comparison")
            if len(self.runs) == 1:
                print("\nShowing results from single run:")
                self.show_single_run(self.runs[0])
            return

        prev_run = self.runs[-2]
        curr_run = self.runs[-1]

        print("\n" + "=" * 80)
        print("BENCHMARK COMPARISON")
        print("=" * 80)
        print(f"\nPrevious: {prev_run['timestamp']}")
        print(f"Current:  {curr_run['timestamp']}")
        print()

        # Compare git commits
        prev_commit = prev_run['system_info'].get('git_commit', 'unknown')[:8]
        curr_commit = curr_run['system_info'].get('git_commit', 'unknown')[:8]
        if prev_commit != curr_commit:
            print(f"Git commits: {prev_commit} → {curr_commit}")
            print()

        # Build comparison table
        print(f"{'Test':<30} {'Previous':<15} {'Current':<15} {'Change':<15} {'Status'}")
        print("-" * 95)

        for prev_result in prev_run['results']:
            desc = prev_result['description']
            subs = prev_result['subscribers']

            # Find matching result in current run
            curr_result = None
            for r in curr_run['results']:
                if r['subscribers'] == subs:
                    curr_result = r
                    break

            if not curr_result:
                continue

            prev_time = float(prev_result['elapsed_seconds'])
            curr_time = float(curr_result['elapsed_seconds'])

            if prev_time > 0:
                change_pct = ((curr_time - prev_time) / prev_time) * 100
                change_str = f"{change_pct:+.1f}%"

                # Determine status
                if abs(change_pct) < 5:
                    status = "✓ OK"
                elif change_pct < 0:
                    status = "✓✓ FASTER"
                else:
                    status = "⚠ SLOWER"
            else:
                change_str = "N/A"
                status = "?"

            prev_throughput = prev_result.get('throughput_subs_per_sec', 0)
            curr_throughput = curr_result.get('throughput_subs_per_sec', 0)

            print(f"{desc:<30} "
                  f"{prev_time:>7.2f}s ({prev_throughput:>5} s/s)  "
                  f"{curr_time:>7.2f}s ({curr_throughput:>5} s/s)  "
                  f"{change_str:<15} {status}")

        print()

        # Memory comparison if available
        if self._has_memory_stats(prev_run) and self._has_memory_stats(curr_run):
            print("\nMemory Usage Comparison:")
            print(f"{'Test':<30} {'Previous':<15} {'Current':<15} {'Change'}")
            print("-" * 75)

            for prev_result in prev_run['results']:
                subs = prev_result['subscribers']
                curr_result = next((r for r in curr_run['results']
                                    if r['subscribers'] == subs), None)

                if curr_result:
                    prev_mem = prev_result.get('max_memory_bytes', 0)
                    curr_mem = curr_result.get('max_memory_bytes', 0)

                    if prev_mem > 0 and curr_mem > 0:
                        prev_mem_mb = prev_mem / 1024 / 1024
                        curr_mem_mb = curr_mem / 1024 / 1024
                        change_pct = ((curr_mem - prev_mem) / prev_mem) * 100

                        print(f"{prev_result['description']:<30} "
                              f"{prev_mem_mb:>7.1f} MB      "
                              f"{curr_mem_mb:>7.1f} MB      "
                              f"{change_pct:+.1f}%")

            print()

    def show_single_run(self, run: Dict[str, Any]):
        """Display results from a single benchmark run"""
        print("\n" + "=" * 80)
        print("BENCHMARK RESULTS")
        print("=" * 80)
        print(f"\nTimestamp: {run['timestamp']}")

        system_info = run['system_info']
        if system_info:
            print(f"Git commit: {system_info.get('git_commit', 'unknown')[:8]}")
            print(f"CPU: {system_info.get('cpu_model', 'unknown')}")
            print(f"Cores: {system_info.get('cpu_cores', 'unknown')}")

        print()
        print(f"{'Test':<30} {'Time':<15} {'Throughput':<20} {'Memory'}")
        print("-" * 85)

        for result in run['results']:
            desc = result['description']
            time = result['elapsed_seconds']
            throughput = result.get('throughput_subs_per_sec', 0)
            memory = result.get('max_memory_bytes', 0)

            memory_str = f"{memory / 1024 / 1024:.1f} MB" if memory > 0 else "N/A"

            print(f"{desc:<30} {time:>7.2f}s       {throughput:>8} subs/sec    {memory_str}")

        print()

    def show_trends(self):
        """Show performance trends across all runs"""
        if len(self.runs) < 2:
            print("Need at least 2 runs to show trends")
            return

        print("\n" + "=" * 80)
        print("PERFORMANCE TRENDS")
        print("=" * 80)
        print()

        # Group by subscriber count
        subs_counts = set()
        for run in self.runs:
            for result in run['results']:
                subs_counts.add(result['subscribers'])

        for subs in sorted(subs_counts):
            print(f"\n{subs} subscribers:")
            print(f"{'Date':<20} {'Time (s)':<15} {'Throughput':<20} {'Trend'}")
            print("-" * 70)

            times = []
            for run in self.runs:
                result = next((r for r in run['results']
                               if r['subscribers'] == subs), None)

                if result:
                    time = float(result['elapsed_seconds'])
                    throughput = result.get('throughput_subs_per_sec', 0)
                    times.append(time)

                    # Calculate trend
                    if len(times) >= 2:
                        prev_time = times[-2]
                        change_pct = ((time - prev_time) / prev_time) * 100
                        if abs(change_pct) < 5:
                            trend = "→ stable"
                        elif change_pct < 0:
                            trend = f"↑ {abs(change_pct):.1f}% faster"
                        else:
                            trend = f"↓ {change_pct:.1f}% slower"
                    else:
                        trend = "—"

                    timestamp = run['timestamp']
                    print(f"{timestamp:<20} {time:>7.2f}s       {throughput:>8} subs/sec    {trend}")

    def export_csv(self, output_file: str):
        """Export all results to CSV for external analysis"""
        import csv

        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp', 'git_commit', 'subscribers', 'days',
                'elapsed_seconds', 'throughput_subs_per_sec',
                'output_size_kb', 'max_memory_bytes'
            ])

            for run in self.runs:
                timestamp = run['timestamp']
                git_commit = run['system_info'].get('git_commit', 'unknown')

                for result in run['results']:
                    writer.writerow([
                        timestamp,
                        git_commit,
                        result['subscribers'],
                        result['days'],
                        result['elapsed_seconds'],
                        result.get('throughput_subs_per_sec', 0),
                        result.get('output_size_kb', 0),
                        result.get('max_memory_bytes', 0)
                    ])

        print(f"Exported results to {output_file}")

    def _has_memory_stats(self, run: Dict[str, Any]) -> bool:
        """Check if run has memory statistics"""
        return any(r.get('max_memory_bytes', 0) > 0 for r in run['results'])


def main():
    parser = argparse.ArgumentParser(
        description='Compare CDR generator benchmark results'
    )
    parser.add_argument(
        '--dir',
        default='benchmark_results',
        help='Directory containing benchmark results (default: benchmark_results)'
    )
    parser.add_argument(
        '--compare',
        action='store_true',
        help='Compare the two most recent runs'
    )
    parser.add_argument(
        '--trends',
        action='store_true',
        help='Show performance trends across all runs'
    )
    parser.add_argument(
        '--export-csv',
        metavar='FILE',
        help='Export all results to CSV file'
    )
    parser.add_argument(
        '--latest',
        action='store_true',
        help='Show results from the latest run only'
    )

    args = parser.parse_args()

    comparator = BenchmarkComparator(args.dir)

    if not comparator.load_all_results():
        return 1

    if args.export_csv:
        comparator.export_csv(args.export_csv)
    elif args.trends:
        comparator.show_trends()
    elif args.latest:
        if comparator.runs:
            comparator.show_single_run(comparator.runs[-1])
    elif args.compare or len(comparator.runs) >= 2:
        comparator.compare_latest_two()
    else:
        # Default: show latest if only one run
        if comparator.runs:
            comparator.show_single_run(comparator.runs[0])

    return 0


if __name__ == '__main__':
    sys.exit(main())
