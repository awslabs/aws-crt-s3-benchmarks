#!/usr/bin/env python3
"""
aggregate-results.py

Collects all benchmark JSON result files and produces a combined
summary CSV, JSON, and terminal table for analysis.

Usage:
    python3 aggregate-results.py <RESULTS_DIR> [--csv output.csv] [--json output.json]

Example (after scp'ing results from all instances):
    python3 aggregate-results.py ./results --csv summary.csv --json summary.json
"""
import argparse
import csv
import json
import sys
from pathlib import Path


def find_result_files(results_dir: Path) -> list[Path]:
    """Find all .json result files recursively."""
    return sorted(results_dir.rglob("*.json"))


def load_result(path: Path) -> dict:
    """Load and validate a single result file."""
    with open(path) as f:
        data = json.load(f)
    if "metadata" not in data or "summary" not in data:
        raise ValueError(f"Invalid result file: {path}")
    return data


def main():
    parser = argparse.ArgumentParser(description="Aggregate benchmark results")
    parser.add_argument("results_dir", help="Directory containing result JSON files")
    parser.add_argument("--csv", default="summary.csv", help="Output CSV path")
    parser.add_argument("--json", default="summary.json", help="Output JSON path")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"ERROR: {results_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    result_files = find_result_files(results_dir)
    if not result_files:
        print(f"No .json files found in {results_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(result_files)} result files")

    all_results = []
    for path in result_files:
        try:
            data = load_result(path)
            all_results.append(data)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"  SKIP {path}: {e}", file=sys.stderr)

    if not all_results:
        print("No valid results", file=sys.stderr)
        sys.exit(1)

    # Sort by runner, workload, instance
    all_results.sort(key=lambda r: (
        r["metadata"]["runner"],
        r["metadata"]["workload"],
        r["metadata"]["instance_type"],
    ))

    # Write JSON
    with open(args.json, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"-> {args.json} ({len(all_results)} entries)")

    # Write CSV
    csv_fields = [
        "instance_id", "instance_type", "runner", "workload",
        "throughput_gbps", "wall_time_secs", "user_time_secs", "sys_time_secs",
        "cpu_percent", "peak_rss_kib", "peak_rss_mib",
        "python_heap_peak_bytes", "python_heap_peak_mib",
        "connections_peak", "connections_avg",
        "num_runs", "best_gbps", "worst_gbps", "median_gbps",
        "bucket", "region", "target_throughput_gbps", "timestamp",
    ]

    with open(args.csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()

        for result in all_results:
            meta = result["metadata"]
            summary = result["summary"]
            runs = result.get("runs", [])

            gbps_values = [r["gbps"] for r in runs if "gbps" in r]
            best = max(gbps_values) if gbps_values else summary["throughput_gbps"]
            worst = min(gbps_values) if gbps_values else summary["throughput_gbps"]
            median = sorted(gbps_values)[len(gbps_values) // 2] if gbps_values else summary["throughput_gbps"]

            row = {
                "instance_id": meta["instance_id"],
                "instance_type": meta["instance_type"],
                "runner": meta["runner"],
                "workload": meta["workload"],
                "throughput_gbps": summary["throughput_gbps"],
                "wall_time_secs": summary["wall_time_secs"],
                "user_time_secs": summary["user_time_secs"],
                "sys_time_secs": summary["sys_time_secs"],
                "cpu_percent": summary["cpu_percent"],
                "peak_rss_kib": summary["peak_rss_kib"],
                "peak_rss_mib": round(summary["peak_rss_kib"] / 1024, 1),
                "python_heap_peak_bytes": summary["python_heap_peak_bytes"],
                "python_heap_peak_mib": round(summary["python_heap_peak_bytes"] / (1024 * 1024), 1),
                "connections_peak": summary.get("connections_peak", 0),
                "connections_avg": summary.get("connections_avg", 0),
                "num_runs": len(runs),
                "best_gbps": best,
                "worst_gbps": worst,
                "median_gbps": median,
                "bucket": meta["bucket"],
                "region": meta["region"],
                "target_throughput_gbps": meta["target_throughput_gbps"],
                "timestamp": meta["timestamp"],
            }
            writer.writerow(row)

    print(f"-> {args.csv} ({len(all_results)} rows)")

    # Terminal summary
    print("\n" + "=" * 120)
    print(f"{'Runner':<15} {'Workload':<28} {'Instance':<14} "
          f"{'Gb/s':>7} {'RSS MiB':>9} {'CPU%':>6} {'Heap MiB':>9} {'Conns':>6} {'Runs':>5}")
    print("-" * 120)
    for result in all_results:
        m = result["metadata"]
        s = result["summary"]
        runs = result.get("runs", [])
        print(f"{m['runner']:<15} {m['workload']:<28} {m['instance_type']:<14} "
              f"{s['throughput_gbps']:>7.3f} {s['peak_rss_kib']/1024:>9.1f} "
              f"{s['cpu_percent']:>6} {s['python_heap_peak_bytes']/(1024*1024):>9.1f} "
              f"{s.get('connections_peak', 0):>6} {len(runs):>5}")
    print("=" * 120)


if __name__ == "__main__":
    main()
