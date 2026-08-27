#!/usr/bin/env python3
"""
bench-wrapper.py

Wraps the benchmark runner with tracemalloc to measure Python heap peak.
Writes peak heap usage (bytes) to a designated file after the run completes.

Usage (called by run-benchmarks.sh, not directly):
    python3 bench-wrapper.py <heap_stats_file> <runner_main.py> <runner_args...>
"""
import sys
import os
import tracemalloc

# Start tracing memory allocations before importing anything else
tracemalloc.start()

# argv layout:
#   [0] = this script
#   [1] = heap_stats_file (where to write peak bytes)
#   [2] = path to runner main.py
#   [3:] = runner args (S3_CLIENT, WORKLOAD, BUCKET, REGION, THROUGHPUT)
heap_stats_file = sys.argv[1]
runner_main_path = sys.argv[2]
runner_args = sys.argv[2:]  # main.py + its args

# Set up sys.argv as if we invoked main.py directly
sys.argv = runner_args

# Add runner directory to path so relative imports work
runner_dir = os.path.dirname(os.path.abspath(runner_main_path))
sys.path.insert(0, runner_dir)

# --- Run the actual benchmark (inline from main.py) ---

import argparse
import time
from runner import (
    BenchmarkConfig,
    BenchmarkRunner,
    bytes_to_gigabit,
    ns_to_secs,
)


def create_runner(id: str, config: BenchmarkConfig) -> BenchmarkRunner:
    if id == 'crt-python':
        from runner.crt import CrtBenchmarkRunner
        return CrtBenchmarkRunner(config)
    if id.startswith('boto3'):
        from runner.boto3 import Boto3BenchmarkRunner
        return Boto3BenchmarkRunner(config, use_crt=id.endswith('crt'))
    if id.startswith('cli'):
        from runner.cli import CliBenchmarkRunner
        return CliBenchmarkRunner(config, use_crt=id.endswith('crt'))
    raise ValueError(f'Unknown S3 client: {id}')


PARSER = argparse.ArgumentParser(description='Python benchmark runner.')
PARSER.add_argument('S3_CLIENT', choices=(
    'crt-python', 'boto3-classic', 'boto3-crt', 'cli-classic', 'cli-crt'))
PARSER.add_argument('WORKLOAD')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)
PARSER.add_argument('--verbose', action='store_true')

args = PARSER.parse_args()
config = BenchmarkConfig(args.WORKLOAD, args.BUCKET, args.REGION,
                         args.TARGET_THROUGHPUT, args.verbose)

runner = create_runner(args.S3_CLIENT, config)
bytes_per_run = config.bytes_per_run()

# Run benchmark loop
app_start_ns = time.perf_counter_ns()
for run_i in range(config.max_repeat_count):
    runner.prepare_run()
    run_start_ns = time.perf_counter_ns()
    runner.run()
    run_secs = ns_to_secs(time.perf_counter_ns() - run_start_ns)
    print(f'Run:{run_i+1} '
          f'Secs:{run_secs:f} '
          f'Gb/s:{bytes_to_gigabit(bytes_per_run) / run_secs:f}',
          flush=True)
    app_secs = ns_to_secs(time.perf_counter_ns() - app_start_ns)
    if app_secs >= config.max_repeat_secs:
        break

# --- Write heap stats ---
current, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()

with open(heap_stats_file, 'w') as f:
    f.write(f'{peak}\n')
