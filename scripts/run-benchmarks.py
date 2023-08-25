#!/usr/bin/env python3
import argparse
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(
    prog='run-benchmarks',
    description='Run benchmarks with a specific runner')
parser.add_argument(
    '--runner-cmd', required=True,
    help='Command to launch runner (e.g. "java -jar target/s3-benchrunner.java")')
parser.add_argument(
    '--bucket', required=True,
    help='S3 bucket name')
parser.add_argument(
    '--region', required=True,
    help='AWS region (e.g. us-west-2)')
parser.add_argument(
    '--throughput', required=True, type=float,
    help='Target network throughput in gigabit/s (e.g. 100.0)')
parser.add_argument(
    '--benchmark', action='append',
    help='Path to specific benchmark JSON file. ' +
    'May be specified multiple times. ' +
    'By default, everything in benchmarks/ is run.')
args = parser.parse_args()

benchmarks = [Path(x) for x in args.benchmark]
if not benchmarks:
    benchmarks_dir = Path(__file__).parent.parent.joinpath('benchmarks')
    benchmarks = benchmarks_dir.glob('*.json')
    assert len(benchmarks) > 0

for benchmark in benchmarks:
    if not benchmark.exists():
        exit(f'benchmark not found: {str(benchmark)}')

    cmd = args.runner_cmd.split()
    cmd += [benchmark, args.bucket, args.region, str(args.throughput)]
    print(f'> {subprocess.list2cmdline(cmd)}')
    run = subprocess.run(cmd, text=True)

    # if runner skipped the benchmark, keep going
    if run.returncode == 123:
        continue

    # TODO: keep going or not?
    if run.returncode != 0:
        exit('benchmark failed')
