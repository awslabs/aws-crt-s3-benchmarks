#!/usr/bin/env python3
import argparse
from pathlib import Path
import shlex
import subprocess
import sys

parser = argparse.ArgumentParser(
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
    'If omitted, everything in benchmarks/ is run.')
parser.add_argument(
    '--files-dir',
    help='Launch runner in this directory. ' +
    'Files are uploaded from and downloaded to here' +
    'If omitted, CWD is used.')

args = parser.parse_args()

if args.benchmark:
    benchmarks = [Path(x) for x in args.benchmark]
    for benchmark in benchmarks:
        if not benchmark.exists():
            exit(f'benchmark not found: {str(benchmark)}')
else:
    benchmarks_dir = Path(__file__).parent.parent.joinpath('benchmarks')
    benchmarks = sorted(benchmarks_dir.glob('*.run.json'))
    if not benchmarks:
        exit(f'no benchmark files found !?!')

for benchmark in benchmarks:
    if not benchmark.exists():
        exit(f'benchmark not found: {str(benchmark)}')

    files_dir = args.files_dir if args.files_dir else str(Path.cwd())

    # split using shell-like syntax,
    # in case runner-cmd has weird stuff like quotes, spaces, etc
    cmd = shlex.split(args.runner_cmd)

    cmd += [str(benchmark), args.bucket, args.region, str(args.throughput)]
    print(f'> {subprocess.list2cmdline(cmd)}', flush=True)
    run = subprocess.run(cmd, text=True, cwd=files_dir)

    # if runner skipped the benchmark, keep going
    if run.returncode == 123:
        continue

    # if runner failed and we're only running 1 benchmark, exit with failure
    # but if we're running multiple benchmarks, keep going
    if run.returncode != 0:
        print('benchmark failed')
        if len(benchmarks) == 1:
            exit(1)
