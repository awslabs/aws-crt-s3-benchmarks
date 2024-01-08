#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import shlex

from utils import run, workload_paths_from_args

parser = argparse.ArgumentParser(
    description='Benchmark workloads with a specific runner')
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
    '--workload', action='append',
    help='Path to specific workload JSON file. ' +
    'May be specified multiple times. ' +
    'If omitted, everything in workloads/ is run.')
parser.add_argument(
    '--files-dir',
    help='Launch runner in this directory. ' +
    'Files are uploaded from and downloaded to here' +
    'If omitted, CWD is used.')

args = parser.parse_args()

workloads = workload_paths_from_args(args.workloads)
for workload in workloads:
    if not workload.exists():
        exit(f'workload not found: {str(workload)}')

    files_dir = args.files_dir if args.files_dir else str(Path.cwd())
    os.chdir(files_dir)

    # split using shell-like syntax,
    # in case runner-cmd has weird stuff like quotes, spaces, etc
    cmd = shlex.split(args.runner_cmd)

    cmd += [str(workload), args.bucket, args.region, str(args.throughput)]
    result = run(cmd, check=False)

    # if runner skipped the workload, keep going
    if result.returncode == 123:
        continue

    # if runner failed and we're only running 1 workload, exit with failure
    # but if we're running multiple workloads, keep going
    if result.returncode != 0:
        print('benchmark failed')
        if len(workloads) == 1:
            exit(1)
