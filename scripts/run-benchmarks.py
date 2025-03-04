#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import shlex

from utils import S3_CLIENTS, run, workload_paths_from_args
from utils.metrics import report_metrics

parser = argparse.ArgumentParser(
    description='Benchmark workloads with a specific runner')
parser.add_argument(
    '--runner-cmd', required=True,
    help='Command to launch runner (e.g. "java -jar target/s3-benchrunner.java")')
parser.add_argument(
    '--s3-client', required=True, choices=S3_CLIENTS.keys(),
    help='S3 client to benchmark (must be supported by runner)')
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
    '--workloads', nargs='+',
    help='Paths to specific workload JSON files. ' +
    'If omitted, everything in workloads/ is run.')
parser.add_argument(
    '--files-dir',
    help='Launch runner in this directory. ' +
    'Files are uploaded from and downloaded to here. ' +
    'If omitted, CWD is used.')
parser.add_argument(
    '--report-metrics', action='store_true',
    help='Report metrics to CloudWatch')
parser.add_argument(
    '--metrics-instance-type',
    help='If reporting metrics: EC2 instance type (e.g. c5n.18xlarge)')
parser.add_argument(
    '--metrics-branch',
    help='If reporting metrics: branch being benchmarked')
parser.add_argument(
    '--network-interface-names',
    type=str,
    default='default',
    help='If reporting metrics: branch being benchmarked')

args = parser.parse_args()

# run each workload
workloads = workload_paths_from_args(args.workloads)
for workload in workloads:
    if not workload.exists():
        exit(f'workload not found: {str(workload)}')

    files_dir = args.files_dir if args.files_dir else str(Path.cwd())
    os.chdir(files_dir)

    # split using shell-like syntax,
    # in case runner-cmd has weird stuff like quotes, spaces, etc
    cmd = shlex.split(args.runner_cmd)

    cmd += [args.s3_client, str(workload), args.bucket,
            args.region, str(args.throughput), str(args.network_interface_names)]

    start_time = datetime.now(timezone.utc)
    result = run(cmd, check=False, capture_output=True)
    end_time = datetime.now(timezone.utc)

    # reporting metrics before checking returncode
    # in case it did a few runs before failing
    if args.report_metrics:
        report_metrics(
            run_stdout=result.stdout,
            run_start_time=start_time,
            run_end_time=end_time,
            s3_client_id=args.s3_client,
            workload_path=workload,
            bucket=args.bucket,
            region=args.region,
            instance_type=args.metrics_instance_type,
            branch=args.metrics_branch,
        )

    # if runner skipped the workload, keep going
    if result.returncode == 123:
        continue

    # if runner failed and we're only running 1 workload, exit with failure
    # but if we're running multiple workloads, keep going
    if result.returncode != 0:
        print('benchmark failed')
        if len(workloads) == 1:
            exit(1)
