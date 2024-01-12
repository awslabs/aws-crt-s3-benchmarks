#!/usr/bin/env python3
import argparse
import boto3  # type: ignore
from datetime import datetime, timezone
import os
import json
from pathlib import Path
import re
import shlex
from typing import Optional

from utils import S3_CLIENTS, run, workload_paths_from_args

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
    help='If reporting metrics: EC2 instance type')
parser.add_argument(
    '--metrics-context',
    help='If reporting metrics: context string')


class MetricsReporter:
    def __init__(self, s3_client_name: str, bucket: str, region: str, throughput: float,
                 instance_type: Optional[str], context_string: Optional[str]):
        self.s3_client_name = s3_client_name
        self.bucket = bucket
        self.region = region
        self.throughput = throughput
        self.instance_type = instance_type
        self.context_string = context_string

        # create this now, so we hit errors early,
        # before we spend a lot of time running the benchmarks
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)

    def report(self, workload_path: Path, stdout: str, start_time: datetime, end_time: datetime):
        throughput_per_run_in_gigabits = self._get_throughputs_in_gigabits_per_sec(
            stdout)
        run_count = len(throughput_per_run_in_gigabits)

        # bail out if no successful runs
        if run_count == 0:
            return

        with open(workload_path) as f:
            workload = json.load(f)

        num_files = 0
        total_file_size = 0
        action = None  # will be "upload", "download", or "mixed"
        for task in workload['tasks']:
            num_files += 1
            total_file_size += task['size']

            if action is None:
                action = task['action']
            elif action != task['action']:
                action = 'mixed'

        avg_file_size = int(total_file_size / num_files)

        dimensions = [
            {'Name': 'S3Client', 'Value': self.s3_client_name},
            {'Name': 'InstanceType', 'Value': self.instance_type or 'Unknown'},
            {'Name': 'Bandwidth', 'Value': str(self.throughput)},
            {'Name': 'Context', 'Value': self.context_string or 'Unknown'},
            {'Name': 'Workload', 'Value': workload_path.name.split('.')[0]},
            {'Name': 'Action', 'Value': action},
            {'Name': 'FilesOnDisk',
                'Value': 'Yes' if workload['filesOnDisk'] else 'No'},
            {'Name': 'NumFiles', 'Value': str(num_files)},
            {'Name': 'TotalSize', 'Value': self._pretty_size(total_file_size)},
            {'Name': 'AvgFileSize', 'Value': self._pretty_size(avg_file_size)},
        ]

        metric_data = []

        # give each run a unique timestamp, even if it's just approximate
        approx_duration_per_run = (end_time - start_time) / run_count

        for run_idx, gigabits_per_sec in enumerate(throughput_per_run_in_gigabits):

            # if we had multiple runs, don't report the first run
            # in which everything is warming up (connection pools, file caching, etc)
            if run_idx == 0 and run_count > 1:
                continue

            approx_timestamp = start_time + \
                approx_duration_per_run * (run_idx + 1)

            metric_data.append({
                'MetricName': 'Throughput',
                'Value': gigabits_per_sec,
                'Unit': 'Gigabits/Second',
                'Timestamp': approx_timestamp,
                'Dimensions': dimensions,
            })

        print('Reporting metrics...')
        import pprint
        pprint.pprint(metric_data)

        self.cloudwatch_client.put_metric_data(
            Namespace='S3Benchmarks',
            MetricData=metric_data,
        )

    def _get_throughputs_in_gigabits_per_sec(self, stdout: str) -> list[float]:
        """
        Examine stdout from runner, and return the throughput (in gigabits/s) for each run.

        For example, given:
        '''
        [ERROR] [2024-01-10T22:46:03Z] [00007f4124174440] [AuthCredentialsProvider] - ...
        Run:1 Secs:8.954 Gb/s:28.8 Mb/s:28780.0 GiB/s:3.4 MiB/s:3430.8
        Run:2 Secs:9.180 Gb/s:28.1 Mb/s:28072.4 GiB/s:3.3 MiB/s:3346.5
        Run:3 Secs:9.321 Gb/s:27.6 Mb/s:27648.3 GiB/s:3.2 MiB/s:3295.9
        Done!
        '''

        Returns [28780.0, 28072.4, 27648.3]
        """
        # Runners print throughput so it's easy for human eyes to read
        # (no scientific notation and only .1f precision).
        # They print throughput at multiple scales (Gb, Mb, GiB, MiB)
        # so that people can read whichever number makes sense to them.
        #
        # Anyway, parse "Mb/s" here since it will be the largest number
        # and therefore retain the most precision. But return it as gigabits
        # since that's how we usually think of throughput.
        pattern = re.compile(r'^Run:\d+ .* Mb/s:([^ ]+) ')
        throughputs = []
        for line in stdout.splitlines():
            m = pattern.match(line)
            if not m:
                continue
            megabits_per_sec = float(m.group(1))
            gigabits_per_sec = megabits_per_sec / 1000
            throughputs.append(gigabits_per_sec)
        return throughputs

    def _pretty_size(self, size_in_bytes: int) -> str:
        size_in_KiB = size_in_bytes / 1024
        size_in_MiB = size_in_bytes / 1024**2
        size_in_GiB = size_in_bytes / 1024**3

        if size_in_GiB > 1:
            return f"{size_in_GiB:.1f} GiB"
        if size_in_MiB > 1:
            return f"{size_in_MiB:.1f} MiB"
        if size_in_KiB > 1:
            return f"{size_in_KiB:.1f} KiB"
        return f"{size_in_bytes} Bytes"


args = parser.parse_args()

# if reporting metrics, create reporter now, so we hit boto3 errors early
if args.report_metrics:
    metrics_reporter = MetricsReporter(
        args.s3_client, args.bucket, args.region, args.throughput,
        args.metrics_instance_type, args.metrics_context)

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
            args.region, str(args.throughput)]

    start_time = datetime.now(timezone.utc)
    result = run(cmd, check=False, capture_output=True)
    end_time = datetime.now(timezone.utc)

    # reporting metrics before checking returncode
    # maybe it got through a few runs before failing
    if args.report_metrics:
        metrics_reporter.report(workload, result.stdout, start_time, end_time)

    # if runner skipped the workload, keep going
    if result.returncode == 123:
        continue

    # if runner failed and we're only running 1 workload, exit with failure
    # but if we're running multiple workloads, keep going
    if result.returncode != 0:
        print('benchmark failed')
        if len(workloads) == 1:
            exit(1)
