import boto3  # type: ignore
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Optional


def report_metrics(*,
                   run_stdout: str,
                   run_start_time: datetime,
                   run_end_time: datetime,
                   s3_client_id: str,
                   workload_path: Path,
                   region: str,
                   metrics_namespace: str,
                   instance_type: Optional[str],
                   branch: Optional[str],
                   ):
    # parse stdout
    throughput_per_run_Gbps = _given_stdout_get_list_throughput_per_run_in_gigabits(
        run_stdout)
    run_count = len(throughput_per_run_Gbps)

    # bail out if no successful runs
    if run_count == 0:
        return

    # prepare metrics data
    dimensions = [
        {'Name': 'S3Client', 'Value': s3_client_id},
        {'Name': 'InstanceType', 'Value': instance_type or 'Unknown'},
        {'Name': 'Branch', 'Value': branch or 'Unknown'},
        {'Name': 'Workload', 'Value': workload_path.name.split('.')[0]},
    ]

    metric_data = []

    # give each run a unique timestamp, even if it's just approximate
    approx_duration_per_run = (run_end_time - run_start_time) / run_count

    for run_idx, gigabits_per_sec in enumerate(throughput_per_run_Gbps):

        # if we had multiple runs, don't report the first run,
        # since things are warming up (connection pools, file caching, etc)
        if run_idx == 0 and run_count > 1:
            continue

        approx_timestamp = run_start_time + \
            approx_duration_per_run * (run_idx + 1)

        metric_data.append({
            'MetricName': 'Throughput',
            'Value': gigabits_per_sec,
            'Unit': 'Gigabits/Second',
            'Timestamp': approx_timestamp,
            'Dimensions': dimensions,
        })

    print('Reporting metrics...')
    cloudwatch_client = boto3.client('cloudwatch', region_name=region)
    cloudwatch_client.put_metric_data(
        Namespace=metrics_namespace,
        MetricData=metric_data,
    )


def _given_stdout_get_list_throughput_per_run_in_gigabits(stdout: str) -> list[float]:
    """
    Examine stdout from runner, and return the throughput (in gigabits/s) for each run.

    For example, given:
    '''
    [ERROR] [2024-01-10T22:46:03Z] [00007f4124174440] [AuthCredentialsProvider] - ...
    Run:1 Secs:8.954437 Gb/s:28.847134
    Run:2 Secs:9.180856 Gb/s:28.116831
    Run:3 Secs:9.321967 Gb/s:27.612145
    Done!
    '''

    Returns [28.847134, 28.116831, 27.612145]
    """
    pattern = re.compile(r'^Run:\d+ .* Gb/s:(\d+\.\d+)')
    throughput_per_run = []
    for line in stdout.splitlines():
        m = pattern.match(line)
        if not m:
            continue
        megabits_per_sec = float(m.group(1))
        gigabits_per_sec = megabits_per_sec / 1000
        throughput_per_run.append(gigabits_per_sec)
    return throughput_per_run


def _pretty_file_size(size_in_bytes: int) -> str:
    """e.g. 2046 -> '2.0 KiB'"""
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
