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
                   bucket: str,
                   region: str,
                   target_throughput_Gbps: float,
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

    # get data from workload
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

    # prepare metrics data
    dimensions = [
        {'Name': 'S3Client', 'Value': s3_client_id},
        {'Name': 'InstanceType', 'Value': instance_type or 'Unknown'},
        {'Name': 'TargetThroughput', 'Value': str(target_throughput_Gbps)},
        {'Name': 'Branch', 'Value': branch or 'Unknown'},
        {'Name': 'Workload', 'Value': workload_path.name.split('.')[0]},
        {'Name': 'Bucket', 'Value': bucket},
        {'Name': 'Action', 'Value': action},
        {'Name': 'FileLocation',
            'Value': 'Disk' if workload['filesOnDisk'] else 'RAM'},
        {'Name': 'NumFiles', 'Value': str(num_files)},
        {'Name': 'TotalSize', 'Value': _pretty_file_size(total_file_size)},
        {'Name': 'AvgFileSize', 'Value': _pretty_file_size(avg_file_size)},
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
        Namespace='S3Benchmarks',
        MetricData=metric_data,
    )


def _given_stdout_get_list_throughput_per_run_in_gigabits(stdout: str) -> list[float]:
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

    Returns [28.780, 28.0724, 27.6483]
    """
    # NOTE: Runners print throughput thats "nice" for humans to read,
    # (no scientific notation and only .1f precision).
    # They print throughput at multiple scales (Gb, Mb, GiB, MiB)
    # so people can look eyeball whichever number makes sense to them.
    #
    # Anyway, parse "Mb/s" here since it will be the largest number
    # and therefore retain the most precision. But return it as gigabits
    # since that's how we usually think of throughput.
    pattern = re.compile(r'^Run:\d+ .* Mb/s:([^ ]+) ')
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
