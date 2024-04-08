import boto3  # type: ignore
from datetime import datetime
from pathlib import Path
import re
from typing import Optional
from utils import get_bucket_storage_class


def report_metrics(*,
                   run_stdout: str,
                   run_start_time: datetime,
                   run_end_time: datetime,
                   s3_client_id: str,
                   workload_path: Path,
                   bucket: str,
                   region: str,
                   instance_type: Optional[str],
                   branch: Optional[str],
                   ):
    # parse stdout
    throughput_per_run_Gbps = _given_stdout_get_list_throughput_per_run_in_gigabits(
        run_stdout)
    seconds_per_run = _given_stdout_get_list_seconds_per_run(run_stdout)
    print(seconds_per_run)
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
        {'Name': 'StorageClass', 'Value': get_bucket_storage_class(bucket)},
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

    print('Reporting metrics throughput...')
    cloudwatch_client = boto3.client('cloudwatch', region_name=region)
    cloudwatch_client.put_metric_data(
        Namespace='S3Benchmarks',
        MetricData=metric_data,
    )
    metric_data = []
    for run_idx, seconds in enumerate(seconds_per_run):

        # if we had multiple runs, don't report the first run,
        # since things are warming up (connection pools, file caching, etc)
        if run_idx == 0 and run_count > 1:
            continue

        approx_timestamp = run_start_time + \
            approx_duration_per_run * (run_idx + 1)

        metric_data.append({
            'MetricName': 'Duration',
            'Value': seconds,
            'Unit': 'Second',
            'Timestamp': approx_timestamp,
            'Dimensions': dimensions,
        })

    print('Reporting metrics seconds...')
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
        gigabits_per_sec = float(m.group(1))
        throughput_per_run.append(gigabits_per_sec)
    return throughput_per_run


def _given_stdout_get_list_seconds_per_run(stdout: str) -> list[float]:
    """
    Examine stdout from runner, and return the seconds for each run.

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
    pattern = re.compile(r'^Run:\d+ Secs:(\d+\.\d+) .*')
    seconds_per_run = []
    for line in stdout.splitlines():
        m = pattern.match(line)
        if not m:
            continue
        seconds = float(m.group(1))
        seconds_per_run.append(seconds)
    return seconds_per_run
