import boto3  # type: ignore
from datetime import datetime
from pathlib import Path
import re
from typing import Optional, List, Tuple
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
    throughput_per_run_Gbps, seconds_per_run = _give_stdout_parse_throughput_in_gigabits_and_duration_in_seconds(
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
        {'Name': 'StorageClass', 'Value': get_bucket_storage_class(bucket)},
    ]

    metric_data = []

    # give each run a unique timestamp, even if it's just approximate
    approx_duration_per_run = (run_end_time - run_start_time) / run_count

    for run_idx, (gigabits_per_sec, duration) in enumerate(zip(throughput_per_run_Gbps, seconds_per_run)):

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

        metric_data.append({
            'MetricName': 'Duration',
            'Value': duration,
            'Unit': 'Seconds',
            'Timestamp': approx_timestamp,
            'Dimensions': dimensions,
        })

    print('Reporting metrics...')
    cloudwatch_client = boto3.client('cloudwatch', region_name=region)
    cloudwatch_client.put_metric_data(
        Namespace='S3Benchmarks',
        MetricData=metric_data,
    )


def _give_stdout_parse_throughput_in_gigabits_and_duration_in_seconds(stdout: str) -> Tuple[List[float], List[float]]:
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

    Returns ([28.847134, 28.116831, 27.612145], [8.954437, 9.180856, 9.321967])
    """
    throughput_pattern = re.compile(r'^Run:\d+ .* Gb/s:(\d+\.\d+)')
    duration_pattern = re.compile(r'^Run:\d+ Secs:(\d+\.\d+) .*')
    throughput_per_run = []
    duration_per_run = []

    for line in stdout.splitlines():
        throughput_match = throughput_pattern.match(line)
        duration_match = duration_pattern.match(line)

        if throughput_match and duration_match:
            throughput_per_run.append(float(throughput_match.group(1)))
            duration_per_run.append(float(duration_match.group(1)))

    return throughput_per_run, duration_per_run
