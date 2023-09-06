#!/usr/bin/env python3
import argparse
import botocore
import boto3
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Optional

PARSER = argparse.ArgumentParser(
    description='Create files (on disk, and in S3 bucket) needed to run the benchmarks')
PARSER.add_argument(
    '--bucket', required=True,
    help='S3 bucket name')
PARSER.add_argument(
    '--region', required=True,
    help='AWS region (e.g. us-west-2)')
PARSER.add_argument(
    '--files-dir', required=True,
    help='Root directory for files to upload and download')
PARSER.add_argument(
    '--benchmark', action='append',
    help='Path to specific benchmark JSON file. ' +
    'May be specified multiple times. ' +
    'By default, processes everything in benchmarks/*.json')


@dataclass
class Task:
    """
    A benchmark task that we need to prepare for.
    These tasks are stored in a dict, by key, so we can prep
    a file once, even if it's used by multiple benchmarks.
    """
    key: str
    first_benchmark_file: Path
    action: str
    size: int  # in bytes
    checksum: Optional[str]
    on_disk: bool


def size_from_str(size_str: str) -> int:
    """Return size in bytes, given string like "5GiB" or "10KiB" or "1" (bytes)"""
    m = re.match(r"(\d+)(KiB|MiB|GiB)?$", size_str)
    if m:
        size = int(m.group(1))
        unit = m.group(2)
        if unit == "KiB":
            size *= 1024
        elif unit == "MiB":
            size *= 1024 * 1024
        elif unit == "GiB":
            size *= 1024 * 1024 * 1024
        return size
    else:
        raise Exception(
            f'Illegal size "{size_str}". Expected something like "1KiB"')


def get_checksum_str_from_head_object_response(head_object_response):
    """Return checksum algorithm name, given response from s3.head_object()"""
    if 'ChecksumCRC32' in head_object_response:
        return 'CRC32'
    if 'ChecksumCRC32C' in head_object_response:
        return 'CRC32C'
    if 'ChecksumSHA1' in head_object_response:
        return 'SHA1'
    if 'ChecksumSHA256' in head_object_response:
        return 'SHA256'
    return None


def gather_tasks(benchmark_filepath: Path, all_tasks: dict[str, Task]):
    """
    Update `all_tasks` with new tasks from benchmark file.
    We check that tasks don't "clash" with one another
    (e.g. downloading the same key twice, but expecting a different size each time).
    """
    with open(benchmark_filepath) as f:
        benchmark = json.load(f)

    # whether the benchmark will use files on disk
    files_on_disk = benchmark['filesOnDisk']

    for task_info in benchmark['tasks']:
        key = task_info['key']
        action = task_info['action']
        size = size_from_str(task_info['size'])

        checksum = task_info.get('checksum')
        if not checksum in (None, 'CRC32', 'CRC32C', 'SHA1', 'SHA256'):
            raise Exception(f'Unknown checksum: {checksum}')

        if key in all_tasks:
            # there's an existing task, check for clashes
            existing_task = all_tasks[key]

            if not action in ('upload', 'download'):
                raise Exception(f'Unknown action: {action}')

            # forbid same key for upload and download.
            # we don't want a failed download messing with our next upload.
            if action != existing_task.action:
                raise Exception(
                    f'Clashing actions: "{action}" != "{existing_task.action}". ' +
                    f'Key: "{key}". ' +
                    f'From: "{str(existing_task.first_benchmark_file)}".')

            # a key can't have two different sizes
            if size != existing_task.size:
                raise Exception(
                    f'Clashing sizes: {size} != {existing_task.size}. ' +
                    f'Key: "{key}". ' +
                    f'From: "{str(existing_task.first_benchmark_file)}".')

            # can't download same key with two different checksums
            # (but it would be OK to upload a file with different checksums)
            if checksum != existing_task.checksum and action == 'download':
                raise Exception(
                    f'Clashing checksums: "{checksum}" != "{existing_task.checksum}". ' +
                    f'Key: "{key}". ' +
                    f'From: "{str(existing_task.first_benchmark_file)}".')

            # it's ok if one benchmark uploads from disk, and another doesn't,
            # but we still need to make a file on disk
            if files_on_disk and not existing_task.on_disk:
                existing_task.first_benchmark_file = benchmark_filepath
                existing_task.on_disk = True

        else:
            # create new task
            all_tasks[key] = Task(
                key=key,
                first_benchmark_file=benchmark_filepath,
                action=action,
                size=size,
                checksum=checksum,
                on_disk=files_on_disk,
            )


def prep_bucket(s3, bucket: str, region: str):
    """Create bucket, if it doesn't already exist"""
    def _print_status(msg):
        print(f's3://{bucket}: {msg}')

    try:
        s3.head_bucket(Bucket=bucket)
        _print_status('✓ bucket already exists')
        return

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise e

    _print_status('creating bucket...')

    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': region})

    # note: no versioning on this bucket, so we don't waste money

    # set lifecycles on bucket, so we don't waste money
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={
            'Rules': [
                {
                    'ID': 'Abort all incomplete multipart uploads after 1 day',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},  # blank string means all
                    'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1},
                },
                # TODO: delete files uploaded by benchmarks after 1 day?
                # we'd probably want to enforce some prefix like "upload/"
            ]})


def prep_file_on_disk(filepath: Path, size: int, quiet=False):
    """Create file on disk, if it doesn't already exist"""
    def _print_status(msg):
        print(f'file://{str(filepath)}: {msg}')

    if filepath.exists():
        # if the file already exists, there's no work to do
        if filepath.stat().st_size == size:
            if not quiet:
                _print_status('✓ file already exists')
            return
        else:
            # file exists, but's it's the wrong size, delete it
            # then move on to recreate it
            _print_status(f'deleting file with wrong size')
            filepath.unlink()

    # create parent dir, if necessary
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # create file
    # using shell commands for speed, and to avoid allocating enormous buffers in python
    _print_status(f'creating file')
    cmd = ['head', '-c', str(size), '/dev/urandom', '>', str(filepath)]
    cmd_str = subprocess.list2cmdline(cmd)
    if os.system(cmd_str) != 0:
        raise Exception(f'Failed running: {cmd_str}')


def prep_file_in_s3(task: Task, s3, bucket: str, tmp_dir: Path):
    """Upload file to S3, if it doesn't already exist"""
    def _print_status(msg):
        print(f's3://{bucket}/{task.key}: {msg}')

    try:
        response = s3.head_object(Bucket=bucket, Key=task.key)
        # file already exists
        # if it's the right size etc, then we can skip the upload
        if response['ContentLength'] != task.size:
            _print_status('re-uploading due to size mismatch')
        elif get_checksum_str_from_head_object_response(response) != task.checksum:
            _print_status('re-uploading due to checksum mismatch')
        else:
            # return early, file already exists
            _print_status('✓ object already exists')
            return

    except botocore.exceptions.ClientError as e:
        # 404 Not Found is expected
        if e.response['Error']['Code'] != '404':
            raise e

    # OK, we need to upload.
    # first, create a tmp file on disk
    # just name it after the size to maximize the chance we can re-use it
    tmp_filepath = tmp_dir.joinpath(f'{task.size}-bytes')
    prep_file_on_disk(tmp_filepath, task.size, quiet=True)

    _print_status('uploading...')
    extra_args = {}
    if task.checksum:
        extra_args['ChecksumAlgorithm'] = task.checksum

    # print progress updates if it's taking a while
    progress_timestamp = time.time()
    progress_bytes = 0

    def _progress_callback(bytes_transferred):
        nonlocal progress_timestamp
        nonlocal progress_bytes
        latest_timestamp = time.time()
        progress_bytes += bytes_transferred
        # only print once per N seconds
        if latest_timestamp - progress_timestamp > 10.0:
            progress_timestamp = latest_timestamp
            ratio = progress_bytes / task.size
            percent = int(ratio * 100)
            print(f'{percent}%')

    s3.upload_file(str(tmp_filepath), bucket, task.key,
                   extra_args, _progress_callback)


def prep_task(task: Task, files_dir: Path, s3, bucket: str):
    """
    Prepare task (e.g. upload file, or create it on disk).
    """
    if task.action == 'upload':
        if task.on_disk:
            # create file on disk, so benchmark can upload it
            prep_file_on_disk(files_dir.joinpath(task.key), task.size)

    elif task.action == 'download':
        # create file in S3, so benchmark can download it
        tmp_dir = files_dir.joinpath('tmp')
        prep_file_in_s3(task, s3, bucket, tmp_dir)

    else:
        raise Exception(f'Unknown action: {task.action}')


if __name__ == '__main__':
    args = PARSER.parse_args()

    # validate benchmarks
    if args.benchmark:
        benchmarks = [Path(x) for x in args.benchmark]
        for benchmark in benchmarks:
            if not benchmark.exists():
                exit(f'benchmark not found: {str(benchmark)}')
    else:
        benchmarks_dir = Path(__file__).parent.parent.joinpath('benchmarks')
        benchmarks = benchmarks_dir.glob('*.json')
        if not benchmarks:
            exit(f'no benchmark files found !?!')

    s3 = boto3.client('s3', region_name=args.region)

    # prep bucket
    prep_bucket(s3, args.bucket, args.region)

    # prep files_dir
    files_dir = Path(args.files_dir).resolve()  # normalize path
    files_dir.mkdir(parents=True, exist_ok=True)

    # gather tasks from all benchmarks
    all_tasks: dict[str, Task] = {}
    for benchmark in sorted(benchmarks):
        try:
            gather_tasks(benchmark, all_tasks)
        except Exception as e:
            print(f'Failure while processing: {str(benchmark)}')
            raise e

    # prep tasks from all benchmarks
    for key, task in all_tasks.items():
        try:
            prep_task(task, files_dir, s3, args.bucket)
        except Exception as e:
            print(
                f'Failure while processing "{key}" from: {str(task.first_benchmark_file)}')
            raise e
