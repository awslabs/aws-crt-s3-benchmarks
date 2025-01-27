#!/usr/bin/env python3
import argparse
import botocore  # type: ignore
import boto3  # type: ignore
import concurrent.futures
from dataclasses import dataclass
import io
import json
import os
from pathlib import Path
import random
import subprocess
import time
from typing import Optional

from utils import workload_paths_from_args, is_s3express_bucket, get_s3express_bucket_az_id

PARSER = argparse.ArgumentParser(
    description='Create files (on disk, and in S3 bucket) needed to run the benchmarks')
PARSER.add_argument(
    '--bucket', required=True,
    help='S3 bucket (will be created if necessary)')
PARSER.add_argument(
    '--region', required=True,
    help='AWS region (e.g. us-west-2)')
PARSER.add_argument(
    '--files-dir', required=True,
    help='Root directory for files to upload and download (e.g. ~/files)')
PARSER.add_argument(
    '--workloads', nargs='+',
    help='Path to specific workload.run.json file. ' +
    'If not specified, everything in workloads/ is prepared ' +
    '(uploading 100+ GiB to S3 and creating 100+ GiB on disk).')


@dataclass
class Task:
    """
    A workload task that we need to prepare for.
    These tasks are stored in a dict, by key, so we can prep
    a file once, even if it's used by multiple workloads.
    """
    key: str
    first_workload_file: Path
    action: str
    size: int  # in bytes
    checksum: Optional[str]
    on_disk: bool


def gather_tasks(workload_filepath: Path, all_tasks: dict[str, Task]):
    """
    Update `all_tasks` with new tasks from workload file.
    We check that tasks don't "clash" with one another
    (e.g. downloading the same key twice, but expecting a different size each time).
    """
    with open(workload_filepath) as f:
        workload = json.load(f)

    # whether the workload will use files on disk
    files_on_disk = workload['filesOnDisk']

    checksum = workload['checksum']
    if not checksum in (None, 'CRC32', 'CRC32C', 'SHA1', 'SHA256'):
        raise Exception(f'Unknown checksum: {checksum}')

    for task_info in workload['tasks']:

        action = task_info['action']
        if not action in ('upload', 'download'):
            raise Exception(f'Unknown action: {action}')

        key = task_info['key']

        # we require uploads to use a key prefixed with "upload/"
        # so we can set a bucket lifetime policy to expire these files automatically
        # so we don't waste money storing files forever that we'll never download
        if action == 'upload':
            if not key.startswith('upload/'):
                raise Exception(
                    f'Bad key: "{key}". Uploads must use "upload/" prefix')
        else:
            if key.startswith('upload/'):
                raise Exception(
                    f'Bad key: "{key}". Only uploads should use "upload/" prefix')

        size = task_info['size']

        if key in all_tasks:
            # there's an existing task, check for clashes
            existing_task = all_tasks[key]

            # forbid same key for upload and download.
            # we don't want a failed download messing with our next upload.
            if action != existing_task.action:
                raise Exception(
                    f'Clashing actions: "{action}" != "{existing_task.action}". ' +
                    f'Key: "{key}". ' +
                    f'From: "{str(existing_task.first_workload_file)}".')

            # a key can't have two different sizes
            if size != existing_task.size:
                raise Exception(
                    f'Clashing sizes: {size} != {existing_task.size}. ' +
                    f'Key: "{key}". ' +
                    f'From: "{str(existing_task.first_workload_file)}".')

            # can't download same key with two different checksums
            # (but it would be OK to upload a file with different checksums)
            if checksum != existing_task.checksum and action == 'download':
                raise Exception(
                    f'Clashing checksums: "{checksum}" != "{existing_task.checksum}". ' +
                    f'Key: "{key}". ' +
                    f'From: "{str(existing_task.first_workload_file)}".')

            # it's ok if one workload uploads from disk, and another doesn't,
            # but we still need to make a file on disk
            if files_on_disk and not existing_task.on_disk:
                existing_task.first_workload_file = workload_filepath
                existing_task.on_disk = True

        else:
            # create new task
            all_tasks[key] = Task(
                key=key,
                first_workload_file=workload_filepath,
                action=action,
                size=size,
                checksum=checksum,
                on_disk=files_on_disk,
            )


def prep_bucket(s3, bucket: str, region: str):
    """Create bucket, if it doesn't already exist"""
    def _print_status(msg):
        print(f's3://{bucket}: {msg}')

    bucket_exists = False
    try:
        s3.head_bucket(Bucket=bucket)
        _print_status('✓ bucket already exists')
        bucket_exists = True

    except botocore.exceptions.ClientError as e:
        # S3 Standard gives 404, S3 Express gives NoSuchBucket
        if e.response['Error']['Code'] not in ('404', 'NoSuchBucket'):
            raise e

    if not bucket_exists:
        _print_status('creating bucket...')

        if is_s3express_bucket(bucket):
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={
                    'Location': {
                        'Type': 'AvailabilityZone',
                        'Name': get_s3express_bucket_az_id(bucket),
                    },
                    'Bucket': {
                        'Type': 'Directory',
                        'DataRedundancy': 'SingleAvailabilityZone'
                    }
                })

        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={'LocationConstraint': region})
        # note: no versioning on this bucket, so we don't waste money

    # Set lifecycle rules on this bucket, so we don't waste money.
    # Do this every time, in case the bucket was made by hand, or made by the CDK stack.
    if is_s3express_bucket(bucket):
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-lifecycle.html#directory-bucket-lifecycle-differences
        # S3 express requires a bucket policy to allow session-based access to perform lifecycle actions
        account_id = boto3.client(
            'sts').get_caller_identity().get('Account')
        bucket_policy = {
            "Version": "2008-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "lifecycle.s3.amazonaws.com"
                    },
                    "Action": "s3express:CreateSession",
                    "Condition": {
                        "StringEquals": {
                            "s3express:SessionMode": "ReadWrite"
                        }
                    },
                    "Resource": [
                        f"arn:aws:s3express:{region}:{account_id}:bucket/{bucket}"
                    ]
                }
            ]
        }
        s3.put_bucket_policy(
            Bucket=bucket, Policy=json.dumps(bucket_policy))
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        ChecksumAlgorithm='CRC32',
        LifecycleConfiguration={
            'Rules': [
                {
                    'ID': 'Abort all incomplete multipart uploads after 1 day',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},  # blank string means all
                    'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1},
                },
                {
                    'ID': 'Objects under upload directory expire after 1 day',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': 'upload/'},
                    'Expiration': {'Days': 1},
                },
            ]})


@dataclass
class ExistingS3Object:
    key: str
    size: int
    checksum: Optional[str]  # checksum algorithms


def get_existing_s3_objects(s3, bucket: str) -> dict[str, ExistingS3Object]:
    """Get info on existing objects, so we can skip uploading ones that already exist."""
    def _print_status(msg):
        print(f's3://{bucket}: {msg}')

    _print_status(f'Checking existing objects...')
    existing_objects: dict[str, ExistingS3Object] = {}

    # list_objects_v2() is paginated, call in loop until we have all the data
    prev_response = None
    while prev_response is None or prev_response['IsTruncated'] is True:
        request_kwargs = {
            'Bucket': bucket,
        }
        if prev_response:
            request_kwargs['ContinuationToken'] = prev_response['NextContinuationToken']

        response = s3.list_objects_v2(**request_kwargs)

        for obj in response.get('Contents', []):
            key = obj['Key']
            size = obj['Size']
            checksum_algorithm_list = obj.get('ChecksumAlgorithm')
            checksum = checksum_algorithm_list[0] if checksum_algorithm_list else None
            existing_objects[key] = ExistingS3Object(key, size, checksum)

        prev_response = response

    return existing_objects


def prep_file_on_disk(filepath: Path, size: int):
    """Create file on disk, if it doesn't already exist"""
    def _print_status(msg):
        print(f'file://{str(filepath)}: {msg}')

    if filepath.exists():
        # if the file already exists, there's no work to do
        if filepath.stat().st_size == size:
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
    _print_status(f'creating file...')
    cmd = ['head', '-c', str(size), '/dev/urandom', '>', str(filepath)]
    cmd_str = subprocess.list2cmdline(cmd)
    if os.system(cmd_str) != 0:
        raise Exception(f'Failed running: {cmd_str}')


class RandomFileStream(io.RawIOBase):
    """
    File-like object used to upload random bytes.
    Use seeded random number generator (RNG) so we can regenerate the same
    contents again after a seek.
    """

    def __init__(self, task):
        super().__init__()
        self._task = task
        self._pos = 0
        # seed random number generator with key
        self._rng = random.Random(task.key)
        self._rng_pos = 0

    def readinto(self, b):
        assert self._pos >= 0 and self._pos <= self._task.size

        # if seek happened, reset generator and get back to current position
        if self._rng_pos != self._pos:
            self._rng = random.Random(self._task.key)
            self._rng_pos = self._pos
            if self._rng_pos > 0:
                self._rng.randbytes(self._rng_pos)

        # figure out amount to read
        remaining = self._task.size - self._pos
        amount = min(remaining, len(b))

        if amount > 0:
            b[:] = self._rng.randbytes(amount)
            self._pos += amount
            self._rng_pos += amount

        return amount

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, pos, whence=0):
        if whence == os.SEEK_SET:
            self._pos = pos
        elif whence == os.SEEK_CUR:
            self._pos += pos
        elif whence == os.SEEK_END:
            self._pos = self._task.size + pos

        return self._pos


def prep_file_in_s3(task: Task, s3, bucket: str, existing_s3_objects: dict[str, ExistingS3Object]):
    """Upload file to S3, if it doesn't already exist"""
    def _print_status(msg):
        print(f's3://{bucket}/{task.key}: {msg}')

    if task.key in existing_s3_objects:
        existing = existing_s3_objects[task.key]
        # file already exists
        # if it's the right size etc, then we can skip the upload
        if existing.size != task.size:
            _print_status('re-uploading due to size mismatch')
        elif (task.checksum is not None) and (existing.checksum != task.checksum):
            # NOTE: S3 Express gives objects checksums even if they were uploaded without any
            _print_status('re-uploading due to checksum mismatch')
        else:
            # return early, file already exists
            return

    # upload fake file stream with random contents
    file_stream = RandomFileStream(task)
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
            _print_status(f'{percent}% uploaded...')

    s3.upload_fileobj(
        file_stream,
        bucket,
        task.key,
        extra_args,
        _progress_callback,
    )


def prep_task(task: Task, files_dir: Path, s3, bucket: str, existing_s3_objects: dict[str, ExistingS3Object]):
    """
    Prepare task (e.g. upload file, or create it on disk).
    """
    if task.action == 'upload':
        if task.on_disk:
            # create file on disk, so runner can upload it
            prep_file_on_disk(files_dir.joinpath(task.key), task.size)

    elif task.action == 'download':
        # create file in S3, so runner can download it
        prep_file_in_s3(task, s3, bucket, existing_s3_objects)

        if task.on_disk:
            # create local dir, for runner to save into
            parent_dir = files_dir.joinpath(task.key).parent
            parent_dir.mkdir(parents=True, exist_ok=True)

    else:
        raise Exception(f'Unknown action: {task.action}')


if __name__ == '__main__':
    args = PARSER.parse_args()

    workloads = workload_paths_from_args(args.workloads)

    s3 = boto3.client('s3', region_name=args.region)

    # prep bucket
    prep_bucket(s3, args.bucket, args.region)

    # gather existing files in bucket
    existing_s3_objects = get_existing_s3_objects(s3, args.bucket)

    # prep files_dir
    files_dir = Path(args.files_dir).resolve()  # normalize path
    files_dir.mkdir(parents=True, exist_ok=True)

    # gather tasks from all workloads
    all_tasks: dict[str, Task] = {}
    for workload in workloads:
        try:
            gather_tasks(workload, all_tasks)
        except Exception as e:
            print(f'Failure while processing: {str(workload)}')
            raise e

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # use thread-pool to prepare all tasks
        future_to_task = {}
        for key, task in all_tasks.items():
            future = executor.submit(
                prep_task, task, files_dir, s3, args.bucket, existing_s3_objects)
            future_to_task[future] = task

        # wait for each prep to complete, and ensure it was successful
        for future in concurrent.futures.as_completed(future_to_task):
            try:
                future.result()
            except Exception as e:
                task = future_to_task[future]
                print(
                    f'Failure while processing "{task.key}" from: {str(task.first_workload_file)}')

                # cancel remaining tasks
                executor.shutdown(cancel_futures=True)

                raise e
