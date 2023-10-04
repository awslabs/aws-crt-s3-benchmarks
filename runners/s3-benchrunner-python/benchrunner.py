#!/usr/bin/env python3
import argparse
import awscrt.auth  # type: ignore
import awscrt.http  # type: ignore
import awscrt.io  # type: ignore
import awscrt.s3  # type: ignore
import boto3  # type: ignore
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import io
import json
import os.path
from pathlib import Path
import sys
import time
from typing import List, Optional, Tuple

PARSER = argparse.ArgumentParser(
    description='Benchmark runner for python libs')
PARSER.add_argument('LIB', choices=('crt', 'boto3'))
PARSER.add_argument('BENCHMARK')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)


def exit_with_skip_code(msg: str):
    print(f'Skipping benchmark - {msg}', file=sys.stderr)
    exit(123)


def ns_to_secs(ns: int) -> float:
    return ns / 1_000_000_000.0


def bytes_to_MiB(bytes: int) -> float:
    return bytes / float(1024**2)


def bytes_to_GiB(bytes: int) -> float:
    return bytes / float(1024**3)


def bytes_to_megabit(bytes: int) -> float:
    return (bytes * 8) / 1_000_000.0


def bytes_to_gigabit(bytes: int) -> float:
    return (bytes * 8) / 1_000_000_000.0


@dataclass
class TaskConfig:
    """A task in the benchmark config's JSON"""
    action: str
    key: str
    size: int


@dataclass
class BenchmarkConfig:
    """Benchmark config, loaded from JSON"""
    files_on_disk: bool
    checksum: str
    max_repeat_count: int
    max_repeat_secs: int
    tasks: list[TaskConfig]

    @staticmethod
    def from_json(benchmark_path: Path) -> 'BenchmarkConfig':
        with open(benchmark_path) as f:
            benchmark = json.load(f)

        version = benchmark['version']
        if version != 2:
            exit_with_skip_code(f'benchmark version not supported: {version}')

        files_on_disk = benchmark['filesOnDisk']
        checksum = benchmark['checksum']
        max_repeat_count = benchmark['maxRepeatCount']
        max_repeat_secs = benchmark['maxRepeatSecs']
        tasks = [TaskConfig(task['action'], task['key'], task['size'])
                 for task in benchmark['tasks']]

        return BenchmarkConfig(files_on_disk, checksum, max_repeat_count, max_repeat_secs, tasks)

    def bytes_per_run(self) -> int:
        return sum([task.size for task in self.tasks])


class Benchmark:
    """Base class for runnable benchmark"""

    def __init__(self, config: BenchmarkConfig, bucket: str, region: str,
                 target_throughput_Gbps: float):
        self.config = config
        self.bucket = bucket
        self.region = region
        self.target_throughput_Gbps = target_throughput_Gbps

        # If we're uploading, and not using files on disk,
        # then generate an in-memory buffer of random data to upload.
        # All uploads will use this same buffer, so make it big enough for the largest file.
        if not self.config.files_on_disk:
            largest_upload = 0
            for task in self.config.tasks:
                if task.action == 'upload' and task.size > largest_upload:
                    largest_upload = task.size
            self._random_data_for_upload = os.urandom(largest_upload)

    def run(self):
        raise NotImplementedError()

    def _new_iostream_to_upload_from_ram(self, size: int) -> io.BytesIO:
        """Return new BytesIO stream, to use when uploading from RAM"""
        # use memoryview to avoid creating a copy of the (possibly very large) underlying bytes
        mv = memoryview(self._random_data_for_upload)
        slice = mv[:size]
        return io.BytesIO(slice)


class CrtBenchmark(Benchmark):
    """Runnable benchmark using aws-crt-python's S3Client"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        elg = awscrt.io.EventLoopGroup(cpu_group=0)
        resolver = awscrt.io.DefaultHostResolver(elg)
        bootstrap = awscrt.io.ClientBootstrap(elg, resolver)
        credential_provider = awscrt.auth.AwsCredentialsProvider.new_default_chain(
            bootstrap)

        signing_config = awscrt.s3.create_default_s3_signing_config(
            region=self.region,
            credential_provider=credential_provider)

        self._s3_client = awscrt.s3.S3Client(
            bootstrap=bootstrap,
            region=self.region,
            signing_config=signing_config,
            throughput_target_gbps=self.target_throughput_Gbps)

    def run(self):
        # kick off all tasks
        futures = [self._make_request(i)
                   for i in range(len(self.config.tasks))]

        # wait until all tasks are done
        for future in futures:
            future.result()

    def _make_request(self, task_i) -> Future:
        task = self.config.tasks[task_i]

        headers = awscrt.http.HttpHeaders()
        headers.add('Host', f'{self.bucket}.s3.{self.region}.amazonaws.com')
        path = f'/{task.key}'
        send_stream = None  # if uploading from ram
        send_filepath = None  # if uploading from disk
        recv_filepath = None  # if downloading to disk

        if task.action == 'upload':
            s3type = awscrt.s3.S3RequestType.PUT_OBJECT
            method = 'PUT'
            headers.add('Content-Length', str(task.size))
            headers.add('Content-Type', 'application/octet-stream')

            if self.config.files_on_disk:
                send_filepath = task.key
            else:
                send_stream = self._new_iostream_to_upload_from_ram(task.size)

        elif task.action == 'download':
            s3type = awscrt.s3.S3RequestType.GET_OBJECT
            method = 'GET'
            headers.add('Content-Length', '0')

            if self.config.files_on_disk:
                recv_filepath = task.key

        future: Future[None] = Future()

        # completion callback sets the future as complete,
        # or exits the program on error
        def on_done(error: Optional[Exception],
                    error_headers: Optional[List[Tuple[str, str]]],
                    error_body: Optional[bytes],
                    **kwargs):

            if error:
                print(f'Task[{task_i}] failed. action:{task.action} ' +
                      f'key:{task.key} error:{repr(error)}')

                # TODO aws-crt-python doesn't expose error_status_code

                if error_headers:
                    for header in error_headers:
                        print(f'{header[0]}: {header[1]}')
                if error_body is not None:
                    print(error_body)

                future.set_exception(error)
            else:
                future.set_result(None)

        self._s3_client.make_request(
            request=awscrt.http.HttpRequest(
                method, path, headers, send_stream),
            type=s3type,
            recv_filepath=recv_filepath,
            send_filepath=send_filepath,
            on_done=on_done)

        return future


class Boto3Benchmark(Benchmark):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._s3_client = boto3.client('s3')

    def _make_request(self, task_i: int):
        task = self.config.tasks[task_i]

        if task.action == 'upload':
            if self.config.files_on_disk:
                self._s3_client.upload_file(task.key, self.bucket, task.key)

            else:
                upload_stream = self._new_iostream_to_upload_from_ram(
                    task.size)
                self._s3_client.upload_fileobj(
                    upload_stream, self.bucket, task.key)

        elif task.action == 'download':
            if self.config.files_on_disk:
                self._s3_client.download_file(self.bucket, task.key, task.key)

            else:
                download_stream = Boto3DownloadFileObj()
                self._s3_client.download_fileobj(
                    self.bucket, task.key, download_stream)

        else:
            raise RuntimeError(f'Unknown action: {task.action}')

    def run(self):
        # boto3 is a synchronous API, but we can run requests in parallel
        # so do that in a threadpool
        with ThreadPoolExecutor() as executor:
            # submit tasks to threadpool
            task_futures = [executor.submit(self._make_request, task_i)
                            for task_i in range(len(self.config.tasks))]
            # wait until all tasks are done
            for task in task_futures:
                task.result()


class Boto3DownloadFileObj:
    """File-like object that Boto3Benchmark downloads into when files_on_disk == False"""

    def write(self, b):
        # lol do nothing
        pass


if __name__ == '__main__':
    args = PARSER.parse_args()
    config = BenchmarkConfig.from_json(Path(args.BENCHMARK))

    # Create CrtBenchmark or Boto3Benchmark
    benchmark_class = CrtBenchmark if args.LIB == 'crt' else Boto3Benchmark
    benchmark = benchmark_class(config, args.BUCKET,
                                args.REGION, args.TARGET_THROUGHPUT)
    bytes_per_run = config.bytes_per_run()

    # Repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    app_start_ns = time.perf_counter_ns()
    for run_i in range(config.max_repeat_count):
        run_start_ns = time.perf_counter_ns()

        benchmark.run()

        run_secs = ns_to_secs(time.perf_counter_ns() - run_start_ns)
        print(f'Run:{run_i+1} ' +
              f'Secs:{run_secs:.3f} ' +
              f'Gb/s:{bytes_to_gigabit(bytes_per_run) / run_secs:.3f} ' +
              f'Mb/s:{bytes_to_megabit(bytes_per_run) / run_secs:.3f} ' +
              f'GiB/s:{bytes_to_GiB(bytes_per_run) / run_secs:.3f} ' +
              f'MiB/s:{bytes_to_MiB(bytes_per_run) / run_secs:.3f}')

        # Break out if we've exceeded max_repeat_secs
        app_secs = ns_to_secs(time.perf_counter_ns() - app_start_ns)
        if app_secs >= config.max_repeat_secs:
            break
