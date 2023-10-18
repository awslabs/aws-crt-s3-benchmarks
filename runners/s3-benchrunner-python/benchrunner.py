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
import os
import os.path
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import List, Optional, Tuple

PARSER = argparse.ArgumentParser(
    description='Python benchmark runner. Pick which S3 library to use.')
PARSER.add_argument('LIB', choices=(
    'crt', 'boto3-python', 'cli-python', 'cli-crt'))
PARSER.add_argument('BENCHMARK')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)
PARSER.add_argument('--verbose', action='store_true')


def exit_with_skip_code(msg: str):
    print(f'Skipping benchmark - {msg}', file=sys.stderr)
    exit(123)


def exit_with_error(msg: str):
    print(f'FAIL - {msg}', file=sys.stderr)
    exit(255)


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
    """Benchmark config"""
    # loaded from json...
    files_on_disk: bool
    checksum: str
    max_repeat_count: int
    max_repeat_secs: int
    tasks: list[TaskConfig]
    # passed on cmdline...
    bucket: str
    region: str
    target_throughput_Gbps: float

    def __init__(self, benchmark_path: str, bucket: str, region: str,
                 target_throughput_Gbps: float, verbose: bool):
        with open(benchmark_path) as f:
            benchmark = json.load(f)

        version = benchmark['version']
        if version != 2:
            exit_with_skip_code(f'benchmark version not supported: {version}')

        self.files_on_disk = benchmark['filesOnDisk']
        self.checksum = benchmark['checksum']
        self.max_repeat_count = benchmark['maxRepeatCount']
        self.max_repeat_secs = benchmark['maxRepeatSecs']
        self.tasks = [TaskConfig(task['action'], task['key'], task['size'])
                      for task in benchmark['tasks']]

        self.bucket = bucket
        self.region = region
        self.target_throughput_Gbps = target_throughput_Gbps
        self.verbose = verbose

    def bytes_per_run(self) -> int:
        return sum([task.size for task in self.tasks])


class Benchmark:
    """Base class for runnable benchmark"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config

        # If we're uploading, and not using files on disk,
        # then generate an in-memory buffer of random data to upload.
        # All uploads will use this same buffer, so make it big enough for the largest file.
        if not self.config.files_on_disk:
            largest_upload = 0
            for task in self.config.tasks:
                if task.action == 'upload' and task.size > largest_upload:
                    largest_upload = task.size
            self._random_data_for_upload = os.urandom(largest_upload)

    @staticmethod
    def create_runner_for_lib(lib: str, config: BenchmarkConfig) -> 'Benchmark':
        """Factory function. Create appropriate subclass, given the lib."""
        if lib == 'crt':
            return CrtBenchmark(config)
        if lib == 'boto3-python':
            return Boto3Benchmark(config)
        if lib.startswith('cli-'):
            return CliBenchmark(config, use_crt=lib.endswith('crt'))
        else:
            raise ValueError(f'Unknown lib: {lib}')

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

    def __init__(self, config: BenchmarkConfig):
        super().__init__(config)

        elg = awscrt.io.EventLoopGroup(cpu_group=0)
        resolver = awscrt.io.DefaultHostResolver(elg)
        bootstrap = awscrt.io.ClientBootstrap(elg, resolver)
        credential_provider = awscrt.auth.AwsCredentialsProvider.new_default_chain(
            bootstrap)

        signing_config = awscrt.s3.create_default_s3_signing_config(
            region=self.config.region,
            credential_provider=credential_provider)

        self._s3_client = awscrt.s3.S3Client(
            bootstrap=bootstrap,
            region=self.config.region,
            signing_config=signing_config,
            throughput_target_gbps=self.config.target_throughput_Gbps)

    def run(self):
        # kick off all tasks
        requests = [self._make_request(i)
                    for i in range(len(self.config.tasks))]

        # wait until all tasks are done
        for request in requests:
            request.finished_future.result()

    def _make_request(self, task_i) -> Future:
        task = self.config.tasks[task_i]

        headers = awscrt.http.HttpHeaders()
        headers.add(
            'Host', f'{self.config.bucket}.s3.{self.config.region}.amazonaws.com')
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
                if self.config.verbose:
                    print(f'aws-crt-python upload from disk: {task.key}')
                send_filepath = task.key
            else:
                if self.config.verbose:
                    print(f'aws-crt-python upload from RAM: {task.key}')
                send_stream = self._new_iostream_to_upload_from_ram(task.size)

        elif task.action == 'download':
            s3type = awscrt.s3.S3RequestType.GET_OBJECT
            method = 'GET'
            headers.add('Content-Length', '0')

            if self.config.files_on_disk:
                if self.config.verbose:
                    print(f'aws-crt-python download to disk: {task.key}')
                recv_filepath = task.key
            else:
                if self.config.verbose:
                    print(f'aws-crt-python download to RAM: {task.key}')

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

        return self._s3_client.make_request(
            request=awscrt.http.HttpRequest(
                method, path, headers, send_stream),
            type=s3type,
            recv_filepath=recv_filepath,
            send_filepath=send_filepath,
            on_done=on_done)


class Boto3Benchmark(Benchmark):
    """Runnable benchmark using boto3.client('s3')"""

    def __init__(self, config: BenchmarkConfig):
        super().__init__(config)

        self._s3_client = boto3.client('s3')

    def _make_request(self, task_i: int):
        task = self.config.tasks[task_i]

        if task.action == 'upload':
            if self.config.files_on_disk:
                if self.config.verbose:
                    print(f'boto3 upload_file("{task.key}")')
                self._s3_client.upload_file(
                    task.key, self.config.bucket, task.key)

            else:
                if self.config.verbose:
                    print(f'boto3 upload_fileobj("{task.key}")')
                upload_stream = self._new_iostream_to_upload_from_ram(
                    task.size)
                self._s3_client.upload_fileobj(
                    upload_stream, self.config.bucket, task.key)

        elif task.action == 'download':
            if self.config.files_on_disk:
                if self.config.verbose:
                    print(f'boto3 download_file("{task.key}")')
                self._s3_client.download_file(
                    self.config.bucket, task.key, task.key)

            else:
                if self.config.verbose:
                    print(f'boto3 download_fileobj("{task.key}")')
                download_stream = Boto3DownloadFileObj()
                self._s3_client.download_fileobj(
                    self.config.bucket, task.key, download_stream)

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


class CliBenchmark(Benchmark):
    def __init__(self, config: BenchmarkConfig, use_crt: bool):
        super().__init__(config)
        self.use_crt = use_crt

        # Write out temp AWS CLI config file, so it uses CRT or not.
        # https://awscli.amazonaws.com/v2/documentation/api/latest/topic/s3-config.html
        self._cli_config_file = tempfile.NamedTemporaryFile(prefix='awsconfig')
        config_text = self._derive_cli_config()
        self._cli_config_file.write(config_text.encode())
        self._cli_config_file.flush()

        os.environ['AWS_CONFIG_FILE'] = self._cli_config_file.name

        if self.config.verbose:
            print(f'--- AWS_CONFIG_FILE ---')
            print(config_text)

        self._cli_cmd, self._stdin_for_cli = self._derive_cli_cmd()

    def _derive_cli_config(self) -> str:
        lines = ['[default]',
                 's3 =']
        if self.use_crt:
            lines += ['  preferred_transfer_client = crt',
                      f'  target_throughput = {self.config.target_throughput_Gbps} Gb/s']
        else:
            lines += ['  preferred_transfer_client = default']

        lines += ['']  # blank line at end of file
        return '\n'.join(lines)

    def _derive_cli_cmd(self) -> Tuple[list[str], Optional[bytes]]:
        """
        Figures out single CLI command that will do everything in the benchmark.
        Exits with skip code if we can't do this benchmark in one CLI command.

        Returns (list_of_cli_args, optional_stdin_for_cli)
        """
        num_tasks = len(self.config.tasks)
        first_task = self.config.tasks[0]

        cmd = ['aws', 's3', 'cp']
        stdin: Optional[bytes] = None

        if num_tasks == 1:
            # doing 1 file is simple, just name the src and dst
            if first_task.action == 'download':
                # src
                cmd.append(f's3://{self.config.bucket}/{first_task.key}')
                # dst
                if self.config.files_on_disk:
                    cmd.append(first_task.key)
                else:
                    cmd.append('-')  # print file to stdout

            else:  # upload
                # src
                if self.config.files_on_disk:
                    cmd.append(first_task.key)
                else:
                    cmd.append('-')  # read file from stdin

                    stdin = self._random_data_for_upload

                # dst
                cmd.append(f's3://{self.config.bucket}/{first_task.key}')
        else:
            # For CLI to do multiple files, we need to cp a directory
            first_task_dir = os.path.split(first_task.key)[0]

            # Check that we can do all files in one cmd
            for task_i in self.config.tasks[1:]:
                if first_task_dir != os.path.split(task_i.key)[0]:
                    exit_with_skip_code(
                        'CLI cannot run benchmark unless all keys are in the same directory')

                if first_task.action != task_i.action:
                    exit_with_skip_code(
                        'CLI cannot run benchmark unless all actions are the same')

                if first_task.key == task_i.key:
                    exit_with_skip_code(
                        'CLI cannot run benchmark that uses same key multiple times')

            if not self.config.files_on_disk:
                exit_with_skip_code(
                    "CLI cannot run benchmark with multiple files unless they're on disk")

            # Add src and dst
            if first_task.action == 'download':
                # src
                cmd.append(f's3://{self.config.bucket}/{first_task_dir}')
                # dst
                cmd.append(first_task_dir)
            else:  # upload
                # src
                cmd.append(first_task_dir)
                # dst
                cmd.append(f's3://{self.config.bucket}/{first_task_dir}')

            # Need --recursive to do multiple files
            cmd.append('--recursive')

            # If not using all files in dir, --exclude "*" and then --include the ones we want.
            if not self._using_all_files_in_dir(first_task.action, first_task_dir):
                cmd += ['--exclude', '*']
                for task in self.config.tasks:
                    cmd += ['--include', os.path.split(task.key)[1]]

        # Add common options, used by all commands
        cmd += ['--region', self.config.region]

        # As of Sept 2023, can't pick checksum for: aws s3 cp
        if self.config.checksum:
            exit_with_skip_code(
                "CLI cannot run benchmark with specific checksum algorithm")

        return cmd, stdin

    def _using_all_files_in_dir(self, action: str, prefix: str) -> bool:
        """
        Return True if benchmark uploads all files in dir, or downloads objects at S3 prefix.
        Returns False if there are files that should be ignored.
        """
        all_task_keys = {task.key for task in self.config.tasks}

        if action == 'download':
            # Check all S3 objects at this prefix
            s3 = boto3.client('s3', region_name=self.config.region)

            # list_objects_v2() is paginated, call in loop until we have all the data
            s3 = boto3.client('s3')
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix):
                for obj in page['Contents']:
                    if not obj['Key'] in all_task_keys:
                        return False

        else:  # upload
            # Check all files in this local dir
            for child_i in Path(prefix).iterdir():
                if child_i.is_file():
                    child_str = str(child_i)
                    if not child_str in all_task_keys:
                        return False

        return True

    def run(self):
        run_kwargs = {'args': self._cli_cmd,
                      'input': self._stdin_for_cli}
        if self.config.verbose:
            # show live output, and immediately raise exception if process fails
            print(f'> {subprocess.list2cmdline(self._cli_cmd)}', flush=True)
            run_kwargs['check'] = True
        else:
            # capture output, and only print if there's an error
            run_kwargs['capture_output'] = True

        result = subprocess.run(**run_kwargs)
        if result.returncode != 0:
            # show command that failed, and stderr if any
            errmsg = f'{subprocess.list2cmdline(self._cli_cmd)}'
            stderr = result.stderr.decode().strip()
            if stderr:
                errmsg += f'\n{stderr}'
            exit_with_error(errmsg)


if __name__ == '__main__':
    args = PARSER.parse_args()
    config = BenchmarkConfig(args.BENCHMARK, args.BUCKET, args.REGION,
                             args.TARGET_THROUGHPUT, args.verbose)

    # create appropriate benchmark runner for given library
    benchmark = Benchmark.create_runner_for_lib(args.LIB, config)

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
              f'MiB/s:{bytes_to_MiB(bytes_per_run) / run_secs:.3f}',
              flush=True)

        # Break out if we've exceeded max_repeat_secs
        app_secs = ns_to_secs(time.perf_counter_ns() - app_start_ns)
        if app_secs >= config.max_repeat_secs:
            break
