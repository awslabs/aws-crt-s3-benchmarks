#!/usr/bin/env python3
import argparse
import boto3  # type: ignore
from dataclasses import dataclass
import json
import os
import os.path
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from typing import Optional, Tuple

PARSER = argparse.ArgumentParser(
    description='Benchmark runner for AWS CLI')
PARSER.add_argument('BENCHMARK')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)
PARSER.add_argument('--verbose', action='store_true',
                    help="Show CLI commands and their output")
PARSER.add_argument('--use-existing-aws-config', action='store_true', default=False,
                    help="If set, your existing AWS_CONFIG_FILE is used. " +
                    "(instead of one that customizes 'preferred_transfer_client')")


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
    def __init__(self, config: BenchmarkConfig, bucket: str, region: str,
                 target_throughput_Gbps: float, verbose: bool,
                 use_existing_aws_config: bool):
        self.config = config
        self.bucket = bucket
        self.region = region
        self.target_throughput_Gbps = target_throughput_Gbps
        self.verbose = verbose
        self.use_existing_aws_config = use_existing_aws_config

        if not self.use_existing_aws_config:
            # Write out temp AWS CLI config file, so it uses CRT
            # https://awscli.amazonaws.com/v2/documentation/api/latest/topic/s3-config.html
            self._config_file = tempfile.NamedTemporaryFile(prefix='awsconfig')
            config_text = self._derive_cli_config()
            self._config_file.write(config_text.encode())
            self._config_file.flush()

            os.environ['AWS_CONFIG_FILE'] = self._config_file.name

            if self.verbose:
                print(f'--- AWS_CONFIG_FILE ---')
                print(config_text)

        self._cli_cmd, self._stdin_for_cli = self._derive_cli_cmd()

    def _derive_cli_config(self) -> str:
        lines = ['[default]',
                 's3 =',
                 '  preferred_transfer_client = crt',
                 f'  target_throughput = {self.target_throughput_Gbps} Gb/s',
                 '']  # blank line at end of file
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
                cmd.append(f's3://{self.bucket}/{first_task.key}')
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

                    # generate random data to upload
                    stdin = os.urandom(first_task.size)

                # dst
                cmd.append(f's3://{self.bucket}/{first_task.key}')
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
                cmd.append(f's3://{self.bucket}/{first_task_dir}')
                # dst
                cmd.append(first_task_dir)
            else:  # upload
                # src
                cmd.append(first_task_dir)
                # dst
                cmd.append(f's3://{self.bucket}/{first_task_dir}')

            # Need --recursive to do multiple files
            cmd.append('--recursive')

            # If not using all files in dir, --exclude "*" and then --include the ones we want.
            if not self._using_all_files_in_dir(first_task.action, first_task_dir):
                cmd += ['--exclude', '*']
                for task in self.config.tasks:
                    cmd += ['--include', os.path.split(task.key)[1]]

        # Add common options, used by all commands
        cmd += ['--region', self.region]

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
            s3 = boto3.client('s3', region_name=self.region)

            # list_objects_v2() is paginated, call in loop until we have all the data
            s3 = boto3.client('s3')
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page['Contents']:
                    if obj['Key'] in all_task_keys:
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
        if self.verbose:
            # show live output, and immediately raise exception if process fails
            print(f'> {subprocess.list2cmdline(self._cli_cmd)}')
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
    config = BenchmarkConfig.from_json(Path(args.BENCHMARK))
    benchmark = Benchmark(config, args.BUCKET, args.REGION, args.TARGET_THROUGHPUT,
                          args.verbose, args.use_existing_aws_config)
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
