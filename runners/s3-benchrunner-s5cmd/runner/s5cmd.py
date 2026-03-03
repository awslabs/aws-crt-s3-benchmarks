import boto3  # type: ignore
import os
import os.path
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Optional, Tuple

from runner import (
    BenchmarkRunner,
    BenchmarkConfig,
    exit_with_error,
    exit_with_skip_code,
)


class S5cmdBenchmarkRunner(BenchmarkRunner):
    """Benchmark runner using s5cmd"""

    def __init__(self, config: BenchmarkConfig):
        super().__init__(config)

        # Check if s5cmd is available
        if not shutil.which('s5cmd'):
            exit_with_error('s5cmd not found in PATH. Please install s5cmd first.')

        self._s5cmd_cmd, self._stdin_for_s5cmd = self._derive_s5cmd_cmd()

    def _derive_s5cmd_cmd(self) -> Tuple[list[str], Optional[bytes]]:
        """
        Figures out single s5cmd command that will do everything in the workload.
        Exits with skip code if we can't do this workload in one s5cmd command.

        Returns (list_of_s5cmd_args, optional_stdin_for_s5cmd)
        """
        num_tasks = len(self.config.tasks)
        first_task = self.config.tasks[0]

        cmd = ['s5cmd']
        stdin: Optional[bytes] = None

        # Add common configuration flags
        cmd += ['--retry-count', '3']

        # The s5cmd benchmark used all default configures.
        # # Configure concurrency based on target throughput
        # # s5cmd default is 256 workers, but we can tune this
        # if self.config.target_throughput_Gbps >= 10.0:
        #     cmd += ['--numworkers', '512']
        # elif self.config.target_throughput_Gbps >= 5.0:
        #     cmd += ['--numworkers', '256']
        # elif self.config.target_throughput_Gbps >= 1.0:
        #     cmd += ['--numworkers', '128']
        # else:
        #     cmd += ['--numworkers', '64']

        # # Configure part size for multipart uploads based on target throughput
        # # Higher throughput = larger parts for better performance
        # if self.config.target_throughput_Gbps >= 10.0:
        #     cmd += ['--part-size', '100MB']
        # elif self.config.target_throughput_Gbps >= 5.0:
        #     cmd += ['--part-size', '50MB']
        # else:
        #     cmd += ['--part-size', '25MB']

        if self.config.verbose:
            version_cmd = ['s5cmd', 'version']
            print(f'> {subprocess.list2cmdline(version_cmd)}', flush=True)
            subprocess.run(version_cmd, check=True)

        cmd.append('cp')

        if num_tasks == 1:
            # Single file operation
            if first_task.action == 'download':
                # src
                cmd.append(f's3://{self.config.bucket}/{first_task.key}')
                # dst
                if self.config.files_on_disk:
                    cmd.append(first_task.key)
                else:
                    cmd.append('-')  # output to stdout

            else:  # upload
                # src
                if self.config.files_on_disk:
                    cmd.append(first_task.key)
                else:
                    cmd.append('-')  # read from stdin
                    stdin = self._random_data_for_upload[:first_task.size]

                # dst
                cmd.append(f's3://{self.config.bucket}/{first_task.key}')

        else:
            # Multiple files - need to use patterns or directory operations

            # Find the directory that contains all files
            root_dir = Path(first_task.key).parent
            if root_dir.name == '':
                exit_with_skip_code(
                    's5cmd cannot run workload unless all keys are in a directory')

            for task in self.config.tasks:
                task_path = Path(task.key)
                while not task_path.is_relative_to(root_dir):
                    root_dir = root_dir.parent
                    if root_dir.name == '':
                        exit_with_skip_code(
                            's5cmd cannot run workload unless all keys are under the same directory')

                if first_task.action != task.action:
                    exit_with_skip_code(
                        's5cmd cannot run workload unless all actions are the same')

            if not self.config.files_on_disk:
                exit_with_skip_code(
                    "s5cmd cannot run workload with multiple files unless they're on disk")

            # Assert that root dir contains ONLY the files from the workload
            self._assert_using_all_files_in_dir(first_task.action, str(root_dir))

            # Add src and dst
            if first_task.action == 'download':
                # src - use wildcard pattern for s5cmd
                cmd.append(f's3://{self.config.bucket}/{str(root_dir)}/*')
                # dst
                cmd.append(str(root_dir) + '/')
            else:  # upload
                # src - use wildcard pattern
                cmd.append(str(root_dir) + '/*')
                # dst
                cmd.append(f's3://{self.config.bucket}/{str(root_dir)}/')

        # s5cmd uses AWS SDK credentials automatically
        if self.config.region:
            # s5cmd respects AWS_DEFAULT_REGION environment variable
            os.environ['AWS_DEFAULT_REGION'] = self.config.region

        # s5cmd doesn't have a quiet mode, but we can suppress some output
        if not self.config.verbose:
            # s5cmd outputs progress by default, we'll capture it
            pass

        # Handle checksum requirements
        if self.config.checksum:
            exit_with_skip_code(
                f"s5cmd does not support checksum algorithm: {self.config.checksum}")

        return cmd, stdin

    def _assert_using_all_files_in_dir(self, action: str, prefix: str):
        """
        Exit if dir is missing files from workload,
        or if dir has extra files not listed in the workload,
        or if the workload uses the same file multiple times.
        """
        remaining_task_keys = set()
        for task in self.config.tasks:
            if task.key in remaining_task_keys:
                exit_with_skip_code(
                    f"s5cmd cannot run workload that uses same key multiple times: {task.key}")
            remaining_task_keys.add(task.key)

        if action == 'download':
            # Check all S3 objects at this prefix
            s3 = boto3.client('s3', region_name=self.config.region)

            # list_objects_v2() is paginated, call in loop until we have all the data
            paginator = s3.get_paginator('list_objects_v2')
            try:
                for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix + '/'):
                    if 'Contents' not in page:
                        continue
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('/'):  # ignore directory objects
                            continue
                        try:
                            remaining_task_keys.remove(key)
                        except KeyError:
                            exit_with_skip_code(
                                f"Found file not listed in workload: s3://{self.config.bucket}/{key}\n" +
                                "s5cmd cannot run multi-file workload unless it downloads the whole directory.")
            except Exception as e:
                exit_with_error(f"Error listing S3 objects: {e}")

            if any(remaining_task_keys):
                exit_with_error(
                    f"File not found in s3://{self.config.bucket}: {next(iter(remaining_task_keys))}")

        else:  # upload
            # Check all files in this local dir
            for root, dirnames, filenames in os.walk(prefix):
                for filename in filenames:
                    key = os.path.join(root, filename)
                    try:
                        remaining_task_keys.remove(key)
                    except KeyError:
                        exit_with_skip_code(
                            f"Found file not listed in workload: {os.getcwd()}/{key}\n" +
                            "s5cmd cannot run multi-file workload unless it uploads the whole directory.")

            if any(remaining_task_keys):
                exit_with_error(
                    f"File not found: {next(iter(remaining_task_keys))}")

    def run(self):
        run_kwargs = {'args': self._s5cmd_cmd,
                      'input': self._stdin_for_s5cmd}

        if self.config.verbose:
            # show live output, and immediately raise exception if process fails
            print(f'> {subprocess.list2cmdline(self._s5cmd_cmd)}', flush=True)
            run_kwargs['check'] = True
        else:
            # capture output, and only print if there's an error
            run_kwargs['capture_output'] = True

        result = subprocess.run(**run_kwargs)
        if result.returncode != 0:
            # show command that failed, and stderr if any
            errmsg = f'{subprocess.list2cmdline(self._s5cmd_cmd)}'
            if hasattr(result, 'stderr') and result.stderr:
                stderr = result.stderr.decode().strip()
                if stderr:
                    errmsg += f'\n{stderr}'
            exit_with_error(errmsg)
