import boto3  # type: ignore
import importlib.util
import os.path
from pathlib import Path
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


class CliBenchmarkRunner(BenchmarkRunner):
    """Benchmark runner using AWS CLI"""

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

        self._verbose(f'--- AWS_CONFIG_FILE ---')
        self._verbose(config_text)

        self._cli_cmd, self._stdin_for_cli = self._derive_cli_cmd()

    def _derive_cli_config(self) -> str:
        lines = ['[default]',
                 's3 =']
        if self.use_crt:
            lines += ['  preferred_transfer_client = crt']

            # target_bandwidth can't be a float, so use Mb/s instead of Gb/s
            megabits = int(self.config.target_throughput_Gbps * 1000)
            lines += [f'  target_bandwidth = {megabits} Mb/s']

        else:
            lines += ['  preferred_transfer_client = classic']

        lines += ['']  # blank line at end of file
        return '\n'.join(lines)

    def _derive_cli_cmd(self) -> Tuple[list[str], Optional[bytes]]:
        """
        Figures out single CLI command that will do everything in the workload.
        Exits with skip code if we can't do this workload in one CLI command.

        Returns (list_of_cli_args, optional_stdin_for_cli)
        """
        num_tasks = len(self.config.tasks)
        first_task = self.config.tasks[0]

        # If awscli was pip installed, run via: python3 -m awscli
        # Otherwise, use the system installation.
        if importlib.util.find_spec('awscli'):
            cmd = [sys.executable, '-m', 'awscli']
        else:
            cmd = ['aws']

        if self.config.verbose:
            version_cmd = cmd + ['--version']
            print(f'> {subprocess.list2cmdline(version_cmd)}', flush=True)
            subprocess.run(version_cmd, check=True)

        cmd += ['s3', 'cp']
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
            # For CLI to do multiple files, we need to cp a directory.

            # Find the directory that is root to all files.
            # We start by using the first task's folder.
            # Then look at every other task, moving the root higher
            # if necessary, until it contains all task files.
            root_dir = Path(first_task.key).parent
            if root_dir.name == '':
                exit_with_skip_code(
                    'CLI cannot run workload unless all keys are in a directory')
            for task_i in self.config.tasks:
                task_path = Path(task_i.key)
                while not task_path.is_relative_to(root_dir):
                    root_dir = root_dir.parent
                    if root_dir.name == '':
                        exit_with_skip_code(
                            'CLI cannot run workload unless all keys are under the same directory')

                if first_task.action != task_i.action:
                    exit_with_skip_code(
                        'CLI cannot run workload unless all actions are the same')

            if not self.config.files_on_disk:
                exit_with_skip_code(
                    "CLI cannot run workload with multiple files unless they're on disk")

            # Assert that root dir contains ONLY the files from the workload.
            # Once upon a time we tried to using --exclude and --include
            # to cherry-pick specific files, but as of Oct 2023 this led to bad performance.
            self._assert_using_all_files_in_dir(
                first_task.action, str(root_dir))

            # Add src and dst
            if first_task.action == 'download':
                # src
                cmd.append(f's3://{self.config.bucket}/{str(root_dir)}')
                # dst
                cmd.append(str(root_dir))
            else:  # upload
                # src
                cmd.append(str(root_dir))
                # dst
                cmd.append(f's3://{self.config.bucket}/{str(root_dir)}')

            # Need --recursive to do multiple files
            cmd.append('--recursive')

        # Add common options, used by all commands
        cmd += ['--region', self.config.region]

        if not self.config.verbose:
            # Progress callbacks may have performance impact
            cmd += ['--quiet']

        # As of Sept 2023, can't pick checksum for: aws s3 cp
        if self.config.checksum:
            exit_with_skip_code(
                "CLI cannot run workload with specific checksum algorithm")

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
                    f"CLI cannot run workload that uses same key multiple times: {task.key}")
            remaining_task_keys.add(task.key)

        if action == 'download':
            # Check all S3 objects at this prefix
            s3 = boto3.client('s3', region_name=self.config.region)

            # list_objects_v2() is paginated, call in loop until we have all the data
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.config.bucket, Prefix=prefix + '/'):
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.endswith('/'):  # ignore directory objects
                        continue
                    try:
                        remaining_task_keys.remove(key)
                    except KeyError:
                        exit_with_skip_code(
                            f"Found file not listed in workload: s3://{self.config.bucket}/{key}\n" +
                            "CLI cannot run multi-file workload unless it downloads the whole directory.")

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
                            "CLI cannot run multi-file workload unless it uploads the whole directory.")

            if any(remaining_task_keys):
                exit_with_error(
                    f"File not found: {next(iter(remaining_task_keys))}")

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
