import boto3  # type: ignore
import os.path
from pathlib import Path
import subprocess
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