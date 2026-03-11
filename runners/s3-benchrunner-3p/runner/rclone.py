import boto3  # type: ignore
import os
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


class RcloneBenchmarkRunner(BenchmarkRunner):
    """Benchmark runner using rclone"""

    def __init__(self, config: BenchmarkConfig, rclone_path: str):
        super().__init__(config)

        # Use the provided rclone executable path
        if not os.path.isfile(rclone_path):
            exit_with_error(
                f'rclone executable not found at: {rclone_path}')
        self._rclone_path = rclone_path

        # Check if bucket is S3 Express
        self._is_s3express = self._is_s3express_bucket(config.bucket)

        # Write out temp rclone config file
        self._rclone_config_file = tempfile.NamedTemporaryFile(
            mode='w', prefix='rclone', suffix='.conf', delete=False)
        config_text = self._derive_rclone_config()
        self._rclone_config_file.write(config_text)
        self._rclone_config_file.flush()
        self._rclone_config_file.close()

        os.environ['RCLONE_CONFIG'] = self._rclone_config_file.name

        self._verbose(f'--- RCLONE_CONFIG ---')
        self._verbose(config_text)

        self._rclone_cmd, self._stdin_for_rclone = self._derive_rclone_cmd()

    def _derive_rclone_config(self) -> str:
        """Create rclone configuration for S3 (including S3 Express support)

        Using only documented options from https://rclone.org/s3/
        """
        lines = ['[remote]',
                 'type = s3',
                 'provider = AWS',
                 'env_auth = true']

        # Add region to config file
        # https://rclone.org/s3/#region
        if self.config.region:
            lines.append(f'region = {self.config.region}')

        # Don't check if bucket exists or try to create it
        # https://rclone.org/s3/#no-check-bucket
        lines.append('no_check_bucket = true')

        # Skip workloads that require checksums since rclone doesn't provide
        # config file options for checksum control
        if self.config.checksum:
            exit_with_skip_code(
                f"rclone only supports MD5 checksum: {self.config.checksum}")

        # Enable directory bucket support for S3 Express
        # https://rclone.org/s3/#s3-directory-bucket
        if self._is_s3express:
            lines.append('directory_bucket = true')

        lines.append('')  # blank line at end of file
        return '\n'.join(lines)

    def _is_s3express_bucket(self, bucket: str) -> bool:
        """Check if bucket is an S3 Express bucket (which rclone doesn't support)"""
        return bucket.endswith('--x-s3')

    def _derive_rclone_cmd(self) -> Tuple[list[str], Optional[bytes]]:
        """
        Figures out single rclone command that will do everything in the workload.
        Exits with skip code if we can't do this workload in one rclone command.

        Returns (list_of_rclone_args, optional_stdin_for_rclone)
        """
        num_tasks = len(self.config.tasks)
        first_task = self.config.tasks[0]

        cmd = [self._rclone_path]
        stdin: Optional[bytes] = None

        # Calculate concurrency based on target throughput
        # Formula: target_throughput (Gbps) / 0.4
        # Example: 100 Gbps / 0.4 = 250 concurrent operations
        concurrency = int(self.config.target_throughput_Gbps / 0.4)

        if self.config.verbose:
            version_cmd = [self._rclone_path, 'version']
            print(f'> {subprocess.list2cmdline(version_cmd)}', flush=True)
            subprocess.run(version_cmd, check=True)

        # Use the remote name from config file (default: "remote")
        # The config file should have a section like [remote]
        s3_remote = 'remote:'

        # Configure S3 region if specified
        if self.config.region:
            os.environ['AWS_REGION'] = self.config.region

        # Determine the rclone command based on workload type
        # For RAM-based uploads (stdin), use rcat which is optimized for streaming
        # For RAM-based downloads, use cat which outputs to stdout (we'll redirect to /dev/null)
        # For everything else, use copy
        use_rcat = (num_tasks == 1 and
                    first_task.action == 'upload' and
                    not self.config.files_on_disk)

        use_cat = (num_tasks == 1 and
                   first_task.action == 'download' and
                   not self.config.files_on_disk)

        if use_rcat:
            cmd.append('rcat')
        elif use_cat:
            cmd.append('cat')
        else:
            cmd.append('copy')

        # Add common configuration flags
        # For uploads: S3-specific multipart upload concurrency
        # https://rclone.org/s3/#s3-upload-concurrency
        cmd += ['--s3-upload-concurrency', str(concurrency)]

        # For downloads: Use multi-thread streams for parallel downloads
        # https://rclone.org/docs/#multi-thread-streams-int
        cmd += ['--multi-thread-streams', str(concurrency)]

        # Always transfer files, don't skip based on timestamps
        # https://rclone.org/docs/#i-ignore-times
        cmd += ['--ignore-times']

        # Disable checksum when not specified
        # https://rclone.org/s3/#s3-disable-checksum
        if not self.config.checksum:
            cmd += ['--s3-disable-checksum']

        # Disable progress output unless verbose
        if not self.config.verbose:
            cmd += ['--progress=false']

        # Set log level
        if self.config.verbose:
            cmd += ['--verbose']
        else:
            cmd += ['--quiet']

        if num_tasks == 1:
            # Single file operation
            if first_task.action == 'download':
                # src
                cmd.append(f'{s3_remote}{self.config.bucket}/{first_task.key}')
                # dst - only for copy command, not for cat
                if self.config.files_on_disk:
                    cmd.append(first_task.key)
                # For cat command, no dst needed - outputs to stdout which we redirect

            else:  # upload
                if self.config.files_on_disk:
                    # For copy command with files on disk
                    cmd.append(first_task.key)
                    # dst
                    cmd.append(
                        f'{s3_remote}{self.config.bucket}/{first_task.key}')
                else:
                    # For rcat command, stdin is implicit - only specify destination
                    stdin = self._random_data_for_upload[:first_task.size]
                    # dst only (rcat reads from stdin automatically)
                    cmd.append(
                        f'{s3_remote}{self.config.bucket}/{first_task.key}')

        else:
            # Multiple files - need to use directory operations

            # Find the directory that contains all files
            root_dir = Path(first_task.key).parent
            if root_dir.name == '':
                exit_with_skip_code(
                    'rclone cannot run workload unless all keys are in a directory')

            for task in self.config.tasks:
                task_path = Path(task.key)
                while not task_path.is_relative_to(root_dir):
                    root_dir = root_dir.parent
                    if root_dir.name == '':
                        exit_with_skip_code(
                            'rclone cannot run workload unless all keys are under the same directory')

                if first_task.action != task.action:
                    exit_with_skip_code(
                        'rclone cannot run workload unless all actions are the same')

            if not self.config.files_on_disk:
                exit_with_skip_code(
                    "rclone cannot run workload with multiple files unless they're on disk")

            # Assert that root dir contains ONLY the files from the workload
            self._assert_using_all_files_in_dir(
                first_task.action, str(root_dir))

            # Add src and dst
            if first_task.action == 'download':
                # src
                cmd.append(f'{s3_remote}{self.config.bucket}/{str(root_dir)}')
                # dst
                cmd.append(str(root_dir))
            else:  # upload
                # src
                cmd.append(str(root_dir))
                # dst
                cmd.append(f'{s3_remote}{self.config.bucket}/{str(root_dir)}')

        # Checksum handling is done in the config file via upload_checksum
        # No additional command-line flags needed
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
                    f"rclone cannot run workload that uses same key multiple times: {task.key}")
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
                                "rclone cannot run multi-file workload unless it downloads the whole directory.")
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
                            "rclone cannot run multi-file workload unless it uploads the whole directory.")

            if any(remaining_task_keys):
                exit_with_error(
                    f"File not found: {next(iter(remaining_task_keys))}")

    def run(self):
        run_kwargs = {'args': self._rclone_cmd,
                      'input': self._stdin_for_rclone}

        # For 'cat' command, redirect stdout to /dev/null
        devnull = None
        if 'cat' in self._rclone_cmd:
            devnull = open('/dev/null', 'w')
            run_kwargs['stdout'] = devnull

        if self.config.verbose:
            # show live output, and immediately raise exception if process fails
            print(f'> {subprocess.list2cmdline(self._rclone_cmd)} > /dev/null' if devnull else f'> {subprocess.list2cmdline(self._rclone_cmd)}', flush=True)
            run_kwargs['check'] = True
            # For verbose mode with cat, still capture stderr
            if devnull:
                run_kwargs['stderr'] = subprocess.PIPE
        else:
            # capture output, and only print if there's an error
            if not devnull:
                run_kwargs['capture_output'] = True
            else:
                run_kwargs['stderr'] = subprocess.PIPE

        try:
            result = subprocess.run(**run_kwargs)
            if result.returncode != 0:
                # show command that failed, and stderr if any
                errmsg = f'{subprocess.list2cmdline(self._rclone_cmd)}'
                if hasattr(result, 'stderr') and result.stderr:
                    stderr = result.stderr.decode().strip()
                    if stderr:
                        errmsg += f'\n{stderr}'
                exit_with_error(errmsg)
        finally:
            if devnull:
                devnull.close()
