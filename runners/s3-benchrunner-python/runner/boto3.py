from concurrent.futures import ThreadPoolExecutor
import os

from runner import BenchmarkConfig, BenchmarkRunner


class Boto3BenchmarkRunner(BenchmarkRunner):
    """Benchmark runner using boto3.client('s3')"""

    def __init__(self, config: BenchmarkConfig, use_crt: bool):
        super().__init__(config)

        # boto3 uses CRT if it's installed, unless env-var BOTO_DISABLE_CRT=true
        # so set the env-var before importing boto3
        os.environ['BOTO_DISABLE_CRT'] = str(not use_crt).lower()
        import boto3  # type: ignore
        import botocore.compat  # type: ignore
        assert use_crt == botocore.compat.HAS_CRT

        self.use_crt = use_crt
        if (use_crt):
            self._verbose('--- boto3-crt ---')
        else:
            self._verbose('--- boto3-python ---')

        self._s3_client = boto3.client('s3')

    def _verbose(self, msg):
        if self.config.verbose:
            print(msg)

    def _make_request(self, task_i: int):
        task = self.config.tasks[task_i]

        if task.action == 'upload':
            if self.config.files_on_disk:
                self._verbose(f'upload_file("{task.key}")')
                self._s3_client.upload_file(
                    task.key, self.config.bucket, task.key)

            else:
                self._verbose(f'upload_fileobj("{task.key}")')
                upload_stream = self._new_iostream_to_upload_from_ram(
                    task.size)
                self._s3_client.upload_fileobj(
                    upload_stream, self.config.bucket, task.key)

        elif task.action == 'download':
            if self.config.files_on_disk:
                self._verbose(f'download_file("{task.key}")')
                self._s3_client.download_file(
                    self.config.bucket, task.key, task.key)

            else:
                self._verbose(f'download_fileobj("{task.key}")')
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
