from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

from runner import BenchmarkConfig, BenchmarkRunner


class Boto3BenchmarkRunner(BenchmarkRunner):
    """Benchmark runner using boto3.client('s3')"""

    def __init__(self, config: BenchmarkConfig, use_crt: bool):
        super().__init__(config)

        # Currently (Jan 2024) users can't explicitly choose CRT in boto3.
        # Boto3 only uses CRT if it's installed, and a series of checks all pass.
        # Therefore, do some monkey-patching to get the results we want.
        try:
            import awscrt  # type: ignore
            import awscrt.s3  # type: ignore

            # when CRT is installed from source, its version is "1.0.0.dev0"
            # but boto3 expects it to be like "<int>.<int>.<int>"
            awscrt.__version__ = awscrt.__version__.removesuffix('.dev0')

            # patch function that boto3 calls to see if it should use CRT
            def patched_is_optimized_for_system():
                return use_crt
            awscrt.s3.is_optimized_for_system = patched_is_optimized_for_system

            # patch function that boto3 calls to get this machine's target throughput
            def patched_recommended_throughput():
                return config.target_throughput_Gbps
            awscrt.s3.get_recommended_throughput_target_gbps = patched_recommended_throughput

        except ModuleNotFoundError:
            # awscrt only needs to be installed if we're benchmarking CRT
            if use_crt:
                raise

        import boto3  # type: ignore
        import boto3.s3.transfer  # type: ignore

        self.use_crt = use_crt
        if (use_crt):
            self._verbose('--- boto3-crt ---')
        else:
            self._verbose('--- boto3-classic ---')

        self._s3_client = boto3.client('s3', region_name=config.region)

        # Set up boto3 TransferConfig
        # NOTE 1: Only SOME settings are used by both CRT and pure-python impl.
        # NOTE 2: Don't set anything that's not publicly documented here:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html
        transfer_kwargs = {
            'preferred_transfer_client': 'auto' if use_crt else 'classic',
        }
        if not use_crt:
            # I've tried tweaking settings to get performance up,
            # but I'm not seeing a difference on benchmarks of single large files...
            # transfer_kwargs['max_concurrency'] = os.cpu_count()
            # transfer_kwargs['max_io_queue'] = 1000
            pass

        self._transfer_config = boto3.s3.transfer.TransferConfig(
            **transfer_kwargs)

        # report settings used by CRT and pure-python impl
        self._verbose_config('multipart_chunksize')
        self._verbose_config('multipart_threshold')
        if not use_crt:
            # report settings that only the pure-python impl uses, including
            # undocumented stuff on the s3transfer.manager.TransferConfig base class
            self._verbose_config('max_bandwidth')
            self._verbose_config('max_request_concurrency')
            self._verbose_config('max_submission_concurrency')
            self._verbose_config('max_request_queue_size')
            self._verbose_config('max_submission_queue_size')
            self._verbose_config('max_io_queue_size')
            self._verbose_config('io_chunksize')
            self._verbose_config('num_download_attempts')
            self._verbose_config('max_in_memory_upload_chunks')
            self._verbose_config('max_in_memory_download_chunks')

    def _verbose_config(self, attr_name):
        self._verbose(
            f'  {attr_name}: {getattr(self._transfer_config, attr_name)}')

    def _make_request(self, task_i: int):
        task = self.config.tasks[task_i]

        call_name = None
        call_kwargs = {
            'Bucket': self.config.bucket,
            'Key': task.key,
            'ExtraArgs': {},
            'Config': self._transfer_config,
        }

        if task.action == 'upload':
            if self.config.files_on_disk:
                call_name = 'upload_file'
                call_kwargs['Filename'] = task.key
            else:
                call_name = 'upload_fileobj'
                call_kwargs['Fileobj'] = self._new_iostream_to_upload_from_ram(
                    task.size)

            # NOTE: botocore will add a checksum for uploads, even if we don't
            # tell it to (falls back to Content-MD5)
            if self.config.checksum:
                call_kwargs['ExtraArgs']['ChecksumAlgorithm'] = self.config.checksum

        elif task.action == 'download':
            if self.config.files_on_disk:
                call_name = 'download_file'
                call_kwargs['Filename'] = task.key
            else:
                call_name = 'download_fileobj'
                call_kwargs['Fileobj'] = Boto3DownloadFileObj()

            # boto3 doesn't validate download checksums unless you tell it to
            if self.config.checksum:
                call_kwargs['ExtraArgs']['ChecksumMode'] = 'ENABLED'

        else:
            raise RuntimeError(f'Unknown action: {task.action}')

        self._verbose(
            f"{call_name} {call_kwargs['Key']} ExtraArgs={call_kwargs['ExtraArgs']}")

        method = getattr(self._s3_client, call_name)
        method(**call_kwargs)

    def run(self):
        # boto3 is a synchronous API, but we can run requests in parallel
        # so do that in a threadpool
        with ThreadPoolExecutor() as executor:
            # submit tasks to threadpool
            task_futures_to_idx = {}
            for task_i in range(len(self.config.tasks)):
                task_future = executor.submit(self._make_request, task_i)
                task_futures_to_idx[task_future] = task_i

            # wait until all tasks are done
            for task_future in as_completed(task_futures_to_idx):
                try:
                    task_future.result()
                except Exception as e:
                    task_i = task_futures_to_idx[task_future]
                    print(f'Failed on task {task_i+1}/{len(self.config.tasks)}: {self.config.tasks[task_i]}',
                          file=sys.stderr)

                    # cancel remaining tasks
                    executor.shutdown(cancel_futures=True)

                    raise e


class Boto3DownloadFileObj:
    """File-like object that Boto3Benchmark downloads into when files_on_disk == False"""

    def write(self, b):
        # lol do nothing
        pass
