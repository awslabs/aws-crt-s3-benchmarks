from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

from runner import BenchmarkConfig, BenchmarkRunner


class S3TransferBenchmarkRunner(BenchmarkRunner):
    """Benchmark runner using s3transfer"""

    def __init__(self, config: BenchmarkConfig, use_crt: bool):
        super().__init__(config)

        import botocore.session
        session = botocore.session.get_session()
        throughput_bytes = int(
            config.target_throughput_Gbps * 1_000_000_000.0 / 8)

        if use_crt:
            import s3transfer.crt

            self._verbose('--- s3transfer-crt ---')

            botocore_creds = session.get_component('credential_provider')

            crt_s3_client = s3transfer.crt.create_s3_crt_client(
                region=config.region,
                botocore_credential_provider=botocore_creds,
                target_throughput=throughput_bytes)

            request_serializer = s3transfer.crt.BotocoreCRTRequestSerializer(
                session)

            self._transfer_mgr = s3transfer.crt.CRTTransferManager(
                crt_s3_client, request_serializer)

        else:
            import s3transfer.manager

            self._verbose('--- s3transfer-python ---')

            botocore_s3_client = session.create_client('s3', config.region)

            self._transfer_mgr = s3transfer.manager.TransferManager(
                botocore_s3_client)

    def _make_request(self, task_i: int):
        task = self.config.tasks[task_i]

        call_name = None
        call_kwargs = {
            'bucket': self.config.bucket,
            'key': task.key,
            'fileobj': None,
            'extra_args': {},
        }

        if task.action == 'upload':
            call_name = 'upload'
            if self.config.files_on_disk:
                call_kwargs['fileobj'] = task.key
            else:
                call_name = 'upload'
                call_kwargs['fileobj'] = self._new_iostream_to_upload_from_ram(
                    task.size)

            # NOTE: botocore will add a checksum for uploads, even if we don't
            # tell it to (falls back to Content-MD5)
            if self.config.checksum:
                call_kwargs['extra_args']['ChecksumAlgorithm'] = self.config.checksum

        elif task.action == 'download':
            call_name = 'download'
            if self.config.files_on_disk:
                call_kwargs['fileobj'] = task.key
            else:
                call_kwargs['fileobj'] = DownloadFileObj()

            # botocore doesn't validate download checksums unless you tell it to
            if self.config.checksum:
                call_kwargs['extra_args']['ChecksumMode'] = 'ENABLED'

        else:
            raise RuntimeError(f'Unknown action: {task.action}')

        self._verbose(
            f"{call_name} {call_kwargs['key']} extra_args={call_kwargs['extra_args']}")

        method = getattr(self._transfer_mgr, call_name)
        method(**call_kwargs)

    def run(self):
        # s3transfer is a synchronous API, but we can run requests in parallel
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
                    executor.shutdown(wait=False, cancel_futures=True)
                    self._transfer_mgr.shutdown(cancel=True)

                    raise e


class DownloadFileObj:
    """File-like object that benchmark downloads into when files_on_disk == False"""

    def write(self, b):
        # lol do nothing
        pass
