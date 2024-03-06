import awscrt.auth  # type: ignore
import awscrt.http  # type: ignore
import awscrt.io  # type: ignore
import awscrt.s3  # type: ignore
from concurrent.futures import as_completed
import re
from threading import Event, Semaphore
from typing import Optional, Tuple

from runner import BenchmarkConfig, BenchmarkRunner


class CrtBenchmarkRunner(BenchmarkRunner):
    """Benchmark runner using aws-crt-python's S3Client"""

    def __init__(self, config: BenchmarkConfig):
        super().__init__(config)

        # S3 Express buckets look like "mybucket--usw2-az3--x-s3" (where "usw2-az3" is the AZ ID)
        s3express_match = re.search("--(.*)--x-s3$", self.config.bucket)
        if s3express_match:
            is_s3express = True
            az_id = s3express_match.group(1)
            self.endpoint = \
                f"{self.config.bucket}.s3express-{az_id}.{self.config.region}.amazonaws.com"
        else:
            is_s3express = False
            self.endpoint = \
                f"{self.config.bucket}.s3.{self.config.region}.amazonaws.com"

        elg = awscrt.io.EventLoopGroup()
        resolver = awscrt.io.DefaultHostResolver(elg)
        bootstrap = awscrt.io.ClientBootstrap(elg, resolver)
        credential_provider = awscrt.auth.AwsCredentialsProvider.new_default_chain(
            bootstrap)

        signing_config = awscrt.s3.create_default_s3_signing_config(
            region=self.config.region,
            credential_provider=credential_provider)

        if is_s3express:
            signing_config = signing_config.replace(
                algorithm=awscrt.s3.AwsSigningAlgorithm.V4_S3EXPRESS)

        self._s3_client = awscrt.s3.S3Client(
            bootstrap=bootstrap,
            region=self.config.region,
            signing_config=signing_config,
            enable_s3express=is_s3express,
            throughput_target_gbps=self.config.target_throughput_Gbps)

        # Cap the number of meta-requests we'll work on simultaneously,
        # so the application doesn't exceed its file-descriptor limits
        # when a workload has tons of files.
        max_concurrency = 10_000
        try:
            from resource import RLIMIT_NOFILE, getrlimit
            current_file_limit, hard_limit = getrlimit(RLIMIT_NOFILE)
            self._verbose(
                f'RLIMIT_NOFILE - current: {current_file_limit} hard:{hard_limit}')
            if current_file_limit > 0:
                # An HTTP connection needs a file-descriptor too, so if we were
                # really transferring every file simultaneously we'd need 2X.
                # Set concurrency less than half to give some wiggle room.
                max_concurrency = min(
                    max_concurrency, int(current_file_limit * 0.40))

        except ModuleNotFoundError:
            # resource module not available on Windows
            pass

        self._verbose(f'max_concurrency: {max_concurrency}')
        self._concurrency_semaphore = Semaphore(max_concurrency)

        # if any request fails, it sets this event
        # so we know to stop scheduling new requests
        self._failed_event = Event()

    def run(self):
        # kick off all tasks
        # respect concurrency semaphore so we don't have too many running at once
        requests = []
        for i in range(len(self.config.tasks)):
            self._concurrency_semaphore.acquire()

            # stop kicking off new tasks if one has failed
            if self._failed_event.is_set():
                break

            requests.append(self._make_request(i))

        # wait until all tasks are done
        request_futures = [r.finished_future for r in requests]
        for finished_future in as_completed(request_futures):
            finished_future.result()

    def _make_request(self, task_i) -> awscrt.s3.S3Request:
        task = self.config.tasks[task_i]

        headers = awscrt.http.HttpHeaders()
        headers.add('Host', self.endpoint)
        path = f'/{task.key}'
        send_stream = None  # if uploading from ram
        send_filepath = None  # if uploading from disk
        recv_filepath = None  # if downloading to disk
        checksum_config = None

        if task.action == 'upload':
            s3type = awscrt.s3.S3RequestType.PUT_OBJECT
            method = 'PUT'
            headers.add('Content-Length', str(task.size))
            headers.add('Content-Type', 'application/octet-stream')

            if self.config.files_on_disk:
                self._verbose(f'aws-crt-python upload from disk: {task.key}')
                send_filepath = task.key
            else:
                self._verbose(f'aws-crt-python upload from RAM: {task.key}')
                send_stream = self._new_iostream_to_upload_from_ram(task.size)

            if self.config.checksum:
                checksum_config = awscrt.s3.S3ChecksumConfig(
                    algorithm=awscrt.s3.S3ChecksumAlgorithm[self.config.checksum],
                    location=awscrt.s3.S3ChecksumLocation.TRAILER)

        elif task.action == 'download':
            s3type = awscrt.s3.S3RequestType.GET_OBJECT
            method = 'GET'
            headers.add('Content-Length', '0')

            if self.config.files_on_disk:
                self._verbose(f'aws-crt-python download to disk: {task.key}')
                recv_filepath = task.key
            else:
                self._verbose(f'aws-crt-python download to RAM: {task.key}')

            if self.config.checksum:
                checksum_config = awscrt.s3.S3ChecksumConfig(
                    validate_response=True)

        # completion callback sets the future as complete,
        # or exits the program on error
        def on_done(error: Optional[BaseException],
                    error_headers: Optional[list[Tuple[str, str]]],
                    error_body: Optional[bytes],
                    **kwargs):
            if error:
                self._failed_event.set()

                print(f'Task[{task_i}] failed. action:{task.action} ' +
                      f'key:{task.key} error:{repr(error)}')

                # TODO aws-crt-python doesn't expose error_status_code

                if error_headers:
                    for header in error_headers:
                        print(f'{header[0]}: {header[1]}')
                if error_body is not None:
                    print(error_body)

            self._concurrency_semaphore.release()

        return self._s3_client.make_request(
            type=s3type,
            request=awscrt.http.HttpRequest(
                method, path, headers, send_stream),
            recv_filepath=recv_filepath,
            send_filepath=send_filepath,
            checksum_config=checksum_config,
            on_done=on_done)
