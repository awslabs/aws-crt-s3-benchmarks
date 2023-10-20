from dataclasses import dataclass
import io
import json
import math
import os
import sys


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


def gigabit_to_bytes(gigabit: float) -> int:
    return math.ceil((gigabit * 1_000_000_000.0) / 8.0)


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


class BenchmarkRunner:
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

    def run(self):
        raise NotImplementedError()

    def _verbose(self, msg):
        if self.config.verbose:
            print(msg)

    def _new_iostream_to_upload_from_ram(self, size: int) -> io.BytesIO:
        """Return new BytesIO stream, to use when uploading from RAM"""
        # use memoryview to avoid creating a copy of the (possibly very large) underlying bytes
        mv = memoryview(self._random_data_for_upload)
        slice = mv[:size]
        return io.BytesIO(slice)
