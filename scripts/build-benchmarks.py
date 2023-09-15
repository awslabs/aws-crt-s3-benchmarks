#!/usr/bin/env python3
import argparse
from pathlib import Path
import json
import re
from typing import Optional

VERSION = 2
DEFAULT_NUM_FILES = 1
DEFAULT_FILES_ON_DISK = True
DEFAULT_CHECKSUM = None
DEFAULT_MAX_REPEAT_COUNT = 10
DEFAULT_MAX_REPEAT_SECS = 600

PARSER = argparse.ArgumentParser(
    description='Build benchmark *.src.json into *.run.json.')
PARSER.add_argument(
    'src_file', nargs='*',
    help='Benchmark src.json file. ' +
    'If none specified, builds everything in benchmarks/*.src.json')


def size_from_str(size_str: str) -> int:
    """
    Return size in bytes, given string like "5GiB" or "10KiB" or "1byte"
    """
    m = re.match(r"(\d+)(KiB|MiB|GiB|bytes|byte)$", size_str)
    if m:
        size = int(m.group(1))
        unit = m.group(2)
        if unit == "KiB":
            size *= 1024
        elif unit == "MiB":
            size *= 1024 * 1024
        elif unit == "GiB":
            size *= 1024 * 1024 * 1024
        return size
    else:
        raise Exception(
            f'Illegal size "{size_str}". Expected something like "1KiB"')


def build_benchmark(src_file: Path):
    """
    Read benchmark src JSON, which describes the benchmark at a high level.
    These files are meant for humans to author.
    Fields are omitted if defaults are being used.

    Write out benchmark dst JSON, which fully describes the benchmark
    and has every field filled in so the runners can use as little code
    as possible to read and interpret them.
    """
    with open(src_file) as f:
        src_json = json.load(f)

    # required fields
    action: str = src_json['action']
    file_size_str: str = src_json['fileSize']
    file_size: int = size_from_str(file_size_str)

    # optional fields
    comment: str = src_json.get('comment', "")
    num_files: int = src_json.get('numFiles', DEFAULT_NUM_FILES)
    files_on_disk: bool = src_json.get('filesOnDisk', DEFAULT_FILES_ON_DISK)
    checksum: Optional[str] = src_json.get('checksum', DEFAULT_CHECKSUM)
    max_repeat_count = src_json.get('maxRepeatCount', DEFAULT_MAX_REPEAT_COUNT)
    max_repeat_secs = src_json.get('maxRepeatSecs', DEFAULT_MAX_REPEAT_SECS)

    # validation
    assert action in ('download', 'upload')
    if checksum is not None:
        assert checksum in ('CRC32', 'CRC32C', 'SHA1', 'SHA256')

    # warn if benchmark's name doesn't match its contents
    expected_name = f'{action}-{file_size_str}'

    if num_files != 1:
        expected_name += f'-{num_files:_}x'

    if checksum:
        expected_name += f'-{checksum.lower()}'

    if not files_on_disk:
        expected_name += '-ram'

    expected_name += '.src.json'

    if expected_name != src_file.name:
        print(f'WARNING: "{src_file.name}" should be named "{expected_name}"')

    # build dst benchmark.run.json
    dst_json = {
        'version': VERSION,
        'comment': comment,
        'filesOnDisk': files_on_disk,
        'checksum': checksum,
        'maxRepeatCount': max_repeat_count,
        'maxRepeatSecs': max_repeat_secs,
        'tasks': [],
    }

    for i in range(num_files):
        task = {
            'action': action,
            'key': f'{action}/{file_size_str}/{i+1}',
            'size': file_size,
        }
        dst_json['tasks'].append(task)

    # write file to disk
    dst_name = src_file.name.split('.')[0] + '.run.json'
    dst_file = src_file.parent.joinpath(dst_name)
    with open(dst_file, 'w') as f:
        json.dump(dst_json, f, indent=4)
        # json.dump() doesn't add final newline
        f.write('\n')


if __name__ == '__main__':
    args = PARSER.parse_args()

    if args.src_file:
        src_files = [Path(x) for x in args.src_file]
        for src_file in src_files:
            if not src_file.exists():
                exit(f'file not found: {src_file}')
            if not src_file.name.endswith('.src.json'):
                exit(f'benchmark src files must end with ".src.json"')
    else:
        benchmarks_dir = Path(__file__).parent.parent.joinpath('benchmarks')
        src_files = sorted(benchmarks_dir.glob('*.src.json'))
        if not src_files:
            exit('no benchmark src files found !?!')

    for src_file in src_files:
        try:
            build_benchmark(src_file)
        except Exception as e:
            print(f'Failed building: {(str(src_file))}')
            raise e
