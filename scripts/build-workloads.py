#!/usr/bin/env python3
import argparse
import math
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
    description='Build workload *.src.json into *.run.json.')
PARSER.add_argument(
    'SRC_FILE', nargs='*',
    help='Path to specific workload.src.json file. ' +
    'If none specified, builds all workloads/*.src.json')


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


def build_workload(src_file: Path):
    """
    Read workload src JSON, which describes the workload at a high level.
    These files are meant for humans to author.
    Fields are omitted if defaults are being used.

    Write out workload dst JSON, which fully describes the workload
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
    assert checksum in (None, 'CRC32', 'CRC32C', 'SHA1', 'SHA256')

    # Come up with "expected" name for the workload and the dir it uses.
    # Workload name will be like: upload-256KiB-10_000x
    # Filepaths will be like:      upload/256KiB-10_000x/00001
    #
    # All files for a given workload are in their own folder because
    # AWS CLI has a bad time unless it's operating on ALL files in a directory.
    # See: https://github.com/awslabs/aws-crt-s3-benchmarks/pull/24
    #
    # Use top-level directories named like "upload/" "download/" so that
    # users can clean an S3 bucket by deleting just 1 or 2 directories

    dirname = f'{file_size_str}'
    dirname += f'-{num_files:_}x'

    if checksum:
        dirname += f'-{checksum.lower()}'

    # suffix is anything that shouldn't go into dir name
    # (i.e. "-ram" because a download workload could use the same files in S3
    # whether or not it's downloading to ram or disk)
    suffix = ''
    if not files_on_disk:
        suffix += '-ram'

    # warn if workload name doesn't match expected
    # people might just be messing around locally, so this isn't a fatal error
    expected_name = f'{action}-{dirname}{suffix}.src.json'
    if expected_name != src_file.name:
        print(f'WARNING: "{src_file.name}" should be named "{expected_name}"')

    # build dst workload.run.json
    dst_json = {
        'version': VERSION,
        'comment': comment,
        'filesOnDisk': files_on_disk,
        'checksum': checksum,
        'maxRepeatCount': max_repeat_count,
        'maxRepeatSecs': max_repeat_secs,
        'tasks': [],
    }

    # format filenames like "00001" -> "10000" for a workload with 1000 files,
    # so the names sort nicely, but aren't wider than they need to be
    int_width = int(math.log10(num_files)) + 1
    int_fmt = f"{{:0{int_width}}}"

    for i in range(num_files):
        filename = int_fmt.format(i + 1)

        task = {
            'action': action,
            'key': f'{action}/{dirname}/{filename}',
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

    if args.SRC_FILE:
        src_files = [Path(x) for x in args.SRC_FILE]
        for src_file in src_files:
            if not src_file.exists():
                exit(f'file not found: {src_file}')
            if not src_file.name.endswith('.src.json'):
                exit(f'workload src files must end with ".src.json"')
    else:
        workloads_dir = Path(__file__).parent.parent.joinpath('workloads')
        src_files = sorted(workloads_dir.glob('*.src.json'))
        if not src_files:
            exit('no workload src files found !?!')

    for src_file in src_files:
        try:
            build_workload(src_file)
        except Exception as e:
            print(f'Failed building: {(str(src_file))}')
            raise e
