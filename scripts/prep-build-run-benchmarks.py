#!/usr/bin/env python3
import argparse
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
from typing import Optional

from utils import print_banner, run, workload_paths_from_args, SCRIPTS_DIR
import utils.build


@dataclass
class Runner:
    # language of runner codebase
    # e.g. "python" for "runners/s3-benchrunner-python"
    lang: str

    # for runners that can benchmark multiple libraries,
    # this is the extra LIB arg.
    # e.g. "crt" for "runners/s3-benchrunner-python/main.py crt"
    lib_arg: Optional[str] = None


RUNNERS = {
    'crt-c': Runner(lang='c'),
    'crt-python': Runner(lang='python', lib_arg='crt'),
    'cli-crt': Runner(lang='python', lib_arg='cli-crt'),
    'cli-classic': Runner(lang='python', lib_arg='cli-python'),
    'boto3-crt': Runner(lang='python', lib_arg='boto3-crt'),
    'boto3-classic': Runner(lang='python', lib_arg='boto3-python'),
    'crt-java': Runner(lang='java'),
}

PARSER = argparse.ArgumentParser(
    description='Do-it-all script that prepares S3 files, builds runners, ' +
    'runs benchmarks, and reports results.')
PARSER.add_argument(
    '--bucket', required=True,
    help='S3 bucket (will be created if necessary)')
PARSER.add_argument(
    '--region', required=True,
    help='AWS region (e.g. us-west-2)')
PARSER.add_argument(
    '--throughput', required=True, type=float,
    help='Target network throughput in gigabit/s (e.g. 100.0)')
PARSER.add_argument(
    '--branch',
    help='If specified, try to use this branch/commit/tag of various Git repos.')
PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')
PARSER.add_argument(
    '--files-dir', required=True,
    help='Root dir for uploading and downloading files. ' +
    'Runners are launched in this directory.')
PARSER.add_argument(
    '--runners', nargs='+', required=True, choices=RUNNERS.keys(),
    help='Library runners (e.g. crt-c,crt-python)')
PARSER.add_argument(
    '--workloads', nargs='+',
    help='Paths to specific workload JSON files. ' +
    'If not specified, everything in workloads/ is run.')


if __name__ == '__main__':
    args = PARSER.parse_args()
    build_dir = Path(args.build_dir).resolve()
    files_dir = Path(args.files_dir).resolve()
    workloads = workload_paths_from_args(args.workloads)

    # prepare S3 files
    print_banner('PREPARE S3 FILES')
    run([
        sys.executable, str(SCRIPTS_DIR/'prep-s3-files.py'),
        '--bucket', args.bucket,
        '--region', args.region,
        '--files-dir', str(files_dir),
        '--workloads', *[str(x) for x in workloads]
    ])

    # track which language runners we've already built
    lang_to_runner_cmd = {}

    for runner_name in args.runners:
        runner = RUNNERS[runner_name]

        # build runner for this language (if necessary)
        # and get cmd to run it
        if not runner.lang in lang_to_runner_cmd:
            print_banner(f'BUILD RUNNER: {runner.lang}')
            runner_cmd = utils.build.build_runner(
                runner.lang, build_dir, args.branch)
            lang_to_runner_cmd[runner.lang] = runner_cmd

        runner_cmd_str = subprocess.list2cmdline(
            lang_to_runner_cmd[runner.lang])

        # add extra arg for library (if necessary)
        if runner.lib_arg:
            runner_cmd_str += f' {runner.lib_arg}'

        print_banner(f'RUN BENCHMARKS: {runner_name}')
        run([
            sys.executable, str(SCRIPTS_DIR/'run-benchmarks.py'),
            '--runner-cmd', runner_cmd_str,
            '--bucket', args.bucket,
            '--region', args.region,
            '--throughput', str(args.throughput),
            '--files-dir', str(files_dir),
            '--workloads', *[str(x) for x in workloads],
        ])
