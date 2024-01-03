#!/usr/bin/env python3

import argparse
from pathlib import Path
import subprocess
import itertools

parser = argparse.ArgumentParser(
    description='Automated script to prepare, build, run and report the benchmarks as configured.')

parser.add_argument('--runner', default=[], action='append',
                    choices=('python', 'java', 'c'))

parser.add_argument(
    '--working-dir',
    help='Working directory. ' +
    'If omitted, CWD is used.')

parser.add_argument(
    '--workload', action='append',
    help='Path to specific workload JSON file. ' +
    'May be specified multiple times. ' +
    'If omitted, everything in workloads/ is run.')

parser.add_argument(
    '--bucket', required=True,
    help='S3 bucket (will be created if necessary)')

parser.add_argument(
    '--region', required=True,
    help='AWS region (e.g. us-west-2)')

parser.add_argument(
    '--throughput', required=True, type=float,
    help='Target network throughput in gigabit/s (e.g. 100.0)')

parser.add_argument(
    '--branch', default='main',
    help='Git branch/commit/tag to use when pulling dependencies.')

benchmarks_root_dir = Path(__file__).parent.parent
runners_dir_dic = {
    'c': {
        'build_scripts': benchmarks_root_dir.joinpath('./runners/s3-benchrunner-c/scripts/build.py')
    },
    'java': {
        'build_scripts': benchmarks_root_dir.joinpath('./runners/s3-benchrunner-crt-java/scripts/build.py')
    },
    'python': {
        'build_scripts': benchmarks_root_dir.joinpath('./runners/s3-benchrunner-python/scripts/build.py')
    },
}


def error_handling():
    # TODO: To handle error from different stage
    exit(-1)


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}', flush=True)
    process = subprocess.Popen(
        subprocess.list2cmdline(cmd_args), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, bufsize=0)
    output = []
    line = process.stdout.readline()
    while (line):
        if not isinstance(line, str):
            line = line.decode('ascii', 'ignore')
        print(line, end='', flush=True)
        output.append(line)
        line = process.stdout.readline()

    process.wait()
    if process.returncode != 0:
        raise Exception(
            f'Command exited with code {process.returncode}')
    return output


if __name__ == '__main__':
    args = parser.parse_args()
    working_dir = Path(args.working_dir) if args.working_dir else Path.cwd()
    files_dir = working_dir.joinpath('files_dir')

    if args.workload:
        workloads = [Path(x) for x in args.workload]
        for workload in workloads:
            if not workload.exists():
                exit(f'workload not found: {str(workload)}')
    else:
        workloads_dir = Path(__file__).parent.parent.joinpath('workloads')
        workloads = sorted(workloads_dir.glob('*.run.json'))
        if not workloads:
            exit(f'no workload files found !?!')

    # Convert to absulte path.
    workloads_args_list = list(itertools.chain.from_iterable(
        ('--workload', str(i.resolve())) for i in workloads))

    # Step 1: Prepare files
    prepare_args = [str(benchmarks_root_dir.joinpath(
                    "scripts/prep-s3-files.py")),
                    '--bucket', args.bucket,
                    '--region', args.region,
                    '--files-dir', str(files_dir)]
    prepare_args += workloads_args_list
    run(prepare_args)

    # Step 2: Build runner
    runner_cmds = {}
    for i in args.runner:
        runner_build_args = [runners_dir_dic[i]['build_scripts'],
                                '--build-dir', str(working_dir.joinpath(f'{i}_runner_building_dir')),
                                '--branch', args.branch]
        output = run(runner_build_args)
        # the last line of the out is the runner cmd (remove the `\n` at the eol)
        runner_cmds[i] = output[-1].splitlines()[0]

    # Step 3: run benchmarks
    for i in args.runner:
        # TODO: python runner cmd has different pattern. Design something to deal with python.
        run_benchmarks_args =  [str(benchmarks_root_dir.joinpath(
                "scripts/run-benchmarks.py")),
                '--runner-cmd', runner_cmds[i],
                '--bucket', args.bucket,
                '--region', args.region,
                '--throughput', str(args.throughput),
                '--files-dir', str(files_dir)]
        run_benchmarks_args += workloads_args_list
        output = run(run_benchmarks_args)
        # TODO: Parse the output to gather result

    # Step 4: report result
    # TODO