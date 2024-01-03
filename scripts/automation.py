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
    '--workloads', default="*",
    help='pattern search from every \'.run.json\' in workloads/. ' +
    'If omitted, \'*\' will be used')

parser.add_argument(
    '--bucket', default="test-bucket-dengket",
    help='S3 bucket (will be created if necessary)')

parser.add_argument(
    '--region', default="us-west-2",
    help='AWS region (e.g. us-west-2)')

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


def resolve_workload_list(user_regex_string):
    # Resolve the benchmark list from the specific regex string
    pattern = "{}.run.json".format(user_regex_string)
    return sorted(benchmarks_root_dir.joinpath('benchmarks').glob(pattern))


if __name__ == '__main__':
    args = parser.parse_args()
    working_dir = Path(args.working_dir) if args.working_dir else Path.cwd()

    workloads_dir = resolve_workload_list(args.workloads)
    workloads_args_list = list(itertools.chain.from_iterable(
        ('--workload', str(i)) for i in workloads_dir))

    # Step 1: Prepare files
    prepare_args = [str(benchmarks_root_dir.joinpath(
     "scripts/prep-s3-files.py")), '--bucket', args.bucket, '--region', args.region, '--files-dir', str(working_dir.joinpath('files_dir'))]
    prepare_args += workloads_args_list
    run(prepare_args)

    # runner_cmds = {}
    runner_cmds = {'python': '/home/ec2-user/fast_ebs/aws-crt-s3-benchmarks/build/python_runner_building_dir/venv/bin/python3 /home/ec2-user/fast_ebs/aws-crt-s3-benchmarks/runners/s3-benchrunner-python/main.py', 'c': '/home/ec2-user/fast_ebs/aws-crt-s3-benchmarks/build/c_runner_building_dir/install/bin/s3-benchrunner-c', 'java': 'java -jar /home/ec2-user/fast_ebs/aws-crt-s3-benchmarks/runners/s3-benchrunner-crt-java/target/s3-benchrunner-crt-java-1.0-SNAPSHOT.jar'}
    # Step 2: Build runner
    # for i in args.runner:
    #     runner_build_args = [runners_dir_dic[i]['build_scripts'],
    #                          '--build-dir', str(working_dir.joinpath(f'{i}_runner_building_dir'))]
    #     output = run(runner_build_args)
    #     # the last line of the out is the runner cmd
    #     runner_cmds[i] = output[-1].splitlines()[0]

    # Step 3: run benchmark
    for i in args.runner:
        print(runner_cmds)
