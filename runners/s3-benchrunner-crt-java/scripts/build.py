#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import subprocess
import sys

ARG_PARSER = argparse.ArgumentParser(
    description='Build runner and its dependencies',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
ARG_PARSER.add_argument(
    '--branch', default='main',
    help='Git branch/commit/tag to use when pulling dependencies.')
ARG_PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}', flush=True)
    subprocess.run(cmd_args, check=True)


def build_aws_crt_java(work_dir: Path, branch: str):
    """fetch latest aws-crt-java and install 1.0.0-SNAPSHOT"""

    awscrt_src = work_dir.joinpath('aws-crt-java')

    root = Path(__file__).parent.parent.parent.parent
    run([sys.executable, str(root.joinpath('scripts/fetch-git-repo.py')),
         '--repo', 'https://github.com/awslabs/aws-crt-java.git',
         '--preferred-branch', branch,
         '--dir', str(awscrt_src)])

    os.chdir(str(awscrt_src))

    # for faster C compilation
    os.environ['CMAKE_BUILD_PARALLEL_LEVEL'] = str(os.cpu_count())

    run(['mvn', 'install', '-Dmaven.test.skip'])


def build_runner() -> Path:
    """
    Build s3-benchrunner-crt-java.
    Returns path to the runner uber-jar.
    """
    runner_src = Path(__file__).parent.parent
    os.chdir(str(runner_src))
    run(['mvn',
         # package along with dependencies in executable uber-java
         'package',
         # use locally installed version of aws-crt-java
         '--activate-profiles', 'snapshot',
         ])

    return runner_src.joinpath('target/s3-benchrunner-crt-java-1.0-SNAPSHOT.jar')


def main(work_dir: Path, branch: str):
    work_dir = work_dir.resolve()  # normalize path
    work_dir.mkdir(parents=True, exist_ok=True)

    build_aws_crt_java(work_dir, branch)

    runner_jar = build_runner()

    # finally, print command for executing the runner
    print("------ RUNNER_CMD ------")
    runner_cmd = ['java', '-jar', str(runner_jar)]
    print(subprocess.list2cmdline(runner_cmd))


if __name__ == '__main__':
    args = ARG_PARSER.parse_args()
    main(Path(args.build_dir), args.branch)
