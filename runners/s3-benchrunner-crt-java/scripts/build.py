#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import subprocess

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
    if not try_run(cmd_args):
        exit(f'FAILED: {subprocess.list2cmdline(cmd_args)}')


def try_run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    result = subprocess.run(cmd_args)
    return result.returncode == 0


def fetch_dep(work_dir: Path, repository: str, branch: str) -> Path:
    """
    Fetch git repo to live in work_dir.
    Returns its location.
    """

    # extract dep name from repository URL
    # i.e. "https://github.com/awslabs/aws-crt-java.git" -> "aws-crt-java"
    dep_name = repository.split('/')[-1].split('.git')[0]

    dep_dir = work_dir.joinpath(dep_name)

    # git clone (if necessary)
    os.chdir(str(work_dir))
    if not dep_dir.exists():
        run(['git', 'clone', f'https://github.com/awslabs/{dep_name}'])

    # git pull before checkout (in case repo was already there and new branch was not fetched)
    run(['git', 'pull'])

    # git checkout branch, but if it doesn't exist use main
    os.chdir(str(dep_dir))
    if not try_run(['git', 'checkout', branch]):
        run(['git', 'checkout', 'main'])

    # update submodules (if necessary)
    run(['git', 'submodule', 'update', '--init'])
    return dep_dir


def build_aws_crt_java(work_dir: Path, branch: str):
    """fetch latest aws-crt-java and install 1.0.0-SNAPSHOT"""

    awscrt_repo = 'https://github.com/awslabs/aws-crt-java.git'
    awscrt_src = fetch_dep(work_dir, awscrt_repo, branch)
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
    print("------ runner-cmd ------")
    runner_cmd = ['java', '-jar', str(runner_jar)]
    print(subprocess.list2cmdline(runner_cmd))


if __name__ == '__main__':
    args = ARG_PARSER.parse_args()
    main(Path(args.build_dir), args.branch)
