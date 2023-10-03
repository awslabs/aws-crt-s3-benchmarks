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
    help='Git branch/commit/tag to use. If other aws-c-* libs have this branch, use it.')
ARG_PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')

DEPS = [
    'aws-c-common',
    'aws-lc',
    's2n',
    'aws-c-cal',
    'aws-c-io',
    'aws-checksums',
    'aws-c-compression',
    'aws-c-http',
    'aws-c-sdkutils',
    'aws-c-auth',
    'aws-c-s3',
]


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    subprocess.run(cmd_args, check=True)


def fetch_dep(work_dir: Path, dep_name: str, branch: str) -> Path:
    """
    Fetch git repo to live in work_dir.
    Returns its location.
    """
    dep_dir = work_dir.joinpath(dep_name)

    root = Path(__file__).parent.parent.parent.parent
    run([sys.executable, str(root.joinpath('scripts/fetch-git-repo.py')),
         '--repo', f'https://github.com/awslabs/{dep_name}.git',
         '--preferred-branch', branch,
         '--dir', str(dep_dir)])

    return dep_dir


def build(work_dir: Path, src_dir: Path):
    """
    Build CMake project
    """
    build_dir = work_dir.joinpath(src_dir.name + '-build')
    install_dir = work_dir.joinpath('install')

    config_cmd = ['cmake',
                  '-S', str(src_dir),
                  '-B', str(build_dir),
                  '-DCMAKE_BUILD_TYPE=Release',
                  f'-DCMAKE_PREFIX_PATH={str(install_dir)}',
                  f'-DCMAKE_INSTALL_PREFIX={str(install_dir)}',
                  ]

    build_cmd = ['cmake',
                 '--build', str(build_dir),
                 '--parallel', str(os.cpu_count()),
                 '--target', 'install',
                 ]

    if src_dir.name == 'aws-lc':
        config_cmd += ['-DDISABLE_GO=ON',
                       '-DBUILD_LIBSSL=OFF',
                       '-DDISABLE_PERL=ON',
                       ]

    if src_dir.name != 's3-benchrunner-c':
        # runner doesn't have tests
        config_cmd += ['-DBUILD_TESTING=OFF']

    run(config_cmd)
    run(build_cmd)


def main(work_dir: Path, branch: str):
    work_dir = work_dir.resolve()  # normalize path
    work_dir.mkdir(parents=True, exist_ok=True)

    # for faster C compilation
    os.environ['CMAKE_BUILD_PARALLEL_LEVEL'] = str(os.cpu_count())

    # fetch and build dependencies
    for dep in DEPS:
        dep_src = fetch_dep(work_dir, dep, branch)
        build(work_dir, dep_src)

    # build runner
    runner_src = Path(__file__).parent.parent
    build(work_dir, runner_src)

    # finally, print command for executing the runner
    print("------ RUNNER_CMD ------")
    runner_cmd = str(work_dir.joinpath('install/bin/s3-benchrunner-c'))
    print(runner_cmd)


if __name__ == '__main__':
    args = ARG_PARSER.parse_args()
    main(Path(args.build_dir), args.branch)
