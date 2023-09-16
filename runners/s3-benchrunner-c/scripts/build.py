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
    if not try_run(cmd_args):
        exit(f'FAILED: {subprocess.list2cmdline(cmd_args)}')


def try_run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    result = subprocess.run(cmd_args)
    return result.returncode == 0


def fetch_dep(work_dir: Path, dep_name: str, branch: str) -> Path:
    """
    Fetch git repo to live in work_dir.
    Returns its location.
    """
    dep_dir = work_dir.joinpath(dep_name)

    # git clone (if necessary)
    os.chdir(str(work_dir))
    if not dep_dir.exists():
        run(['git', 'clone', f'https://github.com/awslabs/{dep_name}'])

    # git checkout branch, but if it doesn't exist use main
    os.chdir(str(dep_dir))
    if not try_run(['git', 'checkout', branch]):
        run(['git', 'checkout', 'main'])

    # git pull (in case repo was already there without latest commits)
    run(['git', 'pull'])
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
