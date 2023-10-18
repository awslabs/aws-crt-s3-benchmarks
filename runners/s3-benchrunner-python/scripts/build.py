#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import subprocess
import sys
from typing import Optional

PARSER = argparse.ArgumentParser(
    description='Build runner and its dependencies')
PARSER.add_argument(
    '--branch',
    help='Git branch/commit/tag to use when pulling dependencies')
PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}', flush=True)
    subprocess.run(cmd_args, check=True)


def fetch_git_repo(url: str, dir: Path, main_branch: str, preferred_branch: Optional[str]):
    # use helper script
    root = Path(__file__).parent.parent.parent.parent
    fetch_cmd = [sys.executable, str(root.joinpath('scripts/fetch-git-repo.py')),
                 '--repo', url,
                 '--main-branch', main_branch,
                 '--dir', str(dir)]
    if preferred_branch:
        fetch_cmd.extend(['--preferred-branch', preferred_branch])
    run(fetch_cmd)


def fetch_and_install(url: str,
                      dir: Path,
                      main_branch: str,
                      preferred_branch: Optional[str],
                      venv_python: str):
    fetch_git_repo(url, dir, main_branch, preferred_branch)

    # install into virtual env
    # use --editable so we don't need to reinstall after simple file edits
    run([venv_python, '-m', 'pip', 'install', '--editable', str(dir)])


if __name__ == '__main__':
    args = PARSER.parse_args()
    work_dir = Path(args.build_dir).resolve()

    # create virtual environment (if necessary) awscli from Github
    # doesn't interfere with system installation of awscli
    venv_dir = work_dir.joinpath('.venv')
    venv_python = str(venv_dir.joinpath('bin/python3'))
    if not venv_dir.exists():
        run([sys.executable, '-m', 'venv', str(venv_dir)])

        # upgrade pip to avoid warnings
        run([venv_python, '-m', 'pip', 'install', '--upgrade', 'pip'])

    fetch_and_install(
        url='https://github.com/aws/aws-cli.git',
        dir=work_dir.joinpath('aws-cli'),
        main_branch='v2',
        preferred_branch=args.branch,
        venv_python=venv_python)

    fetch_and_install(
        url='https://github.com/boto/boto3.git',
        dir=work_dir.joinpath('boto3'),
        main_branch='develop',
        preferred_branch=args.branch,
        venv_python=venv_python)

    fetch_and_install(
        url='https://github.com/boto/s3transfer.git',
        dir=work_dir.joinpath('s3transfer'),
        main_branch='develop',
        preferred_branch=args.branch,
        venv_python=venv_python)

    fetch_and_install(
        url='https://github.com/boto/botocore.git',
        dir=work_dir.joinpath('botocore'),
        main_branch='develop',
        preferred_branch=args.branch,
        venv_python=venv_python)

    # install aws-crt-python
    # set CMAKE_BUILD_PARALLEL_LEVEL for faster C build
    # NOTE: (pip complains that the newly installed 1.0.0.dev0 clashes
    # with the version requirements from other packages, but we ignore this)
    os.environ['CMAKE_BUILD_PARALLEL_LEVEL'] = str(os.cpu_count())
    fetch_and_install(
        url='https://github.com/awslabs/aws-crt-python.git',
        dir=work_dir.joinpath('aws-crt-python'),
        main_branch='main',
        preferred_branch=args.branch,
        venv_python=venv_python)

    runner_dir = Path(__file__).parent.parent.resolve()  # normalize path
    runner_py = str(runner_dir.joinpath('benchrunner.py'))

    # finally, print command for executing the runner, using the virtual environment
    print("------ RUNNER_CMD ------")
    runner_cmd = [venv_python, str(runner_dir.joinpath('benchrunner.py'))]
    print(subprocess.list2cmdline(runner_cmd))
