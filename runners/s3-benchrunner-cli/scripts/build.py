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
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    subprocess.run(cmd_args, check=True)


def fetch_git_repo(url: str, dir: Path, main_branch: str, preferred_branch: Optional[str]):
    # use helper script
    root = Path(__file__).parent.parent.parent.parent
    fetch_cmd = [sys.executable, root.joinpath('scripts/fetch-git-repo.py'),
                 '--repo', url,
                 '--main-branch', main_branch,
                 '--dir', str(dir)]
    if preferred_branch:
        fetch_cmd.extend(['--preferred-branch', preferred_branch])
    run(fetch_cmd)


def build_cli(work_dir: Path, branch: Optional[str], venv_python: str):
    cli_dir = work_dir.joinpath('aws-cli')

    # fetch git repo (if necessary)
    fetch_git_repo('https://github.com/aws/aws-cli.git', cli_dir,
                   main_branch='v2', preferred_branch=branch)

    # install CLI into virtual env
    # use --editable so we don't need to reinstall after simple file edits
    run([venv_python, '-m', 'pip', 'install', '--editable', str(cli_dir)])


def build_crt(work_dir: Path, branch: Optional[str], venv_python: str):
    crt_dir = work_dir.joinpath('aws-crt-python')

    # fetch git repo (if necessary)
    fetch_git_repo('https://github.com/awslabs/aws-crt-python.git', crt_dir,
                   main_branch='main', preferred_branch=branch)

    # for faster C compilation
    os.environ['CMAKE_BUILD_PARALLEL_LEVEL'] = str(os.cpu_count())

    # install into virtual env
    # use --editable so we don't need to reinstall after simple file edits
    run([venv_python, '-m', 'pip', 'install', '--editable', str(crt_dir)])


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

    # install aws-cli from Github
    build_cli(work_dir, args.branch, venv_python)

    # the runner uses boto3 too
    run([venv_python, '-m', 'pip', 'install', 'boto3'])

    # install aws-crt-python from Github
    # (pip complains that the newly installed 1.0.0.dev0 clashes
    # with the version requirements from awscli, but we ignore this)
    build_crt(work_dir, args.branch, venv_python)

    runner_dir = Path(__file__).parent.parent.resolve()  # normalize path
    runner_py = str(runner_dir.joinpath('benchrunner.py'))

    # finally, print command for executing the runner, using the virtual environment
    print("------ RUNNER_CMD ------")
    runner_cmd = [venv_python, runner_dir.joinpath('benchrunner.py')]
    print(subprocess.list2cmdline(runner_cmd))
