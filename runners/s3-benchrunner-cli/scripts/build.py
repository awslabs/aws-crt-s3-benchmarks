#!/usr/bin/env python3
import argparse
from pathlib import Path
import subprocess

ARG_PARSER = argparse.ArgumentParser(
    description='Build runner and its dependencies',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
ARG_PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')


if __name__ == '__main__':
    args = ARG_PARSER.parse_args()

    runner_dir = Path(__file__).parent.parent.resolve()  # normalize path
    runner_py = str(runner_dir.joinpath('benchrunner.py'))

    # TODO: install CLI from github
    # for now, we'll just use what's in the package manager

    # finally, print command for executing the runner
    print("------ RUNNER_CMD ------")
    runner_cmd = [runner_py]
    print(subprocess.list2cmdline(runner_cmd))
