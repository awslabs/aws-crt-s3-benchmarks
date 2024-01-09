#!/usr/bin/env python3
import argparse
from pathlib import Path
import subprocess

from utils import RUNNER_LANGS
import utils.build

PARSER = argparse.ArgumentParser(
    description='Build a runner and its dependencies')
PARSER.add_argument(
    '--lang', choices=RUNNER_LANGS, required=True,
    help='Build s3-benchrunner-<lang>')
PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')
PARSER.add_argument(
    '--branch',
    help='Git branch/commit/tag to use when pulling dependencies')

args = PARSER.parse_args()

build_root_dir = Path(args.build_dir).resolve()

runner_cmd = utils.build.build_runner(
    args.lang, build_root_dir, args.branch)

print("------ RUNNER_CMD ------")
print(subprocess.list2cmdline(runner_cmd))
