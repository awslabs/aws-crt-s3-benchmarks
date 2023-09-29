#!/usr/bin/env python3
import argparse
from pathlib import Path
import subprocess
import sys

PARSER = argparse.ArgumentParser(
    description="Check formatting and type hints in python scripts")


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    result = subprocess.run(cmd_args)
    if result.returncode != 0:
        exit('FAILED')


def get_script_dirs() -> list[str]:
    dirs = []
    root = Path(__file__).parent.parent

    # add this scripts/ dir
    dirs.append(root.joinpath('scripts'))

    # add each runner dir
    for runner_child in root.joinpath('runners').iterdir():
        if runner_child.is_dir():
            dirs.append(runner_child)

    return [str(i) for i in dirs]


def check_formatting(dirs: list[str]):
    cmd_args = [sys.executable, '-m', 'autopep8',
                '--recursive', '--diff', '--exit-code']
    cmd_args.extend(dirs)
    run(cmd_args)


def check_typing(dirs: list[str]):
    # run mypy on each script dir separately,
    # so it doesn't complain about there being multiple build.py files
    for dir in dirs:
        run([sys.executable, '-m', 'mypy', dir])


if __name__ == '__main__':
    args = PARSER.parse_args()
    script_dirs = get_script_dirs()
    check_formatting(script_dirs)
    check_typing(script_dirs)
