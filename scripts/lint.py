#!/usr/bin/env python3
import argparse
import os
import sys

from utils import run, REPO_DIR, RUNNERS, SCRIPTS_DIR

PARSER = argparse.ArgumentParser(
    description="Run linters (e.g. code formatting) for a given language.")
PARSER.add_argument(
    'lang', choices=RUNNERS.keys())


def _lint_c():
    runner_dir = RUNNERS['c'].dir
    files: list[str] = []
    for pattern in ['*.cpp', '*.c', '*.h']:
        for i in runner_dir.glob(pattern):
            files.append(str(i))

    failed = False
    for file in files:
        # using shell commands because it's way shorter than proper python
        if os.system(f'clang-format {file} | diff -u {file} -') != 0:
            failed = True

    if failed:
        # display clang format version
        os.system('clang-format --version')
        exit('FAILED')


def _lint_python():
    dirs = [
        SCRIPTS_DIR,
        RUNNERS['python'].dir,
        REPO_DIR/'cdk',
    ]
    exclude_dirs = ['cdk.out']

    # check formatting
    fmt_args = [sys.executable, '-m', 'autopep8',
                '--recursive', '--diff', '--exit-code']

    for x in exclude_dirs:
        fmt_args.extend(['--exclude', x])

    fmt_args.extend(dirs)
    run(fmt_args)

    # check typing
    mypy_args = [sys.executable, '-m', 'mypy',
                 '--exclude', ','.join(exclude_dirs)]
    mypy_args.extend(dirs)
    run(mypy_args)


def _lint_java():
    runner_dir = RUNNERS['java'].dir
    os.chdir(runner_dir)
    run(['mvn', 'formatter:validate'])


if __name__ == '__main__':
    args = PARSER.parse_args()

    # get lint function by name, and call it
    lint_functions = {
        'c': _lint_c,
        'python': _lint_python,
        'java': _lint_java,
    }
    lint_fn = lint_functions[args.lang]
    lint_fn()
