#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

PARSER = argparse.ArgumentParser(description="Check formatting")

if __name__ == '__main__':
    args = PARSER.parse_args()

    runner_dir = Path(__file__).parent.parent
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
