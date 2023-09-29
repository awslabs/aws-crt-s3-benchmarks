#!/usr/bin/env python3
import argparse
import os
from pathlib import Path

PARSER = argparse.ArgumentParser(description="Check formatting")

GLOB_PATTERNS = [
    '*.cpp',
    '*.h',
]

if __name__ == '__main__':
    runner_dir = Path(__file__).parent.parent
    files = []
    for pattern in GLOB_PATTERNS:
        files.extend(runner_dir.glob(pattern))

    failed = False
    for file in files:
        # using shell commands because it's way shorter than proper python
        if os.system(f'clang-format {file} | diff -u {file} -') != 0:
            failed = True

    if failed:
        # display clang format version
        os.system('clang-format --version')
        exit('FAILED')
