#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import subprocess

PARSER = argparse.ArgumentParser(description="Check formatting")


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    if subprocess.run(cmd_args).returncode != 0:
        exit('FAILED')


if __name__ == '__main__':
    args = PARSER.parse_args()
    runner_dir = Path(__file__).parent.parent
    os.chdir(str(runner_dir))
    run(['mvn', 'formatter:validate'])
