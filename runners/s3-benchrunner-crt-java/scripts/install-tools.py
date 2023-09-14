#!/usr/bin/env python3
import argparse
import subprocess

parser = argparse.ArgumentParser(
    description="Install tools needed to build and run. Assumes we're on Amazon Linux 2023")
args = parser.parse_args()


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    subprocess.run(cmd_args, check=True)


run(['sudo', 'yum', 'install', '-y',
     'git',
     'maven',
     ])
