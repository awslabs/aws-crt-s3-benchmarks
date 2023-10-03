#!/usr/bin/env python3
import argparse
import subprocess

parser = argparse.ArgumentParser(
    description="Install tools needed to build and run. Assumes we're on Amazon Linux 2023")
args = parser.parse_args()


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    subprocess.run(cmd_args, check=True)


run(['sudo', 'dnf', 'install', '-y',
     'git',
     'cmake',  # for building aws-c-***
     'gcc',  # for building aws-c-***
     'gcc-c++',  # for building s3-benchrunner-c
     'maven',  # for building s3-benchrunner-java
     'java-17-amazon-corretto-devel',  # for building s3-benchrunner-java
     'python3-devel',  # for building aws-crt-python
     ])
