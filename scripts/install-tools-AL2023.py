#!/usr/bin/env python3
import argparse
import os
import os.path
import shutil
import urllib.request

from utils import run

parser = argparse.ArgumentParser(
    description="Install tools needed to build and run. Assumes we're on Amazon Linux 2023")
args = parser.parse_args()

# ensure sudo is installed (it's not on the canary docker image),
# so we can issue the same "sudo dnf install ..." commands, whether this script
# is running on a docker image or a dev machine
if not shutil.which('sudo'):
    run(['dnf', 'install', '-y', 'sudo'])

run(['sudo', 'dnf', 'install', '-y',
     'git',
     'python3-pip',  # for installing python packages
     'cmake',  # for building aws-c-***
     'gcc',  # for building aws-c-***
     'gcc-c++',  # for building s3-benchrunner-c
     'openssl-devel',  # for building aws-sdk-cpp
     'libcurl-devel',  # for building aws-sdk-cpp
     'zlib-devel',  # for building aws-sdk-cpp
     'maven',  # for building s3-benchrunner-java
     'java-17-amazon-corretto-devel',  # for building s3-benchrunner-java
     'python3-devel',  # for building aws-crt-python
     ])

# install rust via rustup.sh
# (the version in dnf is too old, in July 2024 it was the 1+ year old rust 1.68)
# do NOT use sudo with rustup
rustup_url = 'https://sh.rustup.rs'
rustup_filepath = '/tmp/rustup.sh'
print(f'downloading: {rustup_url} -> {rustup_filepath} ...')
urllib.request.urlretrieve(rustup_url, rustup_filepath)
run(['sh', rustup_filepath, '-y'])

# add rust to path, so current process can run it without reloading shell
PATH = os.environ['PATH']
if '.cargo/bin' not in PATH:
    os.environ['PATH'] = f"{PATH}:{os.path.expanduser('~/.cargo/bin')}"
