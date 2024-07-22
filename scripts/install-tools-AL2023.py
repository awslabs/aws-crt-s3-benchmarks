#!/usr/bin/env python3
import argparse
import urllib.request

from utils import run

parser = argparse.ArgumentParser(
    description="Install tools needed to build and run. Assumes we're on Amazon Linux 2023")
args = parser.parse_args()


run(['dnf', 'install', '-y',
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
rustup_url = 'https://sh.rustup.rs'
print(f'downloading: {rustup_url}...')
rustup_text = urllib.request.urlopen(rustup_url).read().decode()
rustup_filepath = '/tmp/rustup.sh'
with open(rustup_filepath, 'w') as f:
    f.write(rustup_text)

run(['sh', rustup_filepath])
