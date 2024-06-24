#!/usr/bin/env python3
import argparse

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
