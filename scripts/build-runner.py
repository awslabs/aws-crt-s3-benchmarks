#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import subprocess
import sys
from typing import Optional

from utils import fetch_git_repo, run, get_runner_dir, RUNNER_LANGS

PARSER = argparse.ArgumentParser(
    description='Build a runner and its dependencies')
PARSER.add_argument(
    '--lang', choices=RUNNER_LANGS, required=True,
    help='Build s3-benchrunner-<lang>')
PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')
PARSER.add_argument(
    '--branch',
    help='Git branch/commit/tag to use when pulling dependencies')


def _build_cmake_proj(src_dir: Path, build_dir: Path, install_dir: Path):

    config_cmd = ['cmake',
                  '-S', str(src_dir),
                  '-B', str(build_dir),
                  '-DCMAKE_BUILD_TYPE=Release',
                  f'-DCMAKE_PREFIX_PATH={str(install_dir)}',
                  f'-DCMAKE_INSTALL_PREFIX={str(install_dir)}',
                  ]

    build_cmd = ['cmake',
                 '--build', str(build_dir),
                 '--parallel', str(os.cpu_count()),
                 '--target', 'install',
                 ]

    if src_dir.name == 'aws-lc':
        config_cmd += ['-DDISABLE_GO=ON',
                       '-DBUILD_LIBSSL=OFF',
                       '-DDISABLE_PERL=ON',
                       ]

    if src_dir.name != 's3-benchrunner-c':
        # runner doesn't have tests
        config_cmd += ['-DBUILD_TESTING=OFF']

    run(config_cmd)
    run(build_cmd)


def build_c(work_dir: Path, branch: Optional[str]) -> list[str]:
    """build s3-benchrunner-c"""

    install_dir = work_dir/'install'

    # fetch and build dependencies
    deps = [
        'aws-c-common',
        'aws-lc',
        's2n',
        'aws-c-cal',
        'aws-c-io',
        'aws-checksums',
        'aws-c-compression',
        'aws-c-http',
        'aws-c-sdkutils',
        'aws-c-auth',
        'aws-c-s3',
    ]
    if sys.platform in ['darwin', 'win32']:
        deps.remove('aws-lc')
        deps.remove('s2n')

    for dep_name in deps:
        src_dir = work_dir/dep_name
        fetch_git_repo(url=f'https://github.com/awslabs/{dep_name}.git',
                       dir=src_dir,
                       preferred_branch=branch)

        build_dir = work_dir/f"{dep_name}-build"
        _build_cmake_proj(src_dir, build_dir, install_dir)

    # build s3-benchrunner-c
    _build_cmake_proj(src_dir=get_runner_dir('c'),
                      build_dir=work_dir/'s3-benchrunner-c-build',
                      install_dir=install_dir)

    # return runner cmd
    return [str(install_dir/'bin/s3-benchrunner-c')]


def _fetch_and_install_python_repo(
        url: str,
        dir: Path,
        main_branch: str,
        preferred_branch: Optional[str],
        venv_python: str):
    fetch_git_repo(url, dir, main_branch, preferred_branch)

    # install into virtual env
    # use --editable so we don't need to reinstall after simple file edits
    run([venv_python, '-m', 'pip', 'install', '--editable', str(dir)])


def build_python(work_dir: Path, branch: Optional[str]) -> list[str]:
    """build s3-benchrunner-python"""

    # create virtual environment (if necessary) awscli from Github
    # doesn't interfere with system installation of awscli
    venv_dir = work_dir.joinpath('venv')
    venv_python = str(venv_dir.joinpath('bin/python3'))
    if not venv_dir.exists():
        run([sys.executable, '-m', 'venv', str(venv_dir)])

        # upgrade pip to avoid warnings
        run([venv_python, '-m', 'pip', 'install', '--upgrade', 'pip'])

    _fetch_and_install_python_repo(
        url='https://github.com/aws/aws-cli.git',
        dir=work_dir.joinpath('aws-cli'),
        main_branch='v2',
        preferred_branch=branch,
        venv_python=venv_python)

    _fetch_and_install_python_repo(
        url='https://github.com/boto/boto3.git',
        dir=work_dir.joinpath('boto3'),
        main_branch='develop',
        preferred_branch=branch,
        venv_python=venv_python)

    _fetch_and_install_python_repo(
        url='https://github.com/boto/s3transfer.git',
        dir=work_dir.joinpath('s3transfer'),
        main_branch='develop',
        preferred_branch=branch,
        venv_python=venv_python)

    _fetch_and_install_python_repo(
        url='https://github.com/boto/botocore.git',
        dir=work_dir.joinpath('botocore'),
        main_branch='develop',
        preferred_branch=branch,
        venv_python=venv_python)

    # install aws-crt-python
    # NOTE: (pip complains that the newly installed 1.0.0.dev0 clashes
    # with the version requirements from other packages, but we ignore this)
    _fetch_and_install_python_repo(
        url='https://github.com/awslabs/aws-crt-python.git',
        dir=work_dir.joinpath('aws-crt-python'),
        main_branch='main',
        preferred_branch=branch,
        venv_python=venv_python)

    # return command for executing the runner, using the virtual environment
    return [venv_python, str(get_runner_dir('python')/'main.py')]


def build_java(work_dir: Path, branch: Optional[str]) -> list[str]:
    """build s3-benchrunner-java"""

    # fetch latest aws-crt-java and install 1.0.0-SNAPSHOT
    awscrt_src = work_dir/'aws-crt-java'
    fetch_git_repo(url='https://github.com/awslabs/aws-crt-java.git',
                   dir=awscrt_src,
                   preferred_branch=branch)
    os.chdir(str(awscrt_src))
    run(['mvn', 'clean', 'install', '-Dmaven.test.skip'])

    # Build runner
    runner_src = get_runner_dir('java')
    os.chdir(str(runner_src))
    run(['mvn',
         'clean',
         # package along with dependencies in executable uber-java
         'package',
         # use locally installed version of aws-crt-java
         '--activate-profiles', 'snapshot',
         ])

    # return command for running the jar
    jar_path = runner_src/'target/s3-benchrunner-java-1.0-SNAPSHOT.jar'
    return ['java', '-jar', str(jar_path)]


if __name__ == '__main__':
    args = PARSER.parse_args()

    # for faster C compilation
    os.environ['CMAKE_BUILD_PARALLEL_LEVEL'] = str(os.cpu_count())

    # if --build is "/tmp/build" and --lang is "c" then work_dir is "/tmp/build/c"
    build_root_dir = Path(args.build_dir).resolve()
    work_dir = build_root_dir/args.lang
    work_dir.mkdir(parents=True, exist_ok=True)

    # get build function by name, and call it
    build_fn = globals()[f"build_{args.lang}"]
    runner_cmd = build_fn(work_dir, args.branch)

    print("------ RUNNER_CMD ------")
    print(subprocess.list2cmdline(runner_cmd))