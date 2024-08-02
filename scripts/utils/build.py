"""
This code is in utils/ so multiple scripts can call it like a function.
But scripts/build-runner.py is the main one you'd call from the terminal.
"""
import os
from pathlib import Path
import sys
from typing import Optional

from utils import fetch_git_repo, run, RUNNERS


def _build_cmake_proj(src_dir: Path, build_dir: Path, install_dir: Path, config_extra: list[str] = []):

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

    config_cmd += config_extra

    run(config_cmd)
    run(build_cmd)


def _build_c(work_dir: Path, branch: Optional[str]) -> list[str]:
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

        config_extra = ['-DBUILD_TESTING=OFF']
        if dep_name == 'aws-lc':
            config_extra += ['-DDISABLE_GO=ON',
                             '-DBUILD_LIBSSL=OFF',
                             '-DDISABLE_PERL=ON']

        _build_cmake_proj(src_dir, build_dir, install_dir, config_extra)

    # build s3-benchrunner-c
    _build_cmake_proj(src_dir=RUNNERS['c'].dir,
                      build_dir=work_dir/'s3-benchrunner-c-build',
                      install_dir=install_dir)

    # return runner cmd
    return [str(install_dir/'bin/s3-benchrunner-c')]


def _build_cpp(work_dir: Path, branch: Optional[str]) -> list[str]:
    """build s3-benchrunner-cpp"""

    install_dir = work_dir/'install'

    # build C++ SDK
    fetch_git_repo(url='https://github.com/aws/aws-sdk-cpp.git',
                   dir=work_dir/'aws-sdk-cpp',
                   preferred_branch=branch)
    _build_cmake_proj(
        src_dir=work_dir/'aws-sdk-cpp',
        build_dir=work_dir/'aws-sdk-cpp-build',
        install_dir=install_dir,
        config_extra=['-DBUILD_SHARED_LIBS=OFF',
                      '-DENABLE_TESTING=OFF',
                      '-DBUILD_ONLY=s3-crt;transfer'])

    # build s3-benchrunner-cpp
    _build_cmake_proj(src_dir=RUNNERS['cpp'].dir,
                      build_dir=work_dir/'s3-benchrunner-cpp-build',
                      install_dir=install_dir)

    # return runner cmd
    return [str(install_dir/'bin/s3-benchrunner-cpp')]


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


def _build_python(work_dir: Path, branch: Optional[str]) -> list[str]:
    """build s3-benchrunner-python"""

    # create virtual environment (if necessary) awscli from Github
    # doesn't interfere with system installation of awscli
    venv_dir = work_dir.joinpath('venv')
    venv_python = str(venv_dir.joinpath('bin/python3'))
    if not venv_dir.exists():
        run([sys.executable, '-m', 'venv', str(venv_dir)])

        # upgrade pip to avoid warnings
        # and install wheel so we can build aws-crt-python
        run([venv_python, '-m', 'pip', 'install', '--upgrade', 'pip', 'wheel'])

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
    main_path = RUNNERS['python'].dir/'main.py'
    return [venv_python, str(main_path)]


def _build_java(work_dir: Path, branch: Optional[str]) -> list[str]:
    """build s3-benchrunner-java"""

    # fetch latest aws-crt-java and install 1.0.0-SNAPSHOT
    awscrt_src = work_dir/'aws-crt-java'
    fetch_git_repo(url='https://github.com/awslabs/aws-crt-java.git',
                   dir=awscrt_src,
                   preferred_branch=branch)
    os.chdir(str(awscrt_src))
    run(['mvn', 'clean', 'install', '-Dmaven.test.skip'])

    # fetch latest aws-sdk-java-v2 and install latest SNAPSHOT version
    sdk_src = work_dir/'aws-sdk-java-v2'
    fetch_git_repo(url='https://github.com/aws/aws-sdk-java-v2.git',
                   dir=sdk_src,
                   main_branch='master',
                   preferred_branch=branch)
    os.chdir(str(sdk_src))
    run(['mvn', 'clean', 'install',
         '--projects', ':s3-transfer-manager,:s3,:bom-internal,:bom',
         '--activate-profiles', 'quick',
         '--also-make',
         # use locally installed version of aws-crt-java
         '-Dawscrt.version=1.0.0-SNAPSHOT',
         ])

    # Build runner
    runner_src = RUNNERS['java'].dir
    os.chdir(str(runner_src))
    run(['mvn',
         'clean',
         # package along with dependencies in executable uber-java
         'package',
         # use locally installed version of aws-crt-java
         '-Dawscrt.version=1.0.0-SNAPSHOT',
         ])

    # return command for running the jar
    jar_path = runner_src/'target/s3-benchrunner-java-1.0-SNAPSHOT.jar'
    return ['java', '-jar', str(jar_path)]


def _build_rust(work_dir: Path, branch: Optional[str]) -> list[str]:
    """build s3-benchrunner-rust"""
    runner_src = RUNNERS['rust'].dir
    os.chdir(runner_src)

    if branch:
        print("WARNING: rust runner doesn't currently support --branch")

    # Build runner
    run(['cargo', 'build', '--release'])

    # return runner cmd
    return [str(runner_src/'target/release/s3-benchrunner-rust')]


def build_runner(lang: str, build_root_dir: Path, branch: Optional[str]) -> list[str]:
    """
    Build s3-benchrunner-<lang> and its dependencies.
    Returns command for executing the runner.
    """

    # for faster C compilation
    os.environ['CMAKE_BUILD_PARALLEL_LEVEL'] = str(os.cpu_count())

    # if --build is "/tmp/build" and --lang is "c" then work_dir is "/tmp/build/c"
    work_dir = build_root_dir/lang
    work_dir.mkdir(parents=True, exist_ok=True)

    # get build function by name, and call it
    build_functions = {
        'c': _build_c,
        'cpp': _build_cpp,
        'python': _build_python,
        'java': _build_java,
        'rust': _build_rust,
    }
    build_fn = build_functions[lang]

    # restore cwd after building
    cwd_prev = Path.cwd()

    runner_cmd = build_fn(work_dir, branch)

    os.chdir(cwd_prev)

    return runner_cmd
