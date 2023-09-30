#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import subprocess

PARSER = argparse.ArgumentParser(
    description='Ensure repo is cloned, up to date, and on the right branch')
PARSER.add_argument(
    '--repo', required=True,
    help='Git repo to clone.')
PARSER.add_argument(
    '--preferred-branch',
    help='Preferred branch/commit/tag')
PARSER.add_argument(
    '--main-branch', default='main',
    help='Fallback if --preferred-branch not found (default: main)')
PARSER.add_argument(
    '--dir', required=True,
    help='Directory to clone into')


def run(cmd_args: list[str]):
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    subprocess.run(cmd_args, check=True)


def try_run(cmd_args: list[str]) -> bool:
    print(f'> {subprocess.list2cmdline(cmd_args)}')
    result = subprocess.run(cmd_args)
    return result.returncode == 0


if __name__ == '__main__':
    args = PARSER.parse_args()

    repo_dir = Path(args.dir).resolve()  # normalize path

    # git clone (if necessary)
    using_fresh_clone = not repo_dir.exists()
    if using_fresh_clone:
        repo_dir.parent.mkdir(parents=True, exist_ok=True)
        os.chdir(str(repo_dir.parent))
        run(['git', 'clone', args.repo])
        using_fresh_clone = True

    os.chdir(str(repo_dir))

    # fetch latest branches (unless this is a fresh clone)
    if not using_fresh_clone:
        run(['git', 'fetch'])

    # if preferred branch specified, try to check it out...
    using_preferred_branch = False
    if args.preferred_branch and (args.preferred_branch != args.main_branch):
        if try_run(['git', 'checkout', args.preferred_branch]):
            using_preferred_branch = True

    # ...but fall back using main branch
    if not using_preferred_branch:
        run(['git', 'checkout', args.main_branch])

    # pull latest commit (unless this is a fresh clone)
    if not using_fresh_clone:
        run(['git', 'pull'])
