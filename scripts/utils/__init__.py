import os
from pathlib import Path
import subprocess
from typing import Optional


REPO_DIR = Path(__file__).parent.parent.parent
RUNNERS_DIR = REPO_DIR/'runners'
SCRIPTS_DIR = REPO_DIR/'scripts'
WORKLOADS_DIR = REPO_DIR/'workloads'
RUNNER_LANGS = ['c', 'python', 'java']


def get_runner_dir(lang: str) -> Path:
    return RUNNERS_DIR/f's3-benchrunner-{lang}'


def workload_paths_from_args(workloads: Optional[list[str]]) -> list[Path]:
    """
    Given --workloads arg, return list of workload paths.
    If workload is not specified, return all .run.json files in workloads/ dir.
    """
    if workloads:
        workload_paths = [Path(x).resolve() for x in workloads]
        for workload in workload_paths:
            if not workload.exists():
                raise Exception(f'workload not found: {str(workload)}')
    else:
        workload_paths = sorted(WORKLOADS_DIR.glob('*.run.json'))
        if not workload_paths:
            raise Exception(f'no workload files found !?!')

    return workload_paths


def run(cmd_args: list[str], check=True) -> subprocess.CompletedProcess:
    """Run a subprocess"""
    print(f'{Path.cwd()}> {subprocess.list2cmdline(cmd_args)}', flush=True)
    completed = subprocess.run(cmd_args)
    if check and completed.returncode != 0:
        exit(f"FAILED running: {subprocess.list2cmdline(cmd_args)}")


def fetch_git_repo(url: str, dir: Path, main_branch: str = 'main', preferred_branch: str = None):
    """
    Ensure repo is cloned, up to date, and on the right branch.

    url: Git url to clone
    dir: Directory to clone into
    main_branch: Fallback if preferred_branch not found
    preferred_branch: Preferred branch/commit/tag
    """
    repo_dir = dir.resolve()  # normalize path

    # git clone (if necessary)
    fresh_clone = not repo_dir.exists()
    if fresh_clone:
        run(['git', 'clone', '--branch', main_branch, url, str(repo_dir)])

    cwd_prev = Path.cwd()
    os.chdir(repo_dir)

    # fetch latest branches (not necessary for fresh clone)
    if not fresh_clone:
        run(['git', 'fetch'])

    # if preferred branch specified, try to check it out...
    using_preferred_branch = False
    if preferred_branch and (preferred_branch != main_branch):
        if run(['git', 'checkout', preferred_branch], check=False).returncode == 0:
            using_preferred_branch = True

    # ...otherwise use main branch
    if not using_preferred_branch:
        run(['git', 'checkout', main_branch])

    # pull latest commit (not necessary for fresh clone)
    if not fresh_clone:
        run(['git', 'pull'])

    # update submodules
    run(['git', 'submodule', 'update', '--init'])

    os.chdir(cwd_prev)
