import os
from dataclasses import dataclass
from pathlib import Path
import subprocess
from typing import Optional


REPO_DIR = Path(__file__).parent.parent.parent
RUNNERS_DIR = REPO_DIR/'runners'
SCRIPTS_DIR = REPO_DIR/'scripts'
WORKLOADS_DIR = REPO_DIR/'workloads'

RUNNERS: dict[str, 'Runner'] = {}  # filled in below
S3_CLIENTS: dict[str, 'S3Client'] = {}  # filled in below


@dataclass
class Runner:
    lang: str
    s3_clients: list[str]

    @property
    def dir(self) -> Path:
        return RUNNERS_DIR/f's3-benchrunner-{self.lang}'


@dataclass
class S3Client:
    name: str
    runner: Runner


def _add_runner(runner: Runner):
    RUNNERS[runner.lang] = runner


_add_runner(Runner('c', s3_clients=['crt-c']))
_add_runner(Runner('java', s3_clients=['crt-java']))
_add_runner(Runner('python',
                   s3_clients=['crt-python', 'cli-crt', 'cli-classic', 'boto3-crt', 'boto3-classic']))

for runner in RUNNERS.values():
    for s3_client in runner.s3_clients:
        S3_CLIENTS[s3_client] = S3Client(name=s3_client, runner=runner)


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


def run(cmd_args: list[str], check=True, capture_output=False) -> subprocess.CompletedProcess:
    """Run a subprocess"""
    print(f'{Path.cwd()}> {subprocess.list2cmdline(cmd_args)}', flush=True)

    if capture_output:
        # Subprocess doesn't have built-in support for capturing output
        # AND printing while it comes in, so we have to do it ourselves.
        # We're combining stderr with stdout, for simplicity.
        with subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # line-buffered
        ) as p:
            lines = []
            assert p.stdout is not None  # satisfy type checker
            for line in p.stdout:
                lines.append(line)
                print(line, end='', flush=True)

            p.wait()  # ensure process is 100% finished

            completed = subprocess.CompletedProcess(
                args=cmd_args,
                returncode=p.returncode,
                stdout="".join(lines),
            )
    else:
        # simpler case: just run the command
        completed = subprocess.run(cmd_args, text=True)

    if check and completed.returncode != 0:
        exit(f"FAILED running: {subprocess.list2cmdline(cmd_args)}")
    return completed


def fetch_git_repo(url: str, dir: Path, main_branch: str = 'main', preferred_branch: Optional[str] = None):
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


def print_banner(msg, *, border=5, char='*'):
    """
    print a banner message.
    e.g. print_banner('hello', border=3, char='*') results in:

    *************
    *** hello ***
    *************

    """
    left_side = (char * border) + ' '
    right_side = ' ' + (char * border)
    middle_row = f'{left_side}{msg}{right_side}'
    top_bottom_row = char * len(middle_row)
    print(top_bottom_row)
    print(middle_row)
    print(top_bottom_row)
