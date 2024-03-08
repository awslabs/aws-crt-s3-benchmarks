#!/usr/bin/env python3
import argparse
from pathlib import Path
import subprocess
import sys

from utils import print_banner, run, workload_paths_from_args, S3_CLIENTS, SCRIPTS_DIR
import utils.build


PARSER = argparse.ArgumentParser(
    description='Do-it-all script that prepares S3 files, builds runners, ' +
    'runs benchmarks, and reports results.')
PARSER.add_argument(
    '--bucket', required=True,
    help='S3 bucket (will be created if necessary)')
PARSER.add_argument(
    '--region', required=True,
    help='AWS region (e.g. us-west-2)')
PARSER.add_argument(
    '--throughput', required=True, type=float,
    help='Target network throughput in gigabit/s (e.g. 100.0)')
PARSER.add_argument(
    '--build-dir', required=True,
    help='Root dir for build artifacts')
PARSER.add_argument(
    '--files-dir', required=True,
    help='Root dir for uploading and downloading files. ' +
    'Runners are launched in this directory.')
PARSER.add_argument(
    '--s3-clients', nargs='+', required=True,
    choices=S3_CLIENTS.keys(),
    help='S3 clients to benchmark.')
PARSER.add_argument(
    '--workloads', nargs='+',
    help='Paths to specific workload JSON files. ' +
    'If not specified, everything in workloads/ is run.')
PARSER.add_argument(
    '--branch',
    help='If specified, try to use this branch/commit/tag of various Git repos.')
PARSER.add_argument(
    '--report-metrics',
    help='Report metrics to CloudWatch. Provide namespace for data (e.g. S3Benchmarks).')
PARSER.add_argument(
    '--metrics-instance-type',
    help='If reporting metrics: EC2 instance type (e.g. c5n.18xlarge)')


if __name__ == '__main__':
    args = PARSER.parse_args()
    build_dir = Path(args.build_dir).resolve()
    files_dir = Path(args.files_dir).resolve()
    workloads = workload_paths_from_args(args.workloads)

    # prepare S3 files
    print_banner('PREPARE S3 FILES')
    run([
        sys.executable, str(SCRIPTS_DIR/'prep-s3-files.py'),
        '--bucket', args.bucket,
        '--region', args.region,
        '--files-dir', str(files_dir),
        '--workloads', *[str(x) for x in workloads]
    ])

    # track which runners we've already built
    runner_lang_to_cmd = {}

    for s3_client_id in args.s3_clients:
        runner = S3_CLIENTS[s3_client_id].runner

        # build runner for this language (if necessary)
        # and get cmd args to run it
        if not runner.lang in runner_lang_to_cmd:
            print_banner(f'BUILD RUNNER: {runner.lang}')
            runner_cmd_list = utils.build.build_runner(
                runner.lang, build_dir, args.branch)
            runner_cmd_str = subprocess.list2cmdline(runner_cmd_list)
            runner_lang_to_cmd[runner.lang] = runner_cmd_str

        print_banner(f'RUN BENCHMARKS: {s3_client_id}')
        run_cmd = [
            sys.executable, str(SCRIPTS_DIR/'run-benchmarks.py'),
            '--runner-cmd', runner_lang_to_cmd[runner.lang],
            '--s3-client', s3_client_id,
            '--bucket', args.bucket,
            '--region', args.region,
            '--throughput', str(args.throughput),
            '--files-dir', str(files_dir),
            '--workloads', *[str(x) for x in workloads],
        ]
        if args.report_metrics:
            run_cmd += ['--report-metrics', args.report_metrics]

            if args.metrics_instance_type:
                run_cmd += ['--metrics-instance-type',
                            args.metrics_instance_type]

            # if default branch was used, report "main"
            run_cmd += ['--metrics-branch', args.branch or "main"]

        run(run_cmd)
