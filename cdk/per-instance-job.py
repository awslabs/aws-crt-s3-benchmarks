#!/usr/bin/env python3
"""
The per-instance job is running on a specific EC2 instance type.
It pulls down the aws-crt-s3-benchmarks repo and runs the bechmarks.
"""

import argparse
import subprocess
import sys

import s3_benchmarks


# Use comma separated lists for Batch jobs (instead of normal argparse lists)
# so that it's easy to pass via Batch's job definition parameters:
# https://docs.aws.amazon.com/batch/latest/userguide/job_definition_parameters.html?icmpid=docs_console_unmapped#parameters
def comma_separated_list(arg):
    items = arg.split(',')  # comma separated
    items = [x.strip() for x in items]  # strip whitespace
    items = [x for x in items if x]  # remove empty strings
    if len(items) == 0:
        raise argparse.ArgumentTypeError('List is empty')
    return items


PARSER = argparse.ArgumentParser(
    description="Run S3 benchmarks on each EC2 instance type")
PARSER.add_argument(
    '--bucket', required=True,
    help="S3 bucket name")
PARSER.add_argument(
    '--branch',
    # default to "main" (instead of None or "") to work better with Batch parameters.
    # (Batch seems to omit parameters with empty string values)
    default="main",
    help="If specified, try to use this branch/commit/tag of various Git repos.")
PARSER.add_argument(
    '--instance-type', required=True,
    choices=[x.id for x in s3_benchmarks.ALL_INSTANCE_TYPES],
    help="EC2 instance type this is running on")
PARSER.add_argument(
    '--runners', required=True, type=comma_separated_list,
    help="Library runners, comma separated (e.g. crt-c,crt-python)")
PARSER.add_argument(
    '--workloads', required=True, type=comma_separated_list,
    help="Workloads, comma separated (e.g. upload-Caltech256Sharded,download-Caltech256Sharded)")

if __name__ == '__main__':
    # show in logs exactly how this Batch job was invoked
    print(f"> {sys.executable} {subprocess.list2cmdline(sys.argv)}")

    args = PARSER.parse_args()

    instance_type = next(
        x for x in s3_benchmarks.ALL_INSTANCE_TYPES if x.id == args.instance_type)

    # TODO: git clone aws-crt-s3-benchmarks

    # TODO: kick off scripts that build runners and run benchmarks

    print("PER-INSTANCE JOB DONE!")
