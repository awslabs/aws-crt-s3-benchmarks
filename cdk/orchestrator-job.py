#!/usr/bin/env python3
"""
The orchestrator job iterates over each per-instance benchmarking job,
kicking it off and waiting until it completes before kicking off the next.
"""

import argparse
import boto3  # type: ignore
import datetime
import pprint
import re
import subprocess
import sys
import time
import urllib.parse

import s3_benchmarks

pp = pprint.PrettyPrinter(sort_dicts=False)


# Use comma separated lists (instead of normal argparse lists)
# so that it's easy to pass via Batch's job definition parameters:
# https://docs.aws.amazon.com/batch/latest/userguide/job_definition_parameters.html?icmpid=docs_console_unmapped#parameters
def comma_separated_list(arg):
    items = arg.split(',')  # comma separated
    items = [x.strip() for x in items]  # strip whitespace
    items = [x for x in items if x]  # remove empty strings
    if len(items) == 0:
        raise argparse.ArgumentTypeError("List is empty")
    return items


PARSER = argparse.ArgumentParser(
    description="Run S3 benchmarks on each EC2 instance type")
PARSER.add_argument(
    '--buckets', required=True, type=comma_separated_list,
    help="S3 bucket names, comma separated (e.g. my-bucket,my-bucket--usw2-az3--x-s3)")
PARSER.add_argument(
    '--region', required=True,
    help="AWS region (e.g. us-west-2)")
PARSER.add_argument(
    '--instance-types', required=True, type=comma_separated_list,
    help="EC2 instance types, comma separated (e.g. c5n.18xlarge,p4d.24xlarge)")
PARSER.add_argument(
    '--s3-clients', required=True, type=comma_separated_list,
    help="S3 clients to benchmark, comma separated (e.g. crt-c,crt-python)")
PARSER.add_argument(
    '--workloads', required=True, type=comma_separated_list,
    help="Workloads, comma separated (e.g. upload-Caltech256Sharded,download-Caltech256Sharded)")
PARSER.add_argument(
    '--branch',
    # default to "main" (instead of None or "") to work better with Batch parameters.
    # (Batch seems to omit parameters with empty string values)
    default="main",
    help="If specified, try to use this branch/commit/tag of various Git repos.")


def wait_for_completed_job_description(batch, job_id) -> dict:
    """
    Waits until job is complete, and returns description of finished job.
    Returns first item in response['jobs'], described here:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/describe_jobs.html
    """
    CHECK_EVERY_N_SECS = 30
    PRINT_EVERY_N_SECS = 10 * 60

    start_time = time.time()

    # print URL for viewing job in the Console
    region = batch.meta.region_name
    job_url = f"Job URL: https://{region}.console.aws.amazon.com/batch/home?region={region}#jobs/ec2/detail/{job_id}"
    print(job_url)

    # track what we've already printed
    prev_print_time = start_time
    prev_status = None
    printed_log_url = False

    # loop until job completes
    while True:
        response = batch.describe_jobs(jobs=[job_id])
        description = response['jobs'][0]
        status = description['status']

        # print any status changes
        if status != prev_status:
            prev_status = status
            print(f"Job status -> {status}")

        # print URL for viewing logs in the Console
        if not printed_log_url:
            container = description.get('container')
            if container:
                log_name = container.get('logStreamName')
                if log_name:
                    # Transform name from: S3Benchmarks-PerInstance-c5n-18xlarge/default/9a9668ebf40e49e6890019eb83d1062e
                    # To: https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logEventViewer:group=%2Faws%2Fbatch%2Fjob;stream=S3Benchmarks-PerInstance-c5n-18xlarge%2Fdefault%2F9a9668ebf40e49e6890019eb83d1062e
                    log_url = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logEventViewer:group=%2Faws%2Fbatch%2Fjob;stream="
                    log_url += urllib.parse.quote(log_name, safe='')
                    print(f"Job logs URL: {log_url}")
                    printed_log_url = True

        # if job complete, return description
        if status in ['SUCCEEDED', 'FAILED']:
            return description

        # every once in a while, print that we're still waiting
        now = time.time()
        if now > prev_print_time + PRINT_EVERY_N_SECS:
            prev_print_time = now
            waiting_timedelta = datetime.timedelta(seconds=(now - start_time))
            print(f"Been waiting {waiting_timedelta}...")

        # sleep before querying again
        sys.stdout.flush()  # ensure logs are showing latest status updates
        time.sleep(CHECK_EVERY_N_SECS)

        # Should we kill the job if it's taking too long?
        # The job definition already has a default timeout, but that only
        # applies to the RUNNING state. Jobs can get stuck in the RUNNABLE state
        # forever if something is subtly misconfigured (This happened many
        # times when learning how to set up Batch, but not something
        # that will happen randomly). Not sure what will happen if we're
        # trying to use some "rare" instance type that is often unavailable.


if __name__ == '__main__':
    # show in logs exactly how this Batch job was invoked
    print(f"> {sys.executable} {subprocess.list2cmdline(sys.argv)}")

    args = PARSER.parse_args()

    # ensure all --instance-types are valid
    instance_types = []
    for instance_type_id in args.instance_types:
        try:
            instance_type = s3_benchmarks.INSTANCE_TYPES[instance_type_id]
            instance_types.append(instance_type)
        except KeyError:
            exit(f'No known instance type "{instance_type_id}"')

    # create Batch client
    batch = boto3.client('batch', region_name=args.region)

    # run each per-instance job
    for i, instance_type in enumerate(instance_types):
        print(
            f"--- Benchmarking instance type {i+1}/{len(instance_types)}: {instance_type.id} ---")

        # name doesn't really matter, but it's helpful to indicate what's going on
        # looks like: "c5n-18xlarge_s3clients-1_workloads-12_branch-myexperiment"
        job_name = f"{instance_type.id.replace('.', '-')}_s3clients-{len(args.s3_clients)}_workloads-{len(args.workloads)}"
        if args.branch != "main":
            safe_branch_name = re.sub(r'[^-_a-zA-Z0-9]', '', args.branch)
            job_name += f"_branch-{safe_branch_name}"

        submit_job_kwargs = {
            'jobName': job_name,
            # currently, job queues and definitions have hard-coded names
            'jobQueue': instance_type.resource_name(),
            'jobDefinition': instance_type.resource_name(),
            # pass select args along to per-instance job
            'parameters': {
                'branch': args.branch,
                'buckets': ','.join(args.buckets),
                'workloads': ','.join(args.workloads),
                's3Clients': ','.join(args.s3_clients),
            },
        }

        print("Submitting job:")
        pp.pprint(submit_job_kwargs)
        submit_job_response = batch.submit_job(**submit_job_kwargs)
        job_id = submit_job_response['jobId']

        description = wait_for_completed_job_description(batch, job_id)
        print("Job complete:")
        pp.pprint(description)

    print("ORCHESTRATOR DONE!")
