#!/usr/bin/env python3
import argparse
import time

from runner import (
    BenchmarkConfig,
    BenchmarkRunner,
    bytes_to_MiB,
    bytes_to_GiB,
    bytes_to_megabit,
    bytes_to_gigabit,
    ns_to_secs,
)

PARSER = argparse.ArgumentParser(
    description='Python benchmark runner. Pick which S3 library to use.')
PARSER.add_argument('S3_CLIENT', choices=(
    'crt-python', 'boto3-classic', 'boto3-crt', 'cli-classic', 'cli-crt'))
PARSER.add_argument('WORKLOAD')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)
PARSER.add_argument('--verbose', action='store_true')


def create_runner_given_s3_client_id(id: str, config: BenchmarkConfig) -> BenchmarkRunner:
    """Factory function. Create appropriate subclass, given the S3 client ID."""
    if id == 'crt-python':
        from runner.crt import CrtBenchmarkRunner
        return CrtBenchmarkRunner(config)

    if id.startswith('boto3'):
        from runner.boto3 import Boto3BenchmarkRunner
        return Boto3BenchmarkRunner(config, use_crt=id.endswith('crt'))

    if id.startswith('cli'):
        from runner.cli import CliBenchmarkRunner
        return CliBenchmarkRunner(config, use_crt=id.endswith('crt'))

    else:
        raise ValueError(f'Unknown S3 client: {id}')


if __name__ == '__main__':
    args = PARSER.parse_args()
    config = BenchmarkConfig(args.WORKLOAD, args.BUCKET, args.REGION,
                             args.TARGET_THROUGHPUT, args.verbose)

    # create appropriate benchmark runner
    runner = create_runner_given_s3_client_id(args.S3_CLIENT, config)

    bytes_per_run = config.bytes_per_run()

    # Repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    app_start_ns = time.perf_counter_ns()
    for run_i in range(config.max_repeat_count):
        runner.prepare_run()

        run_start_ns = time.perf_counter_ns()

        runner.run()

        run_secs = ns_to_secs(time.perf_counter_ns() - run_start_ns)
        print(f'Run:{run_i+1} ' +
              f'Secs:{run_secs:f} ' +
              f'Gb/s:{bytes_to_gigabit(bytes_per_run) / run_secs:f}',
              flush=True)

        # Break out if we've exceeded max_repeat_secs
        app_secs = ns_to_secs(time.perf_counter_ns() - app_start_ns)
        if app_secs >= config.max_repeat_secs:
            break
