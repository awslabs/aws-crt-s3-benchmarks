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
    description='s5cmd benchmark runner. Uses s5cmd for S3 operations.')
PARSER.add_argument('WORKLOAD')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)
PARSER.add_argument('--verbose', action='store_true')


def create_runner(config: BenchmarkConfig) -> BenchmarkRunner:
    """Factory function. Create s5cmd benchmark runner."""
    from runner.s5cmd import S5cmdBenchmarkRunner
    return S5cmdBenchmarkRunner(config)


if __name__ == '__main__':
    args = PARSER.parse_args()
    config = BenchmarkConfig(args.WORKLOAD, args.BUCKET, args.REGION,
                             args.TARGET_THROUGHPUT, args.verbose)

    # create s5cmd benchmark runner
    runner = create_runner(config)

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
