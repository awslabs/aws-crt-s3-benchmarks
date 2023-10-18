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
PARSER.add_argument('LIB', choices=(
    'crt', 'boto3-python', 'boto3-crt', 'cli-python', 'cli-crt'))
PARSER.add_argument('BENCHMARK')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)
PARSER.add_argument('--verbose', action='store_true')


def create_runner_for_lib(lib: str, config: BenchmarkConfig) -> BenchmarkRunner:
    """Factory function. Create appropriate subclass, given the lib."""
    use_crt = lib.endswith('crt')

    if lib == 'crt':
        from runner.crt import CrtBenchmarkRunner
        return CrtBenchmarkRunner(config)

    if lib.startswith('boto3'):
        from runner.boto3 import Boto3BenchmarkRunner
        return Boto3BenchmarkRunner(config, use_crt)

    if lib.startswith('cli'):
        from runner.cli import CliBenchmarkRunner
        return CliBenchmarkRunner(config, use_crt)

    else:
        raise ValueError(f'Unknown lib: {lib}')


if __name__ == '__main__':
    args = PARSER.parse_args()
    config = BenchmarkConfig(args.BENCHMARK, args.BUCKET, args.REGION,
                             args.TARGET_THROUGHPUT, args.verbose)

    # create appropriate benchmark runner for given library
    runner = create_runner_for_lib(args.LIB, config)

    bytes_per_run = config.bytes_per_run()

    # Repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    app_start_ns = time.perf_counter_ns()
    for run_i in range(config.max_repeat_count):
        run_start_ns = time.perf_counter_ns()

        runner.run()

        run_secs = ns_to_secs(time.perf_counter_ns() - run_start_ns)
        print(f'Run:{run_i+1} ' +
              f'Secs:{run_secs:.3f} ' +
              f'Gb/s:{bytes_to_gigabit(bytes_per_run) / run_secs:.3f} ' +
              f'Mb/s:{bytes_to_megabit(bytes_per_run) / run_secs:.3f} ' +
              f'GiB/s:{bytes_to_GiB(bytes_per_run) / run_secs:.3f} ' +
              f'MiB/s:{bytes_to_MiB(bytes_per_run) / run_secs:.3f}',
              flush=True)

        # Break out if we've exceeded max_repeat_secs
        app_secs = ns_to_secs(time.perf_counter_ns() - app_start_ns)
        if app_secs >= config.max_repeat_secs:
            break
