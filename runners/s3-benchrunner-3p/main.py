#!/usr/bin/env python3
import argparse
import json
import resource
import statistics
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
    description='Third-party S3 client benchmark runner. Supports various third-party S3 clients.')
PARSER.add_argument('EXECUTABLE_PATH', help='Path to the S3 client executable')
PARSER.add_argument('S3_CLIENT', choices=(
    's5cmd', 'rclone'), help='S3 client to use')
PARSER.add_argument('WORKLOAD')
PARSER.add_argument('BUCKET')
PARSER.add_argument('REGION')
PARSER.add_argument('TARGET_THROUGHPUT', type=float)
PARSER.add_argument('--verbose', action='store_true')


def create_runner(config: BenchmarkConfig, s3_client: str, executable_path: str) -> BenchmarkRunner:
    """Factory function. Create appropriate third-party benchmark runner."""
    if s3_client == 's5cmd':
        from runner.s5cmd import S5cmdBenchmarkRunner
        return S5cmdBenchmarkRunner(config, executable_path)
    elif s3_client == 'rclone':
        from runner.rclone import RcloneBenchmarkRunner
        return RcloneBenchmarkRunner(config, executable_path)
    else:
        raise ValueError(f'Unknown S3 client: {s3_client}')


def _calc_stats(values: list[float]) -> dict:
    """Compute median, mean, min, max, stddev for a list of values."""
    sorted_v = sorted(values)
    n = len(sorted_v)
    mean = statistics.mean(sorted_v)
    return {
        "median": round(statistics.median(sorted_v), 6),
        "mean": round(mean, 6),
        "min": round(sorted_v[0], 6),
        "max": round(sorted_v[-1], 6),
        "stddev": round(statistics.pstdev(sorted_v), 6),
    }


if __name__ == '__main__':
    args = PARSER.parse_args()
    config = BenchmarkConfig(args.WORKLOAD, args.BUCKET, args.REGION,
                             args.TARGET_THROUGHPUT, args.verbose)

    # create appropriate third-party benchmark runner
    runner = create_runner(config, args.S3_CLIENT, args.EXECUTABLE_PATH)

    bytes_per_run = config.bytes_per_run()

    # Repeat benchmark until we exceed max_repeat_count or max_repeat_secs
    durations = []
    app_start_ns = time.perf_counter_ns()
    for run_i in range(config.max_repeat_count):
        runner.prepare_run()

        run_start_ns = time.perf_counter_ns()

        runner.run()

        run_secs = ns_to_secs(time.perf_counter_ns() - run_start_ns)
        durations.append(run_secs)
        print(f'Run:{run_i+1} ' +
              f'Secs:{run_secs:f} ' +
              f'Gb/s:{bytes_to_gigabit(bytes_per_run) / run_secs:f}',
              flush=True)

        # Break out if we've exceeded max_repeat_secs
        app_secs = ns_to_secs(time.perf_counter_ns() - app_start_ns)
        if app_secs >= config.max_repeat_secs:
            break

    # Print standardized STATS JSON
    if durations:
        throughputs = [bytes_to_gigabit(bytes_per_run) / s for s in durations]
        # ru_maxrss from child processes (the 3P tools run as subprocesses)
        peak_rss_kib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        import sys
        if sys.platform == 'darwin':
            peak_rss_mib = peak_rss_kib / (1024 * 1024)
        else:
            peak_rss_mib = peak_rss_kib / 1024
        stats = {
            "runs": len(durations),
            "bytes_per_run": bytes_per_run,
            "peak_rss_mib": round(peak_rss_mib, 1),
            "duration": _calc_stats(durations),
            "throughput_gbps": _calc_stats(throughputs),
        }
        print(f"STATS:{json.dumps(stats)}", flush=True)
