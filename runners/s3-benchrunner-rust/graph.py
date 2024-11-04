#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from graph import PerfTimer, Trace
import graph.allspans
import graph.http

PARSER = argparse.ArgumentParser(description="Graph a benchmark run")

# File contains JSON representation of OTLP TracesData.
# Contents look like:
# {"resourceSpans":[
#   {"resource": {"attributes":[{"key":"service.name","value":{"stringValue":"s3-benchrunner-rust"}}, ...]},
#    "scopeSpans":[
#      {"scope":{"name":"s3-benchrunner-rust"},
#       "spans":[
#         {"traceId":"0e506aee98c24b869337620977f30cbb","spanId":"6fb4c16d1d1652d6", ...},
#         {"traceId":"0e506aee98c24b869337620977f30cbb","spanId":"6440f82fb6fc6299", ...},
#         ...
#
# Official protobuf format specified here:
# https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
#
# Note that when proto data is mapped to JSON, snake_case names become camelCase
# see: https://protobuf.dev/programming-guides/proto3/#json
PARSER.add_argument('TRACE_JSON', help="trace_*.json file to graph.")


def process_file(trace_json_path: Path):
    with PerfTimer(f'Open {trace_json_path}'):
        with open(trace_json_path) as f:
            traces_data = json.load(f)
            trace = Trace(traces_data)
            if len(trace.spans) == 0:
                exit("FAILED: Trace file has no spans")

    http_path = Path(trace_json_path).with_suffix('.http.html')
    with PerfTimer(f'Write {http_path}'):
        fig = graph.http.draw(trace)
        fig.write_html(http_path)

    allspans_path = Path(trace_json_path).with_suffix('.allspans.html')
    with PerfTimer(f'Write {allspans_path}'):
        fig = graph.allspans.draw(trace)
        fig.write_html(allspans_path)


if __name__ == '__main__':
    args = PARSER.parse_args()

    trace_json_arg = Path(args.TRACE_JSON)

    if trace_json_arg.is_dir():
        # If directory passed in, find the latest benchmark, and process the files for each run.
        # Traces are named like:
        # - trace_20241028T212038Z_download-30GiB-1x-ram_run01_022Gbps.json
        # - trace_20241028T212038Z_download-30GiB-1x-ram_run02_034Gbps.json
        # - trace_20241028T212038Z_download-30GiB-1x-ram_run03_023Gbps.json
        all_traces = list(trace_json_arg.glob('trace_*.json'))
        if len(all_traces) == 0:
            exit(f"No trace_*.json found under: {trace_json_arg.absolute()}")
        all_traces = sorted(all_traces)

        # All traces files start with "trace_{DATE}T{TIME}Z_*"
        # so the oldest file is the last alphabetically
        # Find Runs 1, 2, 3, etc by finding all files with this prefix
        oldest = max(all_traces)
        oldest_prefix = oldest.name[0:oldest.name.find('Z_')]
        trace_json_paths = [
            x for x in all_traces if x.name.startswith(oldest_prefix)]
    else:
        # Just process the 1 file
        trace_json_paths = [trace_json_arg]

    for i, trace_json_path in enumerate(trace_json_paths):
        # print banner between each file (if processing more than 1)
        if len(trace_json_paths) > 1:
            msg = f"{i+1}/{len(trace_json_paths)}"
            print(f"-------------- {msg} --------------")

        process_file(trace_json_path)
