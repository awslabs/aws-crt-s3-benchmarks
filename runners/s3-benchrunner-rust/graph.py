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

args = PARSER.parse_args()

trace_json_path = Path(args.TRACE_JSON)

# if directory passed in, pick the newest file
if trace_json_path.is_dir():
    all_traces = list(trace_json_path.glob('trace_*.json'))
    if len(all_traces) == 0:
        exit(f"No trace_*.json found under: {trace_json_path.absolute()}")
    trace_json_path = sorted(all_traces, key=lambda x: x.stat().st_mtime)[-1]

with PerfTimer(f'Open {trace_json_path}'):
    with open(trace_json_path) as f:
        traces_data = json.load(f)
        trace = Trace(traces_data)

allspans_path = Path(trace_json_path).with_suffix('.allspans.html')
with PerfTimer(f'Write {allspans_path}'):
    fig = graph.allspans.draw(trace)
    fig.write_html(allspans_path)

http_path = Path(trace_json_path).with_suffix('.http.html')
with PerfTimer(f'Write {http_path}'):
    fig = graph.http.draw(trace)
    fig.write_html(http_path)
