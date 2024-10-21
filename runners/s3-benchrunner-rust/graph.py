#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from graph import PerfTimer
import graph.allspans

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

trace_json = Path(args.TRACE_JSON)

# if directory passed in, pick the newest file
if trace_json.is_dir():
    all_traces = list(trace_json.glob('trace_*.json'))
    if len(all_traces) == 0:
        exit(f"No trace_*.json found under: {trace_json.absolute()}")
    trace_json = sorted(all_traces, key=lambda x: x.stat().st_mtime)[-1]

with PerfTimer(f'Open {trace_json}'):
    with open(trace_json) as f:
        traces_data = json.load(f)

with PerfTimer('Graph all spans'):
    fig = graph.allspans.draw(traces_data)

html_path = Path(trace_json).with_suffix('.allspans.html')
with PerfTimer(f'Write {html_path}'):
    fig.write_html(html_path)
