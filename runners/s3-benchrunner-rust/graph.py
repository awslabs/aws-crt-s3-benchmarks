#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from graph import PerfTimer
import graph.allspans

PARSER = argparse.ArgumentParser(description="Graph a benchmark run")

# File contains JSON representation of OTLP TracesData
# see: https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
#
# Note that when proto data is mapped to JSON, snake_case names become camelCase
# see: https://protobuf.dev/programming-guides/proto3/#json
PARSER.add_argument('TRACE_JSON', help="trace_*.json file to graph.")

args = PARSER.parse_args()

with PerfTimer(f'Open {args.TRACE_JSON}'):
    with open(args.TRACE_JSON) as f:
        traces_data = json.load(f)

with PerfTimer('Graph all spans'):
    fig = graph.allspans.draw(traces_data)

html_path = Path(args.TRACE_JSON).with_suffix('.html')
with PerfTimer(f'Write {html_path}'):
    fig.write_html(html_path)
