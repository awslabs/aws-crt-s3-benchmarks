from collections import defaultdict
from dataclasses import dataclass
import pandas  # type: ignore
import plotly   # type: ignore
import plotly.express   # type: ignore
import plotly.graph_objs   # type: ignore
from typing import Optional

from . import Trace


def draw(trace: Trace) -> plotly.graph_objs.Figure:
    """
    Draw a timeline that packs together spans representing HTTP requests.

    Each HTTP request is represented by a horizontal line, from its start to end time.
    Each HTTP request is assigned a row (aka Y-Axis) so they won't overlap,
    but the row is otherwise meaningless.

    So like, 7 HTTP requests could be drawn using 3 rows like so:
    2|   -- ----
    1|  ----- -----
    0| -- -----   -----
    """
    min_ns = min(span['startTimeUnixNano'] for span in trace.spans)
    max_ns = max(span['endTimeUnixNano'] for span in trace.spans)

    # when drawing a line, don't make it shorter than this, or it won't be visible
    min_visual_duration_ns = int((max_ns - min_ns) * 0.0001)

    # gaps between adjacent lines should be this large
    gap_between_requests_ns = int((max_ns - min_ns) * 0.001)

    requests = _gather_all_requests(trace)

    columns = defaultdict(list)
    row_end_times_ns: list[int] = []
    for req in requests:
        start_time_ns = req.span['startTimeUnixNano']
        end_time_ns = req.span2['endTimeUnixNano'] if req.span2 else req.span['endTimeUnixNano']

        duration_ns = end_time_ns - start_time_ns
        visual_duration_ns = max(duration_ns, min_visual_duration_ns)
        visual_end_time_ns = start_time_ns + visual_duration_ns

        columns['Name'].append(req.span['niceName'])
        columns['Start Time'].append(pandas.to_datetime(start_time_ns))
        columns['End Time'].append(pandas.to_datetime(end_time_ns))
        columns['Visual End Time'].append(visual_end_time_ns)
        columns['Duration (secs)'].append(duration_ns / 1_000_000_000.0)
        columns['Bucket'].append(
            trace.get_attribute_in_span_or_parent(req.span, 'bucket', ""))
        columns['Key'].append(
            trace.get_attribute_in_span_or_parent(req.span, 'key', ""))
        columns['Span ID'].append(req.span['spanId'])
        columns['Attributes'].append(
            trace.get_span_attributes_hover_data(req.span))
        columns['Span ID#2'].append(req.span2['spanId'] if req.span2 else "")
        columns['Attributes#2'].append(
            trace.get_span_attributes_hover_data(req.span2) if req.span2 else "")

        # find the first row where this request wouldn't overlap, adding a new row if necessary
        row_i = 0
        while row_i < len(row_end_times_ns) and row_end_times_ns[row_i] > start_time_ns:
            row_i += 1
        if row_i == len(row_end_times_ns):
            row_end_times_ns.append(0)
        row_end_times_ns[row_i] = visual_end_time_ns + gap_between_requests_ns

        columns['Row'].append(row_i * 2)

    df = pandas.DataFrame(columns)

    # By default, show all columns in hover text.
    # Omit a column by setting false. You can also set special formatting rules here.
    hover_data = {col: True for col in columns.keys()}
    hover_data['Visual End Time'] = False  # actual "End Time" already shown
    hover_data['Row'] = False  # row is meaningless

    fig = plotly.express.timeline(
        title="HTTP Requests",
        data_frame=df,
        x_start='Start Time',
        x_end='End Time',
        y='Row',
        hover_data=hover_data,
        color='Duration (secs)',
        color_continuous_scale=['green', 'yellow', 'red'],
    )
    fig.update_layout(
        xaxis_title="Start/End Time",
        yaxis_title="Concurrency (approx)",
    )

    return fig


@dataclass
class Request:
    span: dict
    span2: Optional[dict] = None  # Some requests are spread across 2 spans


def _gather_all_requests(trace: Trace) -> list[Request]:
    requests = []

    # The ranged discovery HTTP request is divided into 2 spans:
    # "send-ranged-get-for-discovery" & "collect-body-from-discovery"
    # gather them all, we'll correlate them later...
    initial_discovery_spans = []
    body_discovery_spans = []

    for span in trace.spans:
        name = span['name']

        if name == 'send-ranged-get-for-discovery':
            initial_discovery_spans.append(span)
        elif name == 'collect-body-from-discovery':
            body_discovery_spans.append(span)
        elif name.startswith('send-') or name == 'download-chunk':
            # Simple: 1 span -> 1 HTTP request
            requests.append(Request(span))

    # Correlate the ranged discovery "initial" and "body" spans, to create 1 Request
    # Sweep over "initial" spans (sorted by first-to-end)
    # Then sweep over "body" spans (sorted by first-to-start)
    # Assume they match if we find one with the same 'bucket' and 'key' attributes
    initial_discovery_spans.sort(key=lambda x: x['endTimeUnixNano'])
    body_discovery_spans.sort(key=lambda x: x['startTimeUnixNano'])
    num_unmatched = 0
    for initial_span in initial_discovery_spans:
        bucket = trace.get_attribute_in_span_or_parent(initial_span, 'bucket')
        key = trace.get_attribute_in_span_or_parent(initial_span, 'key')
        found_body_span = False
        for i, body_span in enumerate(body_discovery_spans):
            if bucket == trace.get_attribute_in_span_or_parent(body_span, 'bucket') \
                    and key == trace.get_attribute_in_span_or_parent(body_span, 'key'):
                found_body_span = True
                break

        if found_body_span:
            requests.append(Request(initial_span, body_span))
            # pop "body" span so we don't correlate it with another "initial" span
            body_discovery_spans.pop(i)
        else:
            requests.append(Request(initial_span))
            num_unmatched += 1

    num_unmatched = max(num_unmatched, len(body_discovery_spans))
    if num_unmatched > 0:
        print(
            f"WARNING: {num_unmatched} discovery spans not matched with collect-body spans")

    requests.sort(key=lambda x: x.span['startTimeUnixNano'])
    return requests
