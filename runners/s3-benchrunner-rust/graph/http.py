from collections import defaultdict
from dataclasses import dataclass
import pandas
import plotly
import plotly.express
import plotly.graph_objs
from typing import Optional, Tuple

from . import Trace


def draw(trace: Trace) -> plotly.graph_objs.Figure:
    """
    Draw a timeline that packs together spans representing HTTP requests.

    Each HTTP request is represented by a horizontal line, from its start to end time.
    Each HTTP request is assigned a row (aka Y-Axis) so they won't overlap,
    but the row is otherwise meaningless.

    So like, 7 HTTP requests could be drawn using 3 rows like so:
    2|   -- ----
    1|  ---- -----
    0| -- ----- -----
    """
    min_ns = min(span['startTimeUnixNano'] for span in trace.spans)
    max_ns = max(span['endTimeUnixNano'] for span in trace.spans)

    # when drawing a line, don't make it shorter than this, or it won't be visible
    min_visual_duration_ns = int((max_ns - min_ns) * 0.0001)

    # gaps between adjacent lines should be this large
    gap_between_requests_ns = int((max_ns - min_ns) * 0.001)

    requests = _gather_all_requests(trace)

    columns = defaultdict(list)
    row_end_times_ns = []
    for req in requests:
        duration_ns = req.end_time_ns - req.start_time_ns
        visual_duration_ns = max(duration_ns, min_visual_duration_ns)
        visual_end_time_ns = req.start_time_ns + visual_duration_ns

        columns['Name'].append(req.name)
        columns['Start Time'].append(pandas.to_datetime(req.start_time_ns))
        columns['End Time'].append(pandas.to_datetime(req.end_time_ns))
        columns['Visual End Time'].append(visual_end_time_ns)
        columns['Duration (secs)'].append(duration_ns / 1_000_000_000.0)

        # find the first row where this request wouldn't overlap, adding a new row if necessary
        row_i = 0
        while row_i < len(row_end_times_ns) and row_end_times_ns[row_i] > req.start_time_ns:
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

    fig = plotly.express.timeline(
        data_frame=df,
        x_start='Start Time',
        x_end='End Time',
        y='Row',
        hover_data=hover_data,
        color='Duration (secs)',
        color_continuous_scale=['green', 'yellow', 'red'],
    )

    return fig


@dataclass
class Request:
    name: str
    start_time_ns: int
    end_time_ns: int


def _gather_all_requests(trace) -> list[Request]:
    requests = []
    for span in trace.spans:
        name = span['name']
        if name.startswith('send-') or name == 'download-chunk':
            requests.append(
                Request(span['niceName'], span['startTimeUnixNano'], span['endTimeUnixNano']))
    return requests
