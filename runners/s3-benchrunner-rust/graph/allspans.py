from collections import defaultdict
import pandas as pd  # type: ignore
import plotly.express as px  # type: ignore

from . import Trace


def draw(trace: Trace):
    # sort spans according to parent-child hierarchy
    spans = _sort_spans_by_hierarchy(trace)

    # prepare columns for plotly
    columns = defaultdict(list)
    name_count: dict[str, int] = defaultdict(int)
    for (idx, span) in enumerate(spans):
        name = span['name']
        # nice name includes stuff like part-number
        nice_name = span['niceName']
        # we want each span in its own row, so assign a unique name and use that as Y value
        name_count[nice_name] += 1
        unique_name = f"{nice_name} ({span['spanId']})"

        start_time_ns = span['startTimeUnixNano']
        end_time_ns = span['endTimeUnixNano']
        duration_ns = end_time_ns - start_time_ns
        # ensure span is wide enough to see
        visual_end_time_ns = start_time_ns + max(duration_ns, 50_000_000)

        columns['Name'].append(name)
        columns['Nice Name'].append(nice_name)
        columns['Unique Name'].append(unique_name)
        columns['Start Time'].append(pd.to_datetime(start_time_ns))
        columns['End Time'].append(pd.to_datetime(end_time_ns))
        columns['Visual End Time'].append(pd.to_datetime(visual_end_time_ns))
        columns['Duration (secs)'].append(duration_ns / 1_000_000_000.0)
        columns['Span ID'].append(span['spanId'])
        columns['Parent ID'].append(span['parentSpanId'])
        columns['Attributes'].append(
            trace.get_span_attributes_hover_data(span))

    # if a span name occurs only once, we can just use the nice_name
    for (i, name) in enumerate(columns['Name']):
        if name_count[name] == 1:
            columns['Unique Name'][i] = columns['Nice Name'][i]

    df = pd.DataFrame(columns)

    # By default, show all columns in hover text.
    # Omit a column by setting false. You can also set special formatting rules here.
    hover_data = {col: True for col in columns.keys()}
    hover_data['Unique Name'] = False  # already shown
    hover_data['Visual End Time'] = False  # actual "End Time" already shown

    fig = px.timeline(
        data_frame=df,
        x_start='Start Time',
        x_end='Visual End Time',
        y='Unique Name',
        hover_data=hover_data,
        # spans with same original name get same color
        # TODO: combine name with code.namespace, in case same name used in multiple places
        color='Name',
        # force ordering, otherwise plotly will group by 'color'
        category_orders={'Unique Name': df['Unique Name']},
    )

    # if there are lots of rows, ensure they're not drawn too small
    num_rows = len(spans)
    if num_rows > 20:
        preferred_total_height = 800
        min_row_height = 3
        row_height = preferred_total_height / num_rows
        row_height = int(max(min_row_height, row_height))
        height = num_rows * row_height
        # don't show yaxis labels if they're so squished that some are omitted
        show_yaxis_labels = row_height >= 15
    else:
        # otherwise auto-height
        height = None
        show_yaxis_labels = True

    fig.update_layout(
        title="All Benchmark Spans",
        xaxis_title="Time",
        yaxis_title="Span Name",
        height=height,
        yaxis=dict(
            showticklabels=show_yaxis_labels,
        ),
        hovermode='y unified',  # show hover if mouse anywhere in row
    )

    return fig


def _sort_spans_by_hierarchy(trace: Trace):
    # sort spans in depth-first order, by crawling the parent/child tree starting at root
    sorted_spans = []
    # ids_to_process is FIFO
    # With each loop, we pop the last item in ids_to_process
    # and then append its children, so that we process them next.
    ids_to_process = ['0000000000000000']
    while ids_to_process:
        id = ids_to_process.pop(-1)

        # child_ids are already sorted by start time
        child_ids = [child['spanId'] for child in trace.get_child_spans(id)]

        # reverse child IDs before pushing into ids_to_process,
        # since we pop from BACK and want earlier children to pop sooner
        child_ids.reverse()
        ids_to_process.extend(child_ids)

        if (span := trace.get_span(id)) is not None:
            sorted_spans.append(span)

    # warn if any spans are missing
    if (num_leftover := len(trace.spans) - len(sorted_spans)):
        print(f"WARNING: {num_leftover} spans not shown (missing parents)")

    return sorted_spans
