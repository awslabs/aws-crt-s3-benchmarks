from collections import defaultdict
import pandas as pd  # type: ignore
import plotly.express as px  # type: ignore


def draw(data):
    # Extract relevant span data and sort by hierarchy
    spans_list = []
    for resource_span in data['resourceSpans']:
        for scope_span in resource_span['scopeSpans']:
            spans_list.extend(scope_span['spans'])

    # Sort spans according to parent-child hierarchy
    sorted_spans = _sort_spans_by_hierarchy(spans_list)

    # Prepare columns for plotly
    columns = defaultdict(list)
    name_count = defaultdict(int)
    for span in sorted_spans:

        name = span['name']
        # we want each span in its own row, so assign a unique name and use that Y value
        name_count[name] += 1
        unique_name = f"{name}#{name_count[name]}"

        columns['Name'].append(name)
        columns['Unique Name'].append(unique_name)
        columns['Duration (ns)'].append(
            span['endTimeUnixNano'] - span['startTimeUnixNano'])
        columns['Start Time'].append(pd.to_datetime(span['startTimeUnixNano']))
        columns['End Time'].append(pd.to_datetime(span['endTimeUnixNano']))
        columns['Attributes'].append(_format_attributes(span['attributes']))

    # if a span name occurs only once, remove the "#1" from its unique name
    for (i, name) in enumerate(columns['Name']):
        if name_count[name] == 1:
            columns['Unique Name'][i] = name

    df = pd.DataFrame(columns)

    # By default, show all columns in hover text.
    # Omit a column by setting false. You can also set special formatting rules here.
    hover_data = {col: True for col in columns.keys()}
    hover_data['Name'] = False  # already shown
    hover_data['Unique Name'] = False  # already shown
    hover_data['End Time'] = False  # who cares

    fig = px.timeline(
        data_frame=df,
        x_start='Start Time',
        x_end='End Time',
        y='Unique Name',
        hover_data=hover_data,
        color='Name',  # spans with same original name get same color
    )

    fig.update_layout(
        title="All Benchmark Spans",
        xaxis_title="Time",
        yaxis_title="Span Name",
        hovermode='y unified',  # show hover if mouse anywhere in row
        yaxis=dict(
            autorange='reversed',  # root span at top
        ),
    )

    return fig


def _sort_spans_by_hierarchy(spans):
    # map from ID to span
    id_to_span = {}
    # map from parent ID to to child span IDs
    parent_to_child_ids = defaultdict(list)
    for span in spans:
        id = span['spanId']
        id_to_span[id] = span

        parent_id = span['parentSpanId']
        parent_to_child_ids[parent_id].append(id)

    # sort spans in depth-first order, by crawling the parent/child tree starting at root
    sorted_spans = []
    # ids_to_process is FIFO
    # With each loop, we pop the last item in ids_to_process
    # and then append its children, so that we process them next.
    ids_to_process = ['0000000000000000']
    while ids_to_process:
        id = ids_to_process.pop(-1)
        if id in parent_to_child_ids:
            child_ids = parent_to_child_ids[id]
            # sorted by start time, but reversed because we pop from the BACK of ids_to_process
            child_ids = sorted(
                child_ids, key=lambda x: id_to_span[x]['startTimeUnixNano'], reverse=True)
            ids_to_process.extend(child_ids)

        if id in id_to_span:
            sorted_spans.append(id_to_span.pop(id))

    # we popped spans as we processed them, check if any are left
    if id_to_span:
        num_leftover = len(id_to_span)
        print(f"WARNING: {
              num_leftover} spans not shown due to missing parents")

    return sorted_spans


# Helper function to format attributes for hover text
# Format attributes for hover text. Transform from JSON like:
#   [
#     {"key": "code.namespace", {"value": {"stringValue": "s3_benchrunner_rust::transfer_manager"}},
#     {"key": "code.lineno", "value": {"intValue": 136}}
#   ]
# To string like:
#       code.namespace: s3_benchrunner_rust::transfer_manager
#       code.lineno: 136
def _format_attributes(attributes):
    formatted_attributes = []
    for attr in attributes:
        key = attr['key']
        # Extract the value regardless of type
        value = list(attr['value'].values())[0]

        # trim down long filepaths by omitting everything before "src/"
        if key == 'code.filepath':
            src_idx = value.find("src/")
            if src_idx > 0:
                value = value[src_idx:]

        formatted_attributes.append(f"<br>   {key}: {value}")

    return formatted_attributes
