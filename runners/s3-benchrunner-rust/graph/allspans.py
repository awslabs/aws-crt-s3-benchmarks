from collections import defaultdict
import pandas as pd  # type: ignore
import plotly.express as px  # type: ignore


def draw(data):
    # gather all spans into a single list
    spans = []
    for resource_span in data['resourceSpans']:
        for scope_span in resource_span['scopeSpans']:
            spans.extend(scope_span['spans'])

    # simplify attributes of each span to be simple dict
    for span in spans:
        span['attributes'] = _simplify_attributes(span['attributes'])

    # sort spans according to parent-child hierarchy
    spans = _sort_spans_by_hierarchy(spans)

    # prepare columns for plotly
    columns = defaultdict(list)
    name_count = defaultdict(int)
    for (idx, span) in enumerate(spans):

        name = span['name']
        # we want each span in its own row, so assign a unique name and use that as Y value
        # TODO: improve unique name, using "seq" or "part-num"
        name_count[name] += 1
        unique_name = f"{name}#{name_count[name]}"

        columns['Name'].append(name)
        columns['Unique Name'].append(unique_name)
        columns['Duration (ns)'].append(
            span['endTimeUnixNano'] - span['startTimeUnixNano'])
        columns['Start Time'].append(pd.to_datetime(span['startTimeUnixNano']))
        columns['End Time'].append(pd.to_datetime(span['endTimeUnixNano']))
        columns['Index'].append(idx)
        columns['Span ID'].append(span['spanId'])
        columns['Parent ID'].append(span['parentSpanId'])
        columns['Attributes'].append(
            [f"<br>  {k}: {v}" for (k, v) in span['attributes'].items()])

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
            sorted_spans.append(id_to_span[id])

    # warn if any spans are missing
    if (num_leftover := len(spans) - len(sorted_spans)):
        print(f"WARNING: {num_leftover} spans not shown (missing parents)")

    return sorted_spans


# Transform attributes from like:
#   [
#     {"key": "code.namespace", "value": {"stringValue": "s3_benchrunner_rust::transfer_manager"}},
#     {"key": "code.lineno", "value": {"intValue": 136}}
#   ]
# To like:
#   {
#     "code.namespace": "s3_benchrunner_rust::transfer_manager",
#     "code.lineno": 136,
#   }
def _simplify_attributes(attributes_list):
    simple_dict = {}
    for attr in attributes_list:
        key = attr['key']
        # extract actual value, ignoring value's key which looks like "intValue"
        value = next(iter(attr['value'].values()))

        # trim down long filepaths by omitting everything before "src/"
        if key == 'code.filepath':
            if (src_idx := value.find("src/")) > 0:
                value = value[src_idx:]

        simple_dict[key] = value

    return simple_dict
