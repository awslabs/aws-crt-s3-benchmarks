import time


class PerfTimer:
    """Context manager that prints how long a `with` statement took"""

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start = time.perf_counter()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            end = time.perf_counter()
            print(f"{self.name}: {end - self.start:.3f} sec")


def clean_traces_data(data):
    _simplify_attributes_recursive(data)


# Recurse through contents of json data, replacing all "attributes" lists with simple dicts
def _simplify_attributes_recursive(data):
    if isinstance(data, dict):
        for (k, v) in data.items():
            if k == 'attributes' and type(v) == list:
                data[k] = _simplify_attributes(v)
            else:
                _simplify_attributes_recursive(v)
    elif isinstance(data, list):
        for i in data:
            _simplify_attributes_recursive(i)


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
