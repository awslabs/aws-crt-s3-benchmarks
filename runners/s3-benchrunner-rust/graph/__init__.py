from collections import defaultdict
import time
from typing import Any, Union


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


class Trace:
    """
    Class to hold OTLP TracesData, and help query it.

    Attributes:
        traces_data: JSON representation of OTLP TracesData,
            with the following cleanup applied:
            - span["attributes"] changed into simple dict
            - span["niceName"] added
        spans: list of all Spans from traces_data, sorted by start time
    """

    def __init__(self, json_traces_data: dict):
        self.traces_data = json_traces_data

        # all spans, sorted by start time because that's handy
        self.spans = []
        for resource_span in self.traces_data['resourceSpans']:
            for scope_span in resource_span['scopeSpans']:
                self.spans.extend(scope_span['spans'])
        self.spans.sort(key=lambda x: x['startTimeUnixNano'])

        # do some data cleaning
        for span in self.spans:
            span['attributes'] = Trace._simplify_attributes(span['attributes'])
            span['niceName'] = Trace._nice_name(span)

        self._id_to_span = {x['spanId']: x for x in self.spans}

        self._id_to_child_spans = defaultdict(list)
        for span in self.spans:
            self._id_to_child_spans[span['parentSpanId']].append(span)

    def get_span(self, id: str) -> Union[dict, None]:
        return self._id_to_span.get(id)

    def get_child_spans(self, span_or_id: Union[str, dict]) -> list[dict]:
        if isinstance(span_or_id, str):
            id = span_or_id
        else:
            id = span_or_id['spanId']
        return self._id_to_child_spans[id]

    def get_attribute_in_span_or_parent(self, span, attribute_name, default=None) -> Union[Any, None]:
        """
        Get named attribute from this span, or one of its parents.
        Useful for getting common attributes like "bucket".
        """
        while span is not None:
            if (attribute_val := span['attributes'].get(attribute_name)) is not None:
                return attribute_val
            span = self.get_span(span['parentSpanId'])

        return default

    @staticmethod
    def _simplify_attributes(attributes_list):
        """
        Transform attributes from like:
          [
            {"key": "code.namespace", "value": {
              "stringValue": "s3_benchrunner_rust::transfer_manager"}},
            {"key": "code.lineno", "value": {"intValue": 136}},
          ]
        To like:
          {
            "code.namespace": "s3_benchrunner_rust::transfer_manager",
            "code.lineno": 136,
          }
        """
        simple_dict = {}
        for attr in attributes_list:
            key = attr['key']
            # extract actual value, ignoring value's key which looks like "intValue"
            value = next(iter(attr['value'].values()))

            # trim down long filepaths by omitting everything before "src/"
            if key == 'code.filepath':
                if (src_idx := value.find("src/")) > 0:
                    value = value[src_idx:]

            # trim down excessively long strings
            MAX_STRLEN = 150
            if isinstance(value, str) and len(value) > MAX_STRLEN:
                value = value[:MAX_STRLEN] + "...TRUNCATED"

            simple_dict[key] = value

        return simple_dict

    @staticmethod
    def get_span_attributes_hover_data(span):
        """return span['attributes'] formatted for plotly hover_data"""
        return "".join([f"<br>  {k}={v}" for (k, v) in span['attributes'].items()])

    @staticmethod
    def _nice_name(span):
        name = span['name']
        attributes = span['attributes']
        if (seq := attributes.get('seq')) is not None:
            name += f"[{seq}]"

        if (part_number := attributes.get('part_number')) is not None:
            name += f"#{part_number}"
        return name
