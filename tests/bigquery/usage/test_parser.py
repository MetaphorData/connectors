import re

from google.cloud._helpers import _rfc3339_nanos_to_datetime
from google.cloud.logging_v2 import ProtobufEntry

from metaphor.bigquery.usage.extractor import BigQueryUsageExtractor
from metaphor.common.event_util import EventUtil
from tests.test_utils import load_json


def test_parse_access_log(test_root_dir):
    extractor = BigQueryUsageExtractor()
    extractor._datasets_pattern = [re.compile(".*")]

    log_resources = load_json(test_root_dir + "/bigquery/usage/data/sample_log.json")
    log_entries = []

    for resource in log_resources:
        entry = ProtobufEntry(
            log_name=resource["logName"],
            payload=resource["protoPayload"],
            resource=resource["resource"],
            insert_id=resource["insertId"],
            severity=resource["severity"],
        )
        entry.received_timestamp = _rfc3339_nanos_to_datetime(resource["timestamp"])
        log_entries.append(entry)

    for entry in log_entries:
        extractor._parse_log_entry(entry)

    results = {}
    for key, value in extractor._datasets.items():
        results[key] = EventUtil.clean_nones(value.to_dict())

    assert results == load_json(
        test_root_dir + "/bigquery/usage/data/parse_query_log_result.json"
    )
