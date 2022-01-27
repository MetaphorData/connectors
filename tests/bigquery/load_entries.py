from google.cloud._helpers import _rfc3339_nanos_to_datetime
from google.cloud.logging import Resource
from google.cloud.logging_v2 import ProtobufEntry

from tests.test_utils import load_json


def load_entries(path):
    log_resources = load_json(path)
    log_entries = []

    for resource in log_resources:
        entry = ProtobufEntry(
            log_name=resource["logName"],
            payload=resource["protoPayload"],
            resource=Resource(
                type=resource["resource"]["type"], labels=resource["resource"]["labels"]
            ),
            insert_id=resource["insertId"],
            severity=resource["severity"],
        )
        entry.received_timestamp = _rfc3339_nanos_to_datetime(resource["timestamp"])
        log_entries.append(entry)

    return log_entries
