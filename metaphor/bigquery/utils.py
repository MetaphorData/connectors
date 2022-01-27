import json
from dataclasses import dataclass
from typing import Any, Optional

from smart_open import open

try:
    from google.cloud import logging_v2
    from google.oauth2 import service_account
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise


# ProtobufEntry is a namedtuple and attribute assigned dynamically with different type, mypy fail here
# See: https://googleapis.dev/python/logging/latest/client.html#google.cloud.logging_v2.client.Client.list_entries
#
# from google.cloud.logging_v2 import ProtobufEntry
# LogEntry = ProtobufEntry
LogEntry = Any


@dataclass
class BigQueryResource:
    project_id: str
    dataset_id: str
    table_id: str

    @staticmethod
    def from_str(resource_name: str) -> "BigQueryResource":
        _, project_id, _, dataset_id, _, table_id = resource_name.split("/")
        return BigQueryResource(project_id, dataset_id, table_id)

    def table_name(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"


def build_logging_client(key_path: str, project_id: Optional[str]) -> logging_v2.Client:
    with open(key_path) as fin:
        credentials = service_account.Credentials.from_service_account_info(
            json.load(fin),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        return logging_v2.Client(
            credentials=credentials,
            project=project_id if project_id else credentials.project_id,
        )
