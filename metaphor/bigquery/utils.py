import json
import re
from dataclasses import dataclass
from typing import Any

try:
    import google.cloud.bigquery as bigquery
    from google.cloud import logging_v2
    from google.oauth2 import service_account  # type: ignore[attr-defined]
except ImportError:
    print("Please install metaphor[bigquery] extra\n")
    raise

from smart_open import open

from metaphor.bigquery.config import BigQueryRunConfig

# ProtobufEntry is a namedtuple and attribute assigned dynamically with different type, mypy fail here
# See: https://googleapis.dev/python/logging/latest/client.html#google.cloud.logging_v2.client.Client.list_entries
#
# from google.cloud.logging_v2 import ProtobufEntry
# LogEntry = ProtobufEntry
LogEntry = Any


def build_client(
    project_id: str, credentials: service_account.Credentials
) -> bigquery.Client:
    return bigquery.Client(credentials=credentials, project=project_id)


def build_logging_client(
    project_id: str, credentials: service_account.Credentials
) -> logging_v2.Client:
    return logging_v2.Client(credentials=credentials, project=project_id)


def get_credentials(config: BigQueryRunConfig) -> service_account.Credentials:
    # either "key_path" or "credentials" should be set, otherwise pydantic validator will throw error

    if config.key_path is not None:
        with open(config.key_path) as fin:
            return service_account.Credentials.from_service_account_info(
                json.load(fin),
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

    return service_account.Credentials.from_service_account_info(
        config.credentials.__dict__,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


# Handle yearly, monthly, daily, or hourly partitioning.
# See https://cloud.google.com/bigquery/docs/partitioned-tables.
# This REGEX handles both Partitioned Tables ($ separator) and Sharded Tables (_ separator)
PARTITIONED_TABLE_REGEX = re.compile(
    r"^(.+)[$_](\d{4}|\d{6}|\d{8}|\d{10}|__PARTITIONS_SUMMARY__)$"
)

# Handle table snapshots
# See https://cloud.google.com/bigquery/docs/table-snapshots-intro.
SNAPSHOT_TABLE_REGEX = re.compile(r"^(.+)@(\d{13})$")

# invalid characters in table name except the above partition table and snapshot table
INVALID_TABLE_NAME_CHAR = ["$", "@"]


@dataclass
class BigQueryResource:
    project_id: str
    dataset_id: str
    table_id: str

    def __eq__(self, o: object) -> bool:
        return (
            isinstance(o, BigQueryResource)
            and self.project_id == o.project_id
            and self.dataset_id == o.dataset_id
            and self.table_id == o.table_id
        )

    def __hash__(self) -> int:
        return hash((self.project_id, self.dataset_id, self.table_id))

    @staticmethod
    def from_str(resource_name: str) -> "BigQueryResource":
        (
            projects,
            project_id,
            datasets,
            dataset_id,
            tables,
            table_id,
        ) = resource_name.split("/")
        if projects != "projects" or datasets != "datasets" or tables != "tables":
            raise ValueError(f"invalid BigQuery table reference: {resource_name}")
        return BigQueryResource(project_id, dataset_id, table_id)

    def table_name(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def is_temporary(self) -> bool:
        return self.dataset_id.startswith("_")

    def is_information_schema(self) -> bool:
        return self.table_id.upper().startswith("INFORMATION_SCHEMA.")

    def remove_extras(self) -> "BigQueryResource":
        # Handle partitioned and sharded tables
        matches = PARTITIONED_TABLE_REGEX.match(self.table_id)
        if matches:
            self.table_id = matches.group(1)

        # Handle snapshot tables
        matches = SNAPSHOT_TABLE_REGEX.match(self.table_id)
        if matches:
            self.table_id = matches.group(1)

        # Handle invalid characters
        if any(ch in self.table_id for ch in INVALID_TABLE_NAME_CHAR):
            raise ValueError(f"Invalid table name {self.table_id}")

        return self
