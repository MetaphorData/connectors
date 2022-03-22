from dataclasses import field as dataclass_field
from typing import Set

from pydantic.dataclasses import dataclass

from metaphor.bigquery.config import BigQueryRunConfig


@dataclass
class BigQueryQueryRunConfig(BigQueryRunConfig):
    # The number of days' logs to fetch
    lookback_days: int = 30

    # Batch size for querying the BigQuery audit log
    batch_size: int = 1000

    # Maximum number of recent queries to capture for each table
    max_queries_per_table: int = 100

    # Exclude queries issued by service accounts
    exclude_service_accounts: bool = True

    # Query filter to exclude certain usernames from the processing
    excluded_usernames: Set[str] = dataclass_field(default_factory=lambda: set())
