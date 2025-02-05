from dataclasses import field
from typing import List, Optional, Set

from pydantic import model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.sql.process_query.config import ProcessQueryConfig
from metaphor.common.tag_matcher import TagMatcher
from metaphor.common.utils import must_set_exactly_one

# logs "list_entries" page size, max 1000.
# See https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list
DEFAULT_QUERY_LOG_FETCH_SIZE = 1000

# See https://cloud.google.com/logging/quotas
DEFAULT_MAX_REQUESTS_PER_MINUTE = 59


@dataclass(config=ConnectorConfig)
class BigQueryCredentials:
    """Credentials used to connect to BigQuery"""

    # Project ID to use
    project_id: str

    # Private key ID
    private_key_id: str

    # Private key value
    private_key: str

    # Client email contained in the key file
    client_email: str

    # Client ID contained in the key file
    client_id: str

    type: str = "service_account"
    auth_uri: str = "https://accounts.google.com/o/oauth2/auth"
    token_uri: str = "https://oauth2.googleapis.com/token"


@dataclass(config=ConnectorConfig)
class BigQueryQueryLogConfig:
    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # Exclude queries issued by service accounts
    exclude_service_accounts: bool = False

    # The number of query logs to fetch from BigQuery in one batch
    fetch_size: int = DEFAULT_QUERY_LOG_FETCH_SIZE

    # Fetch the full query SQL from job API if it's truncated in the audit metadata log
    fetch_job_query_if_truncated: bool = True

    # Config to control query processing
    process_query: ProcessQueryConfig = field(
        default_factory=lambda: ProcessQueryConfig()
    )

    # Maximum allowed requests per minute to the log entries API
    max_requests_per_minute: int = DEFAULT_MAX_REQUESTS_PER_MINUTE


@dataclass(config=ConnectorConfig)
class BigQueryLineageConfig:
    # Whether to enable parsing view query to find upstream of the view, default True
    enable_view_lineage: bool = True

    # Whether to enable parsing audit log to find table lineage information, default True
    enable_lineage_from_log: bool = True

    # Number of days back in the query log to process
    lookback_days: int = 1

    # Whether to include self loop in lineage
    include_self_lineage: bool = True

    # The number of access logs fetched in a batch, default to 1000
    batch_size: int = 1000


@dataclass(config=ConnectorConfig)
class BigQueryRunConfig(BaseConfig):
    # List of project IDs to extract metadata from
    project_ids: List[str]

    # Path to service account's JSON key file
    key_path: Optional[str] = None

    # The credentials from the BigQuery JSON key file
    credentials: Optional[BigQueryCredentials] = None

    # Use a different project ID to run BigQuery jobs if set
    job_project_id: Optional[str] = None

    # Max number of concurrent requests to bigquery or logging API, default is 5
    max_concurrency: int = 5

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = field(default_factory=lambda: DatasetFilter())

    # How tags should be assigned to datasets
    tag_matchers: List[TagMatcher] = field(default_factory=lambda: [])

    # configs for fetching query logs
    query_log: BigQueryQueryLogConfig = field(
        default_factory=lambda: BigQueryQueryLogConfig()
    )

    # configs for lineage information
    lineage: BigQueryLineageConfig = field(
        default_factory=lambda: BigQueryLineageConfig()
    )

    @model_validator(mode="after")
    def have_key_path_or_credentials(self) -> "BigQueryRunConfig":
        must_set_exactly_one(self.__dict__, ["key_path", "credentials"])
        return self
