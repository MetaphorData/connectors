from dataclasses import field
from dataclasses import field as dataclass_field
from typing import Optional, Set

from pydantic import root_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.utils import must_set_exactly_one

DEFAULT_QUERY_LOG_FETCH_SIZE = 1000


@dataclass
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


@dataclass
class BigQueryQueryLogConfig:

    # Number of days back of query logs to fetch, if 0, don't fetch query logs
    lookback_days: int = 1

    # Query log filter to exclude certain usernames
    excluded_usernames: Set[str] = field(default_factory=lambda: set())

    # Exclude queries issued by service accounts
    exclude_service_accounts: bool = True

    # The number of query logs to fetch from BigQuery in one batch
    fetch_size: int = DEFAULT_QUERY_LOG_FETCH_SIZE


@dataclass
class BigQueryRunConfig(BaseConfig):
    # Path to service account's JSON key file
    key_path: Optional[str] = None

    # The credentials from the BigQuery JSON key file
    credentials: Optional[BigQueryCredentials] = None

    # Project ID to use. Use the service account's default project if not set
    project_id: Optional[str] = None

    # Use a different project ID to run BigQuery jobs if set
    job_project_id: Optional[str] = None

    # Max number of concurrent requests to bigquery or logging API, default is 5
    max_concurrency: int = 5

    # Include or exclude specific databases/schemas/tables
    filter: DatasetFilter = dataclass_field(default_factory=lambda: DatasetFilter())

    # configs for fetching query logs
    query_log: BigQueryQueryLogConfig = BigQueryQueryLogConfig()

    @root_validator
    def have_key_path_or_credentials(cls, values):
        must_set_exactly_one(values, ["key_path", "credentials"])
        return values
