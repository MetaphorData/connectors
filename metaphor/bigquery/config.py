from dataclasses import field as dataclass_field
from typing import Optional

from pydantic import root_validator
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.utils import must_set_exactly_one


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
class BigQueryRunConfig(BaseConfig):
    # Path to service account's JSON key file
    key_path: Optional[str] = None

    # The credentials from the BigQuery JSON key file
    credentials: Optional[BigQueryCredentials] = None

    # Project ID to use. Use the service account's default project if not set
    project_id: Optional[str] = None

    # Max number of concurrent requests to bigquery or logging API, default is 10
    max_concurrency: int = 10

    # Include or exclude specific databases/schemas/tables
    filter: Optional[DatasetFilter] = dataclass_field(
        default_factory=lambda: DatasetFilter()
    )

    @root_validator
    def have_key_path_or_credentials(cls, values):
        must_set_exactly_one(values, ["key_path", "credentials"])
        return values
