from dataclasses import dataclass
from typing import Optional

from serde import deserialize

from metaphor.common.extractor import RunConfig


@deserialize
@dataclass
class DbtRunConfig(RunConfig):
    manifest: str
    catalog: Optional[str] = None

    # the database service account this DBT project is connected to
    account: Optional[str] = None

    # the dbt docs base URL
    docs_base_url: Optional[str] = None

    # the source code URL for the project directory
    project_source_url: Optional[str] = None

    # TODO: support dbt cloud and derive dbt cloud docs URL
