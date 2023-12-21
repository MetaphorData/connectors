from pyhive import hive
import json
from dataclasses import asdict, dataclass
from typing import Collection, Dict, List, Optional, Type

from requests.auth import HTTPBasicAuth

from metaphor.common.api_request import ApiError, get_request
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import (
    dataset_normalized_name,
    to_dataset_entity_id_from_logical_id,
    to_pipeline_entity_id,
)
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.snowflake import normalize_snowflake_account
from metaphor.common.utils import safe_float
)
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    EntityUpstream,
    FieldMapping,
    FiveTranConnectorStatus,
    FivetranPipeline,
    Pipeline,
    PipelineInfo,
    PipelineLogicalID,
    PipelineMapping,
    PipelineType,
    SourceField,
)

from metaphor.hive.config import HiveRunConfig

logger = get_logger()

PLATFORM_MAPPING = {
    "azure_sql_data_warehouse": DataPlatform.SYNAPSE,
    "big_query": DataPlatform.BIGQUERY,
    "mysql_warehouse": DataPlatform.MYSQL,
    "mysql_rds_warehouse": DataPlatform.MYSQL,
    "aurora_warehouse": DataPlatform.MYSQL,
    "postgres_warehouse": DataPlatform.POSTGRESQL,
    "postgres_rds_warehouse": DataPlatform.POSTGRESQL,
    "aurora_postgres_warehouse": DataPlatform.POSTGRESQL,
    "postgres_gcp_warehouse": DataPlatform.POSTGRESQL,
    "redshift": DataPlatform.REDSHIFT,
    "snowflake": DataPlatform.SNOWFLAKE,
}

SOURCE_PLATFORM_MAPPING = {
    "documentdb": DataPlatform.DOCUMENTDB,
    "aurora": DataPlatform.MYSQL,
    "aurora_postgres": DataPlatform.POSTGRESQL,
    "maria_azure": DataPlatform.MYSQL,
    "mysql_azure": DataPlatform.MYSQL,
    "azure_postgres": DataPlatform.POSTGRESQL,
    "azure_sql_db": DataPlatform.MSSQL,
    "google_cloud_mysql": DataPlatform.MYSQL,
    "google_cloud_postgresql": DataPlatform.POSTGRESQL,
    "google_cloud_sqlserver": DataPlatform.MSSQL,
    "heroku_postgres": DataPlatform.POSTGRESQL,
    "sql_server_hva": DataPlatform.MSSQL,
    "magento_mysql": DataPlatform.MYSQL,
    "magento_mysql_rds": DataPlatform.MYSQL,
    "maria": DataPlatform.MYSQL,
    "maria_rds": DataPlatform.MYSQL,
    "mysql": DataPlatform.MYSQL,
    "mysql_rds": DataPlatform.MYSQL,
    "postgres": DataPlatform.POSTGRESQL,
    "postgres_rds": DataPlatform.POSTGRESQL,
    "sql_server": DataPlatform.MSSQL,
    "sql_server_rds": DataPlatform.MSSQL,
    "snowflake_db": DataPlatform.SNOWFLAKE,
}


@dataclass
class ColumnMetadata:
    name_in_source: str
    name_in_destination: str
    type_in_source: Optional[str]
    type_in_destination: Optional[str]
    is_primary_key: bool
    is_foreign_key: bool


@dataclass
class TableMetadata:
    name_in_source: str
    name_in_destination: str
    columns: List[ColumnMetadata]


@dataclass
class SchemaMetadata:
    # name_in_source could be null
    name_in_source: Optional[str]

    name_in_destination: str
    tables: List[TableMetadata]


class HiveExtractor(BaseExtractor):
    """Hive metadata extractor"""

    _description = "Hive metadata crawler"
    _platform = Platform.HIVE

    @staticmethod
    def from_config_file(config_file: str) -> "HiveExtractor":
        return HiveExtractor(HiveRunConfig.from_yaml_file(config_file))

    def __init__(self, config: HiveRunConfig) -> None:
        super().__init__(config)
        self._conn = hive.

    async def extract(self) -> Collection[ENTITY_TYPES]:
        entities: List[ENTITY_TYPES] = []
        return entities