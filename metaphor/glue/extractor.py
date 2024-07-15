import json
from datetime import datetime
from typing import Any, Collection, Dict, List

import boto3

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.fieldpath import build_schema_field
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.common.utils import unique_list
from metaphor.glue.config import AwsCredentials, GlueRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    CustomMetadata,
    CustomMetadataItem,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStructure,
    MaterializationType,
    SchemaField,
    SchemaType,
    SourceInfo,
    SQLSchema,
)

logger = get_logger()


def create_glue_client(aws: AwsCredentials) -> boto3.client:
    return aws.get_session().client("glue")


class GlueExtractor(BaseExtractor):
    """Glue metadata extractor"""

    BYTES_PER_MEGABYTES = 1024 * 1024
    _description = "Glue metadata crawler"
    _platform = Platform.GLUE

    @staticmethod
    def from_config_file(config_file: str) -> "GlueExtractor":
        return GlueExtractor(GlueRunConfig.from_yaml_file(config_file))

    def __init__(self, config: GlueRunConfig) -> None:
        super().__init__(config)
        self._datasets: Dict[str, Dataset] = {}
        self._aws_config = config.aws

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from Glue")

        self._client = create_glue_client(self._aws_config)
        databases = self._get_databases()

        for database in databases:
            self._get_tables(database)

        return self._datasets.values()

    def _get_databases(self):
        paginator = self._client.get_paginator("get_databases")
        paginator_response = paginator.paginate()

        database_names = []
        for page in paginator_response:
            for database in page["DatabaseList"]:
                database_names.append(database["Name"])
        return unique_list(database_names)

    @staticmethod
    def _get_columns(storageDescriptor: Any) -> List[SchemaField]:
        columns = []
        if storageDescriptor and "Columns" in storageDescriptor:
            for column in storageDescriptor.get("Columns"):
                columns.append(
                    build_schema_field(
                        column.get("Name"), column.get("Type"), column.get("Comment")
                    )
                )
        return columns

    def _get_tables(self, database: str):
        paginator = self._client.get_paginator("get_tables")
        paginator_response = paginator.paginate(DatabaseName=database)

        for page in paginator_response:
            for table in page["TableList"]:
                name = table.get("Name")
                last_updated = table.get("UpdateTime")
                storageDescriptor = table.get("StorageDescriptor")
                columns = self._get_columns(storageDescriptor)
                location = (
                    storageDescriptor.get("Location") if storageDescriptor else None
                )
                table_type = table.get("TableType")
                parameters = table.get("Parameters")
                row_count = parameters.get("numRows") if parameters else None
                description = table.get("Description")

                dataset = self._init_dataset(
                    schema=database,
                    name=name,
                    table_type="TABLE",
                    description=description,
                    row_count=row_count,
                    last_updated=last_updated,
                )

                custom_metadata = []
                if location:
                    custom_metadata.append(
                        CustomMetadataItem(
                            key="location", value=json.dumps({"location": location})
                        )
                    )
                if table_type:
                    custom_metadata.append(
                        CustomMetadataItem(
                            key="glue_table_type",
                            value=json.dumps({"glue_table_type": table_type}),
                        )
                    )
                dataset.custom_metadata = (
                    CustomMetadata(metadata=custom_metadata)
                    if custom_metadata
                    else None
                )

                dataset.schema.fields = columns

    def _init_dataset(
        self,
        schema: str,
        name: str,
        table_type: str,
        description: str,
        row_count: int,
        last_updated: datetime,
    ) -> Dataset:
        normalized_name = dataset_normalized_name(schema=schema, table=name)

        dataset = Dataset()
        dataset.logical_id = DatasetLogicalID(
            platform=DataPlatform.GLUE, name=normalized_name
        )
        dataset.schema = DatasetSchema(
            schema_type=SchemaType.SQL,
            description=description,
            fields=[],
            sql_schema=SQLSchema(
                materialization=(
                    MaterializationType.VIEW
                    if table_type == "VIEW"
                    else (
                        MaterializationType.EXTERNAL
                        if table_type == "EXTERNAL"
                        else MaterializationType.TABLE
                    )
                )
            ),
        )
        dataset.statistics = to_dataset_statistics(rows=row_count)
        dataset.source_info = SourceInfo(last_updated=last_updated)
        dataset.structure = DatasetStructure(schema=schema, table=name)

        self._datasets[normalized_name] = dataset

        return dataset
