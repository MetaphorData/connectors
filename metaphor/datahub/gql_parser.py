from typing import Dict, List, Optional

from gql import Client, gql
from pydantic import BaseModel

from metaphor.common.entity_id import (
    normalize_full_dataset_name,
    to_dataset_entity_id_from_logical_id,
    to_person_entity_id,
)
from metaphor.common.logger import get_logger
from metaphor.datahub.config import DatahubConfig
from metaphor.models.metadata_change_event import (
    AssetDescription,
    ColumnDescriptionAssignment,
)
from metaphor.models.metadata_change_event import DataPlatform as MetaDataPlatform
from metaphor.models.metadata_change_event import Dataset as MetaDataset
from metaphor.models.metadata_change_event import (
    DatasetLogicalID,
    DescriptionAssignment,
    EntityType,
)
from metaphor.models.metadata_change_event import Ownership as MetaOwnership
from metaphor.models.metadata_change_event import (
    OwnershipAssignment,
    SystemTag,
    SystemTags,
    SystemTagSource,
)

logger = get_logger()


DATAHUB_PLATFORM_MAPPING: Dict[str, MetaDataPlatform] = {
    "adlsGen1": MetaDataPlatform.AZURE_DATA_LAKE_STORAGE,
    "adlsGen2": MetaDataPlatform.AZURE_DATA_LAKE_STORAGE,
    "external": MetaDataPlatform.EXTERNAL,
    "hive": MetaDataPlatform.HIVE,
    "s3": MetaDataPlatform.S3,
    "kafka": MetaDataPlatform.KAFKA,
    "kafka-connect": MetaDataPlatform.KAFKA,
    "mariadb": MetaDataPlatform.MYSQL,
    "mongodb": MetaDataPlatform.DOCUMENTDB,
    "mysql": MetaDataPlatform.MYSQL,
    "postgres": MetaDataPlatform.POSTGRESQL,
    "snowflake": MetaDataPlatform.SNOWFLAKE,
    "redshift": MetaDataPlatform.REDSHIFT,
    "mssql": MetaDataPlatform.MSSQL,
    "bigquery": MetaDataPlatform.BIGQUERY,
    "glue": MetaDataPlatform.GLUE,
    "elasticsearch": MetaDataPlatform.ELASTICSEARCH,
    "trino": MetaDataPlatform.TRINO,
    "databricks": MetaDataPlatform.UNITY_CATALOG,
    "gcs": MetaDataPlatform.GCS,
    "dynamodb": MetaDataPlatform.DYNAMODB,
    "delta-lake": MetaDataPlatform.UNITY_CATALOG,
}
"""
Source: https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-service/war/src/main/resources/boot/data_platforms.json

Currently unsupported datahub platforms:
- airflow
- ambry
- clickhouse
- couchbase
- hdfs
- hana
- iceberg
- kusto
- mode
- openapi
- oracle
- pinot
- presto
- tableau
- teradata
- voldemort
- druid
- looker
- feast
- sagemaker
- mlflow
- redash
- athena
- spark
- dbt
- Great Expectations
- powerbi
- presto-on-hive
- metabase
- nifi
- superset
- pulsar
- salesforce
- vertica
- fivetran
- csv
"""


class DatasetProperties(BaseModel):
    description: Optional[str]


class DataPlatformProperties(BaseModel):
    datasetNameDelimiter: str


class DataPlatform(BaseModel):
    name: str
    properties: Optional[DataPlatformProperties]


class OwnerTypeProperties(BaseModel):
    email: Optional[str]


class OwnerType(BaseModel):
    properties: Optional[OwnerTypeProperties]


class OwnershipTypeInfo(BaseModel):
    name: str


class OwnershipTypeEntity(BaseModel):
    info: Optional[OwnershipTypeInfo]


class Owner(BaseModel):
    owner: OwnerType
    ownershipType: Optional[OwnershipTypeEntity]

    @property
    def meta_ownership(self) -> Optional[MetaOwnership]:
        contact_designation_name = None
        if self.ownershipType and self.ownershipType.info:
            contact_designation_name = self.ownershipType.info.name

        person = None
        if self.owner.properties and self.owner.properties.email:
            person = str(to_person_entity_id(self.owner.properties.email))

        if not person and not contact_designation_name:
            return None

        return MetaOwnership(
            contact_designation_name=contact_designation_name,
            person=person,
        )


class Ownership(BaseModel):
    owners: List[Owner]


class TagProperties(BaseModel):
    name: str
    description: Optional[str]


class Tag(BaseModel):
    properties: Optional[TagProperties]


class TagAssociation(BaseModel):
    tag: Tag


class GlobalTags(BaseModel):
    tags: List[TagAssociation]

    @property
    def system_tags(self) -> Optional[SystemTags]:
        if not self.tags:
            return None
        return SystemTags(
            tags=[
                SystemTag(
                    system_tag_source=SystemTagSource.DATAHUB,
                    value=tag.tag.properties.name,
                )
                for tag in self.tags
                if tag.tag.properties
            ]
        )


class SchemaField(BaseModel):
    fieldPath: str
    description: Optional[str]

    def column_description_assignment(self):
        if not self.description:
            return None
        return ColumnDescriptionAssignment(
            asset_descriptions=[AssetDescription(description=self.description)],
            column_name=self.fieldPath,
        )


class SchemaMetadata(BaseModel):
    fields: List[SchemaField]


class Dataset(BaseModel):
    properties: Optional[DatasetProperties]
    platform: DataPlatform
    name: str
    tags: Optional[GlobalTags]
    ownership: Optional[Ownership]
    schemaMetadata: Optional[SchemaMetadata]

    def get_logical_id(self, config: DatahubConfig) -> DatasetLogicalID:
        # It's possible that we want to split the name by the platform delimiters to get part names.
        name = normalize_full_dataset_name(self.name)

        meta_platform = DATAHUB_PLATFORM_MAPPING.get(
            self.platform.name, MetaDataPlatform.UNKNOWN
        )
        if meta_platform is MetaDataPlatform.UNKNOWN:
            logger.warning(
                f"Found unknown data platform for dataset {name}: {self.platform.name}"
            )

        return DatasetLogicalID(
            account=config.get_account(meta_platform),
            name=name,
            platform=meta_platform,
        )

    @property
    def description_assignment(self) -> Optional[DescriptionAssignment]:
        asset_descriptions = None
        if self.properties and self.properties.description:
            asset_descriptions = [AssetDescription(self.properties.description)]

        # XXX: Datahub bug, graphql api does not return any column description.
        column_description_assignments = None
        if self.schemaMetadata:
            raw_column_descs = [
                f.column_description_assignment() for f in self.schemaMetadata.fields
            ]
            filtered_column_descs = [x for x in raw_column_descs if x]
            if len(filtered_column_descs):
                column_description_assignments = filtered_column_descs

        if not asset_descriptions and not column_description_assignments:
            return None
        return DescriptionAssignment(
            asset_descriptions=asset_descriptions,
            column_description_assignments=column_description_assignments,
        )

    @property
    def ownership_assignment(self) -> Optional[OwnershipAssignment]:
        if not self.ownership or not self.ownership.owners:
            return None
        raw_owners = [owner.meta_ownership for owner in self.ownership.owners]
        filtered_owners = [x for x in raw_owners if x]
        if not filtered_owners:
            return None
        return OwnershipAssignment(
            ownerships=filtered_owners,
        )

    @property
    def system_tags(self) -> Optional[SystemTags]:
        if not self.tags:
            return None
        return self.tags.system_tags

    def has_additional_information(self) -> bool:
        return (
            self.ownership_assignment is not None
            or self.description_assignment is not None
            or self.system_tags is not None
        )

    def as_meta_dataset(self, config: DatahubConfig) -> MetaDataset:
        logical_id = self.get_logical_id(config)
        return MetaDataset(
            entity_type=EntityType.DATASET,
            dataset_id=str(to_dataset_entity_id_from_logical_id(logical_id)),
            logical_id=logical_id,
            ownership_assignment=self.ownership_assignment,
            description_assignment=self.description_assignment,
            system_tags=self.system_tags,
        )


def get_dataset(client: Client, urn: str) -> Dataset:
    query = gql(
        """
        query getDatasetInfo ($urn: String!) {
            dataset (urn: $urn) {
                properties {
                    description
                }
                name
                platform {
                    name
                    properties {
                        datasetNameDelimiter
                    }
                }
                tags {
                    tags {
                        tag {
                            properties {
                                name
                                description
                            }
                        }
                    }
                }
                ownership {
                    owners {
                        ownershipType {
                            info {
                                name
                            }
                        }
                        owner {
                        ... on CorpUser {
                            properties {
                                email
                            }
                        }
                        ... on CorpGroup {
                            properties {
                                email
                            }
                        }
                        }
                    }
                }
                schemaMetadata {
                    fields {
                        fieldPath
                        description
                    }
                }
            }
        }
    """
    )
    response = client.execute(query, variable_values={"urn": urn})
    return Dataset.model_validate(response["dataset"])
