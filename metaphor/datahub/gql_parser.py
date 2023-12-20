from typing import Dict, List, Optional

from gql import Client, gql
from pydantic import BaseModel

from metaphor.common.entity_id import normalize_full_dataset_name, to_person_entity_id
from metaphor.common.logger import get_logger
from metaphor.datahub.config import DatahubConfig
from metaphor.models.metadata_change_event import DataPlatform as MetaphorDataPlatform
from metaphor.models.metadata_change_event import Dataset as MetaphorDataset
from metaphor.models.metadata_change_event import (
    DatasetDocumentation,
    DatasetLogicalID,
    DatasetSchema,
    EntityType,
    FieldDocumentation,
)
from metaphor.models.metadata_change_event import Ownership as MetaphorOwnership
from metaphor.models.metadata_change_event import OwnershipAssignment
from metaphor.models.metadata_change_event import SchemaField as MetaphorSchemaField

logger = get_logger()


DATAHUB_PLATFORM_MAPPING: Dict[str, MetaphorDataPlatform] = {
    "adlsGen1": MetaphorDataPlatform.AZURE_DATA_LAKE_STORAGE,
    "adlsGen2": MetaphorDataPlatform.AZURE_DATA_LAKE_STORAGE,
    "external": MetaphorDataPlatform.EXTERNAL,
    "hive": MetaphorDataPlatform.HIVE,
    "s3": MetaphorDataPlatform.S3,
    "kafka": MetaphorDataPlatform.KAFKA,
    "kafka-connect": MetaphorDataPlatform.KAFKA,
    "mariadb": MetaphorDataPlatform.MYSQL,
    "mongodb": MetaphorDataPlatform.DOCUMENTDB,
    "mysql": MetaphorDataPlatform.MYSQL,
    "postgres": MetaphorDataPlatform.POSTGRESQL,
    "snowflake": MetaphorDataPlatform.SNOWFLAKE,
    "redshift": MetaphorDataPlatform.REDSHIFT,
    "mssql": MetaphorDataPlatform.MSSQL,
    "bigquery": MetaphorDataPlatform.BIGQUERY,
    "glue": MetaphorDataPlatform.GLUE,
    "elasticsearch": MetaphorDataPlatform.ELASTICSEARCH,
    "trino": MetaphorDataPlatform.TRINO,
    "databricks": MetaphorDataPlatform.UNITY_CATALOG,
    "gcs": MetaphorDataPlatform.GCS,
    "dynamodb": MetaphorDataPlatform.DYNAMODB,
    "delta-lake": MetaphorDataPlatform.UNITY_CATALOG,
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
    def meta_ownership(self) -> Optional[MetaphorOwnership]:
        contact_designation_name = None
        if self.ownershipType and self.ownershipType.info:
            contact_designation_name = self.ownershipType.info.name

        person = None
        if self.owner.properties and self.owner.properties.email:
            person = str(to_person_entity_id(self.owner.properties.email))

        if not person and not contact_designation_name:
            return None

        return MetaphorOwnership(
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
    def schema_tags(self) -> List[str]:
        return [tag.tag.properties.name for tag in self.tags if tag.tag.properties]


class SchemaField(BaseModel):
    fieldPath: str
    description: Optional[str]
    tags: Optional[GlobalTags]

    @property
    def field_documentation(self):
        if not self.description:
            return None
        return FieldDocumentation(
            field_path=self.fieldPath,
            documentation=self.description,
        )

    def as_field(self) -> Optional[MetaphorSchemaField]:
        if not self.tags and not self.description:
            return None
        return MetaphorSchemaField(
            description=self.description,
            tags=None if not self.tags else self.tags.schema_tags,
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
            self.platform.name, MetaphorDataPlatform.UNKNOWN
        )
        if meta_platform is MetaphorDataPlatform.UNKNOWN:
            logger.warning(
                f"Found unknown data platform {self.platform.name}, will not ingest dataset {name}"
            )

        return DatasetLogicalID(
            account=config.get_account(meta_platform),
            name=name,
            platform=meta_platform,
        )

    @property
    def dataset_documentation(self) -> Optional[DatasetDocumentation]:
        dataset_documentations = None
        if self.properties and self.properties.description:
            dataset_documentations = [self.properties.description]

        # XXX: Datahub bug, graphql api does not return any column description.
        field_documentations = None
        if self.schemaMetadata:
            raw_field_docs = [f.field_documentation for f in self.schemaMetadata.fields]
            filtered_field_docs = [x for x in raw_field_docs if x]
            if len(filtered_field_docs):
                field_documentations = filtered_field_docs

        if not dataset_documentations and not field_documentations:
            return None
        return DatasetDocumentation(
            dataset_documentations=dataset_documentations,
            field_documentations=field_documentations,
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
    def dataset_schema(self) -> Optional[DatasetSchema]:
        tags = None
        if self.tags:
            tags = self.tags.schema_tags

        fields = None
        if self.schemaMetadata:
            raw_fields = [field.as_field() for field in self.schemaMetadata.fields]
            filtered_fields = [field for field in raw_fields if field]
            if filtered_fields:
                fields = filtered_fields

        description = None
        if self.properties and self.properties.description:
            description = self.properties.description

        if not tags and not fields and not description:
            return None

        return DatasetSchema(
            fields=fields,
            tags=tags,
            description=description,
        )

    def has_additional_information(self) -> bool:
        return (
            self.ownership_assignment is not None
            or self.dataset_documentation is not None
            or self.dataset_schema is not None
        )

    def as_metaphor_dataset(self, config: DatahubConfig) -> MetaphorDataset:
        logical_id = self.get_logical_id(config)
        return MetaphorDataset(
            entity_type=EntityType.DATASET,
            logical_id=logical_id,
            ownership_assignment=self.ownership_assignment,
            documentation=self.dataset_documentation,
            schema=self.dataset_schema,
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
                    }
                }
            }
        }
    """
    )
    response = client.execute(query, variable_values={"urn": urn})
    return Dataset.model_validate(response["dataset"])
