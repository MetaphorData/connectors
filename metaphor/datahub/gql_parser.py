from typing import Dict, List, Optional

from gql import Client, gql
from pydantic import BaseModel

from metaphor.common.entity_id import normalize_full_dataset_name, to_person_entity_id
from metaphor.common.logger import get_logger
from metaphor.datahub.config import DatahubConfig
from metaphor.models.metadata_change_event import (
    AssetDescription,
    ColumnDescriptionAssignment,
    ColumnTagAssignment,
)
from metaphor.models.metadata_change_event import DataPlatform as MetaphorDataPlatform
from metaphor.models.metadata_change_event import Dataset as MetaphorDataset
from metaphor.models.metadata_change_event import (
    DatasetLogicalID,
    DescriptionAssignment,
    EntityType,
)
from metaphor.models.metadata_change_event import Ownership as MetaphorOwnership
from metaphor.models.metadata_change_event import OwnershipAssignment, TagAssignment

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
    def metaphor_ownership(self) -> Optional[MetaphorOwnership]:
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
    def tag_names(self) -> List[str]:
        return [tag.tag.properties.name for tag in self.tags if tag.tag.properties]


class SchemaField(BaseModel):
    fieldPath: str
    description: Optional[str]
    tags: Optional[GlobalTags]

    def column_description_assignment(self, author: str):
        if not self.description:
            return None
        return ColumnDescriptionAssignment(
            column_name=self.fieldPath,
            asset_descriptions=[
                AssetDescription(author=author, description=self.description)
            ],
        )

    def column_tag_assignment(self):
        if not self.tags:
            return None
        return ColumnTagAssignment(
            column_name=self.fieldPath,
            tag_names=self.tags.tag_names,
        )


class SchemaMetadata(BaseModel):
    fields: List[SchemaField]


class EditableSchemaMetadata(BaseModel):
    editableSchemaFieldInfo: List[SchemaField]


class Dataset(BaseModel):
    properties: Optional[DatasetProperties]
    editableProperties: Optional[DatasetProperties]
    platform: DataPlatform
    name: str
    tags: Optional[GlobalTags]
    ownership: Optional[Ownership]
    schemaMetadata: Optional[SchemaMetadata]
    editableSchemaMetadata: Optional[EditableSchemaMetadata]

    def get_logical_id(self, config: DatahubConfig) -> DatasetLogicalID:
        # It's possible that we want to split the name by the platform delimiters to get part names.
        name = normalize_full_dataset_name(self.name)

        metaphor_platform = DATAHUB_PLATFORM_MAPPING.get(
            self.platform.name, MetaphorDataPlatform.UNKNOWN
        )
        if metaphor_platform is MetaphorDataPlatform.UNKNOWN:
            logger.warning(
                f"Found unknown data platform {self.platform.name}, will not ingest dataset {name}"
            )

        return DatasetLogicalID(
            account=config.get_account(metaphor_platform),
            name=name,
            platform=metaphor_platform,
        )

    def get_schema_fields(self) -> Optional[List[SchemaField]]:
        if self.editableSchemaMetadata:
            return self.editableSchemaMetadata.editableSchemaFieldInfo
        if self.schemaMetadata:
            return self.schemaMetadata.fields
        return None

    def description_assignment(self, author: str) -> Optional[DescriptionAssignment]:
        asset_descriptions = None
        if self.editableProperties and self.editableProperties.description:
            asset_descriptions = [
                AssetDescription(
                    author=author, description=self.editableProperties.description
                )
            ]
        elif self.properties and self.properties.description:
            asset_descriptions = [
                AssetDescription(author=author, description=self.properties.description)
            ]

        column_descriptions = None
        schema_fields = self.get_schema_fields()
        if schema_fields:
            raw_col_descriptions = [
                f.column_description_assignment(author) for f in schema_fields
            ]
            filtered_col_descriptions = [x for x in raw_col_descriptions if x]
            if len(filtered_col_descriptions):
                column_descriptions = filtered_col_descriptions

        if not asset_descriptions and not column_descriptions:
            return None
        return DescriptionAssignment(
            asset_descriptions=asset_descriptions,
            column_description_assignments=column_descriptions,
        )

    def ownership_assignment(self) -> Optional[OwnershipAssignment]:
        if not self.ownership or not self.ownership.owners:
            return None
        raw_owners = [owner.metaphor_ownership for owner in self.ownership.owners]
        filtered_owners = [x for x in raw_owners if x]
        if not filtered_owners:
            return None
        return OwnershipAssignment(
            ownerships=filtered_owners,
        )

    def tag_assignment(self) -> Optional[TagAssignment]:
        tag_names = None
        if self.tags:
            tag_names = self.tags.tag_names
        column_tag_assignments = None
        schema_fields = self.get_schema_fields()
        if schema_fields:
            raw_column_tag_assignments = [
                field.column_tag_assignment() for field in schema_fields
            ]
            filtered_column_tag_assignments = [
                x for x in raw_column_tag_assignments if x
            ]
            if filtered_column_tag_assignments:
                column_tag_assignments = filtered_column_tag_assignments
        if not tag_names and not column_tag_assignments:
            return None
        return TagAssignment(
            tag_names=tag_names, column_tag_assignments=column_tag_assignments
        )

    def as_metaphor_dataset(self, config: DatahubConfig) -> MetaphorDataset:
        logical_id = self.get_logical_id(config)
        ownership_assignment = self.ownership_assignment()
        if config.description_author_email:
            author = str(to_person_entity_id(config.description_author_email))
        elif (
            ownership_assignment
            and ownership_assignment.ownerships
            and ownership_assignment.ownerships[0].person
        ):
            # Use the first owner as our author, datahub does not keep track of description authors
            author = ownership_assignment.ownerships[0].person
        else:
            # Have to use a placeholder email
            author = str(to_person_entity_id("admin@metaphor.io"))

        return MetaphorDataset(
            entity_type=EntityType.DATASET,
            logical_id=logical_id,
            ownership_assignment=self.ownership_assignment(),
            description_assignment=self.description_assignment(author),
            tag_assignment=self.tag_assignment(),
        )


def get_dataset(client: Client, urn: str) -> Dataset:
    query = gql(
        """
        query getDatasetInfo ($urn: String!) {
            dataset (urn: $urn) {
                properties {
                    description
                }
                editableProperties {
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
                editableSchemaMetadata {
                    editableSchemaFieldInfo {
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
