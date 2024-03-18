from typing import Any, Collection, Dict, List, Literal, Optional

from metaphor.alation.client import Client
from metaphor.alation.config import AlationConfig
from metaphor.alation.schema import Column, Datasource, Schema, Steward, Table
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.entity_id import dataset_normalized_name
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import (
    ColumnTagAssignment,
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    EntityType,
    Ownership,
    OwnershipAssignment,
    TagAssignment,
)

logger = get_logger()


class AlationExtractor(BaseExtractor):
    """Alation metadata extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "AlationExtractor":
        return AlationExtractor(AlationConfig.from_yaml_file(config_file))

    _description = "Alation metadata connector"
    _platform = None

    def __init__(self, config: AlationConfig) -> None:
        super().__init__(config)
        self._config = config
        self._client = Client(config.url, {"Token": config.token})
        self._entities: List[ENTITY_TYPES] = []
        self._schemas: Dict[int, str] = {}  # schema_id to name
        self._datasources: Dict[int, Datasource] = {}  # ds_id to datasource

    def _get_schema(self, schema_id: int) -> str:
        if schema_id not in self._schemas:
            schema_obj = next(
                self._client.get("integration/v2/schema/", {"id": schema_id}), None
            )
            if not schema_obj:
                raise ValueError(f"No schema found with id = {schema_id}")
            schema = Schema.model_validate(schema_obj)
            self._schemas[schema_id] = schema.qualified_name

        return self._schemas[schema_id]

    def _get_datasource(self, ds_id: int):
        if ds_id not in self._datasources:
            ds_obj = next(self._client.get(f"integration/v1/datasource/{ds_id}/"), None)
            if not ds_obj:
                if (
                    next(self._client.get(f"integration/v2/datasource/{ds_id}/"), None)
                    is not None
                ):
                    # TODO: support OCF datasources
                    raise RuntimeError("OCF datasources are currently not supported")

                raise ValueError(f"No datasource found with id = {ds_id}")
            datasource = Datasource.model_validate(ds_obj)
            self._datasources[ds_id] = datasource
        return self._datasources[ds_id]

    def _get_table_columns(self, table_id: int):
        return [
            Column.model_validate(column_obj)
            for column_obj in self._client.get(
                "integration/v2/column/", params={"table_id": table_id}
            )
        ]

    def _get_tags(self, oid: int, otype: Literal["attribute", "table"]):
        return [
            str(tag_obj["name"])
            for tag_obj in self._client.get(
                "integration/tag/", params={"oid": oid, "otype": otype}
            )
        ]

    def _get_tag_assignment(self, table: Table, columns: List[Column]):
        tag_assignment = None
        table_tags = self._get_tags(oid=table.id, otype="table")
        column_tag_assignments: List[ColumnTagAssignment] = []
        for column in columns:
            column_tags = self._get_tags(column.id, otype="attribute")
            if column_tags:
                column_tag_assignments.append(
                    ColumnTagAssignment(
                        column_name=column.name,
                        tag_names=column_tags,
                    )
                )
        if table_tags or column_tag_assignments:
            tag_assignment = TagAssignment(
                column_tag_assignments=(
                    column_tag_assignments if column_tag_assignments else None
                ),
                tag_names=table_tags,
            )
        return tag_assignment

    def _get_ownership_assignment(self, steward: Optional[Steward]):
        if not steward:
            return None

        if steward.otype == "groupprofile":
            path = f"integration/v1/group/{steward.oid}"
        else:
            path = f"integration/v1/user/{steward.oid}"

        owner = next(self._client.get(path), None)
        if not owner:
            raise ValueError(
                f"No owner found for this steward, steward id = {steward.oid}, type =- {steward.otype}"
            )

        return OwnershipAssignment(
            ownerships=[
                Ownership(
                    contact_designation_name=owner["display_name"],
                    person=owner["email"],
                )
            ]
        )

    def _init_dataset(self, table_obj: Any) -> Dataset:
        table = Table.model_validate(table_obj)
        schema = self._get_schema(table.schema_id)
        datasource = self._get_datasource(table.ds_id)
        if datasource.platform == DataPlatform.EXTERNAL:
            raise RuntimeError(f"Unsupported datasource dbtype: {datasource.dbtype}")
        columns = self._get_table_columns(table.id)
        name = dataset_normalized_name(datasource.dbname, schema, table.name)
        dataset = Dataset(
            entity_type=EntityType.DATASET,
            logical_id=DatasetLogicalID(
                account=self._config.get_account(datasource.platform),
                name=name,
                platform=datasource.platform,
            ),
            description_assignment=table.description_assignment(
                self._config.description_author_email, columns
            ),
            tag_assignment=self._get_tag_assignment(table, columns),
            ownership_assignment=self._get_ownership_assignment(table.steward),
        )
        return dataset

    async def extract(self) -> Collection[ENTITY_TYPES]:
        for table_obj in self._client.get("integration/v2/table/", {"limit": 100}):
            try:
                self._entities.append(self._init_dataset(table_obj))
            except Exception:
                logger.exception(f"Cannot extract table, obj = {table_obj}")

        return self._entities
