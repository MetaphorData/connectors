from typing import Dict, List, Optional, Tuple

from pydantic.dataclasses import Field, dataclass

from metaphor.common.entity_id import (
    parts_to_dataset_entity_id,
    to_entity_id_from_virtual_view_logical_id,
)
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.common.utils import unique_list
from metaphor.models.metadata_change_event import (
    EntityUpstream,
    FieldMapping,
    SourceField,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewQuery,
    VirtualViewSchema,
    VirtualViewSchemaField,
    VirtualViewType,
)
from metaphor.quick_sight.data_source_utils import (
    DATA_SOURCE_PLATFORM_MAP,
    get_account,
    get_database,
)
from metaphor.quick_sight.models import (
    DataSet,
    DataSetColumn,
    DataSetLogicalTable,
    DataSetPhysicalTable,
    DataSource,
    ResourceType,
    TransformOperation,
    TypeCustomSql,
    TypeRelationalTable,
)


@dataclass
class ColumnReference:
    upstream_id: Optional[str]
    name: str


@dataclass
class Column:
    output: DataSetColumn
    upstream: List[ColumnReference] = Field(default_factory=list)
    expression: Optional[str] = None


TypeColumnMap = Dict[str, Column]


def _get_source_from_custom_sql(
    resources: Dict[str, ResourceType],
    custom_sql: TypeCustomSql,
) -> Tuple[Optional[List[str]], Optional[VirtualViewQuery]]:
    data_source = resources.get(custom_sql.DataSourceArn)

    if (
        data_source is None
        or not isinstance(data_source, DataSource)
        or data_source.Type is None
    ):
        return None, None

    data_platform = DATA_SOURCE_PLATFORM_MAP.get(data_source.Type)
    if not data_platform:
        return None, None

    database = get_database(data_source)
    account = get_account(data_source)
    query = custom_sql.SqlQuery

    tll = extract_table_level_lineage(
        query,
        platform=data_platform,
        account=account,
        query_id="",
        default_database=database,
    )

    return unique_list([source.id for source in tll.sources]), VirtualViewQuery(
        default_database=database,
        query=custom_sql.SqlQuery,
        source_dataset_account=account,
        source_platform=data_platform,
    )


def _process_transformation(
    columns: TypeColumnMap,
    transformations: List[TransformOperation],
) -> None:
    for transformation in transformations:
        if transformation.CreateColumnsOperation:
            for column in transformation.CreateColumnsOperation.Columns:
                columns[column.ColumnName] = Column(
                    upstream=[],
                    expression=column.Expression,
                    output=DataSetColumn(Name=column.ColumnName),
                )
        elif transformation.ProjectOperation:
            for key in list(columns.keys()):
                if key not in transformation.ProjectOperation.ProjectedColumns:
                    columns.pop(key)

        elif transformation.RenameColumnOperation:
            before = transformation.RenameColumnOperation.ColumnName
            after = transformation.RenameColumnOperation.NewColumnName
            columns[after] = columns[before]
            columns.pop(before)


class LineageProcessor:
    def __init__(
        self,
        resources: Dict[str, ResourceType],
        virtual_views: Dict[str, VirtualView],
        data_set: DataSet,
    ):
        self._resources = resources
        self._virtual_views = virtual_views
        self._data_set = data_set

    def run(self) -> str:
        if (
            not self._data_set.LogicalTableMap
            or not self._data_set.PhysicalTableMap
            or not self._data_set.OutputColumns
        ):
            return ""

        tables: Dict[str, TypeColumnMap] = {}

        self._process_physical_table_map(tables, self._data_set.PhysicalTableMap)
        output_table_id = self._process_logical_table_map(
            tables, self._data_set.LogicalTableMap
        )

        return output_table_id

    def _get_source_from_relation_table(
        self,
        relation_table: TypeRelationalTable,
    ) -> Optional[str]:
        data_source = self._resources.get(relation_table.DataSourceArn)

        if (
            data_source is None
            or not isinstance(data_source, DataSource)
            or data_source.Type is None
        ):
            return None

        data_platform = DATA_SOURCE_PLATFORM_MAP.get(data_source.Type)
        if not data_platform:
            return None

        database = get_database(data_source)
        account = get_account(data_source)

        source_entity_id = str(
            parts_to_dataset_entity_id(
                platform=data_platform,
                account=account,
                database=relation_table.Catalog or database,
                schema=relation_table.Schema,
                table=relation_table.Name,
            )
        )

        return source_entity_id

    def _init_virtual_view(self, table_id: str) -> VirtualView:
        view = VirtualView(
            logical_id=VirtualViewLogicalID(
                name=table_id,
                type=VirtualViewType.QUICK_SIGHT,
            ),
            is_non_prod=True,
        )

        self._virtual_views[table_id] = view
        return view

    def _process_physical_table_map(
        self,
        tables: Dict[str, TypeColumnMap],
        physical_table_map: Dict[str, DataSetPhysicalTable],
    ) -> None:
        for table_id, physical_table in physical_table_map.items():
            column_lineage: TypeColumnMap = {}

            if physical_table.CustomSql:
                source_entities, query = _get_source_from_custom_sql(
                    self._resources, physical_table.CustomSql
                )

                # CLL of custom sql is not supported
                for column in physical_table.CustomSql.Columns:
                    if column.Name is None:
                        continue
                    column_lineage[column.Name] = Column(output=column)

                view = self._init_virtual_view(table_id)
                view.entity_upstream = extract_virtual_view_upstream(
                    column_lineage, source_entities or []
                )
                view.schema = extract_virtual_view_schema(column_lineage, query)

            elif physical_table.RelationalTable:
                source_dataset_id = self._get_source_from_relation_table(
                    physical_table.RelationalTable
                )

                columns = physical_table.RelationalTable.InputColumns

                for column in columns:
                    if column.Name is None:
                        continue
                    column_lineage[column.Name] = Column(
                        upstream=[
                            ColumnReference(
                                upstream_id=source_dataset_id, name=column.Name
                            )
                        ],
                        output=column,
                    )

                view = self._init_virtual_view(table_id)
                view.entity_upstream = extract_virtual_view_upstream(
                    column_lineage, [source_dataset_id] if source_dataset_id else []
                )
                view.schema = extract_virtual_view_schema(column_lineage)

            elif physical_table.S3Source:
                for column in physical_table.S3Source.InputColumns:
                    if column.Name is None:
                        continue
                    column_lineage[column.Name] = Column(output=column)

                view = self._init_virtual_view(table_id)
                view.schema = extract_virtual_view_schema(column_lineage)

            tables[table_id] = column_lineage

        return None

    @staticmethod
    def _entity_id(arn_or_table_id: str) -> str:
        return str(
            to_entity_id_from_virtual_view_logical_id(
                VirtualViewLogicalID(
                    name=arn_or_table_id, type=VirtualViewType.QUICK_SIGHT
                )
            )
        )

    @staticmethod
    def _replace_upstream_id(
        upstream_table: TypeColumnMap,
        upstream_id: str,
    ):
        column_lineage: TypeColumnMap = {}
        for key, value in upstream_table.items():
            if value.output.Name is None:
                continue
            column_lineage[key] = Column(
                output=value.output,
                upstream=[
                    ColumnReference(upstream_id=upstream_id, name=value.output.Name)
                ],
            )
        return column_lineage

    def _process_logical_table_map(
        self,
        tables: Dict[str, TypeColumnMap],
        logical_table_map: Dict[str, DataSetLogicalTable],
    ) -> str:
        logical_tables = list(logical_table_map.items())
        output_table_id = ""
        column_lineage: Dict[str, Column]

        # Walk through the dependence tree, the last table (root) will be the output table
        while logical_tables:
            unresolved = []

            for table_id, logical_table in logical_tables:
                column_lineage = {}
                source = logical_table.Source
                source_entities: List[str] = []
                if source.DataSetArn:
                    arn = source.DataSetArn
                    upstream_data_set = self._resources.get(arn)
                    if upstream_data_set is None:
                        continue
                    assert isinstance(upstream_data_set, DataSet)

                    upstream_id = self._entity_id(arn)
                    source_entities.append(upstream_id)

                    for column in upstream_data_set.OutputColumns or []:
                        if column.Name is None:
                            continue
                        column_lineage[column.Name] = Column(
                            upstream=[
                                ColumnReference(
                                    upstream_id=upstream_id,
                                    name=column.Name,
                                )
                            ],
                            output=column,
                        )

                elif source.PhysicalTableId:
                    upstream_table = tables.get(source.PhysicalTableId)
                    upstream_id = self._entity_id(source.PhysicalTableId)
                    source_entities.append(upstream_id)

                    if upstream_table:
                        column_lineage.update(
                            **self._replace_upstream_id(upstream_table, upstream_id)
                        )
                    else:
                        assert False, "should not happen"

                elif source.JoinInstruction:
                    left_table_id = source.JoinInstruction.LeftOperand
                    right_table_id = source.JoinInstruction.RightOperand

                    source_entities.append(self._entity_id(left_table_id))
                    source_entities.append(self._entity_id(right_table_id))

                    left_table = tables.get(left_table_id)
                    right_table = tables.get(right_table_id)
                    if left_table and right_table:
                        column_lineage.update(
                            **self._replace_upstream_id(
                                left_table, self._entity_id(left_table_id)
                            )
                        )
                        column_lineage.update(
                            **self._replace_upstream_id(
                                right_table, self._entity_id(right_table_id)
                            )
                        )

                if not column_lineage:
                    unresolved.append((table_id, logical_table))
                    continue

                _process_transformation(
                    column_lineage, logical_table.DataTransforms or []
                )

                view = self._init_virtual_view(table_id)
                view.entity_upstream = extract_virtual_view_upstream(
                    column_lineage, source_entities
                )
                view.schema = extract_virtual_view_schema(column_lineage)

                tables[table_id] = column_lineage

                output_table_id = table_id

            logical_tables = unresolved

        # Return root table_id
        return output_table_id


def extract_virtual_view_schema(
    column_lineage: TypeColumnMap,
    query: Optional[VirtualViewQuery] = None,
) -> Optional[VirtualViewSchema]:
    if not column_lineage:
        return None

    fields: List[VirtualViewSchemaField] = []
    for column in column_lineage.values():
        if column.output.Name is None:
            continue

        fields.append(
            VirtualViewSchemaField(
                field_name=column.output.Name,
                field_path=column.output.Name.lower(),
                description=column.output.Description,
                type=column.output.Type,
                formula=column.expression,
                optional_type=("FORMULA" if column.expression else None),
            )
        )
    return (
        VirtualViewSchema(
            fields=sorted(fields, key=lambda f: f.field_path), query=query
        )
        if fields
        else None
    )


def extract_virtual_view_upstream(
    column_lineage: Dict[str, Column], source_entities: List[str]
) -> EntityUpstream:
    field_mappings: List[FieldMapping] = []
    for column_name, upstream_column in column_lineage.items():
        field_mappings.append(
            FieldMapping(
                destination=column_name.lower(),
                sources=[
                    SourceField(source_entity_id=x.upstream_id, field=x.name.lower())
                    for x in upstream_column.upstream
                ],
            )
        )

    return EntityUpstream(
        source_entities=source_entities if source_entities else None,
        field_mappings=field_mappings if field_mappings else None,
    )
