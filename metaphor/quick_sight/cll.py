from typing import Dict, List, Optional, Set

from pydantic.dataclasses import Field, dataclass

from metaphor.common.entity_id import (
    parts_to_dataset_entity_id,
    to_entity_id_from_virtual_view_logical_id,
)
from metaphor.common.sql.table_level_lineage.table_level_lineage import (
    extract_table_level_lineage,
)
from metaphor.models.metadata_change_event import (
    EntityUpstream,
    FieldMapping,
    SourceField,
    VirtualView,
    VirtualViewLogicalID,
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
    upstream: List[ColumnReference] = Field(default_factory=list)
    expression: Optional[str] = None


def _get_source_from_relation_table(
    resources: Dict[str, ResourceType],
    relation_table: TypeRelationalTable,
    source_entities: Set[str],
) -> Optional[str]:
    data_source = resources.get(relation_table.DataSourceArn)

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

    source_entities.add(source_entity_id)
    return source_entity_id


def _get_source_from_custom_sql(
    resources: Dict[str, ResourceType],
    custom_sql: TypeCustomSql,
    source_entities: Set[str],
) -> None:
    data_source = resources.get(custom_sql.DataSourceArn)

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
    query = custom_sql.SqlQuery

    tll = extract_table_level_lineage(
        query,
        platform=data_platform,
        account=account,
        query_id="",
        default_database=database,
    )

    for source in tll.sources:
        source_entities.add(source.id)

    return None


def _process_physical_table_map(
    resources: Dict[str, ResourceType],
    tables: Dict[str, Dict[str, Column]],
    source_entities: Set[str],
    physical_table_map: Dict[str, DataSetPhysicalTable],
) -> None:
    for table_id, physical_table in physical_table_map.items():
        columns: Dict[str, Column] = {}

        if physical_table.CustomSql:
            # Table lineage
            _get_source_from_custom_sql(
                resources, physical_table.CustomSql, source_entities
            )

            # CLL of custom sql is not supported
            for column in physical_table.CustomSql.Columns:
                if column.Name is None:
                    continue
                columns[column.Name] = Column()

        if physical_table.RelationalTable:
            source_dataset_id = _get_source_from_relation_table(
                resources, physical_table.RelationalTable, source_entities
            )

            for column in physical_table.RelationalTable.InputColumns:
                if column.Name is None:
                    continue
                columns[column.Name] = Column(
                    upstream=[
                        ColumnReference(upstream_id=source_dataset_id, name=column.Name)
                    ]
                )

        if physical_table.S3Source:
            for column in physical_table.S3Source.InputColumns:
                if column.Name is None:
                    continue
                columns[column.Name] = Column()

        tables[table_id] = columns
    return None


def _process_transformation(
    columns: Dict[str, Column],
    transformations: List[TransformOperation],
) -> None:
    for transformation in transformations:
        if transformation.CreateColumnsOperation:
            for column in transformation.CreateColumnsOperation.Columns:
                columns[column.ColumnName] = Column(
                    upstream=[], expression=column.Expression
                )
        if transformation.ProjectOperation:
            for key in list(columns.keys()):
                if key not in transformation.ProjectOperation.ProjectedColumns:
                    columns.pop(key)

        if transformation.RenameColumnOperation:
            before = transformation.RenameColumnOperation.ColumnName
            after = transformation.RenameColumnOperation.NewColumnName
            columns[after] = columns[before]
            columns.pop(before)


def _process_logical_table_map(
    resources: Dict[str, ResourceType],
    tables: Dict[str, Dict[str, Column]],
    source_entities: Set[str],
    logical_table_map: Dict[str, DataSetLogicalTable],
) -> Dict[str, Column]:
    logical_tables = list(logical_table_map.items())
    columns: Dict[str, Column]

    while logical_tables:
        unresolved = []
        columns = {}

        for table_id, logical_table in logical_tables:
            if logical_table.Source.DataSetArn:
                arn = logical_table.Source.DataSetArn
                upstream_data_set = resources.get(arn)
                if upstream_data_set is None:
                    continue
                assert isinstance(upstream_data_set, DataSet)

                upstream_id = str(
                    to_entity_id_from_virtual_view_logical_id(
                        VirtualViewLogicalID(name=arn, type=VirtualViewType.QUICK_SIGHT)
                    )
                )
                source_entities.add(upstream_id)
                for column in upstream_data_set.OutputColumns or []:
                    if column.Name is None:
                        continue
                    columns[column.Name] = Column(
                        upstream=[
                            ColumnReference(upstream_id=upstream_id, name=column.Name)
                        ]
                    )

            if logical_table.Source.PhysicalTableId:
                upstream_table = tables.get(logical_table.Source.PhysicalTableId)
                if upstream_table:
                    columns.update(**upstream_table)
                else:
                    assert False, "should not happen"

            if logical_table.Source.JoinInstruction:
                left_table_id = logical_table.Source.JoinInstruction.LeftOperand
                right_table_id = logical_table.Source.JoinInstruction.RightOperand

                left_table = tables.get(left_table_id)
                right_table = tables.get(right_table_id)
                if left_table and right_table:
                    columns.update(**left_table)
                    columns.update(**right_table)

            if not columns:
                unresolved.append((table_id, logical_table))
                continue

            _process_transformation(columns, logical_table.DataTransforms or [])

            tables[table_id] = columns

        logical_tables = unresolved
    return columns


def process_dataset_column_lineage(
    resources: Dict[str, ResourceType], data_set: DataSet, view: VirtualView
):
    if (
        not data_set.LogicalTableMap
        or not data_set.PhysicalTableMap
        or not data_set.OutputColumns
    ):
        return

    tables: Dict[str, Dict[str, Column]] = {}
    source_entities: Set[str] = set()

    _process_physical_table_map(
        resources, tables, source_entities, data_set.PhysicalTableMap
    )

    columns = _process_logical_table_map(
        resources, tables, source_entities, data_set.LogicalTableMap
    )

    output_columns = data_set.OutputColumns or []

    if output_columns:
        fields: List[VirtualViewSchemaField] = []
        for column in output_columns:
            if column.Name is None:
                continue
            reference = columns.get(column.Name)
            fields.append(
                VirtualViewSchemaField(
                    field_name=column.Name,
                    field_path=column.Name.lower(),
                    description=column.Description,
                    type=column.Type,
                    formula=reference.expression if reference else None,
                    optional_type=(
                        "FORMULA" if reference and reference.expression else None
                    ),
                )
            )
        view.schema = VirtualViewSchema(fields=fields)

    field_mappings: List[FieldMapping] = []
    for column_name, upstream_column in columns.items():
        field_mappings.append(
            FieldMapping(
                destination=column_name.lower(),
                sources=[
                    SourceField(source_entity_id=x.upstream_id, field=x.name.lower())
                    for x in upstream_column.upstream
                ],
            )
        )

    view.entity_upstream = EntityUpstream(
        source_entities=sorted(list(source_entities)) if source_entities else None,
        field_mappings=field_mappings if field_mappings else None,
    )
