from typing import Dict, List, Optional, Set, Tuple

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


TypeColumnMap = Dict[str, Column]


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
    tables: Dict[str, TypeColumnMap],
    source_entities: Set[str],
    physical_table_map: Dict[str, DataSetPhysicalTable],
) -> None:
    for table_id, physical_table in physical_table_map.items():
        columns: TypeColumnMap = {}

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

        elif physical_table.RelationalTable:
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

        elif physical_table.S3Source:
            for column in physical_table.S3Source.InputColumns:
                if column.Name is None:
                    continue
                columns[column.Name] = Column()

        tables[table_id] = columns
    return None


def _process_transformation(
    columns: TypeColumnMap,
    transformations: List[TransformOperation],
) -> None:
    for transformation in transformations:
        if transformation.CreateColumnsOperation:
            for column in transformation.CreateColumnsOperation.Columns:
                columns[column.ColumnName] = Column(
                    upstream=[], expression=column.Expression
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


def _process_logical_table_map(
    resources: Dict[str, ResourceType],
    tables: Dict[str, Dict[str, Column]],
    source_entities: Set[str],
    logical_table_map: Dict[str, DataSetLogicalTable],
) -> Dict[str, Column]:
    logical_tables = list(logical_table_map.items())
    columns: Dict[str, Column]

    # Walk through the dependence tree, the last table (root) will be the output table
    while logical_tables:
        unresolved = []
        columns = {}

        for table_id, logical_table in logical_tables:
            source = logical_table.Source
            if source.DataSetArn:
                arn = source.DataSetArn
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

            elif source.PhysicalTableId:
                upstream_table = tables.get(source.PhysicalTableId)
                if upstream_table:
                    columns.update(**upstream_table)
                else:
                    assert False, "should not happen"

            elif source.JoinInstruction:
                left_table_id = source.JoinInstruction.LeftOperand
                right_table_id = source.JoinInstruction.RightOperand

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

    # Return root
    return columns


def process_dataset_lineage(
    resources: Dict[str, ResourceType], data_set: DataSet
) -> Tuple[TypeColumnMap, Set[str]]:
    tables: Dict[str, Dict[str, Column]] = {}
    source_entities: Set[str] = set()

    if (
        not data_set.LogicalTableMap
        or not data_set.PhysicalTableMap
        or not data_set.OutputColumns
    ):
        return {}, source_entities

    _process_physical_table_map(
        resources, tables, source_entities, data_set.PhysicalTableMap
    )

    columns = _process_logical_table_map(
        resources, tables, source_entities, data_set.LogicalTableMap
    )

    return columns, source_entities


def extract_virtual_view_schema(
    data_set: DataSet, columns: TypeColumnMap
) -> Optional[VirtualViewSchema]:
    output_columns = data_set.OutputColumns or []

    if not output_columns:
        return None

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
    return VirtualViewSchema(fields=fields) if fields else None


def extract_virtual_view_upstream(
    columns: Dict[str, Column], source_entities: Set[str]
) -> EntityUpstream:
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

    return EntityUpstream(
        source_entities=sorted(list(source_entities)) if source_entities else None,
        field_mappings=field_mappings if field_mappings else None,
    )
