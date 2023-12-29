import pyarrow
import pyarrow.csv as pv
import pyarrow.json as pj
import pyarrow.parquet as pq
from smart_open import open

from metaphor.models.metadata_change_event import DatasetSchema, SchemaField, SchemaType
from metaphor.s3.config import S3RunConfig
from metaphor.s3.table_data import TableData


def _parse_json_schema(source, suffix: str) -> DatasetSchema:
    if suffix == ".json":
        schema_type = SchemaType.JSON
    elif suffix == ".avro":
        schema_type = SchemaType.AVRO
    else:
        assert False, f"Unknown suffix: {suffix}"

    table: pyarrow.Table = pj.read_json(source)  # TODO: how do we want to parse avro?
    fields = [
        SchemaField(
            field_path=column._name,
            native_type=str(column.type),
        )
        for column in table.columns
    ]

    return DatasetSchema(schema_type=schema_type, fields=fields)


def _parse_schemaless(source, suffix: str) -> DatasetSchema:
    parse_options = pv.ParseOptions()
    if suffix == ".tsv":
        parse_options.delimiter = "\t"

    table: pyarrow.Table = pv.read_csv(source, parse_options=parse_options)
    fields = [
        SchemaField(
            field_path=column._name,
            native_type=str(column.type),
        )
        for column in table.columns
    ]

    return DatasetSchema(schema_type=SchemaType.SCHEMALESS, fields=fields)


def _parse_parquet(source) -> DatasetSchema:
    table = pq.read_table(source)
    fields = [
        SchemaField(
            field_path=column._name,
            native_type=str(column.type),
        )
        for column in table.columns
    ]

    return DatasetSchema(schema_type=SchemaType.PARQUET, fields=fields)


def parse_schema(config: S3RunConfig, table_data: TableData) -> DatasetSchema:
    with open(
        table_data.full_path,
        "rb",
        transport_params={"client": config.s3_client},
    ) as source:
        suffix = table_data.url.suffix
        if suffix in {".csv", ".tsv"}:
            return _parse_schemaless(source, suffix)

        if suffix in {".json", ".avro"}:
            return _parse_json_schema(source, suffix)

        if suffix == ".parquet":
            return _parse_parquet(source)

    assert False, f"Unknown suffix: {suffix}"
