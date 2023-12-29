import pyarrow
import pyarrow.csv as pv
import pyarrow.json as pj
import pyarrow.parquet as pq
from fastavro import reader
from smart_open import open

from metaphor.models.metadata_change_event import DatasetSchema, SchemaField, SchemaType
from metaphor.s3.config import S3RunConfig
from metaphor.s3.table_data import TableData


def _parse_json(source) -> DatasetSchema:
    table: pyarrow.Table = pj.read_json(source)
    fields = [
        SchemaField(
            field_path=column._name,
            native_type=str(column.type),
        )
        for column in table.columns
    ]

    return DatasetSchema(schema_type=SchemaType.JSON, fields=fields)


def _parse_avro(source) -> DatasetSchema:
    fields = []
    avro_reader = reader(source)
    if isinstance(avro_reader.writer_schema, dict):
        fields = [
            SchemaField(field_path=field["name"], native_type=field["type"])
            for field in avro_reader.writer_schema.get("fields", [])
        ]
    return DatasetSchema(schema_type=SchemaType.AVRO, fields=fields)


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

        if suffix == ".json":
            return _parse_json(source)

        if suffix == ".avro":
            return _parse_avro(source)

        if suffix == ".parquet":
            return _parse_parquet(source)

    assert False, f"Unknown suffix: {suffix}"
