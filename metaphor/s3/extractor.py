import re
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Collection, Dict, Iterable, List, Optional

import pyarrow
import pyarrow.csv as pv
import pyarrow.json as pj
import pyarrow.parquet as pq
import yarl
from smart_open import open as smart_open

try:
    from mypy_boto3_s3.service_resource import ObjectSummary
except ImportError:
    pass

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    DatasetLogicalID,
    DatasetSchema,
    DatasetStatistics,
    SchemaField,
    SchemaType,
)
from metaphor.s3.boto_helpers import list_folders
from metaphor.s3.config import S3_SCHEME, S3PathSpec, S3RunConfig

logger = get_logger()
PAGE_SIZE = 1000


@dataclass
class FileObject:
    path: str
    last_modified: datetime
    size: int
    path_spec: S3PathSpec

    @classmethod
    def from_object_summary(
        cls, obj: "ObjectSummary", path_spec: S3PathSpec
    ) -> "FileObject":
        s3_path = f"{S3_SCHEME}{obj.bucket_name}/{obj.key}"
        logger.debug(f"Found file object, path: {s3_path}")
        return cls(
            path=s3_path,
            last_modified=obj.last_modified,
            size=obj.size,
            path_spec=path_spec,
        )


@dataclass
class TableData:
    display_name: str = ""
    full_path: str = ""
    partitions: Optional[OrderedDict] = None
    timestamp: datetime = datetime.min
    table_path: str = ""
    size_in_bytes: int = 0
    number_of_files: int = 0

    @classmethod
    def from_file_object(cls, file_object: FileObject) -> "TableData":
        logger.debug(f"Getting table data for path: {file_object.path}")
        table_name, table_path = file_object.path_spec.extract_table_name_and_path(
            file_object.path
        )
        return TableData(
            display_name=table_name,
            full_path=file_object.path,
            partitions=None,
            timestamp=file_object.last_modified,
            table_path=table_path,
            number_of_files=1,
            size_in_bytes=file_object.size,
        )

    @property
    def url(self) -> yarl.URL:
        return yarl.URL(self.full_path)

    def merge(self, other: "TableData") -> "TableData":
        if not self.table_path:
            return other

        full_path = self.full_path
        timestamp = self.timestamp
        if self.timestamp < other.timestamp and other.size_in_bytes > 0:
            full_path = other.full_path
            timestamp = other.timestamp

        return TableData(
            display_name=self.display_name,
            full_path=full_path,
            partitions=self.partitions,  # TODO
            timestamp=timestamp,
            table_path=self.table_path,
            size_in_bytes=self.size_in_bytes + other.size_in_bytes,
            number_of_files=self.number_of_files + other.number_of_files,
        )


class S3Extractor(BaseExtractor):
    """S3 metadata extractor"""

    _description = "S3 metadata crawler"
    _platform = Platform.S3

    @staticmethod
    def from_config_file(config_file: str) -> "S3Extractor":
        return S3Extractor(S3RunConfig.from_yaml_file(config_file))

    def __init__(self, config: S3RunConfig) -> None:
        super().__init__(config)
        self._config = config
        self._path_specs = config.path_specs

    def _resolve_templated_folders(
        self, bucket_name: str, path_prefix: str
    ) -> Iterable[str]:
        folder_split: List[str] = path_prefix.split("*", 1)
        # If the len of split is 1 it means we don't have * in the prefix
        if len(folder_split) == 1:
            yield path_prefix
            return

        for folder in list_folders(bucket_name, folder_split[0], self._config):
            yield from self._resolve_templated_folders(
                bucket_name, f"{folder}{folder_split[1]}"
            )

    def _browse_path_spec(self, path_spec: S3PathSpec) -> Iterable[FileObject]:
        bucket = self._config.s3_resource.Bucket(path_spec.bucket)
        matches = list(re.finditer(r"{\s*\w+\s*}", path_spec.uri, re.MULTILINE))
        if matches:
            for folder in self._resolve_templated_folders(
                path_spec.bucket, path_spec.path_prefix
            ):
                pass
        else:
            for page in (
                bucket.objects.filter(Prefix=path_spec.path_prefix)
                .page_size(PAGE_SIZE)
                .pages()
            ):
                for obj in page:
                    if path_spec.allow(obj.key):
                        yield FileObject.from_object_summary(obj, path_spec)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        entities = []
        buckets = [
            bucket.get("Name")
            for bucket in self._config.s3_client.list_buckets()["Buckets"]
        ]
        logger.debug(f"Exisiting buckets: {buckets}")

        for path_spec in self._path_specs:
            if path_spec.bucket not in buckets:
                logger.warning(
                    f"Skipping {path_spec}: bucket {path_spec.bucket} does not exist"
                )
                continue
            tables: Dict[str, TableData] = defaultdict(lambda: TableData())
            for file_object in self._browse_path_spec(path_spec):
                table_data = TableData.from_file_object(file_object)
                tables[table_data.table_path] = tables[table_data.table_path].merge(
                    table_data
                )

            for table_path, table_data in tables.items():
                logger.debug(f"Initializing {table_path}")
                dataset = self._init_dataset(table_data)
                entities.append(dataset)

        return entities

    def _parse_json_schema(self, source, suffix: str) -> DatasetSchema:
        if suffix == ".json":
            schema_type = SchemaType.JSON
        elif suffix == ".avro":
            schema_type = SchemaType.AVRO
        else:
            assert False, f"Unknown suffix: {suffix}"

        table: pyarrow.Table = pj.read_json(
            source
        )  # TODO: how do we want to parse avro?
        fields = [
            SchemaField(
                field_path=column._name,
                native_type=str(column.type),
            )
            for column in table.columns
        ]

        return DatasetSchema(schema_type=schema_type, fields=fields)

    def _parse_schemaless(self, source, suffix: str) -> DatasetSchema:
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

    def _parse_parquet(self, source) -> DatasetSchema:
        table = pq.read_table(source)
        fields = [
            SchemaField(
                field_path=column._name,
                native_type=str(column.type),
            )
            for column in table.columns
        ]

        return DatasetSchema(schema_type=SchemaType.PARQUET, fields=fields)

    def _parse_schema(self, table_data: TableData) -> DatasetSchema:
        with smart_open(
            table_data.full_path,
            "rb",
            transport_params={"client": self._config.s3_client},
        ) as source:
            suffix = table_data.url.suffix
            if suffix in {".csv", ".tsv"}:
                return self._parse_schemaless(source, suffix)

            if suffix in {".json", ".avro"}:
                return self._parse_json_schema(source, suffix)

            if suffix == ".parquet":
                return self._parse_parquet(source)

        assert False, f"Unknown suffix: {suffix}"

    def _init_dataset(self, table_data: TableData) -> Dataset:
        return Dataset(
            logical_id=DatasetLogicalID(
                name=table_data.full_path,
                platform=DataPlatform.S3,
            ),
            statistics=DatasetStatistics(
                last_updated=table_data.timestamp,
                data_size_bytes=float(table_data.size_in_bytes),
            ),
            schema=self._parse_schema(table_data),
        )
