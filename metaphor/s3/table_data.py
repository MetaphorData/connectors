from datetime import datetime
from functools import cached_property
from typing import Optional, OrderedDict

import yarl
from pydantic.dataclasses import dataclass

from metaphor.common.logger import get_logger

try:
    from mypy_boto3_s3.service_resource import ObjectSummary
except ImportError:
    pass
from metaphor.s3.config import PathSpec

logger = get_logger()


@dataclass
class FileObject:
    path: str
    last_modified: datetime
    size: int
    path_spec: PathSpec

    @classmethod
    def from_object_summary(
        cls, obj: "ObjectSummary", path_spec: PathSpec
    ) -> "FileObject":
        s3_path = f"s3://{obj.bucket_name}/{obj.key}"
        logger.debug(f"Found file object, path: {s3_path}")
        return cls(
            path=s3_path,
            last_modified=obj.last_modified,
            size=obj.size,
            path_spec=path_spec,
        )


@dataclass
class TableData:
    """
    Represents a table in the S3 storage.

    Use `table_path` as the guid.
    """

    display_name: str = ""
    full_path: str = ""
    partitions: Optional[OrderedDict] = None
    timestamp: datetime = datetime.min
    table_path: str = ""
    size_in_bytes: int = 0
    number_of_files: int = 0

    @property
    def guid(self) -> str:
        return self.table_path

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

    @cached_property
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
