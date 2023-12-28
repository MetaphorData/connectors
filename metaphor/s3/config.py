import os
import re
from dataclasses import field
from fnmatch import fnmatch
from functools import cached_property
from typing import List, Optional, Set, Tuple, Union

import parse
import yarl
from pydantic import BaseModel, ValidationInfo, field_validator, model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.logger import get_logger

try:
    from mypy_boto3_s3 import S3Client, S3ServiceResource
except ImportError:
    # Ignore this since mypy plugins are dev dependencies
    pass


S3_SCHEME: str = "s3://"


logger = get_logger()


class S3PathSpec(BaseModel):
    uri: str
    file_types: Set[str] = field(
        default_factory=lambda: {"avro", "csv", "tsv", "parquet", "json"}
    )
    excludes: List[str] = field(default_factory=list)
    table_name: Optional[str] = None

    def __str__(self) -> str:
        return self.uri

    @cached_property
    def url(self) -> yarl.URL:
        return yarl.URL(self.uri)

    @cached_property
    def bucket(self) -> str:
        return self.url.host  # type: ignore

    @cached_property
    def object_path(self) -> str:
        return self.url.path[1:]  # Drop the first slash

    @cached_property
    def full_path(self) -> str:
        return self.url.with_scheme("").human_repr()[1:]  # Drop the first slash

    @cached_property
    def path_prefix(self) -> str:
        index = re.search(r"[\*|\{]", self.object_path)
        if index:
            return self.object_path[: index.start()]
        else:
            return self.object_path

    @classmethod
    def replace_wildcards(cls, uri: str) -> str:
        replaced = uri
        for i in range(replaced.count("*")):
            replaced = replaced.replace("*", f"{{token[{i}]}}", 1)
        return replaced

    @cached_property
    def _compiled_parser(self):
        parsable = S3PathSpec.replace_wildcards(self.uri)
        logger.debug(f"Parsable uri: {parsable}")
        parser = parse.compile(parsable)
        logger.debug(f"Setting compiled parser: {parser}")
        return parser

    @cached_property
    def _as_glob(self):
        as_glob = re.sub(r"\{[^}]+\}", "*", self.uri)
        logger.debug(f"Setting glob: {as_glob}")
        return as_glob

    def get_named_vars(self, path: str) -> Union[None, parse.Result, parse.Match]:
        return self._compiled_parser.parse(path)

    @model_validator(mode="after")
    def _uri_is_valid(self) -> "S3PathSpec":
        assert self.url.scheme == "s3"
        assert self.url.host
        return self

    def allow(self, key: str):
        return (
            fnmatch(key, self.object_path)
            and yarl.URL(key).suffix[1:] in self.file_types
        )

    @field_validator("table_name", mode="before")
    def table_name_in_include(cls, table_name: str, info: ValidationInfo):
        uri = info.data.get("uri")
        if not uri:
            return table_name

        parsable_uri = S3PathSpec.replace_wildcards(uri)
        parser = parse.compile(parsable_uri)

        if table_name is None:
            if "{table}" in uri:
                table_name = "{table}"
        else:
            logger.debug(f"include fields: {parser.named_fields}")
            logger.debug(f"table_name fields: {parse.compile(table_name).named_fields}")
            if not all(
                x in parser.named_fields for x in parse.compile(table_name).named_fields
            ):
                raise ValueError(
                    f"Not all named variables used in path_spec.table_name {table_name} are specified in path_spec.include {uri}"
                )
        return table_name

    def _extract_table_name(self, named_vars: dict) -> str:
        if self.table_name is None:
            raise ValueError("path_spec.table_name is not set")
        return self.table_name.format_map(named_vars)

    def extract_table_name_and_path(self, path: str) -> Tuple[str, str]:
        parsed_vars = self.get_named_vars(path)
        if (
            not isinstance(parsed_vars, parse.Result)
            or "table" not in parsed_vars.named
        ):
            return os.path.basename(path), path
        else:
            depth = self.uri.count("/", 0, self.uri.find("{table}"))
            table_path = (
                "/".join(path.split("/")[:depth]) + "/" + parsed_vars.named["table"]
            )
            return self._extract_table_name(parsed_vars.named), table_path


@dataclass(config=ConnectorConfig)
class S3RunConfig(BaseConfig):
    aws: AwsCredentials
    endpoint_url: str
    path_specs: List[S3PathSpec] = field(default_factory=list)
    verify_ssl: Union[bool, str] = False

    @cached_property
    def s3_client(self) -> "S3Client":
        return self.aws.get_session().client(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            verify=self.verify_ssl,
        )  # type: ignore

    @cached_property
    def s3_resource(self) -> "S3ServiceResource":
        return self.aws.get_session().resource(
            service_name="s3",
            endpoint_url=self.endpoint_url,
            verify=self.verify_ssl,
        )  # type: ignore
