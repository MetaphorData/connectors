import os
import re
from fnmatch import fnmatch
from functools import cached_property, reduce
from typing import List, Optional, Set, Tuple, Union

import dateutil.parser
import parse
import yarl
from pydantic import BaseModel, Field, model_validator

from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import SchemaField

logger = get_logger()

TABLE_LABEL = "{table}"  # nosec - this ain't a password


class PartitionField(BaseModel):
    """
    Represents a partition column.
    """

    name: Optional[str]
    """
    Name of the partition column. For unnamed columns, this will be
    `Unnamed column {index}`.
    """
    raw_value: str
    index: int

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, PartitionField):
            return self.model_dump_json() == __value.model_dump_json()
        return False

    @cached_property
    def inferred_type(self) -> str:
        """
        Infer partition column type from raw value.

        There should be a more robust way to do this, but for now this will
        be sufficient.
        """
        if not self.name:
            return "string"
        # Somehow this is preferred over try except pass by bandit
        try:
            int(self.raw_value)
            return "int64"
        except Exception:
            try:
                float(self.raw_value)
                return "double"
            except Exception:
                try:
                    dateutil.parser.parse(self.raw_value)
                    return "datetime"
                except Exception:
                    return "string"

    @model_validator(mode="after")
    def _ensure_name(self) -> "PartitionField":
        if not self.name:
            self.name = f"Unnamed column {self.index}"
        return self

    def to_schema_field(self) -> SchemaField:
        return SchemaField(
            field_path=self.name,  # self.name is always going to be a valid string here.
            native_type=self.inferred_type,
        )


class PathSpec(BaseModel):
    uri: str
    """
    The path specification. E.g.
    `s3://foo/bar/*/{dept}/{table}/*/*.csv`
    """

    file_types: Set[str] = Field(
        default_factory=lambda: {"avro", "csv", "tsv", "parquet", "json"}
    )

    excludes: List[str] = Field(default_factory=list)
    """
    The excluded paths.
    """

    def __str__(self) -> str:
        return self.uri

    @cached_property
    def url(self) -> yarl.URL:
        return yarl.URL(self.uri)

    @cached_property
    def bucket(self) -> str:
        """
        For uri = `s3://foo/bar/baz.csv`, this would be `foo`.
        """
        return self.url.host  # type: ignore

    @cached_property
    def object_path(self) -> str:
        """
        For uri = `s3://foo/bar/baz.csv`, this would be `bar/baz.csv`.
        """
        return self.url.path[1:]  # Drop the first slash

    @cached_property
    def full_path(self) -> str:
        """
        For uri = `s3://foo/bar/baz.csv`, this would be `/foo/bar/baz.csv`.
        """
        return self.url.with_scheme("").human_repr()[1:]  # Drop the first slash

    @cached_property
    def path_prefix(self) -> str:
        """
        Returns the prefix of the path before the label or wildcard levels. E.g.
        for uri = `s3://foo/bar/baz/*/*/{table}/*/*.*`, this would be
        `bar/baz/`.
        """
        index = re.search(r"[\*|\{]", self.object_path)
        if index:
            return self.object_path[: index.start()]
        else:
            return self.object_path

    @cached_property
    def _compiled_parser(self):
        parsable = reduce(
            lambda acc, i: acc.replace("*", f"{{token[{i}]}}", 1),
            range(self.uri.count("*")),
            self.uri,
        )
        logger.debug(f"Parsable uri: {parsable}")
        parser = parse.compile(parsable)
        logger.debug(f"Setting compiled parser: {parser}")
        return parser

    def get_named_vars(self, path: str) -> Union[None, parse.Result, parse.Match]:
        return self._compiled_parser.parse(path)

    @cached_property
    def labels(self) -> List[str]:
        """
        The labels in the uri. Below are the rules for different labels:

        - `{table}`
            - The table label. The directories at the level of this label will
              be treated as a dataset. This label can appear at most once in the
              uri. If there are any other kinds of labels, this label must exist.
        - `{partition[N]}` & `{partition_key[N]}`
            - The partition labels. `{partition[N]}` are mandatory, all directories
              within that level will be treated as a partition column. If
              `{partition_key[N]}` is not provided, the partition column is considered
              an unnamed column.
        - All other labels
            - Treated as part of display name, separated by `"/"`.
        """
        return re.findall(r"{\s*\w+\s*}", self.uri)

    @cached_property
    def partitions(self) -> List[str]:
        """
        Partitions are either a single label named `{partition[N]}`, or two labels joined
        by a equal sign: `{partition_key[N]}={partition[N]}`. Both formats can exist within
        a single uri.
        """
        matches = re.findall(
            r"({\s*partition_key\[\d+\]\s*}=)?({\s*partition\[\d+\]\s*})", self.uri
        )
        partitions = []
        for match in matches:
            key, value = match  # key could be empty string
            partitions.append(f"{key}{value}")
        return partitions

    def extract_partitions(self, file_uri: str) -> List[PartitionField]:
        """
        Extract the partition columns from a file uri.
        """
        partition_fields = []
        part_index = 0
        for part, file_part in zip(self.uri.split("/"), file_uri.split("/")):
            if part_index >= len(self.partitions):
                break
            if part == self.partitions[part_index]:
                if "partition_key" in part:
                    tokens = file_part.rsplit("=", 1)
                    if len(tokens) != 2:
                        logger.warning(
                            f"Cannot parse partition, expected partition format {part}, found {file_part}. Skipping this partition"
                        )
                        part_index += 1
                        continue

                    partition_fields.append(
                        PartitionField(
                            name=tokens[0], raw_value=tokens[1], index=part_index
                        )
                    )
                else:
                    partition_fields.append(
                        PartitionField(name=None, raw_value=file_part, index=part_index)
                    )
                part_index += 1

        return partition_fields

    @model_validator(mode="after")
    def _partitions_are_valid(self) -> "PathSpec":
        """
        Make sure the partition labels are valid.
        """
        for index, partition in enumerate(self.partitions):
            indexes = re.findall(r"\[\d+\]", partition)
            assert len(indexes) > 0 and len(indexes) < 3
            value_index = int(indexes[-1][1:-1])
            assert (
                index == value_index
            ), f"Invalid partition index, found {value_index} at pos {index}"
            if len(indexes) == 2:
                key_index = int(indexes[0][1:-1])
                assert (
                    key_index == value_index
                ), f"Partition index does not match: {partition}"
        return self

    @model_validator(mode="after")
    def _uri_is_valid(self) -> "PathSpec":
        """
        Make sure the scheme is `s3` and the bucket is provided.
        """
        assert self.url.scheme == "s3"
        assert self.url.host
        return self

    @model_validator(mode="after")
    def _labels_are_valid(self) -> "PathSpec":
        """
        Make sure the `{table}` label exists if there's any label within the uri.
        """
        if self.labels:
            assert TABLE_LABEL in self.labels
        return self

    def allow_key(self, key: str):
        """
        Check if the object key is accepted by this path spec's uri. Here we are comparing
        against the object path.
        """
        pattern = self.object_path
        for match in self.labels + self.partitions:
            pattern = pattern.replace(match, "*", 1)
        return fnmatch(key, pattern) and key.rsplit(".", 1)[-1] in self.file_types

    def allow_path(self, path: str) -> bool:
        """
        Check if the directory path is accepted by this path spec.

        Note that the `path` parameter should be a full path, i.e. has `s3` scheme,
        with a specified bucket.
        """
        path_slash = path.count("/")
        uri_slash = self.uri.count("/")
        if path_slash > uri_slash:
            return False

        slash_to_remove = (uri_slash - path_slash) + 1
        pattern = self.uri.rsplit("/", slash_to_remove)[0]
        for (
            match
        ) in (
            self.labels
        ):  # Here we don't want to match the partition columns, instead we want to return the table-level directory.
            if match in pattern:
                pattern = pattern.replace(match, "*", 1)
        if not fnmatch(path, pattern):
            logger.debug(f"Unmatched path: {path}")
            return False

        for exclude in self.excludes:
            exclude_slash = exclude.count("/")
            if path_slash < exclude_slash:
                # Can't tell
                continue
            slash_to_remove = (path_slash - exclude_slash) + 1
            path.rsplit("/", slash_to_remove)[0]
            if exclude[-1] == "/":
                exclude_pat = exclude[:-1]
            else:
                exclude_pat = exclude
            if fnmatch(path.rsplit("/", slash_to_remove)[0], exclude_pat):
                logger.debug(f"Path {path} excluded by pattern: {exclude}")
                return False

        return True

    def extract_table_name_and_path(self, path: str) -> Tuple[str, str]:
        parsed_vars = self.get_named_vars(path)
        if (
            not isinstance(parsed_vars, parse.Result)
            or "table" not in parsed_vars.named
        ):
            return os.path.basename(path), path
        else:
            assert TABLE_LABEL in self.uri, f"Cannot find {{table}} in uri: {self.uri}"
            depth = self.uri.count("/", 0, self.uri.find(TABLE_LABEL))
            table_path = (
                "/".join(path.split("/")[:depth]) + "/" + parsed_vars.named["table"]
            )
            table_name = "/".join(
                match.format_map(parsed_vars.named) for match in self.labels
            )
            return table_name, table_path
