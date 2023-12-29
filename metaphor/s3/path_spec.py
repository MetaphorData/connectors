import os
import re
from fnmatch import fnmatch
from functools import cached_property
from typing import List, Set, Tuple, Union

import parse
import yarl
from pydantic import BaseModel, Field, model_validator

from metaphor.common.logger import get_logger

logger = get_logger()

TABLE_TOKEN = "{table}"  # nosec - this ain't a password


class PathSpec(BaseModel):
    uri: str
    file_types: Set[str] = Field(
        default_factory=lambda: {"avro", "csv", "tsv", "parquet", "json"}
    )
    excludes: List[str] = Field(default_factory=list)

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
        parsable = PathSpec.replace_wildcards(self.uri)
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
    def _uri_is_valid(self) -> "PathSpec":
        assert self.url.scheme == "s3"
        assert self.url.host
        return self

    def allow_key(self, key: str):
        return (
            fnmatch(key, self.object_path.replace("{table}", "*"))
            and key.rsplit(".", 1)[-1] in self.file_types
        )

    def allow_path(self, path: str) -> bool:
        path_slash = path.count("/")
        uri_slash = self.uri.count("/")
        if path_slash > uri_slash:
            return False

        slash_to_remove = (uri_slash - path_slash) + 1
        pat = self.uri.rsplit("/", slash_to_remove)[0].replace("{table}", "*")
        if not fnmatch(path, pat):
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
            assert TABLE_TOKEN in self.uri, f"Cannot find {{table}} in uri: {self.uri}"
            depth = self.uri.count("/", 0, self.uri.find(TABLE_TOKEN))
            table_path = (
                "/".join(path.split("/")[:depth]) + "/" + parsed_vars.named["table"]
            )
            return TABLE_TOKEN.format_map(parsed_vars.named), table_path
