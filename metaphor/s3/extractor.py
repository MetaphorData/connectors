import functools
from collections import defaultdict
from typing import Collection, Dict, Iterable, List, Optional

from more_itertools import peekable

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.common.models import to_dataset_statistics
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import Dataset, SourceInfo
from metaphor.s3.boto_helpers import list_folders
from metaphor.s3.config import PathSpec, S3RunConfig
from metaphor.s3.parse_schema import parse_schema
from metaphor.s3.path_spec import TABLE_LABEL
from metaphor.s3.table_data import FileObject, TableData

logger = get_logger()
PAGE_SIZE = 1000


def dir_comp(left: str, right: str) -> int:
    # Try to convert to number and compare if the directory name is a number
    try:
        # Strip = from the directory names, just use the column value
        if "=" in left and "=" in right:
            left = left.rsplit("=", 1)[-1]
            right = right.rsplit("=", 1)[-1]

        return int(left) - int(right)  # This can throw if left / right are pure strings

    except Exception:
        if left == right:
            return 0
        else:
            return 1 if left > right else -1


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

    def get_dir_to_process(
        self, bucket_name: str, folder: str, path_spec: PathSpec
    ) -> Optional[str]:
        """
        Parameters
        ----------
        bucket_name : str
            The bucket name.
        folder : str
            The folder path within the bucket. Ends with a slash.
        path_spec : PathSpec
            The path specification to check the folder against.

        Returns
        -------
        Optional[str]
            The directory to process, or None if no such directory is found.
        """
        if not path_spec.allow_path(f"s3://{bucket_name}/{folder}"):
            return None

        iterator = list_folders(
            bucket_name=bucket_name,
            prefix=folder,
            config=self._config,
        )
        iterator = peekable(iterator)
        if iterator:
            sorted_dirs = sorted(
                iterator,
                key=functools.cmp_to_key(
                    dir_comp
                ),  # If it's a partition column then we want to compare the value, otherwise just compare names
                reverse=True,
            )
            for dir in sorted_dirs:
                if path_spec.allow_path(f"s3://{bucket_name}/{dir}/"):
                    return self.get_dir_to_process(
                        bucket_name=bucket_name,
                        folder=dir + "/",
                        path_spec=path_spec,
                    )
            return folder
        else:
            return folder

    def _browse_path_spec(self, path_spec: PathSpec) -> Iterable[FileObject]:
        """
        Browses thru all eligible file objects in path spec. Resolves the wildcard characters
        and labels and returns actual file paths.
        """

        def yield_file_object(prefix: str):
            for page in (
                bucket.objects.filter(Prefix=prefix).page_size(PAGE_SIZE).pages()
            ):
                for obj in page:
                    if path_spec.allow_key(obj.key):
                        yield FileObject.from_object_summary(obj, path_spec)

        bucket_name = path_spec.bucket
        bucket = self._config.s3_resource.Bucket(bucket_name)
        if path_spec.labels:
            # This branch is for directory based datasets
            object_path = path_spec.object_path
            for label in path_spec.labels:
                if label != TABLE_LABEL:
                    object_path = object_path.replace(label, "*", 1)
            path_prefix = object_path[: object_path.find(TABLE_LABEL)]
            for folder in self._resolve_templated_folders(bucket_name, path_prefix):
                for f in list_folders(bucket_name, folder, self._config):
                    logger.debug(f"Processing folder dataset: {f}")
                    directory = self.get_dir_to_process(bucket_name, f"{f}/", path_spec)
                    if directory:
                        logger.debug(f"Found directory: {directory}")
                        yield from yield_file_object(prefix=directory)
        else:
            # No label in uri, just return the resolved path
            yield from yield_file_object(prefix=path_spec.path_prefix)

    async def extract(self) -> Collection[ENTITY_TYPES]:
        entities = []

        for path_spec in self._path_specs:
            try:
                # Browse through all valid files covered by this path_spec. If there are
                # overlapping files (i.e. different files under a directory that's parsed
                # as a single dataset), they are merged. See `TableData.merge` for the
                # implementation.
                tables: Dict[str, TableData] = defaultdict(lambda: TableData())
                for file_object in self._browse_path_spec(path_spec):
                    table_data = TableData.from_file_object(file_object)
                    tables[table_data.guid] = tables[table_data.table_path].merge(
                        table_data
                    )

                logger.debug(f"Tables: {tables}")
                for table_data in tables.values():
                    logger.debug(f"Initializing dataset with {table_data}")
                    dataset = self._init_dataset(table_data)
                    entities.append(dataset)

            except Exception:
                logger.exception(f"Failed to process path_spec: {path_spec}")

        return entities

    def _init_dataset(self, table_data: TableData) -> Dataset:
        return Dataset(
            display_name=table_data.display_name,
            logical_id=table_data.logical_id,
            statistics=to_dataset_statistics(
                size_bytes=table_data.size_in_bytes,
            ),
            source_info=(
                SourceInfo(last_updated=table_data.timestamp)
                if table_data.timestamp
                else None
            ),
            schema=parse_schema(self._config, table_data),
        )
