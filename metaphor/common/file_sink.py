import json
import logging
import tempfile
from dataclasses import field
from datetime import datetime, timezone
from os import path
from typing import List, Optional
from zipfile import ZIP_DEFLATED, ZipFile

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.logger import LOG_FILE, debug_files, get_logger
from metaphor.common.sink import Sink
from metaphor.common.storage import (
    BaseStorage,
    LocalStorage,
    S3Storage,
    S3StorageConfig,
)
from metaphor.common.utils import chunk_by_size
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata

logger = get_logger()


@dataclass(config=ConnectorConfig)
class FileSinkConfig:
    # Location of the sink directory, where the MCE file and logs will be output to.
    # Can be local file directory, s3://bucket/ or s3://bucket/path/
    directory: str

    # Output logs
    write_logs: bool = True

    # Limit each file to have at most 200 items
    batch_size_count: int = 200

    # Limit each file to < 100 MB in size
    batch_size_bytes: int = 100 * 1000 * 1000

    # IAM role to assume before writing to file
    assume_role_arn: Optional[str] = None

    # IAM credential to access S3 bucket
    s3_auth_config: S3StorageConfig = field(default_factory=lambda: S3StorageConfig())


class FileSink(Sink):
    """File sink functions"""

    def __init__(self, config: FileSinkConfig):
        self.path = f'{config.directory.rstrip("/")}/{int(datetime.now().timestamp())}'
        self.write_logs = config.write_logs
        self.batch_size_count = config.batch_size_count
        self.batch_size_bytes = config.batch_size_bytes
        logger.info(f"Write files to {self.path}")

        if config.directory.startswith("s3://"):
            self._storage: BaseStorage = S3Storage(
                config.assume_role_arn, config.s3_auth_config
            )
        else:
            self._storage = LocalStorage()

    def _sink(self, messages: List[dict]) -> bool:
        """Write records to file with auto-splitting"""

        logger.info("Split MCE records into chunks")
        slices = chunk_by_size(
            messages,
            self.batch_size_count,
            self.batch_size_bytes,
            lambda item: len(json.dumps(item)),
        )

        for part, slice in enumerate(slices):
            file_name = f"{part+1}-of-{len(slices)}.json"
            logger.info(f"Writing {file_name} ({slice.stop - slice.start} records)")
            self._storage.write_file(
                f"{self.path}/{file_name}",
                json.dumps(messages[slice]),
            )

        logger.info(f"Written {len(slices)} MCE files")

        return True

    def sink_logs(self):
        if not self.write_logs:
            logger.info("Skip writing logs")
            return

        logging.shutdown()

        _, zip_file = tempfile.mkstemp(suffix=".zip")
        dir_name = datetime.now(timezone.utc).strftime("%Y-%m-%d %H-%M-%S")

        with ZipFile(zip_file, "w", ZIP_DEFLATED) as file:
            arcname = f"{dir_name}/run.log"
            file.write(LOG_FILE, arcname=arcname)

            for debug_file in debug_files:
                arcname = f"{dir_name}/{path.basename(debug_file)}"
                file.write(debug_file, arcname=arcname)

        with open(zip_file, "rb") as file:
            self._storage.write_file(f"{self.path}/log.zip", file.read(), True)

    def sink_metadata(self, metadata: CrawlerRunMetadata):
        if not self.write_logs:
            logger.info("Skip writing metadata")
            return

        content = json.dumps(EventUtil.clean_nones(metadata.to_dict())).encode()

        self._storage.write_file(f"{self.path}/run.metadata", content, True)

    def write_file(self, filename: str, content: str):
        """Write content into a file in the output sink

        Parameters
        -------
        filename : str
            The filename to store the content under the output sink
        content : str
            The content to write to the file
        """
        self._storage.write_file(f"{self.path}/{filename}", content.encode(), True)

    def remove_file(self, filename: str):
        """Remove a file in the output sink

        Parameters
        -------
        filename : str
            The file to remove
        """
        self._storage.delete_files([f"{self.path}/{filename}"])
