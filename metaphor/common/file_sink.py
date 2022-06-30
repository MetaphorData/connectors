import json
import logging
import math
import tempfile
from dataclasses import field
from os import path
from typing import List, Optional
from zipfile import ZIP_DEFLATED, ZipFile

from metaphor.models.crawler_run_metadata import CrawlerRunMetadata
from pydantic.dataclasses import dataclass

from .event_util import EventUtil
from .logger import LOG_FILE, get_logger
from .sink import Sink
from .storage import BaseStorage, LocalStorage, S3Storage, S3StorageConfig

logger = get_logger(__name__)


@dataclass
class FileSinkConfig:
    # Location of the sink directory, where the MCE file and logs will be output to.
    # Can be local file directory, s3://bucket/ or s3://bucket/path/
    directory: str

    # Output logs
    write_logs: bool = True

    # Maximum number of messages in each output file split
    bach_size: int = 200

    # IAM role to assume before writing to file
    assume_role_arn: Optional[str] = None

    # IAM credential to access S3 bucket
    s3_auth_config: S3StorageConfig = field(default_factory=lambda: S3StorageConfig())


class FileSink(Sink):
    """File sink functions"""

    def __init__(self, config: FileSinkConfig):
        self.path = config.directory.rstrip("/")
        self.bach_size = config.bach_size
        self.write_logs = config.write_logs

        if config.directory.startswith("s3://"):
            self._storage: BaseStorage = S3Storage(
                config.assume_role_arn, config.s3_auth_config
            )
        else:
            self._storage = LocalStorage()

    def _sink(self, messages: List[dict]) -> bool:
        """Write records to file with auto-splitting"""

        # purge existing MCE json files
        existing = self._storage.list_files(self.path, ".json")
        self._storage.delete_files(existing)
        logger.info(f"deleted {len(existing)} MCE files")

        # split MCE into batches and write to files
        bach_size = self.bach_size
        parts = math.ceil(len(messages) / bach_size)

        for i in range(0, parts):
            self._storage.write_file(
                f"{self.path}/{i + 1}-of-{parts}.json",
                json.dumps(messages[i * bach_size : (i + 1) * bach_size]),
            )
        logger.info(f"written {parts} MCE files")

        return True

    def sink_logs(self):
        if not self.write_logs:
            logger.info("Skip writing logs")
            return

        logging.shutdown()

        _, zip_file = tempfile.mkstemp(suffix=".zip")
        arcname = f"{path.basename(self.path)}.log"
        with ZipFile(zip_file, "w", ZIP_DEFLATED) as file:
            file.write(LOG_FILE, arcname=arcname)

        with open(zip_file, "rb") as file:
            self._storage.write_file(f"{self.path}/log.zip", file.read(), True)

    def sink_metadata(self, metadata: CrawlerRunMetadata):
        if not self.write_logs:
            logger.info("Skip writing metadata")
            return

        content = json.dumps(EventUtil.clean_nones(metadata.to_dict())).encode()

        self._storage.write_file(f"{self.path}/run.metadata", content, True)
