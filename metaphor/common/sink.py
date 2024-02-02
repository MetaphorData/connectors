import json
import logging
import tempfile
from dataclasses import field
from datetime import datetime, timezone
from os import path
from typing import Optional
from zipfile import ZIP_DEFLATED, ZipFile

from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.event_util import EventUtil
from metaphor.common.logger import LOG_FILE, debug_files, get_logger
from metaphor.common.storage import (
    BaseStorage,
    LocalStorage,
    S3Storage,
    S3StorageConfig,
)
from metaphor.models.crawler_run_metadata import CrawlerRunMetadata
from metaphor.models.metadata_change_event import MetadataChangeEvent

logger = get_logger()


@dataclass(config=ConnectorConfig)
class SinkConfig:
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


class StreamSink:
    def __init__(
        self, config: SinkConfig, metadata: Optional[CrawlerRunMetadata] = None
    ):
        self.path = f'{config.directory.rstrip("/")}/{int(datetime.now().timestamp())}'
        self.metadata = metadata
        self.log_execution = config.write_logs
        """
        Whether to log the execution process.
        """

        self.items_per_batch = config.batch_size_count
        """
        Maximum number of MCE items per batch.
        """

        self.bytes_per_batch = config.batch_size_bytes
        """
        Maximum bytesize for a batch.
        """

        self._event_util = EventUtil()

        self._current_batch: int = -1
        """
        The batch currently being processed. Starts from -1.
        """
        self._items: int = 0
        """
        How many items we've processed so far in the current batch.
        """
        self._bytes: int = 0
        """
        How many bytes we've processed so far in the current batch.
        """

        self._completed_batches: int = 0
        """
        Number of finished batches.
        """

        logger.info(f"Write files to {self.path}")
        if config.directory.startswith("s3://"):
            self._storage: BaseStorage = S3Storage(
                config.assume_role_arn, config.s3_auth_config
            )
        else:
            self._storage = LocalStorage()

        self._entered = False

    def __enter__(self):
        self._entered = True
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # TODO do something with exceptions here?
        self._exit()

    def _get_batch_file_path(self, batch: int) -> str:
        return f"{self.path}/{batch}.json"

    def _get_current_batch_file_path(self) -> str:
        return self._get_batch_file_path(self._current_batch)

    def _should_start_new_batch(
        self,
        payload_len: int,
    ) -> bool:
        """
        We should move on from the current batch if the item we're working with
        is going to make the current batch exceed the count or bytesize limit.

        If the current batch is less than 0, it means we need to create the first
        batch.
        """
        return self._current_batch < 0 or (
            self._bytes + payload_len
            > self.bytes_per_batch  # If we're gonna exceed the batch byte count limit
            or self._items + 1
            > self.items_per_batch  # If we're gonna exceed the batch item count limit
        )

    def _finalize_current_batch(self) -> None:
        """
        Inserts a `']'` character to the current batch file, then increments `self._completed_batched`
        by 1.
        """
        self._storage.write_file(self._get_current_batch_file_path(), "]", append=True)
        logger.info(
            f"Finished batch #{self._current_batch}, bytesize = {self._bytes}, item count = {self._items}"
        )
        self._completed_batches += 1

    def write_event(self, event: MetadataChangeEvent) -> bool:
        """
        Writes a single `MetadataChangeEvent` to the output folder. Automatically splits the stream
        into multiple batches.

        This method should be called only context managed, i.e. called within a `with` context:

        ```python
        with StreamSink(config, metadata) as sink:
            for event in events:
                sink.write_event(event)
        ```
        """

        if not self._entered:
            raise ValueError(
                "Cannot call this method when StreamSink isn't context managed"
            )

        message = self._event_util.trim_event(event)
        validated_message = self._event_util.validate_message(message)
        if validated_message is None:
            return False

        payload = json.dumps(validated_message)
        payload_size = len(payload)

        if self._should_start_new_batch(
            payload_size + 1
        ):  # The actual payload is always prefixed by a single character.
            if self._current_batch >= 0:
                self._finalize_current_batch()

            # ... then reset the counters...
            self._bytes = 0
            self._items = 0
            self._current_batch += 1

            # For the new batch, we insert a single '[' character to the file's start.
            payload_prefix = "["
        else:
            # Otherwise we are inserting to the current batch file.
            # We separate the current event with the previously inserted ones with a ','.
            payload_prefix = ","

        self._storage.write_file(
            self._get_current_batch_file_path(), payload_prefix + payload, append=True
        )
        self._items += 1
        self._bytes += payload_size + 1
        return True

    def _exit(self) -> None:
        if not self._entered:
            raise ValueError(
                "Cannot call this method when StreamSink isn't context managed"
            )
        self._finalize_batches()
        self._write_execution_logs()
        self._write_run_metadata()
        self._entered = False

    def _finalize_batches(self) -> None:
        """
        Finalizes the entire event stream by finishing the last batch file, and renaming the
        batch files.
        """
        if self._current_batch != -1:
            self._finalize_current_batch()
            for batch in range(self._completed_batches):
                self._storage.rename_file(
                    self._get_batch_file_path(batch),
                    f"{self.path}/{batch+1}-of-{self._completed_batches}.json",
                )
            logger.info(f"Wrote {self._completed_batches} MCE files")

    def _write_execution_logs(self):
        if not self.log_execution:
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

    def _write_run_metadata(self):
        if not self.log_execution:
            logger.info("Skip writing metadata")
            return

        if self.metadata:
            content = json.dumps(
                EventUtil.clean_nones(self.metadata.to_dict())
            ).encode()
            self._storage.write_file(f"{self.path}/run.metadata", content, True)
