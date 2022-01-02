import json
import logging
import math
import tempfile
from dataclasses import dataclass
from os import path
from typing import List, Optional
from zipfile import ZIP_DEFLATED, ZipFile

import boto3
from aws_assume_role_lib import assume_role
from serde import deserialize

from .logger import LOG_FILE, get_logger
from .s3 import write_file
from .sink import Sink

logger = get_logger(__name__)


@deserialize
@dataclass
class FileSinkConfig:
    # Location of the sink file. Can be local file or s3://bucket/object
    path: str

    # Output logs
    write_logs: bool = True

    # Maximum number of messages in each output file split
    bach_size: int = 200

    # IAM role to assume before writing to file
    assume_role_arn: Optional[str] = None


class FileSink(Sink):
    """File sink functions"""

    def __init__(self, config: FileSinkConfig):
        self.path = config.path
        self.bach_size = config.bach_size
        self.write_logs = config.write_logs

        self.s3_session = None
        if config.assume_role_arn is not None:
            self.s3_session = assume_role(boto3.Session(), config.assume_role_arn)
            logger.info(
                f"Assumed role: {self.s3_session.client('sts').get_caller_identity()}"
            )

    def _sink(self, messages: List[dict]) -> bool:
        """Write records to file with auto-splitting"""

        bach_size = self.bach_size
        parts = math.ceil(len(messages) / bach_size)
        prefix = self.path[0:-5]

        for i in range(0, parts):
            write_file(
                f"{prefix}-{i+1}-of-{parts}.json",
                json.dumps(messages[i * bach_size : (i + 1) * bach_size]),
                s3_session=self.s3_session,
            )

        return True

    def sink_logs(self):
        if not self.write_logs:
            logger.info("Skip writing logs")
            return

        logging.shutdown()

        prefix = self.path[0:-5]

        _, zip_file = tempfile.mkstemp(suffix=".zip")
        arcname = f"{path.basename(prefix)}.log"
        with ZipFile(zip_file, "w", ZIP_DEFLATED) as file:
            file.write(LOG_FILE, arcname=arcname)

        with open(zip_file, "rb") as file:
            write_file(
                f"{prefix}-log.zip", file.read(), True, s3_session=self.s3_session
            )
