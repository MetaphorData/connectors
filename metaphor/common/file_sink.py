import json
import logging
from dataclasses import dataclass
from typing import List, Optional

import boto3
from aws_assume_role_lib import assume_role
from serde import deserialize

from .s3 import write_file
from .sink import Sink

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class FileSinkConfig:
    # Location of the sink file. Can be local file or s3://bucket/object
    path: str

    # IAM role to assume before writing to file
    assume_role_arn: Optional[str] = None


class FileSink(Sink):
    """File sink functions"""

    def __init__(self, config: FileSinkConfig):
        self.path = config.path

        session = boto3.Session()
        if config.assume_role_arn is not None:
            session = assume_role(session, config.assume_role_arn)

    def _sink(self, messages: List[dict]) -> bool:
        """Write records to file"""
        write_file(self.path, json.dumps(messages))
        return True
