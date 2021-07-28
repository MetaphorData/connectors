import json
import logging
from dataclasses import dataclass
from typing import List, Optional

import boto3
from aws_assume_role_lib import assume_role
from dataclasses_json import dataclass_json
from smart_open import open

from .sink import Sink

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass_json
@dataclass
class FileSinkConfig:
    # Location of the sink file. Can be local file or s3://bucket/object
    output: str

    # IAM role to assume before writing to file
    assume_role_arn: Optional[str] = None


class FileSink(Sink):
    """File sink functions"""

    def __init__(self, config: FileSinkConfig):
        self.output = config.output

        session = boto3.Session()
        if config.assume_role_arn is not None:
            session = assume_role(session, config.assume_role_arn)

    def _sink(self, messages: List[dict]) -> bool:
        """Write records to file"""

        # Give S3 bucket owner full control over the new object
        # See https://github.com/RaRe-Technologies/smart_open/blob/develop/howto.md#how-to-pass-additional-parameters-to-boto3
        params = None
        if self.output.startswith("s3://"):
            params = {
                "client_kwargs": {
                    "S3.Client.create_multipart_upload": {
                        "ACL": "bucket-owner-full-control"
                    }
                }
            }

        with open(self.output, "w", transport_params=params) as fp:
            json.dump(messages, fp)

        return True
