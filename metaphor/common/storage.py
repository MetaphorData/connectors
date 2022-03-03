import os
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Union
from urllib.parse import urlparse

import boto3
from aws_assume_role_lib import assume_role
from pydantic.dataclasses import dataclass
from smart_open import open

from metaphor.common.logger import get_logger

logger = get_logger(__name__)

# Give S3 bucket owner full control over the new object
# See https://github.com/RaRe-Technologies/smart_open/blob/develop/howto.md#how-to-pass-additional-parameters-to-boto3
OWNER_FULL_CONTROL_ACL = {
    "client_kwargs": {
        "S3.Client.put_object": {"ACL": "bucket-owner-full-control"},
        "S3.Client.create_multipart_upload": {"ACL": "bucket-owner-full-control"},
    }
}


class BaseStorage(ABC):
    """Base class for file storage"""

    @abstractmethod
    def write_file(
        self, path: str, payload: Union[str, bytes], binary_mode=False
    ) -> None:
        """write a file to the given path"""

    @abstractmethod
    def list_files(self, path: str, suffix: Optional[str]) -> List[str]:
        """list all the files under the given path, optionally filter by suffix"""

    @abstractmethod
    def delete_files(self, paths: List[str]) -> None:
        """delete the given file(s)"""


class LocalStorage(BaseStorage):
    """Storage implementation for local file system"""

    def write_file(
        self, path: str, payload: Union[str, bytes], binary_mode=False
    ) -> None:
        os.makedirs(os.path.expanduser(os.path.dirname(path)), exist_ok=True)

        mode = "wb" if binary_mode else "w"
        with open(path, mode) as fp:
            fp.write(payload)

    def list_files(self, path: str, suffix: Optional[str]) -> List[str]:
        directory = os.path.expanduser(path)
        if not os.path.isdir(directory):
            logger.error(f"path {path} is not a directory")
            return []

        return [
            os.path.join(directory, file)
            for file in os.listdir(directory)
            if suffix is None or file.endswith(suffix)
        ]

    def delete_files(self, paths: List[str]) -> None:
        for path in paths:
            os.remove(path)


@dataclass
class S3StorageConfig:
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None


class S3Storage(BaseStorage):
    """Storage implementation for S3"""

    def __init__(
        self,
        assume_role_arn: Optional[str] = None,
        config: S3StorageConfig = S3StorageConfig(),
    ):
        session = boto3.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        if assume_role_arn is not None:
            self._session = assume_role(session, assume_role_arn)
            logger.info(
                f"Assumed role: {self._session.client('sts').get_caller_identity()}"
            )
        else:
            self._session = session

        self._client = self._session.client("s3")

    def write_file(
        self, path: str, payload: Union[str, bytes], binary_mode=False
    ) -> None:
        transport_params = {
            **OWNER_FULL_CONTROL_ACL,
            # Use supplied credentials
            # See https://github.com/RaRe-Technologies/smart_open#s3-credentials
            "client": self._client,
        }

        mode = "wb" if binary_mode else "w"
        with open(path, mode, transport_params=transport_params) as fp:
            fp.write(payload)

    def list_files(self, path: str, suffix: Optional[str]) -> List[str]:
        bucket, key = S3Storage.parse_s3_uri(path)

        resp = self._client.list_objects_v2(
            Bucket=bucket,
            Prefix=key,
        )
        objects = resp.get("Contents", [])

        while resp["IsTruncated"]:
            resp = self._client.list_objects_v2(
                Bucket=bucket,
                Prefix=key,
                ContinuationToken=resp["NextContinuationToken"],
            )
            objects.extend(resp.get("Contents", []))

        return [
            f"s3://{bucket}/{file['Key']}"
            for file in objects
            if suffix is None or file.get("Key").endswith(suffix)
        ]

    def delete_files(self, paths: List[str]) -> None:
        for path in paths:
            bucket, key = S3Storage.parse_s3_uri(path)
            self._client.delete_object(
                Bucket=bucket,
                Key=key,
            )

    @staticmethod
    def parse_s3_uri(uri: str) -> Tuple[str, str]:
        """parse S3 URI and return (bucket, key)"""

        result = urlparse(uri)
        if result.scheme != "s3":
            raise ValueError(f"invalid S3 URI {uri}")

        return result.netloc, result.path.strip("/")
