import os
from typing import List, Tuple, Union
from urllib.parse import urlparse

import boto3
from smart_open import open

# Give S3 bucket owner full control over the new object
# See https://github.com/RaRe-Technologies/smart_open/blob/develop/howto.md#how-to-pass-additional-parameters-to-boto3
OWNER_FULL_CONTROL_ACL = {
    "client_kwargs": {
        "S3.Client.put_object": {"ACL": "bucket-owner-full-control"},
        "S3.Client.create_multipart_upload": {"ACL": "bucket-owner-full-control"},
    }
}


def write_file(
    path: str, payload: Union[str, bytes], binary_mode=False, s3_session=None
) -> None:
    transport_params = None
    if path.startswith("s3://"):
        transport_params = {
            **OWNER_FULL_CONTROL_ACL,
            # Use supplied credentials
            # See https://github.com/RaRe-Technologies/smart_open#s3-credentials
            "client": None if s3_session is None else s3_session.client("s3"),
        }
    else:
        os.makedirs(os.path.expanduser(os.path.dirname(path)), exist_ok=True)

    mode = "wb" if binary_mode else "w"
    with open(path, mode, transport_params=transport_params) as fp:
        fp.write(payload)


def list_files(path: str, suffix: str, s3_session=None) -> List[str]:
    if path.startswith("s3://"):
        client = (s3_session or boto3.Session()).client("s3")
        bucket, key = parse_s3_uri(path)

        resp = client.list_objects_v2(
            Bucket=bucket,
            Prefix=key,
        )
        # TODO: handle pagination

        objects = resp.get("Contents", [])
        return [
            f"s3://{bucket}/{file['Key']}"
            for file in objects
            if file.get("Key").endswith(suffix)
        ]
    else:
        return [
            os.path.join(path, file)
            for file in os.listdir(path)
            if file.endswith(suffix)
        ]


def delete_files(paths: List[str], s3_session=None) -> None:
    for path in paths:
        if path.startswith("s3://"):
            client = (s3_session or boto3.Session()).client("s3")
            bucket, key = parse_s3_uri(path)

            client.delete_object(
                Bucket=bucket,
                Key=key,
            )
        else:
            os.remove(path)


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    result = urlparse(uri)
    if result.scheme != "s3":
        raise ValueError(f"invalid S3 URI {uri}")

    return result.netloc, result.path.strip("/")
