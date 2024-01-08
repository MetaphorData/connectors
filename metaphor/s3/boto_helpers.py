from typing import Iterable

from metaphor.s3.config import S3RunConfig


def list_folders(
    bucket_name: str,
    prefix: str,
    config: S3RunConfig,
) -> Iterable[str]:
    s3_client = config.s3_client
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        for o in page.get("CommonPrefixes", []):
            folder: str = str(o.get("Prefix"))
            if folder.endswith("/"):
                folder = folder[:-1]
            yield folder
