from smart_open import open

# Give S3 bucket owner full control over the new object
# See https://github.com/RaRe-Technologies/smart_open/blob/develop/howto.md#how-to-pass-additional-parameters-to-boto3
OWNER_FULL_CONTROL_ACL = {
    "client_kwargs": {
        "S3.Client.put_object": {"ACL": "bucket-owner-full-control"},
        "S3.Client.create_multipart_upload": {"ACL": "bucket-owner-full-control"},
    }
}


def write_file(path: str, payload: str):
    params = OWNER_FULL_CONTROL_ACL if path.startswith("s3://") else None
    with open(path, "w", transport_params=params) as fp:
        fp.write(payload)
