from typing import Union

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
):

    transport_params = None
    if path.startswith("s3://"):
        transport_params = {
            **OWNER_FULL_CONTROL_ACL,
            # Use supplied credentials
            # See https://github.com/RaRe-Technologies/smart_open#s3-credentials
            "client": None if s3_session is None else s3_session.client("s3"),
        }

    mode = "wb" if binary_mode else "w"
    with open(path, mode, transport_params=transport_params) as fp:
        fp.write(payload)
