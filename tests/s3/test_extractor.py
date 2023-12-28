import pytest
from testcontainers.minio import MinioContainer

from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.s3.config import S3PathSpec, S3RunConfig
from metaphor.s3.extractor import S3Extractor
from tests.test_utils import load_json


@pytest.fixture(scope="module")
def minio_container(test_root_dir: str):
    # Just read the directory as if it's the actual object storage
    container: MinioContainer = MinioContainer(image="minio/minio:RELEASE.2021-11-03T03-36-36Z").with_volume_mapping(host=f"{test_root_dir}/s3/data", container="/data", mode="rw")  # type: ignore
    with container as minio_container:
        yield minio_container


@pytest.mark.asyncio
async def test_extractor(minio_container: MinioContainer, test_root_dir: str) -> None:
    port = minio_container.get_exposed_port(9000)
    extractor = S3Extractor(
        S3RunConfig(
            output=OutputConfig(),
            aws=AwsCredentials(
                access_key_id="minioadmin",
                secret_access_key="minioadmin",
                region_name="us-west-2",
            ),
            path_specs=[
                S3PathSpec(
                    uri="s3://bucket/directory/*/*/*.*",
                    file_types={
                        "csv",
                        "json",
                        "tsv",
                    },
                )
            ],
            endpoint_url=f"http://localhost:{port}",
        )
    )
    events = [EventUtil.trim_event(entity) for entity in await extractor.extract()]
    assert events == load_json(f"{test_root_dir}/s3/expected.json")
