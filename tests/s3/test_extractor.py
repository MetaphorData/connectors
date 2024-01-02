import pytest
from testcontainers.minio import MinioContainer

from metaphor.common.event_util import EventUtil
from metaphor.s3.config import S3RunConfig
from metaphor.s3.extractor import S3Extractor
from tests.test_utils import ignore_datetime_values, load_json


@pytest.fixture(scope="module")
def minio_container(test_root_dir: str):
    # Just read the directory as if it's the actual object storage
    container: MinioContainer = MinioContainer(image="minio/minio:RELEASE.2021-11-03T03-36-36Z").with_volume_mapping(host=f"{test_root_dir}/s3/data", container="/data", mode="rw")  # type: ignore
    with container as minio_container:
        yield minio_container


@pytest.mark.asyncio
async def test_extractor(minio_container: MinioContainer, test_root_dir: str) -> None:
    port = minio_container.get_exposed_port(9000)
    config = S3RunConfig.from_yaml_file(f"{test_root_dir}/s3/config.yml")
    config.endpoint_url = f"http://localhost:{port}"
    extractor = S3Extractor(config)
    events = [EventUtil.trim_event(entity) for entity in await extractor.extract()]
    expected = f"{test_root_dir}/s3/expected.json"

    assert ignore_datetime_values(events) == ignore_datetime_values(load_json(expected))
