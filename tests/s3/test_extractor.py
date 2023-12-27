import pytest
from testcontainers.minio import Minio, MinioContainer


@pytest.fixture(scope="module")
def minio(test_root_dir: str):
    # Just read the directory as if it's the actual object storage
    container: MinioContainer = MinioContainer().with_volume_mapping(host=f"{test_root_dir}/s3/data", container="/data", mode="rw")  # type: ignore
    with container as minio:
        yield minio.get_client()


def test_extractor(minio: Minio) -> None:
    pass
