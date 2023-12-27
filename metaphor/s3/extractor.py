from typing import Collection
try:
    from mypy_boto3_s3 import S3Client
except ImportError:
    # Ignore this since mypy plugins are dev dependencies
    pass

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.s3.config import S3RunConfig


class S3Extractor(BaseExtractor):
    @staticmethod
    def from_config_file(config_file: str) -> "S3Extractor":
        return S3Extractor(S3RunConfig.from_yaml_file(config_file))
    
    def __init__(self, config: S3RunConfig) -> None:
        super().__init__(config)
        self._client: "S3Client" = config.s3_client() # type: ignore

    async def extract(self) -> Collection[ENTITY_TYPES]:
        entities = []
        return entities