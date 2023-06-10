import pytest

from metaphor.common.event_util import EventUtil
from metaphor.custom.metadata.config import CustomMetadataConfig
from metaphor.custom.metadata.extractor import CustomMetadataExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    config = CustomMetadataConfig.from_yaml_file(
        f"{test_root_dir}/custom/metadata/config.yml"
    )
    extractor = CustomMetadataExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/custom/metadata/expected.json")
