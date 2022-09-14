import pytest
from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.manual.metadata.config import CustomMetadataConfig
from metaphor.manual.metadata.extractor import CustomMetadataExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
    config = CustomMetadataConfig.from_yaml_file(
        f"{test_root_dir}/manual/metadata/config.yml"
    )
    extractor = CustomMetadataExtractor(config)

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/manual/metadata/expected.json")
