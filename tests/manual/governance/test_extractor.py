import pytest
from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.manual.governance.config import ManualGovernanceConfig
from metaphor.manual.governance.extractor import ManualGovernanceExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
@freeze_time("2000-01-01")
async def test_extractor(test_root_dir):
    config = ManualGovernanceConfig.from_yaml_file(
        f"{test_root_dir}/manual/governance/config.yml"
    )
    extractor = ManualGovernanceExtractor()

    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(f"{test_root_dir}/manual/governance/expected.json")
