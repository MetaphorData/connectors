import pytest
from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.dbt.extractor import DbtExtractor, DbtRunConfig
from tests.test_utils import load_json


@freeze_time("2000-01-01")
@pytest.mark.asyncio
async def test_extractor(test_root_dir):
    manifest = test_root_dir + "/dbt/data/manifest.json"
    catalog = test_root_dir + "/dbt/data/catalog.json"
    expected = test_root_dir + "/dbt/data/expected_results.json"

    config = DbtRunConfig(output=None, manifest=manifest, catalog=catalog)
    extractor = DbtExtractor()
    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(expected)
