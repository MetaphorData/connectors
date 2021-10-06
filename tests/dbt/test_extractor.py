import pytest
from freezegun import freeze_time

from metaphor.common.event_util import EventUtil
from metaphor.dbt.extractor import DbtExtractor, DbtRunConfig
from tests.test_utils import load_json


@freeze_time("2000-01-01")
@pytest.mark.asyncio
async def test_trial_project(test_root_dir):
    manifest = test_root_dir + "/dbt/data/trial_manifest.json"
    catalog = test_root_dir + "/dbt/data/trial_catalog.json"
    expected = test_root_dir + "/dbt/data/trial_results.json"

    config = DbtRunConfig(
        output=None, account="metaphor", manifest=manifest, catalog=catalog
    )
    extractor = DbtExtractor()
    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(expected)


@freeze_time("2000-01-01")
@pytest.mark.asyncio
async def test_ride_share_project(test_root_dir):
    manifest = test_root_dir + "/dbt/data/ride_share_manifest.json"
    catalog = test_root_dir + "/dbt/data/ride_share_catalog.json"
    expected = test_root_dir + "/dbt/data/ride_share_results.json"

    config = DbtRunConfig(
        output=None, account="metaphor", manifest=manifest, catalog=catalog
    )
    extractor = DbtExtractor()
    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(expected)


@freeze_time("2000-01-01")
@pytest.mark.asyncio
async def test_shopify_project(test_root_dir):
    manifest = test_root_dir + "/dbt/data/shopify_manifest.json"
    catalog = test_root_dir + "/dbt/data/shopify_catalog.json"
    expected = test_root_dir + "/dbt/data/shopify_results.json"

    config = DbtRunConfig(
        output=None, account="metaphor", manifest=manifest, catalog=catalog
    )
    extractor = DbtExtractor()
    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(expected)
