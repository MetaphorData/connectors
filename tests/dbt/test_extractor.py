import pytest
from freezegun import freeze_time

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.dbt.config import DbtRunConfig
from metaphor.dbt.extractor import DbtExtractor
from tests.test_utils import load_json


@pytest.mark.asyncio
async def test_trial_project_v1(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v1",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_trial_project_v2(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v2",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_trial_project_v3(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v3",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_trial_project_v4(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v4",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_trial_project_v5(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v5",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_ride_share_project(test_root_dir):
    await _test_project(test_root_dir + "/dbt/data/ride_share", None, None, True)


@pytest.mark.asyncio
async def test_shopify_project(test_root_dir):
    await _test_project(test_root_dir + "/dbt/data/shopify", None, None, True)


@freeze_time("2000-01-01")
async def _test_project(
    data_dir, docs_base_url=None, project_source_url=None, useCatalog=False
):
    manifest = data_dir + "/manifest.json"
    catalog = data_dir + "/catalog.json" if useCatalog else None
    expected = data_dir + "/results.json"

    config = DbtRunConfig(
        output=OutputConfig(),
        account="metaphor",
        manifest=manifest,
        catalog=catalog,
        docs_base_url=docs_base_url,
        project_source_url=project_source_url,
    )
    extractor = DbtExtractor()
    events = [EventUtil.trim_event(e) for e in await extractor.extract(config)]

    assert events == load_json(expected)
