import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.dbt.artifact_parser import ArtifactParser
from metaphor.dbt.config import DbtRunConfig, MetaOwnership, MetaTag
from metaphor.dbt.extractor import DbtExtractor
from tests.test_utils import load_json


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
async def test_trial_project_v6(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v6",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_trial_project_v7(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v7",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_trial_project_v8(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v8",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_trial_project_v9(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/trial_v9",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/trial",
    )


@pytest.mark.asyncio
async def test_jaffle_v10(test_root_dir):
    await _test_project(
        test_root_dir + "/dbt/data/jaffle_v10",
        "http://localhost:8080",
        "https://github.com/MetaphorData/dbt/tree/main/jaffle-sl-template",
    )


async def _test_project(
    data_dir, docs_base_url=None, project_source_url=None, useCatalog=False
):
    manifest = data_dir + "/manifest.json"
    run_results = data_dir + "/run_results.json"
    expected = data_dir + "/results.json"

    config = DbtRunConfig(
        output=OutputConfig(),
        account="metaphor",
        manifest=manifest,
        run_results=run_results,
        docs_base_url=docs_base_url,
        project_source_url=project_source_url,
        meta_ownerships=[MetaOwnership(meta_key="owner", ownership_type="Maintainer")],
        meta_tags=[MetaTag(meta_key="pii", tag_type="PII")],
    )
    extractor = DbtExtractor(config)
    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(expected)


def test_sanitize_manifest_empty_docs(test_root_dir):
    manifest = {
        "docs": {"foo": "bar"},
    }

    assert ArtifactParser.sanitize_manifest(manifest, "v10") == {
        "docs": {},
    }


def test_sanitize_manifest_strip_null_tests_depends_on(test_root_dir):
    manifest = {
        "nodes": {
            "test.example": {
                "depends_on": {
                    "macros": [None, "macro1"],
                    "nodes": ["node1", None, "node2"],
                }
            }
        }
    }

    assert ArtifactParser.sanitize_manifest(manifest, "v10") == {
        "nodes": {
            "test.example": {
                "depends_on": {
                    "macros": ["macro1"],
                    "nodes": ["node1", "node2"],
                }
            }
        }
    }


def test_sanitize_manifest_strip_semantic_models_labels(test_root_dir):
    manifest = {
        "semantic_models": {
            "model1": {
                "label": "foo",
                "entities": [
                    {
                        "name": "entity1",
                        "label": "bar",
                    }
                ],
                "dimensions": [
                    {
                        "name": "dim1",
                        "label": "baz",
                    }
                ],
                "measures": [
                    {
                        "name": "measure1",
                        "label": "qux",
                    }
                ],
            }
        }
    }

    assert ArtifactParser.sanitize_manifest(manifest, "v10") == {
        "semantic_models": {
            "model1": {
                "entities": [
                    {
                        "name": "entity1",
                    }
                ],
                "dimensions": [
                    {
                        "name": "dim1",
                    }
                ],
                "measures": [
                    {
                        "name": "measure1",
                    }
                ],
            }
        }
    }


def test_sanitize_manifest_strip_metric_type_params_extra_fields(test_root_dir):
    manifest = {
        "metrics": {
            "metric1": {
                "type_params": {
                    "measure": {
                        "name": "measure1",
                        "fill_nulls_with": "foo",
                        "join_to_timespine": "bar",
                    },
                    "input_measures": [
                        {
                            "name": "measure2",
                            "fill_nulls_with": "foo",
                            "join_to_timespine": "bar",
                        }
                    ],
                }
            }
        }
    }

    assert ArtifactParser.sanitize_manifest(manifest, "v10") == {
        "metrics": {
            "metric1": {
                "type_params": {
                    "measure": {
                        "name": "measure1",
                    },
                    "input_measures": [
                        {
                            "name": "measure2",
                        }
                    ],
                }
            }
        }
    }
