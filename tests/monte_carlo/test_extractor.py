from unittest.mock import MagicMock, patch

import pytest

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.monte_carlo.config import MonteCarloRunConfig
from metaphor.monte_carlo.extractor import MonteCarloExtractor
from tests.test_utils import load_json


def dummy_config():
    return MonteCarloRunConfig(
        api_key_id="key_id",
        api_key_secret="key_secret",
        snowflake_account="snow",
        output=OutputConfig(),
    )


@patch("pycarlo.core.Client")
@pytest.mark.asyncio
async def test_extractor(mock_pycarlo_client: MagicMock, test_root_dir: str):
    mock_pycarlo_client.side_effect = [
        {
            "get_tables": {
                "edges": [
                    {
                        "node": {
                            "mcon": "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test1",
                            "warehouse": {"connection_type": "SNOWFLAKE"},
                        }
                    },
                    {
                        "node": {
                            "mcon": "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test2",
                            "warehouse": {"connection_type": "SNOWFLAKE"},
                        }
                    },
                    {
                        "node": {
                            "mcon": "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test3",
                            "warehouse": {"connection_type": "SNOWFLAKE"},
                        }
                    },
                    {
                        "node": {
                            "mcon": "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test4",
                            "warehouse": {"connection_type": "SNOWFLAKE"},
                        }
                    },
                ],
                "page_info": {
                    "end_corsor": "cursor",
                    "has_next_page": False,
                },
            }
        },
        {
            "get_monitors": [
                {
                    "uuid": "e0dc143e-dd8a-4cb9-b4cc-dedec715d955",
                    "name": "auto_monitor_name_cd5b69bd-e465-4545-b3f9-a5d507ea766c",
                    "description": "Field Health for all fields in db:metaphor.test1",
                    "entities": ["db:metaphor.test1"],
                    "entityMcons": [
                        "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test1"
                    ],
                    "priority": "P1",
                    "monitorStatus": "MISCONFIGURED",
                    "monitorFields": None,
                    "creatorId": "yi@metaphor.io",
                    "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
                    "exceptions": None,
                },
                {
                    "uuid": "ce4c4568-35f4-4365-a6fe-95f233fcf6c3",
                    "name": "auto_monitor_name_53c985e6-8f49-4af7-8ef1-7b402a27538b",
                    "description": "Field Health for all fields in db:metaphor.test2",
                    "entities": ["db:metaphor.test2"],
                    "entityMcons": [
                        "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test2"
                    ],
                    "priority": "P2",
                    "monitorStatus": "SUCCESS",
                    "monitorFields": ["foo", "bar"],
                    "creatorId": "yi@metaphor.io",
                    "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
                    "exceptions": None,
                },
                {
                    "uuid": "2c156c8d-ab4a-432f-b8bb-f9ea9f31ed3d",
                    "name": "auto_monitor_name_18637195-a3c4-416e-a3e2-a89cc10adbc8",
                    "description": "Field Health for all fields in db:metaphor.test3",
                    "entities": ["db:metaphor.test3"],
                    "entityMcons": [
                        "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test3"
                    ],
                    "priority": "P3",
                    "monitorStatus": "ERROR",
                    "monitorFields": None,
                    "creatorId": "yi@metaphor.io",
                    "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
                    "exceptions": None,
                },
                {
                    "uuid": "d14af7d8-6342-420a-bb09-5805fad677f1",
                    "name": "auto_monitor_name_693b98e3-950d-472b-83fe-8c8e5b5979f9",
                    "description": "Field Health for all fields in db:metaphor.test4",
                    "entities": ["db:metaphor.test4"],
                    "entityMcons": [
                        "MCON++6418a1e2-9718-4413-9d2b-6a354e01ddf8++a19e22b4-7659-4064-8fd4-8d6122fabe1c++table++db:metaphor.test4"
                    ],
                    "priority": None,
                    "monitorStatus": "ERROR",
                    "exceptions": "Oh no",
                    "monitorFields": None,
                    "creatorId": "yi@metaphor.io",
                    "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
                },
                {
                    "uuid": "d14af7d8-6342-420a-bb09-5805fad677f1",
                    "name": "auto_monitor_name_693b98e3-950d-472b-83fe-8c8e5b5979fa",
                    "description": "Monitor with no entities",
                    "entities": None,
                    "entityMcons": None,
                    "priority": None,
                    "monitorStatus": "ERROR",
                    "exceptions": "No entity, this exception is ignored",
                    "monitorFields": None,
                    "creatorId": "yi@metaphor.io",
                    "prevExecutionTime": "2023-06-23T03:54:35.817000+00:00",
                },
            ]
        },
    ]

    config = dummy_config()
    config.ignored_errors = ["Ignore me"]

    extractor = MonteCarloExtractor(config)
    extractor._client = mock_pycarlo_client

    events = [EventUtil.trim_event(e) for e in await extractor.extract()]

    assert events == load_json(f"{test_root_dir}/monte_carlo/expected.json")
