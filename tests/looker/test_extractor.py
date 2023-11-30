from types import SimpleNamespace
from unittest.mock import MagicMock

from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardElement,
    ResultMakerFilterables,
    ResultMakerWithIdVisConfigAndDynamicFields,
)

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.looker.config import LookerConnectionConfig, LookerRunConfig
from metaphor.looker.extractor import LookerExtractor
from metaphor.looker.lookml_parser import Explore, Model
from tests.test_utils import load_json


def test_fetch_dashboards(test_root_dir) -> None:
    config = LookerRunConfig(
        output=OutputConfig(),
        base_url="http://test",
        client_id="id",
        client_secret="secret",
        lookml_dir=".",
        connections={
            "conn": LookerConnectionConfig(
                "database",
                "account",
            )
        },
    )
    extractor = LookerExtractor(config)
    extractor._sdk = MagicMock()
    extractor._sdk.all_dashboards = MagicMock()
    extractor._sdk.all_dashboards.return_value = [
        SimpleNamespace(id="1"),
    ]

    def mock_dashboard(dashboard_id: str):
        return Dashboard(
            id="model1::1",
            title="first",
            description="first dashboard",
            preferred_viewer="me",
            view_count=123,
            dashboard_elements=[
                DashboardElement(
                    type="vis",
                    result_maker=ResultMakerWithIdVisConfigAndDynamicFields(
                        vis_config={
                            "type": "looker_map",
                        },
                        filterables=[
                            ResultMakerFilterables(
                                model="model1",
                                view="view1",
                            )
                        ],
                    ),
                )
            ],
        )

    models = {
        "model1": Model(
            explores={
                "view1": Explore(
                    name="explore1",
                )
            }
        )
    }

    extractor._sdk.dashboard = mock_dashboard
    dashboards = extractor._fetch_dashboards(models)
    assert len(dashboards) == 1
    events = [EventUtil.trim_event(e) for e in dashboards]
    assert events == load_json(f"{test_root_dir}/looker/expected.json")
