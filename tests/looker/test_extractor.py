from unittest.mock import MagicMock

from looker_sdk.sdk.api40.models import (
    Dashboard,
    DashboardBase,
    DashboardElement,
    FolderBase,
    ResultMakerFilterables,
    ResultMakerWithIdVisConfigAndDynamicFields,
)

from metaphor.common.base_config import OutputConfig
from metaphor.common.event_util import EventUtil
from metaphor.looker.config import LookerConnectionConfig, LookerRunConfig
from metaphor.looker.extractor import LookerExtractor
from metaphor.looker.folder import FolderMetadata
from metaphor.looker.lookml_parser import Explore, Model
from tests.test_utils import load_json


def test_fetch_dashboards(test_root_dir) -> None:
    config = LookerRunConfig(
        output=OutputConfig(),
        base_url="http://test",
        client_id="id",
        client_secret="secret",
        lookml_dir=".",
        include_personal_folders=False,
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
        DashboardBase(id="1"),
        DashboardBase(id="2"),
        DashboardBase(id="3"),
    ]

    mock_dashboards = [
        # In shared folder
        Dashboard(
            id="1",
            title="first",
            description="first dashboard",
            preferred_viewer="me",
            view_count=123,
            folder=FolderBase(id="2", name="folder1"),
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
        ),
        # In personal folder
        Dashboard(
            id="2",
            title="second",
            folder=FolderBase(
                id="3",
                name="personal",
                is_personal=True,
            ),
        ),
        # In personal descendant folder
        Dashboard(
            id="3",
            title="third",
            folder=FolderBase(
                id="4",
                name="personal descendant",
                is_personal=False,
                is_personal_descendant=True,
            ),
        ),
    ]

    models = {
        "model1": Model(
            explores={
                "view1": Explore(
                    name="explore1",
                )
            }
        )
    }

    folders = {
        "1": FolderMetadata(id="1", name="shared"),
        "2": FolderMetadata(id="2", name="folder1", parent_id="1"),
        "3": FolderMetadata(id="3", name="personal"),
        "4": FolderMetadata(id="4", name="personal descendant", parent_id="3"),
    }

    extractor._sdk.dashboard.side_effect = mock_dashboards

    dashboards = extractor._fetch_dashboards(models, folders)
    events = [EventUtil.trim_event(e) for e in dashboards]
    assert events == load_json(f"{test_root_dir}/looker/expected.json")
