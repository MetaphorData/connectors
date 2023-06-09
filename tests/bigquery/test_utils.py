from metaphor.bigquery.config import BigQueryRunConfig
from metaphor.bigquery.utils import get_project_ids
from metaphor.common.base_config import OutputConfig


def test_get_project_ids():

    config = BigQueryRunConfig(
        project_id="project1",
        key_path="foobar",
        output=OutputConfig(),
    )

    assert get_project_ids(config) == ["project1"]

    config = BigQueryRunConfig(
        project_ids=["project1", "project2"],
        key_path="foobar",
        output=OutputConfig(),
    )

    assert get_project_ids(config) == ["project1", "project2"]
