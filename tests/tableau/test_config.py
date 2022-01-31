from metaphor.common.base_config import OutputConfig
from metaphor.tableau.config import TableauRunConfig, TableauTokenAuthConfig


def test_yaml_config(test_root_dir):
    config = TableauRunConfig.from_yaml_file(f"{test_root_dir}/tableau/config.yml")

    assert config == TableauRunConfig(
        server_url="https://10ay.online.tableau.com",
        site_name="abc",
        access_token=TableauTokenAuthConfig(
            token_name="foo",
            token_value="bar",
        ),
        user_password=None,
        snowflake_account="snow",
        bigquery_project_name_to_id_map={"bq_name": "bq_id"},
        output=OutputConfig(),
    )
