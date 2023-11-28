from metaphor.common.base_config import OutputConfig
from metaphor.tableau.config import TableauRunConfig, TableauTokenAuthConfig


def test_yaml_config(test_root_dir):
    config = TableauRunConfig.from_yaml_file(f"{test_root_dir}/tableau/config.yml")

    assert config == TableauRunConfig(
        server_url="https://10ay.online.tableau.com",
        site_name="abc",
        alternative_base_url="https://tableau.my_company.com",
        access_token=TableauTokenAuthConfig(
            token_name="foo",
            token_value="bar",
        ),
        user_password=None,
        snowflake_account="snow",
        bigquery_project_name_to_id_map={"bq_name": "bq_id"},
        disable_preview_image=True,
        output=OutputConfig(),
    )


def test_yaml_config_no_optional(test_root_dir):
    config = TableauRunConfig.from_yaml_file(
        f"{test_root_dir}/tableau/config_no_optional.yml"
    )

    assert config == TableauRunConfig(
        server_url="https://10ay.online.tableau.com",
        site_name="abc",
        access_token=TableauTokenAuthConfig(
            token_name="foo",
            token_value="bar",
        ),
        user_password=None,
        snowflake_account=None,
        bigquery_project_name_to_id_map=dict(),
        disable_preview_image=False,
        output=OutputConfig(),
    )


def test_excluded_projects(test_root_dir) -> None:
    config = TableauRunConfig.from_yaml_file(
        f"{test_root_dir}/tableau/config_extra_excluded_projects.yml"
    )
    assert config.excluded_projects == {"foo", "bar", "Personal Space"}

    config = TableauRunConfig.from_yaml_file(
        f"{test_root_dir}/tableau/config_include_personal_space.yml"
    )
    assert config.excluded_projects == set()

    config = TableauRunConfig.from_yaml_file(f"{test_root_dir}/tableau/config.yml")
    assert config.excluded_projects == {"Personal Space"}
