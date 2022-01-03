from metaphor.tableau.config import (
    TableauPasswordAuthConfig,
    TableauRunConfig,
    TableauTokenAuthConfig,
)


def test_json_config(test_root_dir):
    config = TableauRunConfig.from_json_file(f"{test_root_dir}/tableau/config.json")

    assert config == TableauRunConfig(
        server_url="https://10ax.online.tableau.com/",
        site_name="abc",
        access_token=None,
        user_password=TableauPasswordAuthConfig(
            username="yi@metaphor.io",
            password="xyz",
        ),
        output=None,
    )


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
        output=None,
    )
