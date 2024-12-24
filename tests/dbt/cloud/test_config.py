from metaphor.common.base_config import OutputConfig
from metaphor.dbt.cloud.config import DbtCloudConfig


def test_yaml_config(test_root_dir):
    config = DbtCloudConfig.from_yaml_file(f"{test_root_dir}/dbt/cloud/config.yml")

    assert config == DbtCloudConfig(
        account_id=1234,
        project_ids=[5678, 9012],
        environment_ids=[1, 2, 3],
        service_token="token",
        meta_key_tags="tags",
        base_url="https://cloud.metaphor.getdbt.com",
        output=OutputConfig(),
    )
