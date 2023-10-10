from metaphor.common.base_config import OutputConfig
from metaphor.dbt.cloud.config import DbtCloudConfig


def test_yaml_config(test_root_dir):
    config = DbtCloudConfig.from_yaml_file(f"{test_root_dir}/dbt/cloud/config.yml")

    assert config == DbtCloudConfig(
        account_id=1234,
        job_id=None,
        job_ids=[5678, 9012],
        service_token="token",
        base_url="https://cloud.metaphor.getdbt.com",
        output=OutputConfig(),
    )


def test_yaml_config_legacy(test_root_dir):
    config = DbtCloudConfig.from_yaml_file(
        f"{test_root_dir}/dbt/cloud/config_legacy.yml"
    )

    assert config == DbtCloudConfig(
        account_id=1234,
        job_id=5678,
        job_ids=[],
        service_token="token",
        base_url="https://cloud.metaphor.getdbt.com",
        output=OutputConfig(),
    )
