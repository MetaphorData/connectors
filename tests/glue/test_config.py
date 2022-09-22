from metaphor.common.base_config import OutputConfig
from metaphor.glue.config import AwsCredentials, GlueRunConfig


def test_yaml_config(test_root_dir):
    config = GlueRunConfig.from_yaml_file(f"{test_root_dir}/glue/config.yml")

    assert config == GlueRunConfig(
        aws=AwsCredentials(
            access_key_id="key",
            secret_access_key="secret_access_key",
            region_name="region",
            assume_role_arn="role:arn",
        ),
        output=OutputConfig(),
    )
