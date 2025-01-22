from metaphor.common.aws import AwsCredentials
from metaphor.common.base_config import OutputConfig
from metaphor.quick_sight.config import QuickSightFilter, QuickSightRunConfig


def test_yaml_config(test_root_dir):
    config = QuickSightRunConfig.from_yaml_file(
        f"{test_root_dir}/quick_sight/config.yml"
    )

    assert config == QuickSightRunConfig(
        aws=AwsCredentials(
            region_name="us-east-1",
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            session_token="EXAMPLE-TOKEN",
        ),
        aws_account_id="123456789012",
        filter=QuickSightFilter(
            include_dashboard_ids=["123456789012", "123456789013"],
            exclude_dashboard_ids=["123456789014"],
        ),
        output=OutputConfig(),
    )
