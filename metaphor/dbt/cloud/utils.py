from metaphor.dbt.cloud.discovery_api.generated.get_environment_adapter_type import (
    GetEnvironmentAdapterTypeEnvironment,
)
from metaphor.models.metadata_change_event import DataPlatform

ADAPTER_TYPE_MAPPING = {
    "DATABRICKS": DataPlatform.UNITY_CATALOG,
    "POSTGRES": DataPlatform.POSTGRESQL,
    "SNOWFLAKE": DataPlatform.SNOWFLAKE,
}


def parse_environment(environment: GetEnvironmentAdapterTypeEnvironment):
    adapter_type = (
        environment.adapter_type or "unknown"
    )  # It's possible for the environment to not have an adapter type!
    adapter_type = adapter_type.upper()
    if adapter_type in ADAPTER_TYPE_MAPPING:
        platform = ADAPTER_TYPE_MAPPING[adapter_type]
    else:
        assert (
            adapter_type in DataPlatform.__members__
        ), f"Invalid data platform {adapter_type}"
        platform = DataPlatform[adapter_type]
    return platform, environment.dbt_project_name
