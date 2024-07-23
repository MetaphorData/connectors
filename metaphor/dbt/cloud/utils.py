from metaphor.dbt.cloud.discovery_api.graphql_client.get_environment_adapter_type import (
    GetEnvironmentAdapterTypeEnvironment,
)
from metaphor.models.metadata_change_event import DataPlatform


def adapter_type_to_data_platform(environment: GetEnvironmentAdapterTypeEnvironment):
    environment.adapter_type
    adapter_type = environment.adapter_type or ""
    adapter_type = adapter_type.upper()
    if adapter_type == "DATABRICKS":
        return DataPlatform.UNITY_CATALOG
    assert (
        adapter_type in DataPlatform.__members__
    ), f"Invalid data platform {adapter_type}"
    return DataPlatform[adapter_type]
