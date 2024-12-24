import json
from typing import Any, List, Optional, Union

from metaphor.dbt.cloud.client import DbtEnvironment, DbtProject
from metaphor.dbt.cloud.discovery_api.generated.base_model import UNSET, UnsetType
from metaphor.dbt.cloud.discovery_api.generated.get_lineage import GetLineage
from metaphor.dbt.cloud.discovery_api.generated.get_macros import GetMacros
from metaphor.dbt.cloud.discovery_api.generated.get_metrics import GetMetrics
from metaphor.dbt.cloud.discovery_api.generated.get_models import GetModels
from metaphor.dbt.cloud.discovery_api.generated.get_snapshots import GetSnapshots
from metaphor.dbt.cloud.discovery_api.generated.get_sources import GetSources
from metaphor.dbt.cloud.discovery_api.generated.input_types import LineageFilter


class MockAdminClient:

    def __init__(self, test_root_dir: str):
        self.test_root_dir = test_root_dir

    def list_projects(self) -> List[DbtProject]:
        with open(f"{self.test_root_dir}/dbt/cloud/data/list_project.json") as f:
            data = json.load(f)
        return [DbtProject.model_validate(project) for project in data]

    def list_environments(self, project_id: int) -> List[DbtEnvironment]:
        with open(f"{self.test_root_dir}/dbt/cloud/data/list_environment.json") as f:
            data = json.load(f)
        return [DbtEnvironment.model_validate(env) for env in data]


class MockDiscoveryClient:

    def __init__(self, test_root_dir: str):
        self.test_root_dir = test_root_dir

    def get_models(
        self,
        environment_id: Any,
        first: Union[Optional[int], UnsetType] = UNSET,
        after: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> GetModels:
        with open(f"{self.test_root_dir}/dbt/cloud/data/get_models.json") as f:
            data = json.load(f)
        return GetModels.model_validate(data)

    def get_snapshots(
        self,
        environment_id: Any,
        first: Union[Optional[int], UnsetType] = UNSET,
        after: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> GetSnapshots:
        with open(f"{self.test_root_dir}/dbt/cloud/data/get_snapshots.json") as f:
            data = json.load(f)
        return GetSnapshots.model_validate(data)

    def get_sources(
        self,
        environment_id: Any,
        first: Union[Optional[int], UnsetType] = UNSET,
        after: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> GetSources:
        with open(f"{self.test_root_dir}/dbt/cloud/data/get_sources.json") as f:
            data = json.load(f)
        return GetSources.model_validate(data)

    def get_metrics(
        self,
        environment_id: Any,
        first: Union[Optional[int], UnsetType] = UNSET,
        after: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> GetMetrics:
        with open(f"{self.test_root_dir}/dbt/cloud/data/get_metrics.json") as f:
            data = json.load(f)
        return GetMetrics.model_validate(data)

    def get_macros(
        self,
        environment_id: Any,
        first: Union[Optional[int], UnsetType] = UNSET,
        after: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> GetMacros:
        with open(f"{self.test_root_dir}/dbt/cloud/data/get_macros.json") as f:
            data = json.load(f)
        return GetMacros.model_validate(data)

    def get_lineage(
        self, environment_id: Any, filter: LineageFilter, **kwargs: Any
    ) -> GetLineage:
        with open(f"{self.test_root_dir}/dbt/cloud/data/get_lineage.json") as f:
            data = json.load(f)
        return GetLineage.model_validate(data)
