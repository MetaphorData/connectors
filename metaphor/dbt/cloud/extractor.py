from typing import Collection, Dict

import httpx

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.dbt.cloud.client import DbtAdminAPIClient, DbtProject
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.http import LogTransport
from metaphor.dbt.cloud.parser.common import extract_platform_and_account
from metaphor.dbt.cloud.parser.env_parser import EnvironmentParser
from metaphor.dbt.util import should_be_included
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import Dataset, Metric, VirtualView

logger = get_logger()


class DbtCloudExtractor(BaseExtractor):
    """
    dbt cloud metadata extractor
    """

    _description = "dbt cloud metadata crawler"
    _platform = Platform.DBT_MODEL

    @staticmethod
    def from_config_file(config_file: str) -> "DbtCloudExtractor":
        return DbtCloudExtractor(DbtCloudConfig.from_yaml_file(config_file))

    def __init__(self, config: DbtCloudConfig):
        super().__init__(config)
        self._config = config
        self._account_id = config.account_id
        self._project_ids = config.project_ids
        self._environment_ids = config.environment_ids
        self._base_url = config.base_url
        self._discovery_api_url = config.discovery_api_url

        self._datasets: Dict[str, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._metrics: Dict[str, Metric] = {}

        self._client = DbtAdminAPIClient(
            base_url=self._base_url,
            account_id=self._account_id,
            service_token=config.service_token,
        )
        headers = {
            "Authorization": f"Bearer {config.service_token}",
            "Content-Type": "application/json",
        }
        self._discovery_api_client = DiscoveryAPIClient(
            url=self._discovery_api_url,
            headers=headers,
            http_client=httpx.Client(
                timeout=30,
                headers=headers,
                transport=LogTransport(httpx.HTTPTransport()),
            ),
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from DBT cloud")

        projects = self._client.list_projects()
        for project in projects:
            if not self._project_ids or project.id in self._project_ids:
                await self._extract_project(project)

        datasets = [d for d in self._datasets.values() if should_be_included(d)]
        views = [v for v in self._virtual_views.values() if should_be_included(v)]
        metrics = [m for m in self._metrics.values() if should_be_included(m)]

        return datasets + views + metrics

    async def _extract_project(self, project: DbtProject):
        platform, account = extract_platform_and_account(project)
        project_explore_url = f"{self._base_url}/explore/{self._account_id}/projects/{project.id}/environments/production/details"

        logger.info(f"Extracting project: {project.id}")
        environments = self._client.list_environments(project.id)
        for environment in environments:
            if (
                environment.type == "deployment"
                and environment.deployment_type == "production"
                and (
                    not self._environment_ids or environment.id in self._environment_ids
                )
            ):  # only fetch production environment
                parser = EnvironmentParser(
                    self._discovery_api_client,
                    self._config,
                    platform,
                    account,
                    project_explore_url,
                    self._datasets,
                    self._virtual_views,
                    self._metrics,
                )
                parser.parse(environment.id)
            else:
                logger.info(f"Skipping environment {environment.id}")
