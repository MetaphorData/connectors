from typing import Collection, Dict, List

import httpx

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.dbt.cloud.client import DbtAdminAPIClient
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.parser.parser import Parser
from metaphor.dbt.cloud.utils import parse_environment
from metaphor.models.crawler_run_metadata import Platform

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
        self._job_ids = config.job_ids
        self._project_ids = config.project_ids
        self._base_url = config.base_url
        self._discovery_api_url = config.discovery_api_url

        self._project_accounts: Dict[int, str] = {}
        self._entities: List[ENTITY_TYPES] = []
        self._client = DbtAdminAPIClient(
            base_url=self._base_url,
            account_id=self._account_id,
            service_token=config.service_token,
            included_env_ids=config.environment_ids,
        )
        headers = {
            "Authorization": f"Bearer {config.service_token}",
            "Content-Type": "application/json",
        }
        self._discovery_api_client = DiscoveryAPIClient(
            url=self._discovery_api_url,
            headers=headers,
            http_client=httpx.Client(timeout=None, headers=headers),
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from DBT cloud")

        for project_id in self._project_ids:
            self._job_ids.update(self._client.get_project_jobs(project_id))

        for job_id in self._job_ids:
            await self._extract_job(job_id)

        return self._entities

    async def _extract_job(self, job_id: int):
        if not self._client.is_job_included(job_id):
            logger.info(f"Ignoring job ID: {job_id}")
            return

        logger.info(f"Fetching metadata for job ID: {job_id}")

        run = self._client.get_last_successful_run(job_id)
        if not run:
            logger.warning(f"Cannot find any successful run for job ID: {job_id}")
            return

        if run.run_id in self._entities:
            logger.info(f"Found already extracted run: {run}")
            return

        logger.info(f"Last successful run: {run}")

        account = self._client.get_snowflake_account(run.project_id)
        if account is not None:
            logger.info(f"Snowflake account: {account}")

        docs_base_url = (
            f"{self._base_url}/accounts/{self._account_id}/jobs/{run.job_id}/docs"
        )
        project_explore_url = f"{self._base_url}/explore/{self._account_id}/projects/{run.project_id}/environments/production/details"

        environment = self._discovery_api_client.get_environment_adapter_type(
            run.environment_id
        ).environment
        platform, project_name = parse_environment(environment)

        job_run_parser = Parser(
            self._discovery_api_client,
            self._config,
            platform,
            account,
            project_name,
            docs_base_url,
            project_explore_url=project_explore_url,
        )
        self._entities.extend(job_run_parser.parse_run(run))
