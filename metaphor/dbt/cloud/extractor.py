from typing import Collection, List

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.dbt.cloud.client import DbtAdminAPIClient
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.config import DbtRunConfig
from metaphor.dbt.extractor import DbtExtractor
from metaphor.models.crawler_run_metadata import Platform

logger = get_logger()


class DbtCloudExtractor(BaseExtractor):
    """
    dbt cloud metadata extractor
    """

    @staticmethod
    def from_config_file(config_file: str) -> "DbtCloudExtractor":
        return DbtCloudExtractor(DbtCloudConfig.from_yaml_file(config_file))

    def __init__(self, config: DbtCloudConfig):
        super().__init__(config, "dbt cloud metadata crawler", Platform.DBT_MODEL)
        self._account_id = config.account_id
        self._job_ids = (
            [config.job_id]
            if config.job_id and len(config.job_ids) == 0
            else config.job_ids
        )
        self._service_token = config.service_token
        self._meta_ownerships = config.meta_ownerships
        self._meta_tags = config.meta_tags
        self._base_url = config.base_url

        self._entities: List[ENTITY_TYPES] = []
        self._client = DbtAdminAPIClient(
            base_url=self._base_url,
            account_id=self._account_id,
            service_token=self._service_token,
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from DBT cloud")

        for job_id in self._job_ids:
            await self._extract_job(job_id)

        return self._entities

    async def _extract_job(self, job_id: int):
        logger.info(f"Fetching metadata for job ID: {job_id}")

        project_id, run_id = self._client.get_last_successful_run(job_id)
        logger.info(f"Last successful run ID: {run_id}, project ID: {project_id}")

        account = self._client.get_snowflake_account(project_id)
        if account is not None:
            logger.info(f"Snowflake account: {account}")

        manifest_json = self._client.get_run_artifact(job_id, run_id, "manifest.json")
        logger.info(f"manifest.json saved to {manifest_json}")

        docs_base_url = (
            f"{self._base_url}/accounts/{self._account_id}/jobs/{job_id}/docs"
        )

        # Pass the path of the downloaded manifest file to the dbt Core extractor
        entities = await DbtExtractor(
            DbtRunConfig(
                manifest=manifest_json,
                account=account,
                docs_base_url=docs_base_url,
                output=self._output,
                meta_ownerships=self._meta_ownerships,
                meta_tags=self._meta_tags,
            )
        ).extract()
        self._entities.extend(entities)
