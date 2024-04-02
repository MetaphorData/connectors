from typing import Collection, Dict

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

    _description = "dbt cloud metadata crawler"
    _platform = Platform.DBT_MODEL

    @staticmethod
    def from_config_file(config_file: str) -> "DbtCloudExtractor":
        return DbtCloudExtractor(DbtCloudConfig.from_yaml_file(config_file))

    def __init__(self, config: DbtCloudConfig):
        super().__init__(config)
        self._account_id = config.account_id
        self._job_ids = config.job_ids
        self._project_ids = config.project_ids
        self._service_token = config.service_token
        self._meta_ownerships = config.meta_ownerships
        self._meta_tags = config.meta_tags
        self._base_url = config.base_url

        self._entities: Dict[int, Collection[ENTITY_TYPES]] = {}
        self._client = DbtAdminAPIClient(
            base_url=self._base_url,
            account_id=self._account_id,
            service_token=self._service_token,
            included_env_ids=config.environment_ids,
        )

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching metadata from DBT cloud")

        for project_id in self._project_ids:
            self._job_ids.update(self._client.get_project_jobs(project_id))

        for job_id in self._job_ids:
            await self._extract_last_run(job_id)

        return [item for ls in self._entities.values() for item in ls]

    async def _extract_last_run(self, job_id: int):
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

        manifest_json = self._client.get_run_artifact(run, "manifest.json")
        logger.info(f"manifest.json saved to {manifest_json}")

        try:
            run_results_json = self._client.get_run_artifact(run, "run_results.json")
            logger.info(f"run_results.json saved to {run_results_json}")
        except Exception:
            logger.warning("Cannot locate run_results.json")
            run_results_json = None

        docs_base_url = (
            f"{self._base_url}/accounts/{self._account_id}/jobs/{run.job_id}/docs"
        )

        try:
            # Pass the path of the downloaded manifest file to the dbt Core extractor
            entities = await DbtExtractor(
                DbtRunConfig(
                    manifest=manifest_json,
                    run_results=run_results_json,
                    account=account,
                    docs_base_url=docs_base_url,
                    output=self._output,
                    meta_ownerships=self._meta_ownerships,
                    meta_tags=self._meta_tags,
                )
            ).extract()
            self._entities[run.run_id] = entities
        except Exception as e:
            logger.exception(f"Failed to parse artifacts for run {run}")
            self.extend_errors(e)
