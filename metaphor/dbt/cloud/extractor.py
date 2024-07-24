from collections import defaultdict
from typing import Collection, Dict, List, Optional, Set

import httpx

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.dbt.cloud.client import DbtAdminAPIClient
from metaphor.dbt.cloud.config import DbtCloudConfig
from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.graphql_client.get_job_tests import (
    GetJobTestsJobTests,
)
from metaphor.dbt.cloud.parser.parser import Parser
from metaphor.dbt.cloud.utils import parse_environment
from metaphor.dbt.config import DbtRunConfig
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import (
    DataPlatform,
    Dataset,
    Metric,
    VirtualView,
)

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
        self._meta_key_tags = config.meta_key_tags
        self._base_url = config.base_url
        self._discovery_api_url = config.discovery_api_url

        self._project_accounts: Dict[int, str] = {}
        self._entities: Dict[int, Collection[ENTITY_TYPES]] = {}
        self._client = DbtAdminAPIClient(
            base_url=self._base_url,
            account_id=self._account_id,
            service_token=self._service_token,
            included_env_ids=config.environment_ids,
        )
        headers = {
            "Authorization": f"Bearer {self._service_token}",
            "Content-Type": "application/json",
        }
        self._discovery_api_client = DiscoveryAPIClient(
            url=self._discovery_api_url,
            headers=headers,
            http_client=httpx.Client(timeout=None, headers=headers),
        )

        self._datasets: Dict[str, Dataset] = {}
        self._virtual_views: Dict[str, VirtualView] = {}
        self._metrics: Dict[str, Metric] = {}
        self._referenced_virtual_views: Set[str] = set()

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

        docs_base_url = (
            f"{self._base_url}/accounts/{self._account_id}/jobs/{run.job_id}/docs"
        )

        environment = self._discovery_api_client.get_environment_adapter_type(
            run.environment_id
        ).environment
        platform, project_name = parse_environment(environment)

        conf = DbtRunConfig(
            manifest="",
            run_results=None,  # Instead of getting test results from `run_results.json`, we get them from discovery API after we parse the manifest
            account=account,
            docs_base_url=docs_base_url,
            output=self._output,
            meta_ownerships=self._meta_ownerships,
            meta_tags=self._meta_tags,
            meta_key_tags=self._meta_key_tags,
        )

        Parser(
            self._discovery_api_client,
            conf,
            platform,
            project_name,
        ).parse_run(run)

    def _extend_test_run_results_entities(
        self,
        platform: DataPlatform,
        account: Optional[str],
        job_id: int,
        entities: Collection[ENTITY_TYPES],
    ):
        logger.info("Parsing test run results")

        new_monitor_datasets: List[Dataset] = list()

        # Get all test nodes from discovery API
        test_nodes_by_model_uid: Dict[str, List[GetJobTestsJobTests]] = defaultdict(
            list
        )
        job = self._discovery_api_client.get_job_tests(job_id).job
        assert job is not None
        for test in job.tests:
            for model in [x for x in test.depends_on if x.startswith("model.")]:
                test_nodes_by_model_uid[model].append(test)

        # job = self._discovery_api_client.get_job_models(job_id).job
        # assert job is not None
        # model_names = {
        #     model.unique_id: dataset_normalized_name(
        #         model.database, model.schema_, model.name
        #     )
        #     for model in job.models
        # }

        # # Go thru the virtual views
        # for entity in entities:
        #     if not isinstance(entity, VirtualView):
        #         continue
        #     if not entity.logical_id or not entity.logical_id.name:
        #         continue

        #     model_unique_id = f"model.{entity.logical_id.name}"

        #     if (
        #         model_unique_id not in test_nodes_by_model_uid
        #         or model_unique_id not in model_names
        #     ):
        #         continue

        #     dataset_logical_id = DatasetLogicalID(
        #         name=model_names[model_unique_id],
        #         platform=platform,
        #         account=account,
        #     )

        #     dataset = Dataset(
        #         logical_id=dataset_logical_id,
        #     )

        #     # Go thru the tests in this dbt model
        #     for test in test_nodes_by_model_uid[model_unique_id]:
        #         if not test.name:
        #             continue

        #         status = dbt_run_result_output_data_monitor_status_map[
        #             test.status or "skipped"
        #         ]

        #         add_data_quality_monitor(
        #             dataset,
        #             test.name,
        #             test.column_name,
        #             status,
        #             test.execute_completed_at,
        #         )

        #     if dataset.data_quality and dataset.data_quality.monitors:
        #         new_monitor_datasets.append(dataset)

        # return list(entities) + new_monitor_datasets
