import enum
import signal
from typing import Any, Callable, Dict, List, Tuple

import boto3
from pydantic.dataclasses import dataclass
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from metaphor.common.aws import AwsCredentials
from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.quick_sight.models import Dashboard, DataSet, DataSource, ResourceType

logger = get_logger()


def create_quick_sight_client(aws: AwsCredentials) -> boto3.client:
    return aws.get_session().client("quicksight")


class Endpoint(enum.Enum):
    list_dashboards = "list_dashboards"
    list_data_sets = "list_data_sets"
    list_data_sources = "list_data_sources"


@dataclass
class EndpointDictKeys:
    list_key: str
    item_key: str
    name_key: str


ENDPOINT_SETTING = {
    Endpoint.list_data_sets: EndpointDictKeys("DataSetSummaries", "DataSetId", "Name"),
    Endpoint.list_dashboards: EndpointDictKeys(
        "DashboardSummaryList", "DashboardId", "Name"
    ),
    Endpoint.list_data_sources: EndpointDictKeys("DataSources", "DataSourceId", "Name"),
}


class Client:
    def __init__(
        self,
        aws: AwsCredentials,
        aws_account_id: str,
        resources: Dict[str, ResourceType],
    ):
        self._client = create_quick_sight_client(aws)
        self._aws_account_id = aws_account_id
        self._resources = resources

    def get_resources(self):
        self._get_dataset_detail()
        self._get_dashboard_detail()
        self._get_data_source_detail()

    def _get_resource_ids(self, endpoint: Endpoint) -> List[Tuple[str, str]]:
        """
        List resource entities and return a list of (id, name)
        """
        paginator = self._client.get_paginator(endpoint.value)
        paginator_response = paginator.paginate(AwsAccountId=self._aws_account_id)

        results = []
        entities = []
        settings = ENDPOINT_SETTING[endpoint]
        for page in paginator_response:
            for item in page[settings.list_key]:
                results.append(item)
                entities.append((item[settings.item_key], item[settings.name_key]))

        logger.info(f"Found {len(entities)} entities from {endpoint.value}")
        json_dump_to_debug_file(results, f"{endpoint.value}.json")
        return entities

    @retry(
        retry=retry_if_exception_type(TimeoutError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=10, max=120),
    )
    def _get_resource_detail(self, func: Callable) -> Any:
        """
        Function wrapper to timeout long running queries and handle rate limit with exponential backoff and retry
        """

        def _handle_timeout(signum, frame):
            logger.error("Data fetch timeout")
            raise TimeoutError("Data fetch timeout")

        signal.signal(signal.SIGALRM, _handle_timeout)
        signal.alarm(10)  # set timeout to 10 seconds
        try:
            result = func()
            if result["Status"] == 429:
                logger.error("Rate limit hit, retrying...")
                raise TimeoutError("Rate limit hit")
            return result
        finally:
            signal.alarm(0)

    def _get_dataset_detail(self) -> None:
        results = []
        count = 0
        for dataset_id, name in self._get_resource_ids(Endpoint.list_data_sets):
            try:
                result = self._get_resource_detail(
                    lambda: self._client.describe_data_set(
                        AwsAccountId=self._aws_account_id, DataSetId=dataset_id
                    )
                )
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} datasets")

                results.append(result)
                dataset = DataSet(**(result["DataSet"]))

                if dataset.Arn is None:
                    continue

                self._resources[dataset.Arn] = dataset
            except Exception as e:
                logger.warning(f"Error getting dataset {name} id {dataset_id}: {e}")
                continue

        json_dump_to_debug_file(results, "datasets.json")

    def _get_dashboard_detail(self):
        results = []
        count = 0
        for dashboard_id, name in self._get_resource_ids(Endpoint.list_dashboards):
            try:
                result = self._get_resource_detail(
                    lambda: self._client.describe_dashboard(
                        AwsAccountId=self._aws_account_id, DashboardId=dashboard_id
                    )
                )
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} dashboards")

                results.append(result)
                dashboard = Dashboard(**(result["Dashboard"]))

                if dashboard.Arn is None:
                    continue

                self._resources[dashboard.Arn] = dashboard
            except Exception as e:
                logger.error(f"Error getting dashboard {name} id {dashboard_id}: {e}")
                continue

        json_dump_to_debug_file(results, "dashboards.json")

    def _get_data_source_detail(self):
        results = []
        count = 0
        for data_source_id, name in self._get_resource_ids(Endpoint.list_data_sources):
            try:
                result = self._get_resource_detail(
                    lambda: self._client.describe_data_source(
                        AwsAccountId=self._aws_account_id, DataSourceId=data_source_id
                    )
                )
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} data sources")

                results.append(result)
                data_source = DataSource(**(result["DataSource"]))

                if data_source.Arn is None:
                    continue

                self._resources[data_source.Arn] = data_source
            except Exception as e:
                logger.error(
                    f"Error getting data source {name} id {data_source_id}: {e}"
                )
                continue

        json_dump_to_debug_file(results, "data_sources.json")
