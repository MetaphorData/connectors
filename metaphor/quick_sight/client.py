import enum
from typing import Dict, List

import boto3
from pydantic.dataclasses import dataclass

from metaphor.common.aws import AwsCredentials
from metaphor.common.logger import json_dump_to_debug_file
from metaphor.quick_sight.models import Dashboard, DataSet, DataSource, ResourceType


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


ENDPOINT_SETTING = {
    Endpoint.list_data_sets: EndpointDictKeys("DataSetSummaries", "DataSetId"),
    Endpoint.list_dashboards: EndpointDictKeys("DashboardSummaryList", "DashboardId"),
    Endpoint.list_data_sources: EndpointDictKeys("DataSources", "DataSourceId"),
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

    def _get_resource_ids(self, endpoint: Endpoint) -> List[str]:
        paginator = self._client.get_paginator(endpoint.value)
        paginator_response = paginator.paginate(AwsAccountId=self._aws_account_id)

        ids = []
        settings = ENDPOINT_SETTING[endpoint]
        for page in paginator_response:
            for item in page[settings.list_key]:
                ids.append(item[settings.item_key])
        return ids

    def _get_dataset_detail(self) -> None:
        results = []
        for dataset_id in self._get_resource_ids(Endpoint.list_data_sets):
            result = self._client.describe_data_set(
                AwsAccountId=self._aws_account_id, DataSetId=dataset_id
            )

            results.append(result)

            dataset = DataSet(**(result["DataSet"]))

            if dataset.Arn is None:
                continue

            self._resources[dataset.Arn] = dataset

        json_dump_to_debug_file(results, "datasets.json")

    def _get_dashboard_detail(self):
        results = []
        for dashboard_id in self._get_resource_ids(Endpoint.list_dashboards):
            result = self._client.describe_dashboard(
                AwsAccountId=self._aws_account_id, DashboardId=dashboard_id
            )
            results.append(result)
            dashboard = Dashboard(**(result["Dashboard"]))

            if dashboard.Arn is None:
                continue

            self._resources[dashboard.Arn] = dashboard

        json_dump_to_debug_file(results, "dashboards.json")

    def _get_data_source_detail(self):
        results = []
        for data_source_id in self._get_resource_ids(Endpoint.list_data_sources):
            result = self._client.describe_data_source(
                AwsAccountId=self._aws_account_id, DataSourceId=data_source_id
            )
            results.append(result)

            data_source = DataSource(**(result["DataSource"]))

            if data_source.Arn is None:
                continue

            self._resources[data_source.Arn] = data_source

        json_dump_to_debug_file(results, "data_sources.json")
