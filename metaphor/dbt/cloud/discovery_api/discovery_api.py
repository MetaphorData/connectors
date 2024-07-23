from typing import Dict

from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from metaphor.dbt.cloud.discovery_api.queries.get_job_models import get_job_models
from metaphor.dbt.cloud.discovery_api.queries.get_job_tests import get_job_tests


class DiscoveryAPI:
    """
    A wrapper around dbt cloud's discovery API.
    """

    def __init__(self, url: str, token: str) -> None:
        self._gql_client = Client(
            transport=RequestsHTTPTransport(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            ),
            fetch_schema_from_transport=True,
        )

    def get_all_job_model_names(self, job_id: int):
        res: Dict[str, str] = {}
        for model in get_job_models(self._gql_client, job_id).job.models:
            res[model.uniqueId] = model.normalized_name
        return res

    def get_all_job_tests(self, job_id: int):
        return get_job_tests(self._gql_client, job_id)
