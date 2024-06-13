from datetime import datetime
from typing import Any, Dict, Optional

import requests
from pydantic import BaseModel

from metaphor.common.entity_id import dataset_normalized_name


class DiscoveryTestNode(BaseModel):
    uniqueId: str
    name: Optional[str]
    status: Optional[str]
    columnName: Optional[str]
    executeCompletedAt: Optional[datetime]


class DiscoveryAPI:
    """
    A wrapper around dbt cloud's discovery API.
    """

    def __init__(self, url: str, token: str) -> None:
        self.url = url
        self.token = token

    def _send(self, query: str, variables: Dict[str, Any]):
        resp = requests.post(
            url=self.url,
            headers={
                "authorization": f"Bearer {self.token}",
                "content-type": "application/json",
            },
            json={"query": query, "variables": variables},
            timeout=15,
        )
        return resp.json()["data"]

    def get_model_dataset_name(self, job_id: int, model_unique_id: str):
        query = """
query Model($uniqueId: String!, $jobId: BigInt!) {
    job(id: $jobId) {
        model(uniqueId: $uniqueId) {
            alias
            database
            name
            schema
        }
    }
}
        """
        variables = {
            "uniqueId": model_unique_id,
            "jobId": job_id,
        }

        model = self._send(query, variables)["job"]["model"]
        database, schema, name = (
            model.get("database"),
            model.get("schema"),
            model.get("alias") or model.get("name"),
        )
        return dataset_normalized_name(database, schema, name)

    def get_all_job_tests(self, job_id: int):
        query = """
query Tests($jobId: BigInt!) {
  job(id: $jobId) {
    tests {
        uniqueId
        name
        status
        columnName
        executeCompletedAt
    }
  }
}
        """
        variables = {
            "jobId": job_id,
        }
        tests = self._send(query, variables)["job"]["tests"]
        return [DiscoveryTestNode.model_validate(test) for test in tests]
